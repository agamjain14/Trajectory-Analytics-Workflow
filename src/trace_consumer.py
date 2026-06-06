"""
Pulsar Consumer for OpenTelemetry Trace Data.
Reads OTLP JSON trace messages from a Pulsar topic, flattens span data,
and writes to a Delta Lake table with time-based or size-based batch flushing.
"""

import json
import logging
import os
import signal
import time as time_mod
from datetime import datetime, timezone
from typing import Any

import pulsar
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("trace_consumer")

# --- Configuration ---
PULSAR_URL = os.getenv("PULSAR_URL", "pulsar://localhost:6650")
PULSAR_TOPIC = os.getenv("PULSAR_TOPIC", "persistent://public/default/otlp-traces")
PULSAR_SUBSCRIPTION = os.getenv("PULSAR_SUBSCRIPTION", "trace-delta-writer")
DELTA_TABLE_PATH = os.getenv("DELTA_TABLE_PATH", "./data/trace_delta_table")

# Batching: flush at 250 spans OR 5 minutes, whichever comes first
BATCH_MAX_SIZE = int(os.getenv("CONSUMER_BATCH_MAX_SIZE", "250"))
BATCH_MAX_SECONDS = int(os.getenv("CONSUMER_BATCH_MAX_SECONDS", "300"))  # 5 minutes

# --- Arrow Schema ---
# Delta table columns (flat, readable names):
#   trace_id, session_id, span_id, parent_span_id, operation_name,
#   service_name, start_time_unix_nano, end_time_unix_nano, duration_ns,
#   status_code, status_message, warning, tags (JSON string), references (JSON string),
#   ingested_at
SCHEMA = pa.schema([
    ("trace_id", pa.string()),
    ("session_id", pa.string()),
    ("span_id", pa.string()),
    ("parent_span_id", pa.string()),
    ("operation_name", pa.string()),
    ("service_name", pa.string()),
    ("start_time_unix_nano", pa.int64()),
    ("end_time_unix_nano", pa.int64()),
    ("duration_ns", pa.int64()),
    ("status_code", pa.string()),
    ("status_message", pa.string()),
    ("warning", pa.string()),
    ("tags", pa.string()),       # JSON object: {"key1": "val1", "key2": "val2"}
    ("references", pa.string()), # JSON object: {"key1": "val1", "key2": "val2"}
    ("ingested_at", pa.timestamp("us", tz="UTC")),
])


def _hex_or_empty(byte_str: str | None) -> str:
    """Convert base64/hex encoded ID to hex string."""
    if not byte_str:
        return ""
    try:
        int(byte_str, 16)
        return byte_str
    except ValueError:
        pass
    try:
        import base64
        decoded = base64.b64decode(byte_str)
        return decoded.hex()
    except Exception:
        return str(byte_str)


def _extract_kv_map(attributes: list[dict] | None) -> dict[str, str]:
    """Extract flat key-value map from OTLP attributes array."""
    if not attributes:
        return {}
    result = {}
    for attr in attributes:
        key = attr.get("key", "")
        value_obj = attr.get("value", {})
        value = ""
        for val_key in ("stringValue", "intValue", "doubleValue", "boolValue"):
            if val_key in value_obj:
                value = str(value_obj[val_key])
                break
        if not value and "arrayValue" in value_obj:
            value = json.dumps(value_obj["arrayValue"])
        if not value and "kvlistValue" in value_obj:
            value = json.dumps(value_obj["kvlistValue"])
        result[key] = value
    return result


def _extract_references_map(span: dict) -> dict[str, str]:
    """Extract span links as a flat reference map."""
    links = span.get("links", [])
    refs = {}
    for i, link in enumerate(links):
        trace_id = _hex_or_empty(link.get("traceId"))
        span_id = _hex_or_empty(link.get("spanId", ""))
        refs[f"link.{i}.trace_id"] = trace_id
        refs[f"link.{i}.span_id"] = span_id
        for attr in link.get("attributes", []):
            key = attr.get("key", "")
            val = str(next(iter(attr.get("value", {}).values()), ""))
            refs[f"link.{i}.{key}"] = val
    return refs


def _extract_warning(span: dict) -> str:
    """Extract warning from span events."""
    events = span.get("events", [])
    warnings = []
    for event in events:
        name = event.get("name", "").lower()
        if "warn" in name or "exception" in name or "error" in name:
            attrs = _extract_kv_map(event.get("attributes", []))
            msg = attrs.get("exception.message") or attrs.get("message") or name
            warnings.append(msg)
    status = span.get("status", {})
    if status.get("code") == 2:
        msg = status.get("message", "")
        if msg:
            warnings.append(msg)
    return "; ".join(warnings) if warnings else ""


def flatten_otlp_trace(message_data: dict) -> list[dict[str, Any]]:
    """
    Flatten OTLP JSON trace export format into rows for the Delta table.
    """
    rows = []
    resource_spans_list = message_data.get("resourceSpans", [])

    for resource_spans in resource_spans_list:
        resource = resource_spans.get("resource", {})
        resource_attrs = resource.get("attributes", [])
        service_name = ""
        for attr in resource_attrs:
            if attr.get("key") == "service.name":
                service_name = str(next(iter(attr.get("value", {}).values()), ""))
                break

        scope_spans_list = resource_spans.get("scopeSpans", [])
        for scope_spans in scope_spans_list:
            spans = scope_spans.get("spans", [])
            for span in spans:
                trace_id = _hex_or_empty(span.get("traceId"))
                span_id = _hex_or_empty(span.get("spanId"))
                parent_span_id = _hex_or_empty(span.get("parentSpanId"))

                start_ns = int(span.get("startTimeUnixNano", 0))
                end_ns = int(span.get("endTimeUnixNano", 0))
                duration_ns = end_ns - start_ns if end_ns and start_ns else 0

                # Status
                status = span.get("status", {})
                status_code_map = {0: "UNSET", 1: "OK", 2: "ERROR"}
                status_code = status_code_map.get(status.get("code", 0), "UNSET")
                status_message = status.get("message", "")

                # Tags: flat key-value map from span attributes
                tags = _extract_kv_map(span.get("attributes", []))

                # Session ID is extracted from the span's session.id attribute
                session_id = tags.get("session.id", "")

                # References: flat key-value map from span links
                references = _extract_references_map(span)

                # Warning from events/status
                warning = _extract_warning(span)

                rows.append({
                    "trace_id": trace_id,
                    "session_id": session_id,
                    "span_id": span_id,
                    "parent_span_id": parent_span_id,
                    "operation_name": span.get("name", ""),
                    "service_name": service_name,
                    "start_time_unix_nano": start_ns,
                    "end_time_unix_nano": end_ns,
                    "duration_ns": duration_ns,
                    "status_code": status_code,
                    "status_message": status_message,
                    "warning": warning,
                    "tags": json.dumps(tags),
                    "references": json.dumps(references),
                    "ingested_at": datetime.now(timezone.utc),
                })

    return rows


def write_batch_to_delta(rows: list[dict[str, Any]]) -> None:
    """Write a batch of flattened span rows to the Delta table."""
    if not rows:
        return

    table = pa.Table.from_pylist(rows, schema=SCHEMA)

    if DeltaTable.is_deltatable(DELTA_TABLE_PATH):
        write_deltalake(DELTA_TABLE_PATH, table, mode="append")
    else:
        write_deltalake(DELTA_TABLE_PATH, table, mode="overwrite")

    logger.info(f"Wrote {len(rows)} spans to Delta table at {DELTA_TABLE_PATH}")


def run_consumer() -> None:
    """Main consumer loop: read from Pulsar, batch by size/time, write to Delta."""
    logger.info(f"Connecting to Pulsar at {PULSAR_URL}")
    logger.info(f"Topic: {PULSAR_TOPIC}")
    logger.info(f"Delta table: {DELTA_TABLE_PATH}")
    logger.info(f"Batch policy: {BATCH_MAX_SIZE} spans OR {BATCH_MAX_SECONDS}s")

    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        PULSAR_TOPIC,
        subscription_name=PULSAR_SUBSCRIPTION,
        consumer_type=pulsar.ConsumerType.Shared,
    )

    shutdown = False

    def _signal_handler(signum, frame):
        nonlocal shutdown
        logger.info("Shutdown signal received, draining batch...")
        shutdown = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    batch: list[dict[str, Any]] = []
    batch_start_time = time_mod.monotonic()
    logger.info("Consumer started. Waiting for messages...")

    try:
        while not shutdown:
            try:
                msg = consumer.receive(timeout_millis=2000)
            except Exception:
                # Timeout — check if time-based flush is needed
                elapsed = time_mod.monotonic() - batch_start_time
                if batch and elapsed >= BATCH_MAX_SECONDS:
                    logger.info(f"Time-based flush: {len(batch)} spans after {elapsed:.0f}s")
                    write_batch_to_delta(batch)
                    batch = []
                    batch_start_time = time_mod.monotonic()
                continue

            try:
                data = json.loads(msg.data().decode("utf-8"))
                rows = flatten_otlp_trace(data)
                batch.extend(rows)
                consumer.acknowledge(msg)

                # Size-based flush
                if len(batch) >= BATCH_MAX_SIZE:
                    logger.info(f"Size-based flush: {len(batch)} spans")
                    write_batch_to_delta(batch)
                    batch = []
                    batch_start_time = time_mod.monotonic()

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
                consumer.acknowledge(msg)
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                consumer.negative_acknowledge(msg)

        # Flush remaining on shutdown
        if batch:
            logger.info(f"Shutdown flush: {len(batch)} spans remaining")
            write_batch_to_delta(batch)

    finally:
        consumer.close()
        client.close()
        logger.info("Consumer shut down cleanly.")


if __name__ == "__main__":
    run_consumer()
