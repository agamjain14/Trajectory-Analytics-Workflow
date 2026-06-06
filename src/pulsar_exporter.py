"""
Custom Pulsar Span Exporter.
Exports OTLP trace data directly from the Python app to Apache Pulsar,
bypassing the OTel Collector for Pulsar delivery.
"""

import json
import logging
import os
from typing import Sequence

import pulsar
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

logger = logging.getLogger("pulsar_exporter")


def _span_to_otlp_dict(span: ReadableSpan) -> dict:
    """Convert a ReadableSpan to OTLP-compatible JSON dict."""
    import base64

    context = span.get_span_context()
    trace_id = format(context.trace_id, "032x")
    span_id = format(context.span_id, "016x")
    parent_span_id = ""
    if span.parent and span.parent.span_id:
        parent_span_id = format(span.parent.span_id, "016x")

    # Attributes
    attributes = []
    if span.attributes:
        for k, v in span.attributes.items():
            if isinstance(v, bool):
                attributes.append({"key": k, "value": {"boolValue": v}})
            elif isinstance(v, int):
                attributes.append({"key": k, "value": {"intValue": str(v)}})
            elif isinstance(v, float):
                attributes.append({"key": k, "value": {"doubleValue": v}})
            else:
                attributes.append({"key": k, "value": {"stringValue": str(v)}})

    # Events
    events = []
    if span.events:
        for event in span.events:
            event_attrs = []
            if event.attributes:
                for k, v in event.attributes.items():
                    event_attrs.append({"key": k, "value": {"stringValue": str(v)}})
            events.append({
                "name": event.name,
                "timeUnixNano": str(event.timestamp),
                "attributes": event_attrs,
            })

    # Links
    links = []
    if span.links:
        for link in span.links:
            link_ctx = link.context
            link_attrs = []
            if link.attributes:
                for k, v in link.attributes.items():
                    link_attrs.append({"key": k, "value": {"stringValue": str(v)}})
            links.append({
                "traceId": format(link_ctx.trace_id, "032x"),
                "spanId": format(link_ctx.span_id, "016x"),
                "attributes": link_attrs,
            })

    # Status
    status = {}
    if span.status:
        status_code_map = {0: 0, 1: 1, 2: 2}  # UNSET, OK, ERROR
        status["code"] = span.status.status_code.value if hasattr(span.status.status_code, 'value') else 0
        if span.status.description:
            status["message"] = span.status.description

    # Span kind
    kind_map = {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
    kind = kind_map.get(span.kind.value if hasattr(span.kind, 'value') else 0, 0)

    return {
        "traceId": trace_id,
        "spanId": span_id,
        "parentSpanId": parent_span_id,
        "name": span.name,
        "kind": kind,
        "startTimeUnixNano": str(span.start_time),
        "endTimeUnixNano": str(span.end_time),
        "attributes": attributes,
        "events": events,
        "links": links,
        "status": status,
    }


class PulsarSpanExporter(SpanExporter):
    """Exports spans to Apache Pulsar in OTLP JSON format."""

    def __init__(
        self,
        pulsar_url: str | None = None,
        topic: str | None = None,
        service_name: str = "trajectory-analytics-ai-app",
    ):
        self._pulsar_url = pulsar_url or os.getenv("PULSAR_URL", "pulsar://localhost:6650")
        self._topic = topic or os.getenv("PULSAR_TOPIC", "persistent://public/default/otlp-traces")
        self._service_name = service_name
        self._client: pulsar.Client | None = None
        self._producer: pulsar.Producer | None = None
        self._connected = False
        self._connect()

    def _connect(self):
        """Connect to Pulsar (best effort, retries on export)."""
        try:
            self._client = pulsar.Client(self._pulsar_url)
            self._producer = self._client.create_producer(self._topic)
            self._connected = True
            logger.info(f"Connected to Pulsar at {self._pulsar_url}, topic={self._topic}")
        except Exception as e:
            logger.warning(f"Failed to connect to Pulsar (will retry on export): {e}")
            self._connected = False

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        """Export spans to Pulsar as OTLP JSON."""
        if not self._connected:
            self._connect()
            if not self._connected:
                return SpanExportResult.FAILURE

        try:
            # Build OTLP resourceSpans structure
            span_dicts = [_span_to_otlp_dict(s) for s in spans]

            # Get resource attributes from first span
            resource_attrs = []
            if spans and spans[0].resource:
                for k, v in spans[0].resource.attributes.items():
                    resource_attrs.append({"key": k, "value": {"stringValue": str(v)}})

            message = {
                "resourceSpans": [
                    {
                        "resource": {"attributes": resource_attrs},
                        "scopeSpans": [
                            {
                                "scope": {"name": "trajectory.ai.app"},
                                "spans": span_dicts,
                            }
                        ],
                    }
                ]
            }

            payload = json.dumps(message).encode("utf-8")
            self._producer.send(payload)
            logger.debug(f"Exported {len(spans)} spans to Pulsar")
            return SpanExportResult.SUCCESS

        except Exception as e:
            logger.error(f"Failed to export spans to Pulsar: {e}")
            self._connected = False
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        """Flush and close Pulsar connection."""
        try:
            if self._producer:
                self._producer.flush()
                self._producer.close()
            if self._client:
                self._client.close()
            logger.info("Pulsar exporter shut down.")
        except Exception as e:
            logger.warning(f"Error during Pulsar exporter shutdown: {e}")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Force flush pending messages."""
        try:
            if self._producer:
                self._producer.flush()
            return True
        except Exception:
            return False
