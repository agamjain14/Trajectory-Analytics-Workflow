"""
Spark Job: Raw Trace Delta → agent_steps Delta Table.
Reads from trace_consumer output, classifies spans into 6 kinds,
filters noise, flattens meaningful tags into sparse columns.

Span Kinds: ENTRY, PLAN, AGENT, REASON, RETRIEVE, TOOL
"""

import json
import logging
import os

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("agent_steps_job")

# --- Configuration ---
INPUT_DELTA_PATH = os.getenv("INPUT_DELTA_PATH", "./data/trace_delta_table")
OUTPUT_DELTA_PATH = os.getenv("OUTPUT_DELTA_PATH", "./data/agent_steps")

# --- Output Schema (sparse) ---
AGENT_STEPS_SCHEMA = pa.schema([
    # Core identity
    ("trace_id", pa.string()),
    ("session_id", pa.string()),
    ("span_id", pa.string()),
    ("parent_span_id", pa.string()),
    # Classification
    ("span_kind", pa.string()),
    ("sub_span_kind", pa.string()),
    # Timing
    ("start_ts_ms", pa.int64()),
    ("duration_ms", pa.float64()),
    ("end_ts_ms", pa.int64()),
    # ENTRY
    ("method", pa.string()),
    # AGENT
    ("agent_name", pa.string()),
    # REASON
    ("model", pa.string()),
    ("input_tokens", pa.string()),
    ("output_tokens", pa.string()),
    # RETRIEVE
    ("collection", pa.string()),
    ("query_text", pa.string()),
    ("returned_rows", pa.string()),
    # TOOL
    ("rpc_method", pa.string()),
    ("url", pa.string()),
    ("status_code", pa.string()),
    # ENTRY
    ("user_message", pa.string()),
    # Context / Prompt / Response (sparse, populated per span_kind)
    ("context", pa.string()),
    ("prompt", pa.string()),
    ("response", pa.string()),
])


# --- Spans to drop (noise) ---
DROP_OPERATIONS = {
    "session.resolve",
    "session.save_user_message",
    "session.load_context",
    "session.merge_context",
    "chat.conversational",
}

# Drop session.turn.* pattern and session CRUD endpoints
DROP_PREFIXES = [
    "session.turn.",
    "GET /api/sessions",
    "POST /api/sessions",
]

# Also drop one-time init spans
DROP_OPERATIONS.add("get_or_create_collection knowledge_base")


def _should_drop(operation_name: str) -> bool:
    """Check if this span should be dropped."""
    if operation_name in DROP_OPERATIONS:
        return True
    for prefix in DROP_PREFIXES:
        if operation_name.startswith(prefix):
            return True
    return False


def classify_span(tags: dict) -> tuple[str, str] | None:
    """
    Classify a span into (span_kind, sub_span_kind).
    Returns None if span should be dropped.
    """
    # ENTRY: has http.route (only /api/chat kept — CRUD already dropped by _should_drop)
    if "http.route" in tags:
        return ("ENTRY", tags["http.route"])

    # PLAN: orchestration.type == plan_execution
    if tags.get("orchestration.type") == "plan_execution":
        return ("PLAN", "plan_execution")

    # AGENT: has agent.name
    if "agent.name" in tags:
        return ("AGENT", tags.get("agent.operation", "unknown"))

    # REASON: has gen_ai.system
    if "gen_ai.system" in tags:
        return ("REASON", tags.get("gen_ai.operation.name", "unknown"))

    # RETRIEVE: has db.system
    if "db.system" in tags:
        return ("RETRIEVE", tags.get("db.operation.name", "unknown"))

    # TOOL (MCP): has rpc.system
    if "rpc.system" in tags:
        return ("TOOL", "MCP")

    # TOOL (HTTP): has url.full + http.request.method but no http.route
    if "url.full" in tags and "http.request.method" in tags:
        return ("TOOL", "HTTP")

    return None  # drop unclassified


def _flatten_tags(span_kind: str, tags: dict) -> dict:
    """Extract sparse columns from tags based on span_kind."""
    flat = {}

    if span_kind == "ENTRY":
        flat["method"] = tags.get("http.request.method")
        flat["user_message"] = tags.get("orchestration.input.user_message")

    elif span_kind == "AGENT":
        flat["agent_name"] = tags.get("agent.name")
        # Context: all agent.parameter.* as JSON
        params = {k.replace("agent.parameter.", ""): v for k, v in tags.items() if k.startswith("agent.parameter.")}
        if params:
            flat["context"] = json.dumps(params)

    elif span_kind == "REASON":
        flat["model"] = tags.get("gen_ai.request.model")
        flat["input_tokens"] = tags.get("gen_ai.usage.input_tokens")
        flat["output_tokens"] = tags.get("gen_ai.usage.output_tokens")
        flat["context"] = tags.get("gen_ai.prompt.system")
        flat["prompt"] = tags.get("gen_ai.prompt.user")
        flat["response"] = tags.get("gen_ai.response.content")

    elif span_kind == "RETRIEVE":
        flat["collection"] = tags.get("db.collection.name")
        flat["query_text"] = tags.get("db.query.text")
        flat["returned_rows"] = tags.get("db.response.returned_rows")
        flat["prompt"] = tags.get("db.query.text")

    elif span_kind == "TOOL":
        flat["rpc_method"] = tags.get("rpc.method")
        flat["url"] = tags.get("url.full")
        flat["status_code"] = tags.get("http.response.status_code")
        # Context: all tool.parameter.* as JSON
        params = {k.replace("tool.parameter.", ""): v for k, v in tags.items() if k.startswith("tool.parameter.")}
        if params:
            flat["context"] = json.dumps(params)
        flat["response"] = tags.get("tool.response")

    return flat


def process_spans(input_path: str, output_path: str) -> int:
    """
    Read raw spans from input delta table, classify, filter, flatten,
    and write to agent_steps delta table.
    Returns number of rows written.
    """
    if not DeltaTable.is_deltatable(input_path):
        logger.error(f"Input delta table not found at {input_path}")
        return 0

    dt = DeltaTable(input_path)
    raw_table = dt.to_pyarrow_table()
    logger.info(f"Read {raw_table.num_rows} raw spans from {input_path}")

    rows = []
    for i in range(raw_table.num_rows):
        operation_name = raw_table.column("operation_name")[i].as_py()

        # Drop noise spans
        if _should_drop(operation_name):
            continue

        # Parse tags JSON
        tags_json = raw_table.column("tags")[i].as_py()
        try:
            tags = json.loads(tags_json) if tags_json else {}
        except json.JSONDecodeError:
            tags = {}

        # Classify
        classification = classify_span(tags)
        if classification is None:
            continue

        span_kind, sub_span_kind = classification

        # Timing: convert nanoseconds to milliseconds
        start_ns = raw_table.column("start_time_unix_nano")[i].as_py() or 0
        end_ns = raw_table.column("end_time_unix_nano")[i].as_py() or 0
        duration_ns = raw_table.column("duration_ns")[i].as_py() or 0

        start_ts_ms = start_ns // 1_000_000
        end_ts_ms = end_ns // 1_000_000
        duration_ms = duration_ns / 1_000_000

        # Build row
        row = {
            "trace_id": raw_table.column("trace_id")[i].as_py(),
            "session_id": raw_table.column("session_id")[i].as_py(),
            "span_id": raw_table.column("span_id")[i].as_py(),
            "parent_span_id": raw_table.column("parent_span_id")[i].as_py(),
            "span_kind": span_kind,
            "sub_span_kind": sub_span_kind,
            "start_ts_ms": start_ts_ms,
            "duration_ms": duration_ms,
            "end_ts_ms": end_ts_ms,
            # Sparse columns (all None by default)
            "method": None,
            "agent_name": None,
            "model": None,
            "input_tokens": None,
            "output_tokens": None,
            "collection": None,
            "query_text": None,
            "returned_rows": None,
            "rpc_method": None,
            "url": None,
            "status_code": None,
            "user_message": None,
            "context": None,
            "prompt": None,
            "response": None,
        }

        # Overlay flattened tag values
        flat = _flatten_tags(span_kind, tags)
        row.update(flat)

        rows.append(row)

    if not rows:
        logger.warning("No spans matched classification. Nothing to write.")
        return 0

    # Post-filter: drop REASON spans that are direct children of ENTRY
    # (these are intent-classification LLM calls — routing decisions, not agent work)
    entry_span_ids = {r["span_id"] for r in rows if r["span_kind"] == "ENTRY"}
    rows = [r for r in rows if not (r["span_kind"] == "REASON" and r["parent_span_id"] in entry_span_ids)]

    if not rows:
        logger.warning("All spans filtered out after post-processing.")
        return 0

    # Write to delta
    out_table = pa.Table.from_pylist(rows, schema=AGENT_STEPS_SCHEMA)

    if DeltaTable.is_deltatable(output_path):
        write_deltalake(output_path, out_table, mode="overwrite")
    else:
        write_deltalake(output_path, out_table, mode="overwrite")

    logger.info(f"Wrote {len(rows)} classified spans to {output_path}")
    return len(rows)


def print_summary(output_path: str) -> None:
    """Print a summary of the agent_steps table."""
    if not DeltaTable.is_deltatable(output_path):
        return

    dt = DeltaTable(output_path)
    table = dt.to_pyarrow_table()

    print(f"\n{'='*60}")
    print(f" agent_steps: {table.num_rows} rows")
    print(f"{'='*60}")

    # Count by span_kind
    kinds = {}
    for i in range(table.num_rows):
        kind = table.column("span_kind")[i].as_py()
        sub = table.column("sub_span_kind")[i].as_py()
        key = f"{kind}/{sub}"
        kinds[key] = kinds.get(key, 0) + 1

    print(f"\n{'span_kind/sub_span_kind':<35} {'count':>5}")
    print(f"{'-'*35} {'-'*5}")
    for key in sorted(kinds.keys()):
        print(f"  {key:<33} {kinds[key]:>5}")

    # Distinct traces
    traces = set()
    for i in range(table.num_rows):
        traces.add(table.column("trace_id")[i].as_py())
    print(f"\n  Distinct traces: {len(traces)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    written = process_spans(INPUT_DELTA_PATH, OUTPUT_DELTA_PATH)
    if written > 0:
        print_summary(OUTPUT_DELTA_PATH)
