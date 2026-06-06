"""
Phase 1: Trajectory Template Generation Job.
Reads agent_steps Delta Table, groups spans by trace_id,
orders by start_ts_ms, extracts step sequence, and hashes into
trajectory signatures for drift detection.
"""

import hashlib
import json
import logging
import os
from collections import defaultdict

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("trajectory_job")

# --- Configuration ---
INPUT_DELTA_PATH = os.getenv("AGENT_STEPS_PATH", "./data/agent_steps")
OUTPUT_DELTA_PATH = os.getenv("TRAJECTORY_PATH", "./data/trajectory_templates")

# --- Output Schema ---
TRAJECTORY_SCHEMA = pa.schema([
    ("trace_id", pa.string()),
    ("session_id", pa.string()),
    ("trajectory_signature", pa.string()),
    ("step_sequence", pa.string()),           # pipe-delimited: ENTRY|PLAN|AGENT|REASON|...
    ("step_sequence_detailed", pa.string()),  # includes sub_span_kind: ENTRY:/api/chat|PLAN:plan_execution|...
    ("step_count", pa.int32()),
    ("llm_call_count", pa.int32()),
    ("tool_call_count", pa.int32()),
    ("retrieve_count", pa.int32()),
    ("agent_count", pa.int32()),
    ("retry_count", pa.int32()),
    ("error_count", pa.int32()),
    ("total_duration_ms", pa.float64()),
    ("start_ts_ms", pa.int64()),
    ("end_ts_ms", pa.int64()),
])


def compute_signature(step_sequence: str) -> str:
    """Hash a step sequence into a compact trajectory signature."""
    digest = hashlib.sha256(step_sequence.encode()).hexdigest()[:12]
    return f"tpl_{digest}"


def build_trajectories(input_path: str, output_path: str) -> int:
    """
    Read agent_steps, group by trace_id, order by time,
    extract trajectory templates.
    """
    if not DeltaTable.is_deltatable(input_path):
        logger.error(f"Input delta table not found at {input_path}")
        return 0

    dt = DeltaTable(input_path)
    table = dt.to_pyarrow_table()
    logger.info(f"Read {table.num_rows} agent_steps rows")

    # Group spans by trace_id
    traces = defaultdict(list)
    for i in range(table.num_rows):
        trace_id = table.column("trace_id")[i].as_py()
        traces[trace_id].append({
            "span_kind": table.column("span_kind")[i].as_py(),
            "sub_span_kind": table.column("sub_span_kind")[i].as_py(),
            "start_ts_ms": table.column("start_ts_ms")[i].as_py(),
            "end_ts_ms": table.column("end_ts_ms")[i].as_py(),
            "duration_ms": table.column("duration_ms")[i].as_py(),
            "session_id": table.column("session_id")[i].as_py(),
        })

    logger.info(f"Found {len(traces)} distinct traces")

    rows = []
    for trace_id, spans in traces.items():
        # Order by start time
        spans.sort(key=lambda s: s["start_ts_ms"] or 0)

        # Extract sequences
        step_kinds = [s["span_kind"] for s in spans]
        step_detailed = [f"{s['span_kind']}:{s['sub_span_kind']}" for s in spans]

        step_sequence = "|".join(step_kinds)
        step_sequence_detailed = "|".join(step_detailed)

        # Compute counts
        llm_calls = sum(1 for s in spans if s["span_kind"] == "REASON")
        tool_calls = sum(1 for s in spans if s["span_kind"] == "TOOL")
        retrieves = sum(1 for s in spans if s["span_kind"] == "RETRIEVE")
        agents = sum(1 for s in spans if s["span_kind"] == "AGENT")
        # Retry detection: multiple consecutive same-kind spans to same parent
        # Simplified: count REASON spans beyond first per agent
        retry_count = max(0, llm_calls - agents - 1)  # rough heuristic
        error_count = 0  # TODO: detect from status codes

        # Timing
        start_ts = min(s["start_ts_ms"] for s in spans if s["start_ts_ms"])
        end_ts = max(s["end_ts_ms"] for s in spans if s["end_ts_ms"])
        total_duration = end_ts - start_ts if end_ts and start_ts else 0.0

        # Session (take from first span)
        session_id = spans[0]["session_id"] or ""

        # Hash
        signature = compute_signature(step_sequence)

        rows.append({
            "trace_id": trace_id,
            "session_id": session_id,
            "trajectory_signature": signature,
            "step_sequence": step_sequence,
            "step_sequence_detailed": step_sequence_detailed,
            "step_count": len(spans),
            "llm_call_count": llm_calls,
            "tool_call_count": tool_calls,
            "retrieve_count": retrieves,
            "agent_count": agents,
            "retry_count": retry_count,
            "error_count": error_count,
            "total_duration_ms": float(total_duration),
            "start_ts_ms": start_ts,
            "end_ts_ms": end_ts,
        })

    if not rows:
        logger.warning("No trajectories generated.")
        return 0

    out_table = pa.Table.from_pylist(rows, schema=TRAJECTORY_SCHEMA)
    write_deltalake(output_path, out_table, mode="overwrite")
    logger.info(f"Wrote {len(rows)} trajectory templates to {output_path}")
    return len(rows)


def print_summary(output_path: str) -> None:
    """Print trajectory template summary."""
    if not DeltaTable.is_deltatable(output_path):
        return

    dt = DeltaTable(output_path)
    table = dt.to_pyarrow_table()

    print(f"\n{'='*70}")
    print(f" trajectory_templates: {table.num_rows} traces")
    print(f"{'='*70}")

    # Count by signature
    sigs = defaultdict(int)
    for i in range(table.num_rows):
        sig = table.column("trajectory_signature")[i].as_py()
        sigs[sig] += 1

    print(f"\n  Unique trajectory templates: {len(sigs)}")
    print(f"\n  {'signature':<20} {'count':>5} {'share':>8}")
    print(f"  {'-'*20} {'-'*5} {'-'*8}")
    total = table.num_rows
    for sig, count in sorted(sigs.items(), key=lambda x: -x[1])[:10]:
        print(f"  {sig:<20} {count:>5} {count/total*100:>6.1f}%")

    # Avg step counts
    avg_steps = sum(table.column("step_count")[i].as_py() for i in range(table.num_rows)) / max(table.num_rows, 1)
    avg_llm = sum(table.column("llm_call_count")[i].as_py() for i in range(table.num_rows)) / max(table.num_rows, 1)
    avg_tool = sum(table.column("tool_call_count")[i].as_py() for i in range(table.num_rows)) / max(table.num_rows, 1)

    print(f"\n  Avg steps/trace:     {avg_steps:.1f}")
    print(f"  Avg LLM calls/trace: {avg_llm:.1f}")
    print(f"  Avg tool calls/trace:{avg_tool:.1f}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    written = build_trajectories(INPUT_DELTA_PATH, OUTPUT_DELTA_PATH)
    if written > 0:
        print_summary(OUTPUT_DELTA_PATH)
