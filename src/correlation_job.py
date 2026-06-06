"""
Phase 4: Correlation Job.
Joins trajectory_templates, quality_scores, request_routing, gpu_metrics,
and network_metrics to produce:
  1. trace_correlated — per-trace enriched view with all signals
  2. analytics_windows — time-bucketed aggregation with correlation verdicts
"""

import logging
import math
import os
from collections import defaultdict

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("correlation_job")

# --- Configuration ---
TRAJECTORY_PATH = os.getenv("TRAJECTORY_PATH", "./data/trajectory_templates")
QUALITY_PATH = os.getenv("QUALITY_SCORES_PATH", "./data/quality_scores")
ROUTING_PATH = os.getenv("ROUTING_PATH", "./data/request_routing")
GPU_METRICS_PATH = os.getenv("GPU_METRICS_PATH", "./data/gpu_metrics")
NETWORK_METRICS_PATH = os.getenv("NETWORK_METRICS_PATH", "./data/network_metrics")

OUTPUT_CORRELATED_PATH = os.getenv("CORRELATED_PATH", "./data/trace_correlated")
OUTPUT_WINDOWS_PATH = os.getenv("ANALYTICS_WINDOWS_PATH", "./data/analytics_windows")

# Window sizes in milliseconds
WINDOW_SIZES_MS = {
    "5min": 5 * 60 * 1000,
    "30min": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
}

# Correlation thresholds
QUALITY_DROP_THRESHOLD = -0.5       # quality drift below this = "dropped"
TRAJECTORY_SHIFT_THRESHOLD = 0.20   # dominant share drop > this = "shifted"
GPU_CONTENTION_THRESHOLD = 0.7      # avg contention above this = "pressured"
NIC_PRESSURE_THRESHOLD = 0.7        # avg NIC bandwidth above this = "pressured"

# --- Output Schemas ---
TRACE_CORRELATED_SCHEMA = pa.schema([
    ("trace_id", pa.string()),
    ("session_id", pa.string()),
    ("start_ts_ms", pa.int64()),
    ("end_ts_ms", pa.int64()),
    ("total_duration_ms", pa.float64()),
    # Trajectory signals
    ("trajectory_signature", pa.string()),
    ("step_sequence", pa.string()),
    ("step_count", pa.int32()),
    ("llm_call_count", pa.int32()),
    ("tool_call_count", pa.int32()),
    ("retrieve_count", pa.int32()),
    ("retry_count", pa.int32()),
    # Quality signals
    ("quality_overall", pa.float64()),
    ("quality_completeness", pa.float64()),
    ("quality_coherence", pa.float64()),
    ("quality_hallucination", pa.float64()),
    ("quality_groundedness", pa.float64()),
    ("quality_relevance", pa.float64()),
    ("quality_explanation", pa.string()),
    # GPU signals (aggregated across all REASON spans in this trace)
    ("gpu_contention_avg", pa.float64()),
    ("gpu_contention_max", pa.float64()),
    ("gpu_queue_delay_avg", pa.float64()),
    ("gpu_queue_delay_max", pa.float64()),
    ("gpu_memory_pressure_avg", pa.float64()),
    # Network signals
    ("nic_bandwidth_avg", pa.float64()),
    ("nic_bandwidth_max", pa.float64()),
    ("switch_util_avg", pa.float64()),
    ("packet_drop_avg", pa.float64()),
    # Routing (primary pod — most frequently used)
    ("primary_pod_id", pa.string()),
    ("primary_node_id", pa.string()),
    ("primary_gpu_id", pa.string()),
])

ANALYTICS_WINDOW_SCHEMA = pa.schema([
    ("window_start_ms", pa.int64()),
    ("window_end_ms", pa.int64()),
    ("window_size", pa.string()),
    # Trajectory signals
    ("trace_count", pa.int32()),
    ("dominant_trajectory", pa.string()),
    ("dominant_share", pa.float64()),
    ("unique_templates", pa.int32()),
    ("trajectory_entropy", pa.float64()),
    ("avg_step_count", pa.float64()),
    ("avg_llm_calls", pa.float64()),
    ("avg_tool_calls", pa.float64()),
    # Quality signals
    ("avg_quality", pa.float64()),
    ("min_quality", pa.float64()),
    ("max_quality", pa.float64()),
    ("quality_variance", pa.float64()),
    ("quality_drift", pa.float64()),
    ("avg_hallucination", pa.float64()),
    # GPU signals
    ("avg_contention_index", pa.float64()),
    ("max_contention_index", pa.float64()),
    ("avg_queue_delay_ms", pa.float64()),
    ("p95_queue_delay_ms", pa.float64()),
    # Network signals
    ("avg_nic_bandwidth_pct", pa.float64()),
    ("avg_switch_util", pa.float64()),
    ("max_packet_drop_rate", pa.float64()),
    # Correlation verdict
    ("correlation_flag", pa.string()),
    ("correlation_details", pa.string()),
])


def _load_delta(path: str, name: str) -> pa.Table | None:
    """Load a Delta table, return None if not found."""
    if not DeltaTable.is_deltatable(path):
        logger.warning(f"{name} not found at {path}")
        return None
    dt = DeltaTable(path)
    table = dt.to_pyarrow_table()
    logger.info(f"Loaded {name}: {table.num_rows} rows")
    return table


def _table_to_dicts(table: pa.Table) -> list[dict]:
    """Convert PyArrow table to list of dicts."""
    return [
        {col: table.column(col)[i].as_py() for col in table.column_names}
        for i in range(table.num_rows)
    ]


def _shannon_entropy(counts: list[int]) -> float:
    """Compute Shannon entropy of a distribution."""
    total = sum(counts)
    if total == 0:
        return 0.0
    probs = [c / total for c in counts if c > 0]
    return -sum(p * math.log2(p) for p in probs)


def _percentile(values: list[float], p: float) -> float:
    """Compute p-th percentile (0-100)."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * p / 100.0)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


def step1_per_trace_correlation() -> list[dict]:
    """
    Step 1: Join trajectory_templates + quality_scores + request_routing + metrics.
    Produces per-trace enriched rows.
    """
    # Load all tables
    traj_table = _load_delta(TRAJECTORY_PATH, "trajectory_templates")
    qual_table = _load_delta(QUALITY_PATH, "quality_scores")
    routing_table = _load_delta(ROUTING_PATH, "request_routing")
    gpu_table = _load_delta(GPU_METRICS_PATH, "gpu_metrics")
    net_table = _load_delta(NETWORK_METRICS_PATH, "network_metrics")

    if not traj_table or not qual_table:
        logger.error("Cannot proceed without trajectory_templates and quality_scores")
        return []

    # Index quality scores by trace_id
    qual_by_trace = {}
    if qual_table:
        for row in _table_to_dicts(qual_table):
            qual_by_trace[row["trace_id"]] = row

    # Index routing by trace_id (group all REASON spans per trace)
    routing_by_trace = defaultdict(list)
    if routing_table:
        for row in _table_to_dicts(routing_table):
            routing_by_trace[row["trace_id"]].append(row)

    # Index GPU metrics by (node_id, gpu_id) → sorted list of (timestamp, metrics)
    gpu_by_location = defaultdict(list)
    if gpu_table:
        for row in _table_to_dicts(gpu_table):
            key = (row["node_id"], row["gpu_id"])
            gpu_by_location[key].append(row)
        # Sort by timestamp
        for key in gpu_by_location:
            gpu_by_location[key].sort(key=lambda r: r["timestamp_ms"])

    # Index network metrics by node_id → sorted list
    net_by_node = defaultdict(list)
    if net_table:
        for row in _table_to_dicts(net_table):
            net_by_node[row["node_id"]].append(row)
        for key in net_by_node:
            net_by_node[key].sort(key=lambda r: r["timestamp_ms"])

    # Process each trajectory
    traj_rows = _table_to_dicts(traj_table)
    correlated_rows = []

    for traj in traj_rows:
        trace_id = traj["trace_id"]
        start_ts = traj["start_ts_ms"]
        end_ts = traj["end_ts_ms"]

        # Quality scores for this trace
        qual = qual_by_trace.get(trace_id, {})

        # GPU metrics: find metrics for the GPUs that served this trace's REASON spans
        gpu_contention_values = []
        gpu_queue_values = []
        gpu_memory_values = []
        nic_bw_values = []
        switch_util_values = []
        packet_drop_values = []
        pod_counts = defaultdict(int)

        routings = routing_by_trace.get(trace_id, [])
        for r in routings:
            pod_counts[r["vllm_pod_id"]] += 1
            node_id = r["node_id"]
            gpu_id = r["gpu_id"]
            ts = r["timestamp_ms"] or start_ts

            # Find GPU metrics near this span's time
            gpu_key = (node_id, gpu_id)
            for gm in gpu_by_location.get(gpu_key, []):
                # Within 10 seconds of the request
                if abs(gm["timestamp_ms"] - ts) <= 10000:
                    gpu_contention_values.append(gm["contention_index"])
                    gpu_queue_values.append(gm["queue_delay_ms"])
                    gpu_memory_values.append(gm["memory_used_pct"])

            # Find network metrics for this node near this time
            for nm in net_by_node.get(node_id, []):
                if abs(nm["timestamp_ms"] - ts) <= 10000:
                    nic_bw_values.append(nm["nic_bandwidth_pct"])
                    switch_util_values.append(nm["switch_port_util"])
                    packet_drop_values.append(nm["packet_drop_rate"])

        # Determine primary pod (most frequently used)
        primary_pod = max(pod_counts, key=pod_counts.get) if pod_counts else ""
        primary_node = ""
        primary_gpu = ""
        if routings:
            for r in routings:
                if r["vllm_pod_id"] == primary_pod:
                    primary_node = r["node_id"]
                    primary_gpu = r["gpu_id"]
                    break

        correlated_rows.append({
            "trace_id": trace_id,
            "session_id": traj["session_id"],
            "start_ts_ms": start_ts,
            "end_ts_ms": end_ts,
            "total_duration_ms": traj["total_duration_ms"],
            # Trajectory
            "trajectory_signature": traj["trajectory_signature"],
            "step_sequence": traj["step_sequence"],
            "step_count": traj["step_count"],
            "llm_call_count": traj["llm_call_count"],
            "tool_call_count": traj["tool_call_count"],
            "retrieve_count": traj["retrieve_count"],
            "retry_count": traj["retry_count"],
            # Quality
            "quality_overall": qual.get("overall", 0.0),
            "quality_completeness": qual.get("completeness", 0.0),
            "quality_coherence": qual.get("coherence", 0.0),
            "quality_hallucination": qual.get("hallucination", 0.0),
            "quality_groundedness": qual.get("groundedness", 0.0),
            "quality_relevance": qual.get("relevance", 0.0),
            "quality_explanation": qual.get("explanation", ""),
            # GPU (averaged across all REASON spans in this trace)
            "gpu_contention_avg": round(sum(gpu_contention_values) / max(len(gpu_contention_values), 1), 4),
            "gpu_contention_max": round(max(gpu_contention_values) if gpu_contention_values else 0.0, 4),
            "gpu_queue_delay_avg": round(sum(gpu_queue_values) / max(len(gpu_queue_values), 1), 2),
            "gpu_queue_delay_max": round(max(gpu_queue_values) if gpu_queue_values else 0.0, 2),
            "gpu_memory_pressure_avg": round(sum(gpu_memory_values) / max(len(gpu_memory_values), 1), 2),
            # Network
            "nic_bandwidth_avg": round(sum(nic_bw_values) / max(len(nic_bw_values), 1), 2),
            "nic_bandwidth_max": round(max(nic_bw_values) if nic_bw_values else 0.0, 2),
            "switch_util_avg": round(sum(switch_util_values) / max(len(switch_util_values), 1), 2),
            "packet_drop_avg": round(sum(packet_drop_values) / max(len(packet_drop_values), 1), 6),
            # Routing
            "primary_pod_id": primary_pod,
            "primary_node_id": primary_node,
            "primary_gpu_id": primary_gpu,
        })

    return correlated_rows


def step2_windowed_aggregation(correlated_rows: list[dict]) -> list[dict]:
    """
    Step 2: Bucket correlated traces into time windows and compute aggregates.
    Step 3: Apply correlation verdicts.
    """
    if not correlated_rows:
        return []

    # Determine overall time range
    all_starts = [r["start_ts_ms"] for r in correlated_rows if r["start_ts_ms"]]
    if not all_starts:
        return []

    global_start = min(all_starts)
    global_end = max(r["end_ts_ms"] for r in correlated_rows if r["end_ts_ms"])

    window_rows = []
    prev_quality = {}  # Track previous window quality for drift calculation

    for window_name, window_ms in WINDOW_SIZES_MS.items():
        window_start = global_start
        prev_avg_quality = None

        while window_start < global_end:
            window_end = window_start + window_ms

            # Traces in this window (by start_ts_ms)
            window_traces = [r for r in correlated_rows
                             if r["start_ts_ms"] and window_start <= r["start_ts_ms"] < window_end]

            if not window_traces:
                window_start = window_end
                continue

            # --- Trajectory signals ---
            sig_counts = defaultdict(int)
            for t in window_traces:
                sig_counts[t["trajectory_signature"]] += 1

            dominant_sig = max(sig_counts, key=sig_counts.get)
            dominant_share = sig_counts[dominant_sig] / len(window_traces)
            unique_templates = len(sig_counts)
            entropy = _shannon_entropy(list(sig_counts.values()))
            avg_steps = sum(t["step_count"] for t in window_traces) / len(window_traces)
            avg_llm = sum(t["llm_call_count"] for t in window_traces) / len(window_traces)
            avg_tool = sum(t["tool_call_count"] for t in window_traces) / len(window_traces)

            # --- Quality signals ---
            quality_vals = [t["quality_overall"] for t in window_traces if t["quality_overall"] > 0]
            avg_quality = sum(quality_vals) / max(len(quality_vals), 1)
            min_quality = min(quality_vals) if quality_vals else 0.0
            max_quality = max(quality_vals) if quality_vals else 0.0
            quality_var = (sum((q - avg_quality) ** 2 for q in quality_vals) / max(len(quality_vals), 1)) if quality_vals else 0.0
            hallucination_vals = [t["quality_hallucination"] for t in window_traces if t["quality_hallucination"] > 0]
            avg_hallucination = sum(hallucination_vals) / max(len(hallucination_vals), 1)

            # Drift: difference from previous window
            quality_drift = 0.0
            if prev_avg_quality is not None and avg_quality > 0:
                quality_drift = avg_quality - prev_avg_quality
            prev_avg_quality = avg_quality if avg_quality > 0 else prev_avg_quality

            # --- GPU signals ---
            gpu_cont_vals = [t["gpu_contention_avg"] for t in window_traces]
            avg_contention = sum(gpu_cont_vals) / max(len(gpu_cont_vals), 1)
            max_contention = max(t["gpu_contention_max"] for t in window_traces) if window_traces else 0.0
            gpu_queue_vals = [t["gpu_queue_delay_avg"] for t in window_traces]
            avg_queue = sum(gpu_queue_vals) / max(len(gpu_queue_vals), 1)
            p95_queue = _percentile(gpu_queue_vals, 95)

            # --- Network signals ---
            nic_bw_vals = [t["nic_bandwidth_avg"] for t in window_traces]
            avg_nic_bw = sum(nic_bw_vals) / max(len(nic_bw_vals), 1)
            switch_vals = [t["switch_util_avg"] for t in window_traces]
            avg_switch = sum(switch_vals) / max(len(switch_vals), 1)
            pkt_vals = [t["packet_drop_avg"] for t in window_traces]
            max_pkt_drop = max(pkt_vals) if pkt_vals else 0.0

            # --- Step 3: Correlation Verdict ---
            quality_dropped = quality_drift < QUALITY_DROP_THRESHOLD
            trajectory_shifted = dominant_share < (1.0 - TRAJECTORY_SHIFT_THRESHOLD)
            gpu_pressured = avg_contention > GPU_CONTENTION_THRESHOLD
            net_pressured = (avg_nic_bw / 100.0) > NIC_PRESSURE_THRESHOLD or (avg_switch / 100.0) > 0.8

            if quality_dropped and trajectory_shifted and gpu_pressured:
                flag = "gpu_induced_degradation"
                details = (f"Quality drift={quality_drift:.2f}, dominant_share={dominant_share:.2f}, "
                           f"GPU contention={avg_contention:.2f}")
            elif quality_dropped and trajectory_shifted and net_pressured:
                flag = "network_induced_degradation"
                details = (f"Quality drift={quality_drift:.2f}, dominant_share={dominant_share:.2f}, "
                           f"NIC bandwidth={avg_nic_bw:.1f}%")
            elif quality_dropped and trajectory_shifted:
                flag = "app_layer_degradation"
                details = (f"Quality drift={quality_drift:.2f}, trajectory shifted but no infra pressure")
            elif quality_dropped and not trajectory_shifted:
                flag = "quality_drop_stable_trajectory"
                details = f"Quality drift={quality_drift:.2f}, trajectories unchanged"
            elif trajectory_shifted and not quality_dropped:
                flag = "trajectory_drift_no_quality_impact"
                details = f"Dominant share={dominant_share:.2f}, but quality stable"
            else:
                flag = "normal"
                details = ""

            window_rows.append({
                "window_start_ms": window_start,
                "window_end_ms": window_end,
                "window_size": window_name,
                # Trajectory
                "trace_count": len(window_traces),
                "dominant_trajectory": dominant_sig,
                "dominant_share": round(dominant_share, 4),
                "unique_templates": unique_templates,
                "trajectory_entropy": round(entropy, 4),
                "avg_step_count": round(avg_steps, 2),
                "avg_llm_calls": round(avg_llm, 2),
                "avg_tool_calls": round(avg_tool, 2),
                # Quality
                "avg_quality": round(avg_quality, 4),
                "min_quality": round(min_quality, 2),
                "max_quality": round(max_quality, 2),
                "quality_variance": round(quality_var, 4),
                "quality_drift": round(quality_drift, 4),
                "avg_hallucination": round(avg_hallucination, 4),
                # GPU
                "avg_contention_index": round(avg_contention, 4),
                "max_contention_index": round(max_contention, 4),
                "avg_queue_delay_ms": round(avg_queue, 2),
                "p95_queue_delay_ms": round(p95_queue, 2),
                # Network
                "avg_nic_bandwidth_pct": round(avg_nic_bw, 2),
                "avg_switch_util": round(avg_switch, 2),
                "max_packet_drop_rate": round(max_pkt_drop, 6),
                # Correlation
                "correlation_flag": flag,
                "correlation_details": details,
            })

            window_start = window_end

    return window_rows


def run_correlation() -> None:
    """Run the full correlation pipeline."""
    # Step 1: Per-trace correlation
    logger.info("Step 1: Per-trace enrichment...")
    correlated_rows = step1_per_trace_correlation()

    if not correlated_rows:
        logger.error("No correlated traces produced. Check input tables.")
        return

    # Write trace_correlated
    out_table = pa.Table.from_pylist(correlated_rows, schema=TRACE_CORRELATED_SCHEMA)
    write_deltalake(OUTPUT_CORRELATED_PATH, out_table, mode="overwrite")
    logger.info(f"Wrote {len(correlated_rows)} correlated traces to {OUTPUT_CORRELATED_PATH}")

    # Step 2+3: Windowed aggregation with verdicts
    logger.info("Step 2+3: Windowed aggregation and correlation verdicts...")
    window_rows = step2_windowed_aggregation(correlated_rows)

    if window_rows:
        win_table = pa.Table.from_pylist(window_rows, schema=ANALYTICS_WINDOW_SCHEMA)
        write_deltalake(OUTPUT_WINDOWS_PATH, win_table, mode="overwrite")
        logger.info(f"Wrote {len(window_rows)} analytics windows to {OUTPUT_WINDOWS_PATH}")

    # Print summary
    print_summary(correlated_rows, window_rows)


def print_summary(correlated_rows: list[dict], window_rows: list[dict]) -> None:
    """Print correlation results summary."""
    print(f"\n{'='*70}")
    print(f" Correlation Job Complete")
    print(f"{'='*70}")

    # Per-trace stats
    print(f"\n  Correlated traces: {len(correlated_rows)}")
    if correlated_rows:
        avg_q = sum(r["quality_overall"] for r in correlated_rows) / len(correlated_rows)
        avg_gpu = sum(r["gpu_contention_avg"] for r in correlated_rows) / len(correlated_rows)
        print(f"  Avg quality:       {avg_q:.2f}")
        print(f"  Avg GPU contention:{avg_gpu:.3f}")

        # Traces with high contention
        high_cont = [r for r in correlated_rows if r["gpu_contention_avg"] > 0.7]
        if high_cont:
            avg_q_high = sum(r["quality_overall"] for r in high_cont) / len(high_cont)
            print(f"\n  Traces with GPU contention > 0.7: {len(high_cont)}")
            print(f"  Their avg quality: {avg_q_high:.2f}")

    # Window stats
    print(f"\n  Analytics windows: {len(window_rows)}")
    if window_rows:
        flags = defaultdict(int)
        for w in window_rows:
            flags[w["correlation_flag"]] += 1
        print(f"\n  Correlation verdicts:")
        for flag, count in sorted(flags.items(), key=lambda x: -x[1]):
            print(f"    {flag:<40} {count:>4}")

        # Show alert windows
        alerts = [w for w in window_rows if w["correlation_flag"] != "normal"]
        if alerts:
            print(f"\n  Alert windows (non-normal):")
            for w in alerts[:10]:
                print(f"    [{w['window_size']}] {w['correlation_flag']}")
                if w["correlation_details"]:
                    print(f"      → {w['correlation_details']}")

    print(f"{'='*70}\n")


if __name__ == "__main__":
    run_correlation()
