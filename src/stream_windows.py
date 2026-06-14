"""
Stream 6: trace_correlated -> analytics_windows

Spark Structured Streaming job. Time-buckets the per-trace correlated view into
windows (5min / 30min / 1h) and answers the core thesis questions:

  Q1  How do agent execution paths change OVER TIME?
      Per window we compute the trajectory-signature distribution, its dominant
      path share, Shannon entropy, and Jensen-Shannon drift vs a baseline
      window. Rising entropy / rising JSD = structural drift.

  Q2  When quality drops, do trajectories drift AND does GPU contention co-occur?
      Per window we raise three independent "lights":
        - quality_drop      : mean quality fell vs baseline (or below abs floor)
        - trajectory_drift  : JSD vs baseline exceeded threshold
        - gpu_pressure      : mean GPU contention exceeded threshold
        - network_pressure  : packet drops / latency elevated
      A rule table turns the combination of lights into a correlation_flag
      verdict (gpu_induced_degradation, app_layer_degradation, ...). We also
      compute the within-window Pearson correlation between GPU contention and
      quality as a statistical cross-check.

Recompute strategy: each trigger we recompute ALL windows from the full
trace_correlated table and OVERWRITE analytics_windows. The table is tiny and
fully derived, so this avoids incremental-state complexity.

Checkpoint: data/checkpoints/windows
"""

import logging
import math
import os
from collections import Counter, defaultdict

from pyspark.sql import SparkSession, DataFrame

from src.streaming_config import (
    CORRELATED_PATH, ANALYTICS_WINDOWS_PATH,
    CHECKPOINT_BASE, TRIGGER_INTERVAL,
    CORRELATED_SCHEMA, ANALYTICS_WINDOWS_SCHEMA,
    create_spark_session, ensure_delta_table,
)

logger = logging.getLogger("stream_windows")

# --- Thresholds (env-tunable) ---
DRIFT_JSD_THRESHOLD = float(os.getenv("DRIFT_JSD_THRESHOLD", "0.15"))
QUALITY_DROP_DELTA = float(os.getenv("QUALITY_DROP_DELTA", "0.5"))   # vs baseline, 0..5 scale
QUALITY_BAD_ABS = float(os.getenv("QUALITY_BAD_ABS", "3.0"))         # absolute floor
GPU_PRESSURE_THRESHOLD = float(os.getenv("GPU_PRESSURE_THRESHOLD", "0.6"))
NET_LATENCY_THRESHOLD = float(os.getenv("NET_LATENCY_THRESHOLD", "500.0"))  # microseconds

WINDOW_SIZES = [
    ("5min", 5 * 60 * 1000),
    ("30min", 30 * 60 * 1000),
    ("1h", 60 * 60 * 1000),
]


# --- Pure-python math helpers ---

def shannon_entropy(counts: list[int]) -> float:
    """Shannon entropy (bits) of a count distribution. 0 = single path."""
    total = sum(counts)
    if total <= 0:
        return 0.0
    h = 0.0
    for c in counts:
        if c <= 0:
            continue
        p = c / total
        h -= p * math.log2(p)
    return round(h, 4)


def _kl(p: dict, m: dict) -> float:
    s = 0.0
    for k, pv in p.items():
        if pv <= 0:
            continue
        mv = m.get(k, 0.0)
        if mv > 0:
            s += pv * math.log2(pv / mv)
    return s


def jensen_shannon(dist_a: dict, dist_b: dict) -> float:
    """JSD between two signature->probability dicts. Bounded [0,1] (log2)."""
    keys = set(dist_a) | set(dist_b)
    if not keys:
        return 0.0
    p = {k: dist_a.get(k, 0.0) for k in keys}
    q = {k: dist_b.get(k, 0.0) for k in keys}
    m = {k: 0.5 * (p[k] + q[k]) for k in keys}
    jsd = 0.5 * _kl(p, m) + 0.5 * _kl(q, m)
    return round(max(0.0, min(1.0, jsd)), 4)


def pearson(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation. Returns 0.0 if undefined (n<3 or zero variance)."""
    n = len(xs)
    if n < 3:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return 0.0
    return round(sxy / math.sqrt(sxx * syy), 4)


def signature_distribution(traces: list) -> dict:
    """signature -> probability for a list of correlated trace rows."""
    counts = Counter(t.trajectory_signature for t in traces if t.trajectory_signature)
    total = sum(counts.values())
    if total == 0:
        return {}
    return {sig: c / total for sig, c in counts.items()}


def _avg(vals: list[float]) -> float:
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


# --- Verdict logic (Q2) ---

def classify_window(quality_drop, trajectory_drift, gpu_pressure, network_pressure) -> str:
    if quality_drop and trajectory_drift and gpu_pressure:
        return "gpu_induced_degradation"
    if quality_drop and trajectory_drift and network_pressure:
        return "network_induced_degradation"
    if quality_drop and trajectory_drift:
        return "app_layer_degradation"
    if trajectory_drift and not quality_drop:
        return "trajectory_drift_no_quality_impact"
    if quality_drop and not trajectory_drift:
        return "quality_drop_stable_trajectory"
    return "normal"


def build_windows_for_size(traces: list, size_label: str, size_ms: int) -> list[dict]:
    """Bucket traces into fixed windows of size_ms and compute all metrics."""
    buckets = defaultdict(list)
    for t in traces:
        if not t.start_ts_ms:
            continue
        wstart = (t.start_ts_ms // size_ms) * size_ms
        buckets[wstart].append(t)

    if not buckets:
        return []

    ordered_starts = sorted(buckets.keys())
    # Baseline = earliest window for this size (the calm reference point).
    baseline_start = ordered_starts[0]
    baseline_traces = buckets[baseline_start]
    baseline_dist = signature_distribution(baseline_traces)
    baseline_quality = _avg([t.quality_overall for t in baseline_traces])

    rows = []
    for wstart in ordered_starts:
        wtraces = buckets[wstart]
        n = len(wtraces)
        is_baseline = 1 if wstart == baseline_start else 0

        dist = signature_distribution(wtraces)
        if dist:
            dominant_sig, dominant_share = max(dist.items(), key=lambda kv: kv[1])
        else:
            dominant_sig, dominant_share = "", 0.0
        counts = list(Counter(
            t.trajectory_signature for t in wtraces if t.trajectory_signature
        ).values())
        entropy = shannon_entropy(counts)
        drift_jsd = 0.0 if is_baseline else jensen_shannon(dist, baseline_dist)

        q_overall = _avg([t.quality_overall for t in wtraces])
        q_halluc = _avg([t.quality_hallucination for t in wtraces])
        q_delta = round(q_overall - baseline_quality, 4)

        gpu_vals = [t.gpu_contention_avg for t in wtraces if t.gpu_contention_avg is not None]
        gpu_avg = round(_avg(gpu_vals), 4)
        gpu_max = round(max(gpu_vals), 4) if gpu_vals else 0.0
        net_latency = round(_avg([t.inter_node_latency_avg for t in wtraces]), 2)
        pkt_drops = sum(int(t.packet_drop_total or 0) for t in wtraces)

        # --- Q2 lights ---
        quality_drop = int(
            (q_delta <= -QUALITY_DROP_DELTA) or (q_overall > 0 and q_overall < QUALITY_BAD_ABS)
        )
        trajectory_drift = int(drift_jsd >= DRIFT_JSD_THRESHOLD)
        gpu_pressure = int(gpu_avg >= GPU_PRESSURE_THRESHOLD)
        network_pressure = int(pkt_drops > 0 or net_latency >= NET_LATENCY_THRESHOLD)

        corr = pearson(
            [t.gpu_contention_avg or 0.0 for t in wtraces],
            [t.quality_overall or 0.0 for t in wtraces],
        )

        flag = classify_window(quality_drop, trajectory_drift, gpu_pressure, network_pressure)

        details = (
            f"q_avg={q_overall:.2f} (delta {q_delta:+.2f}), halluc={q_halluc:.2f}, "
            f"drift_jsd={drift_jsd:.3f}, entropy={entropy:.2f}, "
            f"gpu_contention={gpu_avg:.3f}, corr(gpu,q)={corr:+.2f}, "
            f"n={n}"
        )

        rows.append({
            "window_size": size_label,
            "window_start_ms": int(wstart),
            "window_end_ms": int(wstart + size_ms),
            "trace_count": n,
            "dominant_signature": dominant_sig,
            "dominant_share": round(dominant_share, 4),
            "unique_signatures": len(dist),
            "trajectory_entropy": entropy,
            "trajectory_drift_jsd": drift_jsd,
            "is_baseline": is_baseline,
            "quality_overall_avg": round(q_overall, 4),
            "quality_hallucination_avg": round(q_halluc, 4),
            "quality_overall_baseline": round(baseline_quality, 4),
            "quality_delta": q_delta,
            "gpu_contention_avg": gpu_avg,
            "gpu_contention_max": gpu_max,
            "net_latency_avg": net_latency,
            "packet_drop_total": pkt_drops,
            "quality_drop": quality_drop,
            "trajectory_drift": trajectory_drift,
            "gpu_pressure": gpu_pressure,
            "network_pressure": network_pressure,
            "contention_quality_corr": corr,
            "correlation_flag": flag,
            "correlation_details": details,
        })
    return rows


def process_batch(spark: SparkSession, micro_batch_df: DataFrame, batch_id: int):
    # Recompute from the full correlated table (ignore the micro-batch payload).
    corr_df = spark.read.format("delta").load(CORRELATED_PATH)
    traces = [r for r in corr_df.collect() if r.start_ts_ms]
    if not traces:
        logger.info(f"Batch {batch_id}: no correlated traces yet")
        return

    all_rows = []
    for label, size_ms in WINDOW_SIZES:
        all_rows.extend(build_windows_for_size(traces, label, size_ms))

    if not all_rows:
        return

    out_df = spark.createDataFrame(all_rows, schema=ANALYTICS_WINDOWS_SCHEMA)
    (
        out_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(ANALYTICS_WINDOWS_PATH)
    )

    alerts = sum(1 for r in all_rows if r["correlation_flag"] != "normal")
    logger.info(
        f"Batch {batch_id}: wrote {len(all_rows)} windows across "
        f"{len(WINDOW_SIZES)} sizes | {alerts} non-normal windows"
    )


def main():
    spark = create_spark_session("Stream_Windows")
    spark.sparkContext.setLogLevel("WARN")

    ensure_delta_table(spark, CORRELATED_PATH, CORRELATED_SCHEMA)
    ensure_delta_table(spark, ANALYTICS_WINDOWS_PATH, ANALYTICS_WINDOWS_SCHEMA)

    stream_df = (
        spark.readStream.format("delta")
        .option("skipChangeCommits", "true")
        .load(CORRELATED_PATH)
    )

    query = (
        stream_df.writeStream
        .foreachBatch(lambda df, bid: process_batch(spark, df, bid))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "windows"))
        .queryName("stream_windows")
        .start()
    )

    logger.info(
        f"stream_windows started | source={CORRELATED_PATH} -> {ANALYTICS_WINDOWS_PATH} "
        f"| trigger={TRIGGER_INTERVAL}"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
