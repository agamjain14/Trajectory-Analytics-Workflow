"""
Stream 5: quality_scores → trace_correlated

Spark Structured Streaming job.
Reads new quality_scores, joins with trajectory + routing + GPU/net metrics.
Checkpoint: data/checkpoints/correlated
"""

import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable

from src.streaming_config import (
    TRAJECTORY_PATH, QUALITY_PATH, ROUTING_PATH,
    GPU_METRICS_PATH, NETWORK_METRICS_PATH, CORRELATED_PATH,
    CHECKPOINT_BASE, TRIGGER_INTERVAL, CORRELATED_SCHEMA,
    create_spark_session, ensure_delta_table,
)

logger = logging.getLogger("stream_correlated")


def process_batch(spark: SparkSession, micro_batch_df: DataFrame, batch_id: int):
    if micro_batch_df.isEmpty():
        return

    ingestion_time = datetime.now(timezone.utc)
    ingestion_date = ingestion_time.strftime("%Y-%m-%d")
    ingestion_hour = ingestion_time.hour

    # Get impacted trace_ids from new quality_scores
    impacted_traces = micro_batch_df.select("trace_id").distinct().collect()
    impacted_trace_ids = [row.trace_id for row in impacted_traces]

    if not impacted_trace_ids:
        return

    # Load trajectory for impacted traces
    traj_rows = (
        spark.read.format("delta").load(TRAJECTORY_PATH)
        .filter(F.col("trace_id").isin(impacted_trace_ids))
        .collect()
    )

    if not traj_rows:
        logger.warning(f"Batch {batch_id}: no trajectories found for impacted traces")
        return

    # Load quality scores
    qual_rows = (
        spark.read.format("delta").load(QUALITY_PATH)
        .filter(F.col("trace_id").isin(impacted_trace_ids))
        .collect()
    )

    # Load routing
    routing_rows = (
        spark.read.format("delta").load(ROUTING_PATH)
        .filter(F.col("trace_id").isin(impacted_trace_ids))
        .collect()
    )

    qual_by_trace = {r.trace_id: r for r in qual_rows}
    routing_by_trace = defaultdict(list)
    for r in routing_rows:
        routing_by_trace[r.trace_id].append(r)

    # Time range for GPU/network lookup
    all_starts = [r.start_ts_ms for r in traj_rows if r.start_ts_ms]
    all_ends = [r.end_ts_ms for r in traj_rows if r.end_ts_ms]
    if not all_starts:
        return

    time_start = min(all_starts) - 10000
    time_end = max(all_ends) + 10000

    gpu_rows = (
        spark.read.format("delta").load(GPU_METRICS_PATH)
        .filter(F.col("timestamp_ms").between(time_start, time_end))
        .collect()
    )
    net_rows = (
        spark.read.format("delta").load(NETWORK_METRICS_PATH)
        .filter(F.col("timestamp_ms").between(time_start, time_end))
        .collect()
    )

    gpu_by_loc = defaultdict(list)
    for r in gpu_rows:
        gpu_by_loc[(r.node_id, r.gpu_id)].append(r)

    net_by_node = defaultdict(list)
    for r in net_rows:
        net_by_node[r.node_id].append(r)

    # Build correlated rows
    correlated_rows = []
    for traj in traj_rows:
        trace_id = traj.trace_id
        qual = qual_by_trace.get(trace_id)
        routings = routing_by_trace.get(trace_id, [])

        gpu_contention_vals = []
        gpu_queue_vals = []
        gpu_mem_vals = []
        nic_bw_vals = []
        sw_util_vals = []
        pkt_drop_vals = []
        pod_counts = defaultdict(int)

        for r in routings:
            pod_counts[r.vllm_pod_id] += 1
            ts = r.timestamp_ms or traj.start_ts_ms

            for gm in gpu_by_loc.get((r.node_id, r.gpu_id), []):
                if abs(gm.timestamp_ms - ts) <= 10000:
                    gpu_contention_vals.append(gm.contention_index)
                    gpu_queue_vals.append(gm.queue_delay_ms)
                    gpu_mem_vals.append(gm.memory_used_pct)

            for nm in net_by_node.get(r.node_id, []):
                if abs(nm.timestamp_ms - ts) <= 10000:
                    nic_bw_vals.append(nm.nic_bandwidth_pct)
                    sw_util_vals.append(nm.switch_port_util)
                    pkt_drop_vals.append(nm.packet_drop_rate)

        primary_pod = max(pod_counts, key=pod_counts.get) if pod_counts else ""
        primary_node = ""
        primary_gpu = ""
        for r in routings:
            if r.vllm_pod_id == primary_pod:
                primary_node = r.node_id
                primary_gpu = r.gpu_id
                break

        correlated_rows.append({
            "trace_id": trace_id,
            "session_id": traj.session_id or "",
            "start_ts_ms": traj.start_ts_ms,
            "end_ts_ms": traj.end_ts_ms,
            "total_duration_ms": traj.total_duration_ms,
            "trajectory_signature": traj.trajectory_signature,
            "step_sequence": traj.step_sequence,
            "step_count": traj.step_count,
            "llm_call_count": traj.llm_call_count,
            "tool_call_count": traj.tool_call_count,
            "retrieve_count": traj.retrieve_count,
            "retry_count": traj.retry_count,
            "quality_overall": float(qual.overall) if qual else 0.0,
            "quality_completeness": float(qual.completeness) if qual else 0.0,
            "quality_coherence": float(qual.coherence) if qual else 0.0,
            "quality_hallucination": float(qual.hallucination) if qual else 0.0,
            "quality_groundedness": float(qual.groundedness) if qual else 0.0,
            "quality_relevance": float(qual.relevance) if qual else 0.0,
            "quality_explanation": qual.explanation if qual else "",
            "gpu_contention_avg": round(sum(gpu_contention_vals) / max(len(gpu_contention_vals), 1), 4),
            "gpu_contention_max": round(max(gpu_contention_vals) if gpu_contention_vals else 0.0, 4),
            "gpu_queue_delay_avg": round(sum(gpu_queue_vals) / max(len(gpu_queue_vals), 1), 2),
            "gpu_queue_delay_max": round(max(gpu_queue_vals) if gpu_queue_vals else 0.0, 2),
            "gpu_memory_pressure_avg": round(sum(gpu_mem_vals) / max(len(gpu_mem_vals), 1), 2),
            "nic_bandwidth_avg": round(sum(nic_bw_vals) / max(len(nic_bw_vals), 1), 2),
            "nic_bandwidth_max": round(max(nic_bw_vals) if nic_bw_vals else 0.0, 2),
            "switch_util_avg": round(sum(sw_util_vals) / max(len(sw_util_vals), 1), 2),
            "packet_drop_avg": round(sum(pkt_drop_vals) / max(len(pkt_drop_vals), 1), 6),
            "primary_pod_id": primary_pod,
            "primary_node_id": primary_node,
            "primary_gpu_id": primary_gpu,
            "ingestion_date": ingestion_date,
            "ingestion_hour": ingestion_hour,
        })

    if not correlated_rows:
        return

    updates_df = spark.createDataFrame(correlated_rows, schema=CORRELATED_SCHEMA)
    target_table = DeltaTable.forPath(spark, CORRELATED_PATH)
    (
        target_table.alias("target")
        .merge(
            updates_df.alias("source"),
            "target.trace_id = source.trace_id AND target.session_id = source.session_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"Batch {batch_id}: Merged {len(correlated_rows)} correlated traces")


def main():
    spark = create_spark_session("Stream_Correlated")
    spark.sparkContext.setLogLevel("WARN")

    ensure_delta_table(spark, CORRELATED_PATH, CORRELATED_SCHEMA)

    stream_df = spark.readStream.format("delta").load(QUALITY_PATH)

    query = (
        stream_df.writeStream
        .foreachBatch(lambda df, bid: process_batch(spark, df, bid))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "correlated"))
        .queryName("stream_correlated")
        .start()
    )

    logger.info(f"stream_correlated started | source={QUALITY_PATH} | trigger={TRIGGER_INTERVAL}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
