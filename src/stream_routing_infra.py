"""
Stream 4: agent_steps → request_routing + gpu_metrics + network_metrics

Spark Structured Streaming job.
Reads new agent_steps, generates simulated GPU/network metrics for the time window,
and assigns REASON spans to vLLM pods via deterministic routing.
Checkpoint: data/checkpoints/routing_infra
"""

import hashlib
import logging
import os
import random
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable

from src.streaming_config import (
    AGENT_STEPS_PATH, GPU_METRICS_PATH, NETWORK_METRICS_PATH, ROUTING_PATH,
    CHECKPOINT_BASE, TRIGGER_INTERVAL, SIM_SEED, TOPOLOGY,
    GPU_METRICS_SCHEMA, NETWORK_METRICS_SCHEMA, ROUTING_SCHEMA,
    create_spark_session, ensure_delta_table,
)

logger = logging.getLogger("stream_routing_infra")


def _clamp(val: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, val)))


def _compute_contention_index(gpu_util, sm_occ, mem_pct, queue_delay, power_pct) -> float:
    norm_queue = min(queue_delay / 500.0, 1.0)
    return _clamp(
        0.25 * (gpu_util / 100.0) + 0.20 * (sm_occ / 100.0) +
        0.20 * (mem_pct / 100.0) + 0.25 * norm_queue + 0.10 * (power_pct / 100.0),
        0.0, 1.0
    )


def _is_contention_window(timestamp_ms: int, rng: random.Random) -> float:
    cycle_ms = 20 * 60 * 1000
    position_in_cycle = timestamp_ms % cycle_ms
    contention_start = 15 * 60 * 1000

    if position_in_cycle >= contention_start:
        progress = (position_in_cycle - contention_start) / (5 * 60 * 1000)
        intensity = 0.6 + 0.3 * (1.0 - abs(2.0 * progress - 1.0))
        return intensity
    return 0.0


def generate_infra_metrics(spark: SparkSession, window_start_ms: int, window_end_ms: int):
    rng = random.Random(SIM_SEED)
    ingestion_time = datetime.now(timezone.utc)
    ingestion_date = ingestion_time.strftime("%Y-%m-%d")
    ingestion_hour = ingestion_time.hour

    metric_interval_ms = 5000
    gpu_rows = []
    net_rows = []

    timestamp = window_start_ms
    while timestamp <= window_end_ms:
        for node in TOPOLOGY["nodes"]:
            node_id = node["id"]
            node_offset = int(node_id.split("-")[1]) * 7 * 60 * 1000

            for gpu in node["gpus"]:
                gpu_id = gpu["id"]
                gpu_offset = int(gpu_id.split("-")[1]) * 3 * 60 * 1000
                intensity = _is_contention_window(timestamp + node_offset + gpu_offset, rng)

                if intensity > 0:
                    gpu_util = _clamp(rng.gauss(50 + intensity * 48, 4), 0, 100)
                    sm_occ = _clamp(rng.gauss(45 + intensity * 50, 5), 0, 100)
                    mem_pct = _clamp(rng.gauss(50 + intensity * 42, 4), 0, 100)
                    queue_delay = max(0.0, rng.gauss(intensity * 300, 60))
                    power_pct = _clamp(rng.gauss(55 + intensity * 40, 5), 0, 100)
                    pcie_bw = _clamp(rng.gauss(40 + intensity * 50, 8), 0, 100)
                else:
                    gpu_util = _clamp(rng.gauss(40, 12), 5, 75)
                    sm_occ = _clamp(rng.gauss(45, 10), 10, 70)
                    mem_pct = _clamp(rng.gauss(48, 8), 20, 65)
                    queue_delay = max(0.0, rng.gauss(3, 2))
                    power_pct = _clamp(rng.gauss(55, 8), 30, 75)
                    pcie_bw = _clamp(rng.gauss(25, 8), 5, 50)

                contention_idx = _compute_contention_index(gpu_util, sm_occ, mem_pct, queue_delay, power_pct)

                gpu_rows.append({
                    "timestamp_ms": timestamp,
                    "node_id": node_id,
                    "gpu_id": gpu_id,
                    "gpu_uuid": gpu["uuid"],
                    "gpu_utilization": float(round(gpu_util, 2)),
                    "sm_occupancy": float(round(sm_occ, 2)),
                    "memory_used_pct": float(round(mem_pct, 2)),
                    "queue_delay_ms": float(round(queue_delay, 2)),
                    "power_draw_pct": float(round(power_pct, 2)),
                    "pcie_bandwidth_pct": float(round(pcie_bw, 2)),
                    "contention_index": float(round(contention_idx, 4)),
                    "ingestion_date": ingestion_date,
                    "ingestion_hour": ingestion_hour,
                })

            nic_intensity = _is_contention_window(timestamp + node_offset, rng)
            if nic_intensity > 0:
                nic_bw = _clamp(rng.gauss(30 + nic_intensity * 65, 8), 0, 100)
                drop_rate = max(0.0, rng.gauss(nic_intensity * 0.05, 0.01))
                sw_util = _clamp(rng.gauss(30 + nic_intensity * 60, 8), 0, 100)
                latency = max(50.0, rng.gauss(100 + nic_intensity * 800, 100))
            else:
                nic_bw = _clamp(rng.gauss(20, 8), 2, 45)
                drop_rate = max(0.0, rng.gauss(0.001, 0.0005))
                sw_util = _clamp(rng.gauss(18, 6), 2, 40)
                latency = max(20.0, rng.gauss(80, 15))

            net_rows.append({
                "timestamp_ms": timestamp,
                "node_id": node_id,
                "nic_id": node["nic"]["id"],
                "switch_port": node["switch_port"],
                "nic_bandwidth_pct": float(round(nic_bw, 2)),
                "packet_drop_rate": float(round(drop_rate, 6)),
                "switch_port_util": float(round(sw_util, 2)),
                "latency_us": float(round(latency, 2)),
                "ingestion_date": ingestion_date,
                "ingestion_hour": ingestion_hour,
            })

        timestamp += metric_interval_ms

    if gpu_rows:
        gpu_df = spark.createDataFrame(gpu_rows, schema=GPU_METRICS_SCHEMA)
        gpu_target = DeltaTable.forPath(spark, GPU_METRICS_PATH)
        (
            gpu_target.alias("target")
            .merge(
                gpu_df.alias("source"),
                "target.timestamp_ms = source.timestamp_ms AND target.node_id = source.node_id AND target.gpu_id = source.gpu_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    if net_rows:
        net_df = spark.createDataFrame(net_rows, schema=NETWORK_METRICS_SCHEMA)
        net_target = DeltaTable.forPath(spark, NETWORK_METRICS_PATH)
        (
            net_target.alias("target")
            .merge(
                net_df.alias("source"),
                "target.timestamp_ms = source.timestamp_ms AND target.node_id = source.node_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    logger.info(f"Generated {len(gpu_rows)} GPU + {len(net_rows)} network metrics")


def process_routing(spark: SparkSession, micro_batch_df: DataFrame, batch_id: int):
    ingestion_time = datetime.now(timezone.utc)
    ingestion_date = ingestion_time.strftime("%Y-%m-%d")
    ingestion_hour = ingestion_time.hour

    impacted_traces = micro_batch_df.select("trace_id").distinct().collect()
    impacted_trace_ids = [row.trace_id for row in impacted_traces]

    if not impacted_trace_ids:
        return

    reason_spans = (
        spark.read.format("delta").load(AGENT_STEPS_PATH)
        .filter(F.col("trace_id").isin(impacted_trace_ids))
        .filter(F.col("span_kind") == "REASON")
        .collect()
    )

    if not reason_spans:
        return

    pods = TOPOLOGY["pods"]
    routing_rows = []

    for span in reason_spans:
        pod_index = int(hashlib.md5(span.span_id.encode()).hexdigest(), 16) % len(pods)
        selected_pod = pods[pod_index]
        node = next(n for n in TOPOLOGY["nodes"] if n["id"] == selected_pod["node"])
        gpu = next(g for g in node["gpus"] if g["id"] == selected_pod["gpu"])

        routing_rows.append({
            "trace_id": span.trace_id,
            "span_id": span.span_id,
            "vllm_pod_id": selected_pod["id"],
            "node_id": node["id"],
            "gpu_id": selected_pod["gpu"],
            "gpu_uuid": gpu["uuid"],
            "nic_id": node["nic"]["id"],
            "switch_port": node["switch_port"],
            "timestamp_ms": span.start_ts_ms,
            "ingestion_date": ingestion_date,
            "ingestion_hour": ingestion_hour,
        })

    if not routing_rows:
        return

    updates_df = spark.createDataFrame(routing_rows, schema=ROUTING_SCHEMA)
    target_table = DeltaTable.forPath(spark, ROUTING_PATH)
    (
        target_table.alias("target")
        .merge(
            updates_df.alias("source"),
            "target.trace_id = source.trace_id AND target.span_id = source.span_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"Batch {batch_id}: Routed {len(routing_rows)} REASON spans")


def process_batch(spark: SparkSession, micro_batch_df: DataFrame, batch_id: int):
    if micro_batch_df.isEmpty():
        return

    # Generate infra metrics for the time window covered by this batch
    min_ts = micro_batch_df.agg(F.min("start_ts_ms")).collect()[0][0]
    max_ts = micro_batch_df.agg(F.max("end_ts_ms")).collect()[0][0]
    if min_ts and max_ts:
        generate_infra_metrics(spark, min_ts - 60000, max_ts + 60000)

    # Assign routing
    process_routing(spark, micro_batch_df, batch_id)


def main():
    spark = create_spark_session("Stream_RoutingInfra")
    spark.sparkContext.setLogLevel("WARN")

    ensure_delta_table(spark, GPU_METRICS_PATH, GPU_METRICS_SCHEMA)
    ensure_delta_table(spark, NETWORK_METRICS_PATH, NETWORK_METRICS_SCHEMA)
    ensure_delta_table(spark, ROUTING_PATH, ROUTING_SCHEMA)

    stream_df = spark.readStream.format("delta").load(AGENT_STEPS_PATH)

    query = (
        stream_df.writeStream
        .foreachBatch(lambda df, bid: process_batch(spark, df, bid))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "routing_infra"))
        .queryName("stream_routing_infra")
        .start()
    )

    logger.info(f"stream_routing_infra started | source={AGENT_STEPS_PATH} | trigger={TRIGGER_INTERVAL}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
