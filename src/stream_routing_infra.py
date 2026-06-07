"""
Stream 4: Ingest real GPU/network metrics + route REASON spans.

Spark Structured Streaming job.
- Reads real GPU metrics from collector JSONL files → writes to gpu_metrics Delta table.
- Reads real network metrics from collector JSONL files → writes to network_metrics Delta table.
- Reads new agent_steps REASON spans → assigns routing based on which node handled inference.
Checkpoint: data/checkpoints/routing_infra
"""

import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable

from src.streaming_config import (
    AGENT_STEPS_PATH, GPU_METRICS_PATH, NETWORK_METRICS_PATH, ROUTING_PATH,
    CHECKPOINT_BASE, TRIGGER_INTERVAL, TOPOLOGY,
    AGENT_STEPS_SCHEMA, GPU_METRICS_SCHEMA, NETWORK_METRICS_SCHEMA, ROUTING_SCHEMA,
    NODE_1_ID, NODE_2_ID,
    create_spark_session, ensure_delta_table,
)

logger = logging.getLogger("stream_routing_infra")

# Paths where collectors write JSONL
GPU_METRICS_RAW = os.getenv("GPU_METRICS_RAW", "./data/gpu_metrics_raw")
NET_METRICS_RAW = os.getenv("NET_METRICS_RAW", "./data/net_metrics_raw")


def ingest_gpu_metrics(spark: SparkSession):
    """Read collector JSONL files and merge into gpu_metrics Delta table."""
    raw_path = os.path.join(GPU_METRICS_RAW, "*.jsonl")
    try:
        raw_df = spark.read.schema(GPU_METRICS_SCHEMA).json(raw_path)
        if raw_df.isEmpty():
            return 0

        target = DeltaTable.forPath(spark, GPU_METRICS_PATH)
        (
            target.alias("target")
            .merge(
                raw_df.alias("source"),
                "target.timestamp_ms = source.timestamp_ms AND target.node_id = source.node_id AND target.gpu_id = source.gpu_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        count = raw_df.count()
        logger.info(f"Ingested {count} GPU metric rows")
        return count
    except Exception as e:
        logger.debug(f"No GPU metrics to ingest: {e}")
        return 0


def ingest_network_metrics(spark: SparkSession):
    """Read collector JSONL files and merge into network_metrics Delta table."""
    raw_path = os.path.join(NET_METRICS_RAW, "*.jsonl")
    try:
        raw_df = spark.read.schema(NETWORK_METRICS_SCHEMA).json(raw_path)
        if raw_df.isEmpty():
            return 0

        target = DeltaTable.forPath(spark, NETWORK_METRICS_PATH)
        (
            target.alias("target")
            .merge(
                raw_df.alias("source"),
                "target.timestamp_ms = source.timestamp_ms AND target.node_id = source.node_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        count = raw_df.count()
        logger.info(f"Ingested {count} network metric rows")
        return count
    except Exception as e:
        logger.debug(f"No network metrics to ingest: {e}")
        return 0


def process_routing(spark: SparkSession, micro_batch_df: DataFrame, batch_id: int):
    """Assign REASON spans to nodes based on which Ollama instance handled them."""
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

    nodes = TOPOLOGY["nodes"]
    routing_rows = []

    for span in reason_spans:
        # Determine which node handled this span from span attributes
        # In real deployment, this comes from the load balancer response header.
        # Here we use the node_id embedded in the span context by the LLM client.
        span_context = span.context or ""
        if NODE_2_ID in span_context:
            node_id = NODE_2_ID
        else:
            node_id = NODE_1_ID

        # Find GPU info from latest GPU metrics for this node
        pod_id = f"ollama-{node_id}"
        gpu_id = "gpu-0"  # Single GPU per node on NC4as_T4_v3
        gpu_uuid = ""

        # Try to get UUID from collected metrics
        try:
            latest_gpu = (
                spark.read.format("delta").load(GPU_METRICS_PATH)
                .filter(F.col("node_id") == node_id)
                .orderBy(F.desc("timestamp_ms"))
                .limit(1)
                .collect()
            )
            if latest_gpu:
                gpu_uuid = latest_gpu[0].gpu_uuid or ""
                gpu_id = latest_gpu[0].gpu_id or "gpu-0"
        except Exception:
            pass

        # Queue wait: difference between span start and actual inference start
        # Approximated as a fraction of total duration for now
        queue_wait_ms = 0.0
        if span.duration_ms and span.duration_ms > 100:
            # First 5-15% is typically queue wait on a loaded GPU
            queue_wait_ms = round(span.duration_ms * 0.05, 2)

        routing_rows.append({
            "trace_id": span.trace_id,
            "span_id": span.span_id,
            "vllm_pod_id": pod_id,
            "node_id": node_id,
            "gpu_id": gpu_id,
            "gpu_uuid": gpu_uuid,
            "queue_wait_ms": queue_wait_ms,
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

    # Ingest real metrics from collector files
    ingest_gpu_metrics(spark)
    ingest_network_metrics(spark)

    # Assign routing
    process_routing(spark, micro_batch_df, batch_id)


def main():
    spark = create_spark_session("Stream_RoutingInfra")
    spark.sparkContext.setLogLevel("WARN")

    ensure_delta_table(spark, AGENT_STEPS_PATH, AGENT_STEPS_SCHEMA)
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
