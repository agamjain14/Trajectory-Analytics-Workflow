"""
Stream 1: trace_delta_table → agent_steps

Spark Structured Streaming job.
Reads raw OTLP spans, classifies them, and Delta MERGEs into agent_steps.
Checkpoint: data/checkpoints/agent_steps
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable

from src.streaming_config import (
    SOURCE_PATH, AGENT_STEPS_PATH, CHECKPOINT_BASE, TRIGGER_INTERVAL,
    LOOKBACK_HOURS, SOURCE_SCHEMA, AGENT_STEPS_SCHEMA,
    create_spark_session, ensure_delta_table,
    should_drop, classify_span, flatten_tags,
)

logger = logging.getLogger("stream_agent_steps")


def process_batch(spark: SparkSession, micro_batch_df: DataFrame, batch_id: int):
    if micro_batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, skipping")
        return

    ingestion_time = datetime.now(timezone.utc)
    ingestion_date = ingestion_time.strftime("%Y-%m-%d")
    ingestion_hour = ingestion_time.hour

    # Get impacted trace_ids
    impacted_traces = micro_batch_df.select("trace_id").distinct().collect()
    impacted_trace_ids = [row.trace_id for row in impacted_traces]
    logger.info(f"Batch {batch_id}: {len(impacted_trace_ids)} impacted traces")

    if not impacted_trace_ids:
        return

    # Lookback: re-read ALL spans for impacted traces
    min_start_ns = micro_batch_df.agg(F.min("start_time_unix_nano")).collect()[0][0] or 0
    lookback_ns = min_start_ns - (LOOKBACK_HOURS * 3600 * 1_000_000_000)

    source_df = (
        spark.read.format("delta").load(SOURCE_PATH)
        .filter(F.col("trace_id").isin(impacted_trace_ids))
        .filter(F.col("start_time_unix_nano") >= lookback_ns)
    )
    source_rows = source_df.collect()
    logger.info(f"Batch {batch_id}: lookback returned {len(source_rows)} spans")

    # Classify and flatten
    classified_rows = []
    for row in source_rows:
        operation_name = row.operation_name or ""
        if should_drop(operation_name):
            continue

        try:
            tags = json.loads(row.tags) if row.tags else {}
        except json.JSONDecodeError:
            tags = {}

        classification = classify_span(tags)
        if classification is None:
            continue

        span_kind, sub_span_kind = classification
        start_ns = row.start_time_unix_nano or 0
        end_ns = row.end_time_unix_nano or 0
        duration_ns = row.duration_ns or 0

        classified_row = {
            "trace_id": row.trace_id,
            "session_id": row.session_id or "",
            "span_id": row.span_id,
            "parent_span_id": row.parent_span_id or "",
            "span_kind": span_kind,
            "sub_span_kind": sub_span_kind,
            "start_ts_ms": start_ns // 1_000_000,
            "duration_ms": float(duration_ns / 1_000_000),
            "end_ts_ms": end_ns // 1_000_000,
            "method": None, "agent_name": None, "model": None,
            "input_tokens": None, "output_tokens": None,
            "collection": None, "query_text": None, "returned_rows": None,
            "rpc_method": None, "url": None, "status_code": None,
            "context": None, "prompt": None, "response": None,
            "ingestion_date": ingestion_date,
            "ingestion_hour": ingestion_hour,
        }

        flat = flatten_tags(span_kind, tags)
        classified_row.update(flat)
        classified_rows.append(classified_row)

    if not classified_rows:
        logger.warning(f"Batch {batch_id}: No spans classified")
        return

    # Drop REASON spans that are direct children of ENTRY
    entry_span_ids = {r["span_id"] for r in classified_rows if r["span_kind"] == "ENTRY"}
    classified_rows = [
        r for r in classified_rows
        if not (r["span_kind"] == "REASON" and r["parent_span_id"] in entry_span_ids)
    ]

    if not classified_rows:
        return

    # Delta MERGE
    updates_df = spark.createDataFrame(classified_rows, schema=AGENT_STEPS_SCHEMA)
    target_table = DeltaTable.forPath(spark, AGENT_STEPS_PATH)
    (
        target_table.alias("target")
        .merge(
            updates_df.alias("source"),
            "target.trace_id = source.trace_id AND target.session_id = source.session_id AND target.span_id = source.span_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"Batch {batch_id}: Merged {len(classified_rows)} rows into agent_steps")


def main():
    spark = create_spark_session("Stream_AgentSteps")
    spark.sparkContext.setLogLevel("WARN")

    ensure_delta_table(spark, SOURCE_PATH, SOURCE_SCHEMA)
    ensure_delta_table(spark, AGENT_STEPS_PATH, AGENT_STEPS_SCHEMA)

    stream_df = spark.readStream.format("delta").load(SOURCE_PATH)

    query = (
        stream_df.writeStream
        .foreachBatch(lambda df, bid: process_batch(spark, df, bid))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "agent_steps"))
        .queryName("stream_agent_steps")
        .start()
    )

    logger.info(f"stream_agent_steps started | source={SOURCE_PATH} | trigger={TRIGGER_INTERVAL}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
