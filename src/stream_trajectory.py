"""
Stream 2: agent_steps → trajectory_templates

Spark Structured Streaming job.
Reads new agent_steps, groups by trace, computes trajectory signature.
Checkpoint: data/checkpoints/trajectory
"""

import hashlib
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable

from src.streaming_config import (
    AGENT_STEPS_PATH, TRAJECTORY_PATH, CHECKPOINT_BASE, TRIGGER_INTERVAL,
    AGENT_STEPS_SCHEMA, TRAJECTORY_SCHEMA, create_spark_session, ensure_delta_table,
)

logger = logging.getLogger("stream_trajectory")


def compute_trajectory_signature(step_sequence: str) -> str:
    digest = hashlib.sha256(step_sequence.encode()).hexdigest()[:12]
    return f"tpl_{digest}"


def process_batch(spark: SparkSession, micro_batch_df: DataFrame, batch_id: int):
    if micro_batch_df.isEmpty():
        return

    ingestion_time = datetime.now(timezone.utc)
    ingestion_date = ingestion_time.strftime("%Y-%m-%d")
    ingestion_hour = ingestion_time.hour

    # Get impacted trace_ids from new agent_steps rows
    impacted_traces = micro_batch_df.select("trace_id").distinct().collect()
    impacted_trace_ids = [row.trace_id for row in impacted_traces]

    if not impacted_trace_ids:
        return

    # Read ALL agent_steps for these traces (full picture)
    agent_steps_df = (
        spark.read.format("delta").load(AGENT_STEPS_PATH)
        .filter(F.col("trace_id").isin(impacted_trace_ids))
    )
    steps_rows = agent_steps_df.collect()

    # Group by trace and compute trajectory
    traces = defaultdict(list)
    for row in steps_rows:
        traces[row.trace_id].append(row)

    traj_rows = []
    for trace_id, spans in traces.items():
        spans_sorted = sorted(spans, key=lambda s: s.start_ts_ms or 0)
        step_kinds = [s.span_kind for s in spans_sorted]
        step_detailed = [f"{s.span_kind}:{s.sub_span_kind}" for s in spans_sorted]

        step_sequence = "|".join(step_kinds)
        step_sequence_detailed = "|".join(step_detailed)
        signature = compute_trajectory_signature(step_sequence)

        llm_calls = sum(1 for s in spans_sorted if s.span_kind == "REASON")
        tool_calls = sum(1 for s in spans_sorted if s.span_kind == "TOOL")
        retrieves = sum(1 for s in spans_sorted if s.span_kind == "RETRIEVE")
        agents = sum(1 for s in spans_sorted if s.span_kind == "AGENT")
        retry_count = max(0, llm_calls - agents - 1)

        start_times = [s.start_ts_ms for s in spans_sorted if s.start_ts_ms]
        end_times = [s.end_ts_ms for s in spans_sorted if s.end_ts_ms]
        start_ts = min(start_times) if start_times else 0
        end_ts = max(end_times) if end_times else 0

        session_id = spans_sorted[0].session_id or ""

        traj_rows.append({
            "trace_id": trace_id,
            "session_id": session_id,
            "trajectory_signature": signature,
            "step_sequence": step_sequence,
            "step_sequence_detailed": step_sequence_detailed,
            "step_count": len(spans_sorted),
            "llm_call_count": llm_calls,
            "tool_call_count": tool_calls,
            "retrieve_count": retrieves,
            "agent_count": agents,
            "retry_count": retry_count,
            "error_count": 0,
            "total_duration_ms": float(end_ts - start_ts),
            "start_ts_ms": start_ts,
            "end_ts_ms": end_ts,
            "ingestion_date": ingestion_date,
            "ingestion_hour": ingestion_hour,
        })

    if not traj_rows:
        return

    updates_df = spark.createDataFrame(traj_rows, schema=TRAJECTORY_SCHEMA)
    target_table = DeltaTable.forPath(spark, TRAJECTORY_PATH)
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
    logger.info(f"Batch {batch_id}: Merged {len(traj_rows)} trajectories")


def main():
    spark = create_spark_session("Stream_Trajectory")
    spark.sparkContext.setLogLevel("WARN")

    ensure_delta_table(spark, AGENT_STEPS_PATH, AGENT_STEPS_SCHEMA)
    ensure_delta_table(spark, TRAJECTORY_PATH, TRAJECTORY_SCHEMA)

    stream_df = (
        spark.readStream.format("delta")
        .option("skipChangeCommits", "true")
        .load(AGENT_STEPS_PATH)
    )

    query = (
        stream_df.writeStream
        .foreachBatch(lambda df, bid: process_batch(spark, df, bid))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "trajectory"))
        .queryName("stream_trajectory")
        .start()
    )

    logger.info(f"stream_trajectory started | source={AGENT_STEPS_PATH} | trigger={TRIGGER_INTERVAL}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
