---
description: Scaffold a new Spark Structured Streaming job
agent: insert
---

# New streaming job: {{name}}

Create `src/stream_{{name}}.py` following existing job patterns.

**Structure:**
1. Module docstring: source → sink, checkpoint path.
2. Import from `streaming_config`: `SOURCE_PATH`, target path constant, `CHECKPOINT_BASE`, `TRIGGER_INTERVAL`, `LOOKBACK_HOURS`, relevant schema, `create_spark_session`, `ensure_delta_table`.
3. `process_batch(spark, micro_batch_df, batch_id)` — core logic per micro-batch.
4. `run()` — entry point: create Spark session, ensure target Delta table, start `readStream` → `foreachBatch` → `awaitTermination`.
5. `if __name__ == "__main__": run()`.

**Conventions:**
- Source: `delta` format from `SOURCE_PATH` (or another Delta table).
- Sink: Delta MERGE into target path, partitioned by `ingestion_date` / `ingestion_hour`.
- Checkpoint: `data/checkpoints/{{name}}/`.
- Trigger: `availableNow=True` with `TRIGGER_INTERVAL`.
- Lookback: re-read impacted `trace_id`s using `LOOKBACK_HOURS`.
- Skip empty batches early.
- Add the target path constant and schema to `streaming_config.py`.
- Logger: `logging.getLogger("stream_{{name}}")`.
