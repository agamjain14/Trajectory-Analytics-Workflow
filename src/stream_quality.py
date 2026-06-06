"""
Stream 3: agent_steps → quality_scores

Spark Structured Streaming job.
Reads new agent_steps, builds eval context per trace, calls LLM-as-judge.
Checkpoint: data/checkpoints/quality
"""

import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable

from src.streaming_config import (
    AGENT_STEPS_PATH, QUALITY_PATH, CHECKPOINT_BASE, TRIGGER_INTERVAL,
    OLLAMA_BASE_URL, EVAL_MODEL, QUALITY_SCHEMA,
    create_spark_session, ensure_delta_table,
)

logger = logging.getLogger("stream_quality")

EVAL_PROMPT_TEMPLATE = """You are an expert evaluator. Score this agent response (1-5 scale).

DIMENSIONS:
- COMPLETENESS: Addresses all parts of the request?
- COHERENCE: Logically structured and consistent?
- HALLUCINATION: Fabricated claims not in evidence? (5=none, 1=severe)
- GROUNDEDNESS: Grounded in tool/retrieval results?
- RELEVANCE: Stays on-topic?

USER REQUEST: {user_request}
REASONING CHAIN: {reasoning_chain}
EVIDENCE: {evidence}
FINAL RESPONSE: {final_response}

Respond with JSON only:
{{"completeness": <int>, "coherence": <int>, "hallucination": <int>, "groundedness": <int>, "relevance": <int>, "overall": <float>, "explanation": "<brief>"}}
"""


def _extract_trace_context(spans) -> dict:
    user_request = ""
    reasoning_steps = []
    evidence_parts = []
    final_response = ""

    sorted_spans = sorted(spans, key=lambda s: s.start_ts_ms or 0)

    for span in sorted_spans:
        kind = span.span_kind
        if kind == "ENTRY":
            user_request = span.prompt or span.context or ""
        elif kind == "REASON":
            model = span.model or "unknown"
            prompt = span.prompt or ""
            response = span.response or ""
            reasoning_steps.append(f"[LLM:{model}]: {prompt[:200]}")
            if response:
                final_response = response
        elif kind == "RETRIEVE":
            collection = span.collection or ""
            query = span.query_text or ""
            evidence_parts.append(f"[RAG:{collection}] query='{query[:100]}'")
        elif kind == "TOOL":
            method = span.rpc_method or span.url or "unknown"
            response = span.response or ""
            evidence_parts.append(f"[Tool:{method}] → {response[:300]}")
        elif kind == "AGENT":
            agent_name = span.agent_name or "unknown"
            reasoning_steps.append(f"[Agent:{agent_name}]")

    return {
        "user_request": user_request or "(not captured)",
        "reasoning_chain": "\n".join(reasoning_steps) or "(no reasoning)",
        "evidence": "\n".join(evidence_parts) or "(no evidence)",
        "final_response": final_response or "(no response)",
    }


def _call_eval_llm(prompt: str) -> dict:
    import ollama
    try:
        client = ollama.Client(host=OLLAMA_BASE_URL)
        response = client.chat(
            model=EVAL_MODEL,
            messages=[
                {"role": "system", "content": "You are a strict JSON-only evaluator."},
                {"role": "user", "content": prompt},
            ],
            options={"temperature": 0.1},
        )
        content = response["message"]["content"]
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        scores = json.loads(content.strip())
        for key in ("completeness", "coherence", "hallucination", "groundedness", "relevance"):
            scores[key] = max(1, min(5, int(scores.get(key, 3))))
        if "overall" not in scores:
            dims = [scores[k] for k in ("completeness", "coherence", "hallucination", "groundedness", "relevance")]
            scores["overall"] = round(sum(dims) / len(dims), 2)
        else:
            scores["overall"] = round(float(scores["overall"]), 2)
        scores.setdefault("explanation", "")
        return scores
    except Exception as e:
        logger.error(f"LLM eval failed: {e}")
        return {"completeness": 0, "coherence": 0, "hallucination": 0,
                "groundedness": 0, "relevance": 0, "overall": 0.0,
                "explanation": f"eval_error: {str(e)[:100]}"}


def process_batch(spark: SparkSession, micro_batch_df: DataFrame, batch_id: int):
    if micro_batch_df.isEmpty():
        return

    ingestion_time = datetime.now(timezone.utc)
    ingestion_date = ingestion_time.strftime("%Y-%m-%d")
    ingestion_hour = ingestion_time.hour

    impacted_traces = micro_batch_df.select("trace_id").distinct().collect()
    impacted_trace_ids = [row.trace_id for row in impacted_traces]

    if not impacted_trace_ids:
        return

    # Read agent_steps for impacted traces
    agent_steps_df = (
        spark.read.format("delta").load(AGENT_STEPS_PATH)
        .filter(F.col("trace_id").isin(impacted_trace_ids))
    )
    steps_rows = agent_steps_df.collect()

    traces = defaultdict(list)
    for row in steps_rows:
        traces[row.trace_id].append(row)

    quality_rows = []
    for trace_id, spans in traces.items():
        ctx = _extract_trace_context(spans)
        prompt = EVAL_PROMPT_TEMPLATE.format(**ctx)
        scores = _call_eval_llm(prompt)

        start_times = [s.start_ts_ms for s in spans if s.start_ts_ms]
        end_times = [s.end_ts_ms for s in spans if s.end_ts_ms]
        session_id = spans[0].session_id or ""

        quality_rows.append({
            "trace_id": trace_id,
            "session_id": session_id,
            "completeness": float(scores["completeness"]),
            "coherence": float(scores["coherence"]),
            "hallucination": float(scores["hallucination"]),
            "groundedness": float(scores["groundedness"]),
            "relevance": float(scores["relevance"]),
            "overall": float(scores["overall"]),
            "explanation": scores.get("explanation", ""),
            "eval_model": EVAL_MODEL,
            "eval_timestamp_ms": int(time.time() * 1000),
            "start_ts_ms": min(start_times) if start_times else 0,
            "end_ts_ms": max(end_times) if end_times else 0,
            "ingestion_date": ingestion_date,
            "ingestion_hour": ingestion_hour,
        })

    if not quality_rows:
        return

    updates_df = spark.createDataFrame(quality_rows, schema=QUALITY_SCHEMA)
    target_table = DeltaTable.forPath(spark, QUALITY_PATH)
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
    logger.info(f"Batch {batch_id}: Evaluated {len(quality_rows)} traces")


def main():
    spark = create_spark_session("Stream_Quality")
    spark.sparkContext.setLogLevel("WARN")

    ensure_delta_table(spark, QUALITY_PATH, QUALITY_SCHEMA)

    stream_df = spark.readStream.format("delta").load(AGENT_STEPS_PATH)

    query = (
        stream_df.writeStream
        .foreachBatch(lambda df, bid: process_batch(spark, df, bid))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "quality"))
        .queryName("stream_quality")
        .start()
    )

    logger.info(f"stream_quality started | source={AGENT_STEPS_PATH} | trigger={TRIGGER_INTERVAL}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
