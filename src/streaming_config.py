"""
Shared configuration and utilities for streaming jobs.
Each streaming job is independent — this module provides common constants,
Spark session creation, Delta table initialization, and span classification.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType
)
from delta import DeltaTable, configure_spark_with_delta_pip

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

# --- Paths ---
BASE_DATA_PATH = os.getenv("DATA_PATH", "./data")
SOURCE_PATH = os.path.join(BASE_DATA_PATH, "trace_delta_table")
AGENT_STEPS_PATH = os.path.join(BASE_DATA_PATH, "agent_steps")
TRAJECTORY_PATH = os.path.join(BASE_DATA_PATH, "trajectory_templates")
QUALITY_PATH = os.path.join(BASE_DATA_PATH, "quality_scores")
GPU_METRICS_PATH = os.path.join(BASE_DATA_PATH, "gpu_metrics")
NETWORK_METRICS_PATH = os.path.join(BASE_DATA_PATH, "network_metrics")
ROUTING_PATH = os.path.join(BASE_DATA_PATH, "request_routing")
CORRELATED_PATH = os.path.join(BASE_DATA_PATH, "trace_correlated")
CHECKPOINT_BASE = os.path.join(BASE_DATA_PATH, "checkpoints")

# --- Streaming config ---
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 minutes")
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "1"))

# --- LLM config ---
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
EVAL_MODEL = os.getenv("EVAL_MODEL", "llama3.2")

# --- Judge LLM (stronger model for quality evaluation) ---
JUDGE_BACKEND = os.getenv("JUDGE_BACKEND", "ollama")  # "ollama" or "azure"
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")

# --- Cluster Topology (discovered at runtime from collectors) ---
NODE_1_URL = os.getenv("NODE_1_URL", "http://localhost:11434")
NODE_2_URL = os.getenv("NODE_2_URL", "http://localhost:11434")
NODE_1_ID = os.getenv("NODE_1_ID", "node-1")
NODE_2_ID = os.getenv("NODE_2_ID", "node-2")

TOPOLOGY = {
    "cluster": "gpu-cluster",
    "nodes": [
        {"id": NODE_1_ID, "url": NODE_1_URL},
        {"id": NODE_2_ID, "url": NODE_2_URL},
    ],
}

# --- Schemas ---
AGENT_STEPS_SCHEMA = StructType([
    StructField("trace_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("span_id", StringType(), False),
    StructField("parent_span_id", StringType(), True),
    StructField("span_kind", StringType(), True),
    StructField("sub_span_kind", StringType(), True),
    StructField("start_ts_ms", LongType(), True),
    StructField("duration_ms", DoubleType(), True),
    StructField("end_ts_ms", LongType(), True),
    StructField("method", StringType(), True),
    StructField("agent_name", StringType(), True),
    StructField("model", StringType(), True),
    StructField("input_tokens", IntegerType(), True),
    StructField("output_tokens", IntegerType(), True),
    StructField("collection", StringType(), True),
    StructField("query_text", StringType(), True),
    StructField("returned_rows", IntegerType(), True),
    StructField("rpc_method", StringType(), True),
    StructField("url", StringType(), True),
    StructField("status_code", StringType(), True),
    StructField("context", StringType(), True),
    StructField("prompt", StringType(), True),
    StructField("response", StringType(), True),
    StructField("ingestion_date", StringType(), False),
    StructField("ingestion_hour", IntegerType(), False),
])

TRAJECTORY_SCHEMA = StructType([
    StructField("trace_id", StringType()), StructField("session_id", StringType()),
    StructField("trajectory_signature", StringType()),
    StructField("step_sequence", StringType()), StructField("step_sequence_detailed", StringType()),
    StructField("step_count", IntegerType()), StructField("llm_call_count", IntegerType()),
    StructField("tool_call_count", IntegerType()), StructField("retrieve_count", IntegerType()),
    StructField("agent_count", IntegerType()), StructField("retry_count", IntegerType()),
    StructField("error_count", IntegerType()), StructField("total_duration_ms", DoubleType()),
    StructField("start_ts_ms", LongType()), StructField("end_ts_ms", LongType()),
    StructField("ingestion_date", StringType()), StructField("ingestion_hour", IntegerType()),
])

QUALITY_SCHEMA = StructType([
    StructField("trace_id", StringType()), StructField("session_id", StringType()),
    StructField("completeness", DoubleType()), StructField("coherence", DoubleType()),
    StructField("hallucination", DoubleType()), StructField("groundedness", DoubleType()),
    StructField("relevance", DoubleType()), StructField("overall", DoubleType()),
    StructField("explanation", StringType()), StructField("eval_model", StringType()),
    StructField("eval_timestamp_ms", LongType()),
    StructField("start_ts_ms", LongType()), StructField("end_ts_ms", LongType()),
    StructField("ingestion_date", StringType()), StructField("ingestion_hour", IntegerType()),
])

GPU_METRICS_SCHEMA = StructType([
    StructField("timestamp_ms", LongType()), StructField("node_id", StringType()),
    StructField("gpu_id", StringType()), StructField("gpu_uuid", StringType()),
    StructField("gpu_utilization", DoubleType()),
    StructField("memory_controller_util", DoubleType()),
    StructField("memory_used_pct", DoubleType()),
    StructField("temperature_c", DoubleType()),
    StructField("power_draw_pct", DoubleType()),
    StructField("clock_sm_mhz", DoubleType()),
    StructField("clock_mem_mhz", DoubleType()),
    StructField("throttle_active", IntegerType()),
    StructField("pcie_tx_mbps", DoubleType()),
    StructField("pcie_rx_mbps", DoubleType()),
    StructField("ecc_errors_total", IntegerType()),
    StructField("contention_index", DoubleType()),
    StructField("ingestion_date", StringType()), StructField("ingestion_hour", IntegerType()),
])

NETWORK_METRICS_SCHEMA = StructType([
    StructField("timestamp_ms", LongType()), StructField("node_id", StringType()),
    StructField("inter_node_latency_us", DoubleType()),
    StructField("throughput_tx_mbps", DoubleType()),
    StructField("throughput_rx_mbps", DoubleType()),
    StructField("packet_drop_count", IntegerType()),
    StructField("tcp_retransmit_count", IntegerType()),
    StructField("ingestion_date", StringType()), StructField("ingestion_hour", IntegerType()),
])

ROUTING_SCHEMA = StructType([
    StructField("trace_id", StringType()), StructField("span_id", StringType()),
    StructField("vllm_pod_id", StringType()), StructField("node_id", StringType()),
    StructField("gpu_id", StringType()), StructField("gpu_uuid", StringType()),
    StructField("queue_wait_ms", DoubleType()),
    StructField("timestamp_ms", LongType()),
    StructField("ingestion_date", StringType()), StructField("ingestion_hour", IntegerType()),
])

CORRELATED_SCHEMA = StructType([
    StructField("trace_id", StringType()), StructField("session_id", StringType()),
    StructField("start_ts_ms", LongType()), StructField("end_ts_ms", LongType()),
    StructField("total_duration_ms", DoubleType()),
    StructField("trajectory_signature", StringType()), StructField("step_sequence", StringType()),
    StructField("step_count", IntegerType()), StructField("llm_call_count", IntegerType()),
    StructField("tool_call_count", IntegerType()), StructField("retrieve_count", IntegerType()),
    StructField("retry_count", IntegerType()),
    StructField("quality_overall", DoubleType()), StructField("quality_completeness", DoubleType()),
    StructField("quality_coherence", DoubleType()), StructField("quality_hallucination", DoubleType()),
    StructField("quality_groundedness", DoubleType()), StructField("quality_relevance", DoubleType()),
    StructField("quality_explanation", StringType()),
    StructField("gpu_contention_avg", DoubleType()), StructField("gpu_contention_max", DoubleType()),
    StructField("gpu_temperature_avg", DoubleType()), StructField("gpu_temperature_max", DoubleType()),
    StructField("gpu_memory_pressure_avg", DoubleType()),
    StructField("gpu_throttle_count", IntegerType()),
    StructField("gpu_power_avg", DoubleType()),
    StructField("inter_node_latency_avg", DoubleType()),
    StructField("inter_node_latency_max", DoubleType()),
    StructField("packet_drop_total", IntegerType()),
    StructField("tcp_retransmit_total", IntegerType()),
    StructField("queue_wait_avg", DoubleType()), StructField("queue_wait_max", DoubleType()),
    StructField("primary_pod_id", StringType()), StructField("primary_node_id", StringType()),
    StructField("primary_gpu_id", StringType()),
    StructField("ingestion_date", StringType()), StructField("ingestion_hour", IntegerType()),
])


# --- Spark session factory ---
def create_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .master("local[*]")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ensure_delta_table(spark: SparkSession, path: str, schema: StructType):
    """Create an empty partitioned Delta table if it doesn't exist."""
    if not DeltaTable.isDeltaTable(spark, path):
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").partitionBy("ingestion_date", "ingestion_hour").save(path)


# --- Span classification utilities ---
DROP_OPERATIONS = {
    "session.resolve", "session.save_user_message", "session.load_context",
    "session.merge_context", "chat.conversational",
    "get_or_create_collection knowledge_base",
}
DROP_PREFIXES = ["session.turn.", "GET /api/sessions", "POST /api/sessions"]


def should_drop(operation_name: str) -> bool:
    if operation_name in DROP_OPERATIONS:
        return True
    for prefix in DROP_PREFIXES:
        if operation_name.startswith(prefix):
            return True
    return False


def classify_span(tags: dict) -> Optional[tuple]:
    if "http.route" in tags:
        return ("ENTRY", tags["http.route"])
    if tags.get("orchestration.type") == "plan_execution":
        return ("PLAN", "plan_execution")
    if "agent.name" in tags:
        return ("AGENT", tags.get("agent.operation", "unknown"))
    if "gen_ai.system" in tags:
        return ("REASON", tags.get("gen_ai.operation.name", "unknown"))
    if "db.system" in tags:
        return ("RETRIEVE", tags.get("db.operation.name", "unknown"))
    if "rpc.system" in tags:
        return ("TOOL", "MCP")
    if "url.full" in tags and "http.request.method" in tags:
        return ("TOOL", "HTTP")
    return None


def safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def flatten_tags(span_kind: str, tags: dict) -> dict:
    flat = {}
    if span_kind == "ENTRY":
        flat["method"] = tags.get("http.request.method")
    elif span_kind == "AGENT":
        flat["agent_name"] = tags.get("agent.name")
        params = {k.replace("agent.parameter.", ""): v for k, v in tags.items() if k.startswith("agent.parameter.")}
        if params:
            flat["context"] = json.dumps(params)
    elif span_kind == "REASON":
        flat["model"] = tags.get("gen_ai.request.model")
        flat["input_tokens"] = safe_int(tags.get("gen_ai.usage.input_tokens"))
        flat["output_tokens"] = safe_int(tags.get("gen_ai.usage.output_tokens"))
        flat["context"] = tags.get("gen_ai.prompt.system")
        flat["prompt"] = tags.get("gen_ai.prompt.user")
        flat["response"] = tags.get("gen_ai.response.content")
    elif span_kind == "RETRIEVE":
        flat["collection"] = tags.get("db.collection.name")
        flat["query_text"] = tags.get("db.query.text")
        flat["returned_rows"] = safe_int(tags.get("db.response.returned_rows"))
        flat["prompt"] = tags.get("db.query.text")
    elif span_kind == "TOOL":
        flat["rpc_method"] = tags.get("rpc.method")
        flat["url"] = tags.get("url.full")
        flat["status_code"] = tags.get("http.response.status_code")
        params = {k.replace("tool.parameter.", ""): v for k, v in tags.items() if k.startswith("tool.parameter.")}
        if params:
            flat["context"] = json.dumps(params)
        flat["response"] = tags.get("tool.response")
    return flat
