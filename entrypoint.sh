#!/bin/bash
set -e

MODE="${DEPLOY_MODE:-local}"
PORT="${PORT:-8000}"

echo "==> Trajectory Analytics Workflow"
echo "    Mode: $MODE | LLM: $LLM_BACKEND | Port: $PORT"

case "$MODE" in
  local)
    # Local mode: run chat server (includes analytics endpoints)
    echo "==> Starting MCP Tool Server on port 8001..."
    python3 -m src.mcp_server &
    echo "==> Starting Trace Consumer (Pulsar → Delta Lake)..."
    python3 -m src.trace_consumer &
    sleep 1
    echo "==> Starting Chat Server + Analytics API (local mode)"
    exec uvicorn src.chat_server:app --host 0.0.0.0 --port "$PORT"
    ;;
  cloud)
    # Cloud mode: run chat server with Azure OpenAI backend
    echo "==> Starting Chat Server + Analytics API (cloud mode)"
    exec uvicorn src.chat_server:app --host 0.0.0.0 --port "$PORT"
    ;;
  analytics-only)
    # Lightweight mode: only analytics API (no chat, no LLM)
    echo "==> Starting Analytics API only"
    exec uvicorn src.analytics_api:app --host 0.0.0.0 --port "$PORT"
    ;;
  collector)
    # Collector mode: run GPU + network collectors (for Vast.ai nodes)
    echo "==> Starting GPU + Network collectors"
    python3 -m src.gpu_collector &
    python3 -m src.network_collector
    ;;
  streaming)
    # Pre-download Delta/Spark Ivy dependencies with a single session
    # to avoid race conditions when 5 sessions start concurrently
    echo "==> Pre-downloading Spark/Delta dependencies..."
    python3 -c "
from src.streaming_config import (
    create_spark_session, ensure_delta_table,
    SOURCE_PATH, AGENT_STEPS_PATH, TRAJECTORY_PATH, QUALITY_PATH,
    GPU_METRICS_PATH, NETWORK_METRICS_PATH, ROUTING_PATH, CORRELATED_PATH,
    SOURCE_SCHEMA, AGENT_STEPS_SCHEMA, TRAJECTORY_SCHEMA, QUALITY_SCHEMA,
    GPU_METRICS_SCHEMA, NETWORK_METRICS_SCHEMA, ROUTING_SCHEMA, CORRELATED_SCHEMA,
)
spark = create_spark_session('TableInit')
ensure_delta_table(spark, SOURCE_PATH, SOURCE_SCHEMA)
ensure_delta_table(spark, AGENT_STEPS_PATH, AGENT_STEPS_SCHEMA)
ensure_delta_table(spark, TRAJECTORY_PATH, TRAJECTORY_SCHEMA)
ensure_delta_table(spark, QUALITY_PATH, QUALITY_SCHEMA)
ensure_delta_table(spark, GPU_METRICS_PATH, GPU_METRICS_SCHEMA)
ensure_delta_table(spark, NETWORK_METRICS_PATH, NETWORK_METRICS_SCHEMA)
ensure_delta_table(spark, ROUTING_PATH, ROUTING_SCHEMA)
ensure_delta_table(spark, CORRELATED_PATH, CORRELATED_SCHEMA)
spark.stop()
print('All Delta tables initialized')
"
    echo "==> Starting streaming ETL jobs (all 5 concurrently)..."
    python3 -m src.stream_agent_steps &
    python3 -m src.stream_trajectory &
    python3 -m src.stream_quality &
    python3 -m src.stream_routing_infra &
    python3 -m src.stream_correlated &
    echo "==> All 5 streaming jobs launched"
    # Wait for any to exit (crash = restart via Docker)
    wait -n
    echo "==> A streaming job exited, container stopping"
    ;;
  *)
    echo "Unknown DEPLOY_MODE: $MODE"
    echo "Valid modes: local, cloud, analytics-only, collector, streaming"
    exit 1
    ;;
esac
