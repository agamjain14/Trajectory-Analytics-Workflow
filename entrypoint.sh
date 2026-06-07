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
    # Long-running: start all streaming jobs concurrently
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
