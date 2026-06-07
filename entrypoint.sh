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
    # One-shot: run all streaming jobs then exit
    echo "==> Running streaming ETL jobs"
    python3 -m src.stream_routing_infra
    python3 -m src.stream_correlated
    python3 -m src.stream_trajectory
    python3 -m src.stream_quality
    echo "==> Streaming jobs complete"
    ;;
  *)
    echo "Unknown DEPLOY_MODE: $MODE"
    echo "Valid modes: local, cloud, analytics-only, collector, streaming"
    exit 1
    ;;
esac
