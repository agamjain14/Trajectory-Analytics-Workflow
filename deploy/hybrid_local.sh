#!/usr/bin/env bash
# hybrid_local.sh — run the FULL analytics pipeline LOCALLY while routing only
# LLM inference to Vast.ai. Detached/background-friendly so CI (a self-hosted
# runner on this machine) can start it and return without blocking the job.
#
# What runs locally (queryable): Docker infra (Pulsar, OTel, Jaeger, Prometheus,
# Grafana), MCP tool server, chat app, trace consumer, all 5 Spark streaming
# jobs, and Delta tables. Inference is dispatched to OLLAMA_NODES (Vast.ai).
# GPU/network metrics are collected ON Vast.ai and POSTed to this host's
# /ingest/* endpoints; in METRICS_MODE=real they land as raw JSONL that the
# Spark routing job ingests (single writer — no dual-write).
#
# Usage:
#   OLLAMA_NODES=http://<vast-ip>:11434 bash deploy/hybrid_local.sh start
#   OLLAMA_NODES="http://ip1:11434,http://ip2:11434" bash deploy/hybrid_local.sh start
#   bash deploy/hybrid_local.sh stop
#   bash deploy/hybrid_local.sh status
set -euo pipefail

cd "$(dirname "$0")/.."

ACTION="${1:-start}"
LOG_DIR="${LOG_DIR:-/tmp/trajectory-hybrid}"
PID_FILE="$LOG_DIR/pids"
mkdir -p "$LOG_DIR"

PROC_PATTERNS=(
  "uvicorn src.mcp_server:app"
  "uvicorn src.chat_server:app"
  "src.trace_consumer"
  "src.stream_agent_steps"
  "src.stream_trajectory"
  "src.stream_quality"
  "src.stream_routing_infra"
  "src.stream_correlated"
)

stop_all() {
  echo "==> Stopping hybrid local pipeline..."
  if [ -f "$PID_FILE" ]; then
    while read -r pid; do
      [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
  fi
  for pat in "${PROC_PATTERNS[@]}"; do
    pkill -f "$pat" 2>/dev/null || true
  done
  echo "==> Bringing down Docker infra..."
  docker compose down 2>/dev/null || true
  echo "==> Stopped."
}

status_all() {
  echo "=== Hybrid local processes ==="
  for pat in "${PROC_PATTERNS[@]}"; do
    if pgrep -f "$pat" >/dev/null 2>&1; then
      echo "  RUNNING  $pat"
    else
      echo "  stopped  $pat"
    fi
  done
  echo ""
  echo "=== Ingest status ==="
  curl -s http://localhost:8000/ingest/status || echo "  app not reachable on :8000"
  echo ""
}

case "$ACTION" in
  stop)   stop_all;   exit 0 ;;
  status) status_all; exit 0 ;;
  start)  ;;
  *) echo "Usage: $0 {start|stop|status}"; exit 1 ;;
esac

# ── start ──
: "${OLLAMA_NODES:?Set OLLAMA_NODES=http://<vast-ip>:11434 (Vast.ai Ollama URL[s], comma-separated for multi-node)}"

# Judge / LLM-as-judge (stream_quality) ALSO runs its inference on Vast.ai.
# The Spark quality job is orchestrated locally (it reads/writes local Delta),
# but it dispatches the eval LLM call to OLLAMA_BASE_URL using EVAL_MODEL.
# Default the judge host to the FIRST Vast.ai node in OLLAMA_NODES.
JUDGE_URL="${JUDGE_URL:-${OLLAMA_NODES%%,*}}"

export LLM_BACKEND=ollama
export OLLAMA_MODEL="${OLLAMA_MODEL:-llama3.2}"
export METRICS_MODE=real
export DEPLOY_MODE=local
export DATA_DIR=./data
export DATA_PATH=./data
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}"
export TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-30 seconds}"

# Judge runs on Vast.ai: point the eval client at the remote Ollama node.
export JUDGE_BACKEND=ollama
export OLLAMA_BASE_URL="$JUDGE_URL"
export EVAL_MODEL="${EVAL_MODEL:-llama3.2}"

echo "============================================"
echo "  HYBRID LOCAL PIPELINE"
echo "  Inference -> $OLLAMA_NODES"
echo "  Judge     -> $JUDGE_URL (EVAL_MODEL=$EVAL_MODEL)"
echo "  Metrics   -> METRICS_MODE=real (Spark owns Delta)"
echo "============================================"

# Prerequisites
command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker not found"; exit 1; }
docker info >/dev/null 2>&1 || { echo "ERROR: Docker daemon not running"; exit 1; }
java -version >/dev/null 2>&1 || { echo "ERROR: Java not found (needed for Spark)"; exit 1; }

# venv + deps
if [ -d .venv ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi
python3 -m pip install -q -r requirements.txt 2>/dev/null || true
python3 -m pip install -q -r requirements-streaming.txt 2>/dev/null || true

# Fresh PID file
: > "$PID_FILE"
record() { echo "$1" >> "$PID_FILE"; }

# Docker infrastructure
echo "[1/4] Starting Docker infrastructure..."
docker compose up -d otel-collector jaeger prometheus grafana pulsar
for i in $(seq 1 30); do
  if curl -s http://localhost:8081/admin/v2/clusters >/dev/null 2>&1; then
    echo "  Pulsar ready"; break
  fi
  sleep 1
done

# Initialize Delta tables
echo "[2/4] Initializing Delta tables..."
python3 -c "
from src.streaming_config import (
    create_spark_session, ensure_delta_table,
    SOURCE_PATH, AGENT_STEPS_PATH, TRAJECTORY_PATH, QUALITY_PATH,
    GPU_METRICS_PATH, NETWORK_METRICS_PATH, ROUTING_PATH, CORRELATED_PATH,
    SOURCE_SCHEMA, AGENT_STEPS_SCHEMA, TRAJECTORY_SCHEMA, QUALITY_SCHEMA,
    GPU_METRICS_SCHEMA, NETWORK_METRICS_SCHEMA, ROUTING_SCHEMA, CORRELATED_SCHEMA,
)
spark = create_spark_session('TableInit')
for p, s in [
    (SOURCE_PATH, SOURCE_SCHEMA), (AGENT_STEPS_PATH, AGENT_STEPS_SCHEMA),
    (TRAJECTORY_PATH, TRAJECTORY_SCHEMA), (QUALITY_PATH, QUALITY_SCHEMA),
    (GPU_METRICS_PATH, GPU_METRICS_SCHEMA), (NETWORK_METRICS_PATH, NETWORK_METRICS_SCHEMA),
    (ROUTING_PATH, ROUTING_SCHEMA), (CORRELATED_PATH, CORRELATED_SCHEMA),
]:
    ensure_delta_table(spark, p, s)
spark.stop()
print('  All Delta tables initialized')
"

# Application processes (detached)
echo "[3/4] Starting app, trace consumer, MCP server..."
nohup python3 -m uvicorn src.mcp_server:app --host 0.0.0.0 --port 8001 \
  > "$LOG_DIR/mcp.log" 2>&1 & record $!
nohup python3 -m src.trace_consumer \
  > "$LOG_DIR/trace_consumer.log" 2>&1 & record $!
nohup python3 -m uvicorn src.chat_server:app --host 0.0.0.0 --port 8000 \
  > "$LOG_DIR/app.log" 2>&1 & record $!
sleep 3

# Spark streaming jobs (detached)
echo "[4/4] Starting 5 Spark streaming jobs..."
for job in stream_agent_steps stream_trajectory stream_quality stream_routing_infra stream_correlated; do
  nohup python3 -m "src.$job" > "$LOG_DIR/$job.log" 2>&1 & record $!
done

echo ""
echo "============================================"
echo "  HYBRID LOCAL PIPELINE RUNNING (detached)"
echo "============================================"
echo "  Chat:      http://localhost:8000/static/index.html"
echo "  Analytics: http://localhost:8000/static/analytics.html"
echo "  Status:    http://localhost:8000/ingest/status"
echo "  Logs:      $LOG_DIR/"
echo "  PIDs:      $PID_FILE"
echo ""
echo "  On the Vast.ai node, run the collector role with:"
echo "    INGEST_URL=http://<this-host-reachable-url>:8000 \\"
echo "    NODE_ID=node-1 ROLE=collector bash deploy/vastai_setup.sh"
echo ""
echo "  Stop with: bash deploy/hybrid_local.sh stop"
echo "============================================"
