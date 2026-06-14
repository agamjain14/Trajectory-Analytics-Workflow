#!/usr/bin/env bash
# run_local.sh — ONE command to run the COMPLETE pipeline locally.
#
# Starts EVERYTHING:
#   1. Docker infra   — Pulsar, OTel Collector, Jaeger, Prometheus, Grafana
#   2. Ollama         — local LLM (llama3.2)
#   3. MCP server     — tool server (search_flights, search_hotels, etc.) on :8001
#   4. Chat server    — FastAPI app on :8000 (includes synthetic metrics via live_metrics)
#   5. Trace consumer — Pulsar → Delta Lake bridge
#   6. Delta tables   — pre-initialized for all streaming jobs
#   7. Spark streaming — 5 structured streaming ETL jobs (agent_steps, trajectory, quality, routing, correlated)
#
# Prerequisites:
#   - Docker Desktop running
#   - Ollama installed (ollama serve)
#   - Python 3.11+ with .venv
#   - Java 17+ (for Spark)
#
# Usage: bash run_local.sh
#
# Ctrl+C stops everything cleanly.
set -euo pipefail

cd "$(dirname "$0")"

# ── Collect background PIDs for cleanup ──
PIDS=()
cleanup() {
    echo ""
    echo "==> Shutting down all services..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    echo "==> All services stopped. Docker infra still running (use 'docker compose down' to stop)."
}
trap cleanup EXIT INT TERM

echo "============================================"
echo "  Trajectory Analytics — FULL LOCAL PIPELINE"
echo "============================================"
echo ""

# ── [1/7] Prerequisites ──
echo "[1/7] Checking prerequisites..."

if ! command -v docker &>/dev/null; then
    echo "ERROR: Docker not found. Install Docker Desktop."; exit 1
fi
if ! docker info &>/dev/null 2>&1; then
    echo "ERROR: Docker daemon not running. Start Docker Desktop."; exit 1
fi
if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found"; exit 1
fi
if ! java -version &>/dev/null 2>&1; then
    echo "ERROR: Java not found. Install: brew install openjdk"; exit 1
fi

# Check/start Ollama
if ! curl -s http://localhost:11434/api/tags >/dev/null 2>&1; then
    echo "  Ollama not running. Starting..."
    if command -v ollama &>/dev/null; then
        ollama serve &disown
        sleep 3
    else
        echo "ERROR: Ollama not installed. Get it: https://ollama.com/download"; exit 1
    fi
fi
if ! ollama list 2>/dev/null | grep -q "llama3.2"; then
    echo "  Pulling llama3.2..."
    ollama pull llama3.2
fi
echo "  ✓ Docker, Python, Java, Ollama ready"

# ── [2/7] Python deps ──
echo ""
echo "[2/7] Installing Python dependencies..."
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi
python3 -m pip install -q -r requirements.txt 2>/dev/null
python3 -m pip install -q -r requirements-streaming.txt 2>/dev/null
echo "  ✓ Core + streaming deps installed"

# ── [3/7] Docker infrastructure ──
echo ""
echo "[3/7] Starting Docker infrastructure..."
docker compose up -d otel-collector jaeger prometheus grafana pulsar
echo "  ✓ Pulsar, OTel Collector, Jaeger, Prometheus, Grafana"
echo "  Waiting for Pulsar to be ready..."
for i in $(seq 1 30); do
    if curl -s http://localhost:8081/admin/v2/clusters >/dev/null 2>&1; then
        echo "  ✓ Pulsar ready"
        break
    fi
    sleep 1
done

# ── Environment ──
export LLM_BACKEND=ollama
export DEPLOY_MODE=local
export OLLAMA_BASE_URL=http://localhost:11434
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export DATA_DIR=./data
export DATA_PATH=./data
export TRIGGER_INTERVAL="30 seconds"

# ── [4/7] MCP tool server ──
echo ""
echo "[4/7] Starting MCP tool server on :8001..."
python3 -m uvicorn src.mcp_server:app --host 0.0.0.0 --port 8001 &
PIDS+=($!)
sleep 2
echo "  ✓ MCP server running"

# ── [5/7] Chat server + Trace consumer ──
echo ""
echo "[5/7] Starting chat server on :8000 + trace consumer..."
python3 -m src.trace_consumer &
PIDS+=($!)
python3 -m uvicorn src.chat_server:app --host 0.0.0.0 --port 8000 &
PIDS+=($!)
sleep 3
echo "  ✓ Chat server + trace consumer running"

# ── [6/7] Initialize Delta tables ──
echo ""
echo "[6/7] Initializing Delta tables for Spark streaming..."
python3 -c "
from src.streaming_config import (
    create_spark_session, ensure_delta_table,
    SOURCE_PATH, AGENT_STEPS_PATH, TRAJECTORY_PATH, QUALITY_PATH,
    GPU_METRICS_PATH, NETWORK_METRICS_PATH, ROUTING_PATH, CORRELATED_PATH, ANALYTICS_WINDOWS_PATH,
    SOURCE_SCHEMA, AGENT_STEPS_SCHEMA, TRAJECTORY_SCHEMA, QUALITY_SCHEMA,
    GPU_METRICS_SCHEMA, NETWORK_METRICS_SCHEMA, ROUTING_SCHEMA, CORRELATED_SCHEMA, ANALYTICS_WINDOWS_SCHEMA,
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
ensure_delta_table(spark, ANALYTICS_WINDOWS_PATH, ANALYTICS_WINDOWS_SCHEMA)
spark.stop()
print('  ✓ All 9 Delta tables initialized')
"

# ── [7/7] Spark Streaming jobs ──
echo ""
echo "[7/7] Starting 6 Spark streaming jobs..."
python3 -m src.stream_agent_steps &
PIDS+=($!)
python3 -m src.stream_trajectory &
PIDS+=($!)
python3 -m src.stream_quality &
PIDS+=($!)
python3 -m src.stream_routing_infra &
PIDS+=($!)
python3 -m src.stream_correlated &
PIDS+=($!)
python3 -m src.stream_windows &
PIDS+=($!)
sleep 5
echo "  ✓ All streaming jobs launched"

echo ""
echo "============================================"
echo "  FULL PIPELINE RUNNING"
echo "============================================"
echo ""
echo "  App:"
echo "    Chat UI:         http://localhost:8000/static/index.html"
echo "    Analytics:       http://localhost:8000/static/analytics.html"
echo "    Topology:        http://localhost:8000/static/topology.html"
echo "    API Docs:        http://localhost:8000/docs"
echo "    MCP Tools:       http://localhost:8001/docs"
echo ""
echo "  Infra:"
echo "    Jaeger (traces): http://localhost:16686"
echo "    Prometheus:      http://localhost:9090"
echo "    Grafana:         http://localhost:3000  (admin/admin)"
echo "    Pulsar Admin:    http://localhost:8081"
echo ""
echo "  Data Pipeline:"
echo "    Traces:   Chat → OTel → Pulsar → trace_consumer → Delta (trace_delta_table)"
echo "    Spark:    5 streaming jobs reading trace_delta_table → analytics Delta tables"
echo "    Metrics:  Synthetic GPU/network data via live_metrics (every 5s)"
echo "    LLM:      Ollama (llama3.2 local)"
echo ""
echo "  Delta tables:  ./data/{trace_delta_table,agent_steps,trajectory_templates,"
echo "                  quality_scores,gpu_metrics,network_metrics,request_routing,trace_correlated}"
echo ""
echo "  Press Ctrl+C to stop all services."
echo "============================================"
echo ""

# Wait for any child to exit — keeps the script alive
wait
