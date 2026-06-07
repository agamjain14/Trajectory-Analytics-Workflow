#!/usr/bin/env bash
# run_local.sh — Run the FULL pipeline locally.
# Uses: Ollama (local LLM) + Docker (Pulsar, OTel, Jaeger) + synthetic metrics
#
# What happens:
#   1. Checks Ollama is running
#   2. Starts docker-compose (Pulsar + OTel + Jaeger + Prometheus + Grafana)
#   3. Starts the app (FastAPI) which auto-generates synthetic GPU data
#   4. You chat → traces flow through Pulsar → appear in Jaeger → analytics updates live
#
# Prerequisites:
#   - Docker Desktop running
#   - Ollama installed and running (ollama serve)
#   - Python 3.11+ with venv activated
#
# Usage: bash run_local.sh
set -euo pipefail

cd "$(dirname "$0")"

echo "============================================"
echo "  Trajectory Analytics — LOCAL MODE"
echo "============================================"
echo ""

# --- Check prerequisites ---
echo "[1/4] Checking prerequisites..."

if ! command -v docker &>/dev/null; then
    echo "ERROR: Docker not found. Install Docker Desktop."; exit 1
fi
if ! docker info &>/dev/null 2>&1; then
    echo "ERROR: Docker daemon not running. Start Docker Desktop."; exit 1
fi
if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found"; exit 1
fi

# Check Ollama
if ! curl -s http://localhost:11434/api/tags >/dev/null 2>&1; then
    echo "  Ollama not running. Starting..."
    if command -v ollama &>/dev/null; then
        ollama serve &
        sleep 3
    else
        echo "ERROR: Ollama not installed. Get it: https://ollama.com/download"; exit 1
    fi
fi

# Pull model if needed
if ! ollama list 2>/dev/null | grep -q "llama3.2"; then
    echo "  Pulling llama3.2..."
    ollama pull llama3.2
fi

echo "  ✓ Docker, Python, Ollama ready"

# --- Install Python deps ---
echo ""
echo "[2/4] Installing Python dependencies..."

# Activate venv if available
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

python3 -m pip install -q -r requirements.txt 2>/dev/null

# --- Start infrastructure (Pulsar + OTel + Jaeger + Prometheus + Grafana) ---
echo ""
echo "[3/4] Starting infrastructure (docker-compose)..."

# Set local env
export LLM_BACKEND=ollama
export DEPLOY_MODE=local
export OLLAMA_BASE_URL=http://host.docker.internal:11434

# Start only infrastructure services (not the app — it runs natively below)
docker compose up -d otel-collector jaeger prometheus grafana pulsar
echo "  ✓ Infrastructure running"
echo "    Waiting for Pulsar to be ready..."
sleep 10

# --- Start the application ---
echo ""
echo "[4/4] Starting application..."
echo ""
echo "============================================"
echo "  ALL SERVICES RUNNING"
echo "============================================"
echo ""
echo "  Chat + Dashboard:  http://localhost:8000/static/index.html"
echo "  Analytics:         http://localhost:8000/static/analytics.html"
echo "  Topology:          http://localhost:8000/static/topology.html"
echo "  API Docs:          http://localhost:8000/docs"
echo "  Data Source:       http://localhost:8000/ingest/status"
echo ""
echo "  Jaeger (traces):   http://localhost:16686"
echo "  Prometheus:        http://localhost:9090"
echo "  Grafana:           http://localhost:3000  (admin/admin)"
echo "  Pulsar Admin:      http://localhost:8081"
echo ""
echo "  GPU/Network data: SYNTHETIC (auto-generated every 5s)"
echo "  LLM: Ollama (llama3.2 local)"
echo ""
echo "  Press Ctrl+C to stop app. Run 'docker compose down' to stop infra."
echo "============================================"
echo ""

# Run the app natively (not in Docker) so it can reach local Ollama
export LLM_BACKEND=ollama
export OLLAMA_BASE_URL=http://localhost:11434
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export DATA_DIR=./data

# Start MCP tool server (search_flights, search_hotels, etc.) in background
echo "  Starting MCP tool server on :8001..."
uvicorn src.mcp_server:app --host 0.0.0.0 --port 8001 &
MCP_PID=$!
trap "kill $MCP_PID 2>/dev/null" EXIT

uvicorn src.chat_server:app --host 0.0.0.0 --port 8000 --reload
