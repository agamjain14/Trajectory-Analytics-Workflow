#!/usr/bin/env bash
# Setup script for Vast.ai GPU nodes.
# Supports two roles:
#   primary   — runs App + Observability + Streaming + Ollama + Collectors
#   collector — runs only Ollama + Collectors (pushes metrics to primary)
#
# Usage (primary node):
#   ROLE=primary NODE_ID=node-1 NODE_2_IP=174.116.164.194 bash deploy/vastai_setup.sh
#
# Usage (collector node):
#   ROLE=collector NODE_ID=node-2 INGEST_URL=http://<node1-ip>:8000 PEER_IP=<node1-ip> bash deploy/vastai_setup.sh
#
# Optional (Azure Arc):
#   AZURE_TENANT_ID=<tenant> AZURE_SUBSCRIPTION_ID=<sub>
set -euo pipefail

ROLE="${ROLE:-collector}"
NODE_ID="${NODE_ID:?Set NODE_ID (e.g. node-1)}"
PEER_IP="${PEER_IP:-}"
GITHUB_REPO="${GITHUB_REPO:-https://github.com/YOUR_USER/Trajectory-Analytics-Workflow.git}"

echo "==> Setting up $ROLE node: $NODE_ID"

# --- Common setup ---
apt-get update && apt-get install -y python3-pip python3-venv iputils-ping git curl wget

if [ ! -d /workspace/trajectory ]; then
  git clone "$GITHUB_REPO" /workspace/trajectory
fi
cd /workspace/trajectory

# Install Ollama
if ! command -v ollama &>/dev/null; then
  curl -fsSL https://ollama.com/install.sh | sh
fi
export OLLAMA_HOST=0.0.0.0:11434
ollama serve &
sleep 5
ollama pull llama3.2

if [ "$ROLE" = "primary" ]; then
  # --- Primary: full stack ---
  NODE_2_IP="${NODE_2_IP:-}"

  pip install -r requirements.txt

  # Start observability (needs docker)
  if command -v docker &>/dev/null; then
    echo "==> Starting observability stack (Docker)..."
    docker compose -f docker-compose.yml up -d otel-collector jaeger prometheus grafana pulsar 2>/dev/null || \
      echo "  ⚠ Docker compose failed — start observability manually"
  else
    echo "  ⚠ Docker not available — install docker.io for observability stack"
  fi

  # Start app
  export LLM_BACKEND=ollama
  export OLLAMA_BASE_URL=http://localhost:11434
  export OLLAMA_NODES="http://localhost:11434,http://${NODE_2_IP}:11434"
  export OLLAMA_MODEL=llama3.2
  export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
  export DATA_DIR=./data
  export DEPLOY_MODE=local
  export NODE_1_URL=http://localhost:11434
  export NODE_2_URL="http://${NODE_2_IP}:11434"
  export NODE_1_ID=node-1
  export NODE_2_ID=node-2

  echo "==> Starting app..."
  nohup uvicorn src.chat_server:app --host 0.0.0.0 --port 8000 > /tmp/app.log 2>&1 &
  sleep 3

  # Start Pulsar → Delta bridge (consumes OTLP spans, writes trace_delta_table)
  echo "==> Starting trace consumer (Pulsar → Delta)..."
  nohup python3 -m src.trace_consumer > /tmp/trace_consumer.log 2>&1 &

  # Collectors push to local app
  INGEST_URL="http://localhost:8000"
  echo "==> Starting collectors (local push)..."
  INGEST_URL="$INGEST_URL" NODE_ID="$NODE_ID" PEER_IP="$PEER_IP" \
    nohup python3 -m src.gpu_collector > /tmp/gpu_collector.log 2>&1 &
  INGEST_URL="$INGEST_URL" NODE_ID="$NODE_ID" PEER_IP="$PEER_IP" \
    nohup python3 -m src.network_collector > /tmp/net_collector.log 2>&1 &

  # Start streaming jobs (Spark local[*])
  echo "==> Starting Spark streaming jobs (local master)..."
  export DATA_PATH=./data
  # Judge uses stronger model on Node 2
  export EVAL_MODEL=qwen2.5:7b
  export OLLAMA_BASE_URL="http://${NODE_2_IP}:11434"
  export JUDGE_BACKEND=ollama

  nohup python3 -m src.stream_agent_steps > /tmp/stream_agent_steps.log 2>&1 &
  nohup python3 -m src.stream_routing_infra > /tmp/stream_routing.log 2>&1 &
  nohup python3 -m src.stream_correlated > /tmp/stream_correlated.log 2>&1 &
  nohup python3 -m src.stream_trajectory > /tmp/stream_trajectory.log 2>&1 &
  nohup python3 -m src.stream_quality > /tmp/stream_quality.log 2>&1 &

  echo ""
  echo "==> Primary node ready!"
  echo "  App:       http://localhost:8000/static/index.html"
  echo "  Analytics: http://localhost:8000/static/analytics.html"
  echo "  Status:    http://localhost:8000/ingest/status"

else
  # --- Collector: LLM + metrics only ---
  INGEST_URL="${INGEST_URL:?Set INGEST_URL for collector role (e.g. http://<node1-ip>:8000)}"

  pip install pynvml psutil requests

  # Pull judge model (stronger eval on Node 2)
  ollama pull qwen2.5:7b

  echo "==> Starting collectors (pushing to $INGEST_URL)..."
  INGEST_URL="$INGEST_URL" NODE_ID="$NODE_ID" PEER_IP="$PEER_IP" \
    nohup python3 -m src.gpu_collector > /tmp/gpu_collector.log 2>&1 &
  INGEST_URL="$INGEST_URL" NODE_ID="$NODE_ID" PEER_IP="$PEER_IP" \
    nohup python3 -m src.network_collector > /tmp/net_collector.log 2>&1 &

  echo ""
  echo "==> Collector node ready!"
  echo "  Pushing metrics to: $INGEST_URL"
  echo "  Ollama serving on:  0.0.0.0:11434"
fi

# --- Azure Arc (optional, requires systemd — skipped on containers) ---
if [ -n "${AZURE_TENANT_ID:-}" ] && [ -n "${AZURE_SUBSCRIPTION_ID:-}" ]; then
  # Check if systemd is available (Arc requires it)
  if pidof systemd >/dev/null 2>&1 || [ -d /run/systemd/system ]; then
    echo "==> Installing Azure Arc agent..."
    wget -q https://aka.ms/azcmagent -O /tmp/install_linux_azcmagent.sh
    bash /tmp/install_linux_azcmagent.sh || { echo "  ⚠ Arc install failed (expected on containers). Skipping."; }

    if command -v azcmagent &>/dev/null; then
      if [ -n "${AZURE_CLIENT_ID:-}" ] && [ -n "${AZURE_CLIENT_SECRET:-}" ]; then
        echo "==> Registering with Azure Arc (service principal)..."
        azcmagent connect \
          --resource-group "${AZURE_RG:-trajectory-rg}" \
          --location "${AZURE_LOCATION:-eastus}" \
          --tenant-id "$AZURE_TENANT_ID" \
          --subscription-id "$AZURE_SUBSCRIPTION_ID" \
          --service-principal-id "$AZURE_CLIENT_ID" \
          --service-principal-secret "$AZURE_CLIENT_SECRET" || echo "  ⚠ Arc registration failed. Continuing without Arc."
      else
        echo "==> Registering with Azure Arc (interactive)..."
        azcmagent connect \
          --resource-group "${AZURE_RG:-trajectory-rg}" \
          --location "${AZURE_LOCATION:-eastus}" \
          --tenant-id "$AZURE_TENANT_ID" \
          --subscription-id "$AZURE_SUBSCRIPTION_ID" || echo "  ⚠ Arc registration failed. Continuing without Arc."
      fi
      echo "  ✓ Node registered in Azure Arc"
    fi
  else
    echo "==> Skipping Azure Arc (no systemd — container environment detected)"
  fi
else
  echo "==> Skipping Azure Arc (set AZURE_TENANT_ID + AZURE_SUBSCRIPTION_ID to enable)"
fi

echo "==> Done."
