#!/usr/bin/env bash
# Setup script for Vast.ai GPU nodes.
# Run on each rented node to install dependencies and start collectors.
# Usage: NODE_ID=node-1 PEER_IP=<other_node_ip> bash deploy/vastai_setup.sh
set -euo pipefail

NODE_ID="${NODE_ID:?Set NODE_ID (e.g. node-1)}"
PEER_IP="${PEER_IP:?Set PEER_IP (IP of the other node)}"
COLLECT_DIR="/workspace/collected_data"

echo "==> Setting up node: $NODE_ID"

# Install system deps
apt-get update && apt-get install -y python3-pip iputils-ping git

# Clone repo
if [ ! -d /workspace/trajectory ]; then
  git clone https://github.com/YOUR_USER/Trajectory-Analytics-Workflow.git /workspace/trajectory
fi
cd /workspace/trajectory

# Install Python deps (only what collectors need)
pip install pynvml psutil

# Create output dirs
mkdir -p "$COLLECT_DIR/gpu_metrics_raw" "$COLLECT_DIR/net_metrics_raw"

# Install and start Ollama (for generating LLM traces)
if ! command -v ollama &>/dev/null; then
  curl -fsSL https://ollama.com/install.sh | sh
fi
ollama serve &
sleep 5
ollama pull llama3.2

echo "==> Starting collectors in background..."

# Start GPU collector
NODE_ID="$NODE_ID" nohup python3 -m src.gpu_collector \
  > "$COLLECT_DIR/gpu_collector_$NODE_ID.log" 2>&1 &
echo "GPU collector PID: $!"

# Start network collector
NODE_ID="$NODE_ID" PEER_IP="$PEER_IP" nohup python3 -m src.network_collector \
  > "$COLLECT_DIR/net_collector_$NODE_ID.log" 2>&1 &
echo "Network collector PID: $!"

echo "==> Collectors running. Data in $COLLECT_DIR/"
echo "==> To generate LLM load, run: python3 -m src.main"
echo "==> After ~2 hours, scp $COLLECT_DIR back to your machine."
