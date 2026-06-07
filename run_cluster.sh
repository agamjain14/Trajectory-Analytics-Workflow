#!/usr/bin/env bash
# run_cluster.sh — Deploy full 2-node Vast.ai GPU cluster from your local machine.
#
# Architecture:
#   Node 1 (primary): App + Observability + Streaming + Ollama + Collectors
#   Node 2 (collector): Ollama + Collectors (pushes metrics to Node 1)
#   Both nodes registered with Azure Arc
#
# Usage:
#   bash run_cluster.sh
set -euo pipefail

cd "$(dirname "$0")"

# --- Node config ---
NODE1_IP="142.126.17.171"
NODE1_SSH_PORT="43918"
NODE2_IP="174.116.164.194"
NODE2_SSH_PORT="42248"
SSH_KEY="~/.ssh/innovation"
GITHUB_REPO="${GITHUB_REPO:-https://github.com/YOUR_USER/Trajectory-Analytics-Workflow.git}"

echo "============================================"
echo "  Trajectory Analytics — 2-NODE GPU CLUSTER"
echo "============================================"
echo "  Node 1 (primary):   $NODE1_IP:$NODE1_SSH_PORT"
echo "  Node 2 (collector): $NODE2_IP:$NODE2_SSH_PORT"
echo ""

# --- Step 1: Test connectivity ---
echo "[1/4] Testing SSH connectivity..."
ssh -p "$NODE1_SSH_PORT" -i "$SSH_KEY" -o ConnectTimeout=5 root@"$NODE1_IP" "echo ok" >/dev/null 2>&1 || \
  { echo "ERROR: Cannot SSH to Node 1"; exit 1; }
ssh -p "$NODE2_SSH_PORT" -i "$SSH_KEY" -o ConnectTimeout=5 root@"$NODE2_IP" "echo ok" >/dev/null 2>&1 || \
  { echo "ERROR: Cannot SSH to Node 2"; exit 1; }
echo "  ✓ Both nodes reachable"

# --- Step 2: Copy setup script to both nodes ---
echo "[2/4] Uploading setup script..."
scp -P "$NODE1_SSH_PORT" -i "$SSH_KEY" deploy/vastai_setup.sh root@"$NODE1_IP":/tmp/vastai_setup.sh
scp -P "$NODE2_SSH_PORT" -i "$SSH_KEY" deploy/vastai_setup.sh root@"$NODE2_IP":/tmp/vastai_setup.sh
echo "  ✓ Scripts uploaded"

# --- Step 3: Setup Node 2 first (collector) ---
echo "[3/4] Setting up Node 2 (collector)..."
ssh -p "$NODE2_SSH_PORT" -i "$SSH_KEY" root@"$NODE2_IP" bash -c "'
export ROLE=collector
export NODE_ID=node-2
export PEER_IP=$NODE1_IP
export INGEST_URL=http://$NODE1_IP:8000
export GITHUB_REPO=$GITHUB_REPO
export AZURE_TENANT_ID=\"${AZURE_TENANT_ID:-}\"
export AZURE_SUBSCRIPTION_ID=\"${AZURE_SUBSCRIPTION_ID:-}\"
bash /tmp/vastai_setup.sh
'"
echo "  ✓ Node 2 ready"

# --- Step 4: Setup Node 1 (primary) ---
echo "[4/4] Setting up Node 1 (primary)..."
ssh -p "$NODE1_SSH_PORT" -i "$SSH_KEY" root@"$NODE1_IP" bash -c "'
export ROLE=primary
export NODE_ID=node-1
export PEER_IP=$NODE2_IP
export NODE_2_IP=$NODE2_IP
export GITHUB_REPO=$GITHUB_REPO
export AZURE_TENANT_ID=\"${AZURE_TENANT_ID:-}\"
export AZURE_SUBSCRIPTION_ID=\"${AZURE_SUBSCRIPTION_ID:-}\"
bash /tmp/vastai_setup.sh
'"
echo "  ✓ Node 1 ready"

echo ""
echo "============================================"
echo "  CLUSTER DEPLOYED"
echo "============================================"
echo ""
echo "  Access via SSH tunnel:"
echo "    ssh -p $NODE1_SSH_PORT -i $SSH_KEY root@$NODE1_IP \\"
echo "      -L 8000:localhost:8000 -L 16686:localhost:16686 -L 3000:localhost:3000"
echo ""
echo "  Then open:"
echo "    Chat:      http://localhost:8000/static/index.html"
echo "    Analytics: http://localhost:8000/static/analytics.html"
echo "    Topology:  http://localhost:8000/static/topology.html"
echo "    Jaeger:    http://localhost:16686"
echo "    Grafana:   http://localhost:3000"
echo "    Status:    http://localhost:8000/ingest/status"
echo ""
echo "  Cost: ~\$0.136/hr combined (\$0.071 + \$0.065)"
echo "============================================"
