#!/usr/bin/env bash
# Deploy app stack to Azure VM (D4s_v7: 4 vCPU, 16GB RAM, ~$0.178/hr).
# Runs docker-compose: App + Pulsar + OTel + Jaeger + Prometheus + Grafana
# LLM inference runs on Vast.ai (Ollama). GPU metrics pushed to /ingest/* endpoints.
#
# Prerequisites:
#   - az CLI logged in (az login)
#   - Git repo pushed to GitHub (for VM to clone)
#
# Usage:
#   export OLLAMA_BASE_URL="http://<vastai-ip>:11434"
#   bash deploy/azure_deploy.sh
set -euo pipefail

# --- Config (override via env) ---
RG="${AZURE_RG:-trajectory-rg}"
LOCATION="${AZURE_LOCATION:-eastus}"
VM_NAME="${AZURE_VM:-trajectory-vm}"
VM_SIZE="${AZURE_VM_SIZE:-Standard_D4s_v7}"
ADMIN_USER="azureuser"
GITHUB_REPO="${GITHUB_REPO:-https://github.com/YOUR_USER/Trajectory-Analytics-Workflow.git}"

echo "============================================"
echo "  Deploying to Azure VM ($VM_SIZE)"
echo "  Cost: ~\$0.178/hr (\$130/month if 24/7)"
echo "============================================"

# --- Step 1: Resource Group ---
echo ""
echo "[1/5] Creating resource group: $RG..."
az group create --name "$RG" --location "$LOCATION" -o none 2>/dev/null || true

# --- Step 2: Create VM ---
echo "[2/5] Creating VM: $VM_NAME ($VM_SIZE)..."
VM_EXISTS=$(az vm show -g "$RG" -n "$VM_NAME" --query "id" -o tsv 2>/dev/null || echo "")

if [ -z "$VM_EXISTS" ]; then
    az vm create \
        --resource-group "$RG" \
        --name "$VM_NAME" \
        --image Ubuntu2404 \
        --size "$VM_SIZE" \
        --admin-username "$ADMIN_USER" \
        --generate-ssh-keys \
        --public-ip-sku Standard \
        -o none
    echo "  ✓ VM created"
else
    # Make sure it's running
    az vm start -g "$RG" -n "$VM_NAME" -o none 2>/dev/null || true
    echo "  ✓ VM already exists (started)"
fi

# --- Step 3: Open ports ---
echo "[3/5] Opening ports..."
for PORT_INFO in "8000:1000" "16686:1001" "9090:1002" "3000:1003" "8081:1004"; do
    PORT="${PORT_INFO%%:*}"
    PRIORITY="${PORT_INFO##*:}"
    az vm open-port -g "$RG" -n "$VM_NAME" --port "$PORT" --priority "$PRIORITY" -o none 2>/dev/null || true
done
echo "  ✓ Ports open: 8000 (app), 16686 (Jaeger), 9090 (Prometheus), 3000 (Grafana), 8081 (Pulsar)"

# --- Step 4: Get VM IP ---
VM_IP=$(az vm show -g "$RG" -n "$VM_NAME" -d --query "publicIps" -o tsv)
echo "  VM Public IP: $VM_IP"

# --- Step 5: Setup & Deploy on VM ---
echo "[4/5] Setting up Docker + deploying application..."

# Create the .env content
ENV_CONTENT="LLM_BACKEND=${LLM_BACKEND:-ollama}
OLLAMA_BASE_URL=${OLLAMA_BASE_URL:-http://host.docker.internal:11434}
OLLAMA_MODEL=${OLLAMA_MODEL:-llama3.2}
DEPLOY_MODE=cloud"

az vm run-command invoke \
    --resource-group "$RG" \
    --name "$VM_NAME" \
    --command-id RunShellScript \
    --scripts "
set -e

# Install Docker
if ! command -v docker &>/dev/null; then
    curl -fsSL https://get.docker.com | sh
    usermod -aG docker $ADMIN_USER
fi

# Install Docker Compose plugin
if ! docker compose version &>/dev/null 2>&1; then
    apt-get update && apt-get install -y docker-compose-plugin
fi

# Clone or update repo
REPO_DIR='/home/$ADMIN_USER/app'
if [ -d \"\$REPO_DIR/.git\" ]; then
    cd \"\$REPO_DIR\" && git pull origin main
else
    rm -rf \"\$REPO_DIR\"
    git clone $GITHUB_REPO \"\$REPO_DIR\"
fi
cd \"\$REPO_DIR\"

# Write .env
cat > .env << 'ENVEOF'
$ENV_CONTENT
ENVEOF

# Ensure data directory exists
mkdir -p data/gpu_metrics data/network_metrics data/agent_steps data/trace_correlated

# Build and start
docker compose down 2>/dev/null || true
docker compose up -d --build

echo 'Deployment complete!'
docker compose ps
" -o none

echo ""
echo "[5/5] Verifying deployment..."
sleep 10
if curl -s --max-time 10 "http://$VM_IP:8000/docs" >/dev/null 2>&1; then
    echo "  ✓ App is responding!"
else
    echo "  ⚠ App may still be starting (containers building). Check in 2-3 minutes."
fi

echo ""
echo "============================================"
echo "  DEPLOYMENT COMPLETE"
echo "============================================"
echo ""
echo "  Application:       http://$VM_IP:8000/static/index.html"
echo "  Analytics:         http://$VM_IP:8000/static/analytics.html"
echo "  API Docs:          http://$VM_IP:8000/docs"
echo "  Jaeger (traces):   http://$VM_IP:16686"
echo "  Prometheus:        http://$VM_IP:9090"
echo "  Grafana:           http://$VM_IP:3000  (admin/admin)"
echo "  Pulsar Admin:      http://$VM_IP:8081"
echo ""
echo "  Data Source Status: http://$VM_IP:8000/ingest/status"
echo ""
echo "  ─── Connect Vast.ai GPU nodes (optional) ───"
echo "  On each Vast.ai node, run:"
echo "    INGEST_URL=http://$VM_IP:8000 NODE_ID=node-1 python3 -m src.gpu_collector"
echo "    INGEST_URL=http://$VM_IP:8000 NODE_ID=node-1 PEER_IP=<other> python3 -m src.network_collector"
echo ""
echo "  ─── Cost management ───"
echo "  Stop VM:   az vm deallocate -g $RG -n $VM_NAME  (stops billing)"
echo "  Start VM:  az vm start -g $RG -n $VM_NAME"
echo "  Delete:    az group delete -g $RG --yes"
echo "============================================"
