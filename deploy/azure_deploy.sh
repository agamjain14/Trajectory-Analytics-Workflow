#!/usr/bin/env bash
# Deploy to Azure Container Apps (consumption tier, scales to zero).
# Prerequisites: az CLI logged in, Docker image pushed to ACR.
# Usage: bash deploy/azure_deploy.sh
set -euo pipefail

# Config - override via env
RG="${AZURE_RG:-trajectory-rg}"
LOCATION="${AZURE_LOCATION:-eastus}"
ACR_NAME="${AZURE_ACR:-trajectoryacr}"
APP_NAME="${AZURE_APP:-trajectory-analytics}"
ENV_NAME="${AZURE_ENV:-trajectory-env}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

echo "==> Creating resource group: $RG"
az group create --name "$RG" --location "$LOCATION" -o none

echo "==> Creating Azure Container Registry: $ACR_NAME"
az acr create --resource-group "$RG" --name "$ACR_NAME" --sku Basic --admin-enabled true -o none

echo "==> Building and pushing Docker image..."
az acr build --registry "$ACR_NAME" --image "trajectory:$IMAGE_TAG" . --no-logs

ACR_SERVER="$ACR_NAME.azurecr.io"
ACR_PASSWORD=$(az acr credential show -n "$ACR_NAME" --query "passwords[0].value" -o tsv)

echo "==> Creating Container Apps environment: $ENV_NAME"
az containerapp env create \
  --name "$ENV_NAME" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  -o none

echo "==> Deploying Container App: $APP_NAME"
az containerapp create \
  --name "$APP_NAME" \
  --resource-group "$RG" \
  --environment "$ENV_NAME" \
  --image "$ACR_SERVER/trajectory:$IMAGE_TAG" \
  --registry-server "$ACR_SERVER" \
  --registry-username "$ACR_NAME" \
  --registry-password "$ACR_PASSWORD" \
  --target-port 8000 \
  --ingress external \
  --min-replicas 0 \
  --max-replicas 1 \
  --cpu 0.5 \
  --memory 1.0Gi \
  --env-vars \
    "DEPLOY_MODE=cloud" \
    "LLM_BACKEND=azure" \
    "AZURE_OPENAI_ENDPOINT=${AZURE_OPENAI_ENDPOINT:?Set AZURE_OPENAI_ENDPOINT}" \
    "AZURE_OPENAI_API_KEY=${AZURE_OPENAI_API_KEY:?Set AZURE_OPENAI_API_KEY}" \
    "AZURE_OPENAI_DEPLOYMENT=${AZURE_OPENAI_DEPLOYMENT:-gpt-4o-mini}" \
  -o none

FQDN=$(az containerapp show --name "$APP_NAME" --resource-group "$RG" \
  --query "properties.configuration.ingress.fqdn" -o tsv)

echo ""
echo "==> Deployed! Live URL:"
echo "    https://$FQDN"
echo ""
echo "    Dashboard: https://$FQDN/static/index.html"
echo "    Analytics: https://$FQDN/static/analytics.html"
echo "    API docs:  https://$FQDN/docs"
