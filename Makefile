# Trajectory Analytics Workflow - Makefile
# One-command operations for local and cloud deployment.

.PHONY: help local up down logs cloud-deploy cloud-destroy collect stream test clean

# Default target
help:
	@echo ""
	@echo "  Trajectory Analytics Workflow"
	@echo "  ============================="
	@echo ""
	@echo "  LOCAL (turnkey, one command):"
	@echo "    make local          - Spin up everything locally (Ollama + App + Observability)"
	@echo "    make down           - Stop all local services"
	@echo "    make logs           - Tail application logs"
	@echo "    make test           - Run load test against local"
	@echo ""
	@echo "  CLOUD (Azure deployment):"
	@echo "    make cloud-deploy   - Deploy to Azure Container Apps"
	@echo "    make cloud-destroy  - Tear down Azure resources"
	@echo ""
	@echo "  DATA COLLECTION (Vast.ai):"
	@echo "    make stream         - Run streaming ETL jobs locally"
	@echo ""
	@echo "  UTILITIES:"
	@echo "    make build          - Build Docker image"
	@echo "    make clean          - Remove data/checkpoints, volumes"
	@echo ""
	@echo "  After 'make local', visit:"
	@echo "    Chat:      http://localhost:8000/static/index.html"
	@echo "    Analytics: http://localhost:8000/static/analytics.html"
	@echo "    Topology:  http://localhost:8000/static/topology.html"
	@echo "    Jaeger:    http://localhost:16686"
	@echo "    Grafana:   http://localhost:3000"
	@echo "    API Docs:  http://localhost:8000/docs"
	@echo ""

# ============================================================
# LOCAL - Turnkey one-command
# ============================================================

local: up
	@echo ""
	@echo "==> All services starting..."
	@echo "    Ollama will pull llama3.2 on first run (~2GB download)"
	@echo "    Wait ~60s for model download, then visit:"
	@echo "    http://localhost:8000/static/index.html"
	@echo ""

up:
	docker compose -f docker-compose.local.yml up -d --build
	@echo "==> Services starting. Run 'make logs' to watch progress."

down:
	docker compose -f docker-compose.local.yml down

logs:
	docker compose -f docker-compose.local.yml logs -f app

logs-all:
	docker compose -f docker-compose.local.yml logs -f

# ============================================================
# BUILD
# ============================================================

build:
	docker build -t trajectory-analytics .

# ============================================================
# CLOUD - Azure Container Apps
# ============================================================

cloud-deploy:
	@test -n "$(AZURE_OPENAI_ENDPOINT)" || (echo "ERROR: Set AZURE_OPENAI_ENDPOINT" && exit 1)
	@test -n "$(AZURE_OPENAI_API_KEY)" || (echo "ERROR: Set AZURE_OPENAI_API_KEY" && exit 1)
	DEPLOY_MODE=cloud LLM_BACKEND=azure bash deploy/azure_deploy.sh

cloud-destroy:
	@echo "==> Destroying Azure resources..."
	az group delete --name $${AZURE_RG:-trajectory-rg} --yes --no-wait
	@echo "==> Resource group deletion initiated."

# ============================================================
# DATA COLLECTION & STREAMING
# ============================================================

stream:
	@echo "==> Running streaming ETL jobs..."
	python3 -m src.stream_routing_infra
	python3 -m src.stream_correlated
	python3 -m src.stream_trajectory
	python3 -m src.stream_quality
	@echo "==> Done. Delta tables updated in data/"

# ============================================================
# TESTING
# ============================================================

test:
	python3 deploy/load_test.py --url http://localhost:8000 --users 3 --duration 60

test-heavy:
	python3 deploy/load_test.py --url http://localhost:8000 --users 8 --duration 300

# ============================================================
# CLEANUP
# ============================================================

clean:
	rm -rf data/checkpoints/
	docker compose -f docker-compose.local.yml down -v
	@echo "==> Cleaned checkpoints and volumes."
