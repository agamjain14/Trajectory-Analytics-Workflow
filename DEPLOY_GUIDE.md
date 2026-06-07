# Deployment Runbook: 2-Node Vast.ai GPU Cluster

## Why This Architecture

This project needs **real GPU metrics** (NVML: utilization, temperature, power, PCIe throughput) and **real network metrics** (psutil: latency, throughput, retransmits). Azure OpenAI is a managed API — no hardware access, no metrics. So inference runs on Vast.ai with Ollama, giving direct GPU access for real telemetry.

## Cluster Layout

```
Node 1 (142.126.17.171)                    Node 2 (174.116.164.194)
├── Ollama (LLM inference)                  ├── Ollama (LLM inference)
├── gpu_collector.py (NVML → push)          ├── gpu_collector.py (NVML → push)
├── network_collector.py (psutil → push)    ├── network_collector.py (psutil → push)
├── App (FastAPI: chat, analytics, UI)      └── qwen2.5:7b (judge model)
├── OTel Collector → Jaeger
├── Prometheus + Grafana
├── Apache Pulsar
├── Spark streaming jobs (local[*])
└── Delta Lake tables (./data/)
```

Node 1 runs everything (app + observability + streaming + LLM).
Node 2 runs LLM + collectors, pushes metrics to Node 1.
App load-balances chat inference across both nodes (round-robin).
Quality evaluation (LLM-as-judge) uses **qwen2.5:7b** on Node 2 — stronger than llama3.2 for scoring.

When Vast.ai is off → app auto-falls back to synthetic metrics after 30s.

---

## Node Info

| | Node 1 | Node 2 |
|--|--------|--------|
| Public IP | 142.126.17.171 | 174.116.164.194 |
| SSH port | 43918 | 42248 |
| Instance ID | 39865743 | 39865909 |
| Internal IP | 192.168.23.10 | 192.168.40.115 |
| 8080 mapped | 44201 | 42174 |

---

## Prerequisites (local machine)

```bash
# Verify SSH key
ls ~/.ssh/innovation
# Should exist. Public key already added to both nodes.

# Test connectivity
ssh -p 43918 -i ~/.ssh/innovation root@142.126.17.171 "hostname"
ssh -p 42248 -i ~/.ssh/innovation root@174.116.164.194 "hostname"
```

---

## Step 1: Setup Node 1 (primary — runs everything)

```bash
ssh -p 43918 -i ~/.ssh/innovation root@142.126.17.171
```

```bash
# Install system deps
apt-get update && apt-get install -y python3-pip python3-venv iputils-ping git curl wget openjdk-17-jre-headless docker.io docker-compose-v2

# Clone repo
git clone https://github.com/<YOUR_USER>/Trajectory-Analytics-Workflow.git /workspace/trajectory
cd /workspace/trajectory

# Install Python deps
pip install -r requirements.txt

# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh
ollama serve &
sleep 5
ollama pull llama3.2
```

### Start observability stack (Docker)

```bash
cd /workspace/trajectory

# Start Jaeger, Prometheus, Grafana, OTel Collector, Pulsar
docker compose -f docker-compose.yml up -d otel-collector jaeger prometheus grafana pulsar
```

### Start the app

```bash
cd /workspace/trajectory

export LLM_BACKEND=ollama
export OLLAMA_BASE_URL=http://localhost:11434
export OLLAMA_NODES="http://localhost:11434,http://174.116.164.194:11434"
export OLLAMA_MODEL=llama3.2
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export DATA_DIR=./data
export DEPLOY_MODE=local

# Node topology for streaming config
export NODE_1_URL=http://localhost:11434
export NODE_2_URL=http://174.116.164.194:11434
export NODE_1_ID=node-1
export NODE_2_ID=node-2

uvicorn src.chat_server:app --host 0.0.0.0 --port 8000 &
```

### Start collectors on Node 1

```bash
export NODE_ID=node-1
export PEER_IP=174.116.164.194
export INGEST_URL=http://localhost:8000

nohup python3 -m src.gpu_collector > /tmp/gpu_collector.log 2>&1 &
nohup python3 -m src.network_collector > /tmp/net_collector.log 2>&1 &
```

### Start Spark streaming jobs (local master, single machine)

```bash
cd /workspace/trajectory
export DATA_PATH=./data
export OLLAMA_BASE_URL=http://localhost:11434
export EVAL_MODEL=llama3.2
export JUDGE_BACKEND=ollama

# Run all 4 streaming jobs
python3 -m src.stream_agent_steps &
python3 -m src.stream_routing_infra &
python3 -m src.stream_correlated &
python3 -m src.stream_trajectory &
python3 -m src.stream_quality &
```

Spark runs with `local[*]` master — no executors, no Spark cluster. Uses in-process threads.

---

## Step 2: Setup Node 2 (LLM + collectors only)

```bash
ssh -p 42248 -i ~/.ssh/innovation root@174.116.164.194
```

```bash
# Install deps
apt-get update && apt-get install -y python3-pip iputils-ping git curl

# Clone repo
git clone https://github.com/<YOUR_USER>/Trajectory-Analytics-Workflow.git /workspace/trajectory
cd /workspace/trajectory

# Install only what collectors need
pip install pynvml psutil requests

# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh
ollama serve &
sleep 5
ollama pull llama3.2

# Start collectors — push to Node 1
export NODE_ID=node-2
export PEER_IP=142.126.17.171
export INGEST_URL=http://142.126.17.171:8000

nohup python3 -m src.gpu_collector > /tmp/gpu_collector.log 2>&1 &
nohup python3 -m src.network_collector > /tmp/net_collector.log 2>&1 &
```

**Note:** Node 2's Ollama must be accessible from Node 1. Ollama listens on 0.0.0.0:11434 by default. If firewalled, set `OLLAMA_HOST=0.0.0.0:11434` before `ollama serve`.

---

## Step 3: Verify

From your local machine:

```bash
# App UI (via Node 1's mapped port)
open http://142.126.17.171:8000/static/index.html

# Analytics dashboard
open http://142.126.17.171:8000/static/analytics.html

# Check metric sources
curl http://142.126.17.171:8000/ingest/status
# → {"gpu_source": "real", "net_source": "real", ...}

# Jaeger traces
open http://142.126.17.171:16686

# Grafana dashboards
open http://142.126.17.171:3000
```

Or via SSH tunnel from your machine:
```bash
ssh -p 43918 -i ~/.ssh/innovation root@142.126.17.171 \
  -L 8000:localhost:8000 \
  -L 16686:localhost:16686 \
  -L 3000:localhost:3000 \
  -L 9090:localhost:9090
```
Then access everything on `localhost:8000`, `localhost:16686`, etc.

---

## Port Reference

| Service | Node 1 internal | Node 1 public (mapped) |
|---------|-----------------|----------------------|
| App | 8000 | 142.126.17.171:44201 (8080→8080) or direct 8000 via tunnel |
| Jaeger UI | 16686 | tunnel |
| Grafana | 3000 | tunnel |
| Prometheus | 9090 | tunnel |
| OTel gRPC | 4317 | tunnel |
| Pulsar | 6650/8081 | tunnel |
| Ollama (Node 1) | 11434 | internal only |
| Ollama (Node 2) | 11434 | 174.116.164.194:11434 |

---

## Shutdown

```bash
# Node 1: stop all
ssh -p 43918 -i ~/.ssh/innovation root@142.126.17.171 "pkill -f uvicorn; pkill -f gpu_collector; pkill -f network_collector; pkill -f stream_; cd /workspace/trajectory && docker compose down"

# Node 2: stop collectors + ollama
ssh -p 42248 -i ~/.ssh/innovation root@174.116.164.194 "pkill -f gpu_collector; pkill -f network_collector; pkill ollama"
```

Then stop the Vast.ai instances from the dashboard to stop billing.

---

## Cost

| Component | Cost |
|-----------|------|
| Vast.ai Node 1 | ~$0.071/hr |
| Vast.ai Node 2 | ~$0.065/hr |
| **Both nodes running 8 hrs** | **~$1.09** |
| **3-day hackathon (8 hrs/day)** | **~$3.26** |
