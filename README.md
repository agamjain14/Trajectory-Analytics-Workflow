# Trajectory Analytics Workflow

## Team

- **Agam Jain**

## Problem Statement

Multi-agent AI systems are opaque—when an orchestrator delegates to sub-agents making LLM calls, RAG retrievals, and tool invocations, operators cannot:

1. **Trace execution paths** across the full agent call graph
2. **Score response quality** and correlate quality drops to trajectory patterns
3. **Attribute degradation to infrastructure** (GPU contention, network latency, routing)

**Solution:** End-to-end observable AI agent system—a travel planner instrumented with OpenTelemetry, feeding Spark Structured Streaming that extracts trajectory templates, scores quality via LLM-as-judge, infrastructure metrics, and joins everything into a correlated analytics view.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          AI AGENT APPLICATION LAYER                         │
│                                                                              │
│   User ──▶ Chat Server (FastAPI :8000)                                       │
│                 │                                                            │
│            Orchestrator Agent                                                │
│            ┌────┼─────┬──────┐                                               │
│            ▼    ▼     ▼      ▼                                               │
│       Research Flight Hotel Itinerary                                        │
│       Agent   Agent  Agent  Agent                                            │
│       (RAG+   (Tool  (Tool  (LLM                                            │
│        LLM)   +LLM)  +LLM)  Synthesis)                                      │
│         │       │      │       │                                             │
│    ChromaDB  Builtin/MCP Tools │           All spans instrumented with       │
│         │       │      │       │           OpenTelemetry (traces, metrics,   │
│       Ollama ◀─┘──────┘───────┘           structured logs)                  │
└──────────┬───────────────────────────────────────────────────────────────────┘
           │
           │ OTLP gRPC/HTTP (spans, metrics, logs)
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        TELEMETRY COLLECTION LAYER                           │
│                                                                              │
│   OTel Collector ──▶ Jaeger (traces)                                         │
│        │          ──▶ Prometheus (metrics)                                    │
│        │          ──▶ Grafana (dashboards)                                    │
│                                                                              │
│   Pulsar Span Exporter ──▶ Apache Pulsar ──▶ Trace Consumer ──▶ Delta Lake   │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                   SPARK STRUCTURED STREAMING PIPELINE                       │
│                                                                              │
│   trace_delta_table (raw OTLP spans)                                         │
│         │                                                                    │
│         ▼ Stream 1                                                           │
│   agent_steps (classified & flattened spans)                                 │
│         │                                                                    │
│         ├──▶ Stream 2 ──▶ trajectory_templates (trajectory signatures)       │
│         ├──▶ Stream 3 ──▶ quality_scores (LLM-as-judge via Ollama)           │
│         └──▶ Stream 4 ──▶ gpu_metrics, network_metrics, request_routing      │
│                           (simulated infra: GPU contention, topology routing) │
│                                                                              │
│   quality_scores                                                             │
│         └──▶ Stream 5 ──▶ trace_correlated (joined: trajectory + quality     │
│                            + routing + GPU/network metrics)                  │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          ANALYTICS & DASHBOARD                              │
│                                                                              │
│   Analytics API (FastAPI :8002) ──▶ Dashboard UI                             │
│     /api/traces, /api/quality, /api/correlation-summary                      │
│     LLM-powered natural language summaries of quality & infrastructure       │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Low-Level Design — Streaming Pipeline

```
═══════════════════════════════════════════════════════════════════════════════
 TELEMETRY EXPORT
═══════════════════════════════════════════════════════════════════════════════

  Chat Server / Agents
       │
       ├── OTLP gRPC ──▶ OTel Collector ──▶ Jaeger (traces)
       │                                 ──▶ Prometheus (metrics)
       │
       └── SDK ──▶ Pulsar Span Exporter ──▶ Apache Pulsar (topic: otlp-traces)

═══════════════════════════════════════════════════════════════════════════════
 INGESTION (trace_consumer)
═══════════════════════════════════════════════════════════════════════════════

  Apache Pulsar ──▶ trace_consumer ──▶ [trace_delta_table] (raw OTLP spans)

═══════════════════════════════════════════════════════════════════════════════
 SPARK STRUCTURED STREAMING (5 independent jobs, Delta MERGE upserts)
═══════════════════════════════════════════════════════════════════════════════

  [trace_delta_table]
       │
       ▼
  ┌─ Stream 1 (stream_agent_steps) ─────────────────────────────────────────┐
  │  Classify & flatten spans → span_kind, agent, model, tokens, etc.       │
  │  Output: [agent_steps]                                                  │
  └─────────────────────────────────────────────────────────────────────────┘
       │
       ├───────────────────────────┬────────────────────────────────┐
       ▼                           ▼                                ▼
  ┌─ Stream 2 ──────────┐   ┌─ Stream 3 ──────────┐   ┌─ Stream 4 ──────────────┐
  │ stream_trajectory    │   │ stream_quality       │   │ stream_routing_infra    │
  │                      │   │                      │   │                         │
  │ Group by trace_id,   │   │ Build eval context,  │   │ Simulate GPU contention,│
  │ compute step sequence│   │ call Ollama as judge,│   │ network latency,        │
  │ & signature hash     │   │ score 5 dimensions   │   │ topology-aware routing  │
  │                      │   │                      │   │                         │
  │ Output:              │   │ Output:              │   │ Outputs:                │
  │ [trajectory_templates]   │ [quality_scores]     │   │ [gpu_metrics]           │
  └───────────┬──────────┘   └──────────┬───────────┘   │ [network_metrics]       │
              │                          │               │ [request_routing]       │
              │                          │               └────────┬────────────────┘
              │                          │                        │
              │                          ▼                        │
              │         ┌─ Stream 5 (stream_correlated) ─────────────────────┐
              │         │  Joins all tables for final correlated view:       │
              ├────────▶│  trajectory + quality + routing + gpu + network    │
              │         │                                                    │
              │         │  Output: [trace_correlated]                        │
              │         └──────────────────┬─────────────────────────────────┘
              │                            │
              ▼                            ▼
═══════════════════════════════════════════════════════════════════════════════
 ANALYTICS API (FastAPI :8002)
═══════════════════════════════════════════════════════════════════════════════

  Reads: trajectory_templates, quality_scores, trace_correlated,
         gpu_metrics, network_metrics, request_routing, agent_steps,
         analytics_windows, topology
  Serves: /analytics/trajectories, /analytics/quality, /analytics/gpu,
          /analytics/network, /analytics/correlation/traces,
          /analytics/correlation/windows, /analytics/routing,
          /analytics/summary (LLM-powered)
  Dashboard UI: /analytics
```

## Hybrid Deployment (Mac + Vast.ai GPU)

Run the **full analytics pipeline locally on a Mac** and offload only the work that needs a physical GPU (LLM inference, LLM-as-judge, GPU/network metrics) to a **Vast.ai GPU node**. NVML metrics require a real GPU, so only collectors and Ollama run remotely.

### High-Level Design

```
┌─────────────────────────── MAC (full pipeline) ───────────────────────────┐
│  Chat app :8000 · MCP :8001 · trace_consumer · 5 Spark streams · Delta     │
│  Docker infra: Pulsar · OTel Collector · Jaeger · Prometheus · Grafana     │
│  Analytics API + Dashboard                                                 │
└───────────┬───────────────────────────────────────────┬───────────────────┘
            │ -L 11435 → Vast Ollama (inference + judge) │ -R 8000 ← collectors
            ▼                                             ▲
┌─────────────────────────── VAST.ai (GPU only) ────────────────────────────┐
│  Ollama :11434 (llama3.2 inference + qwen2.5:7b judge)                     │
│  gpu_collector + network_collector → POST /ingest/* on Mac                 │
└────────────────────────────────────────────────────────────────────────────┘
```

- `-L 11435:localhost:11434` — Mac dispatches inference + judge calls to Vast Ollama.
- `-R 8000:localhost:8000` — Vast collectors push real GPU/network metrics back to the Mac app.
- `METRICS_MODE=real` makes Spark the single writer; collector JSONL is the only metrics source.

### Where Each Component Runs

| Component | Runs on | Why |
|-----------|---------|-----|
| Chat app + agents + OTel instrumentation | Mac | Spans are emitted in-process by the agent code (`src/telemetry.py`); the app dispatches inference to remote Ollama over the tunnel |
| MCP tool server, `trace_consumer`, 5 Spark streams, Delta tables | Mac | Full analytics pipeline is local |
| Docker infra (Pulsar, OTel Collector, Jaeger, Prometheus, Grafana) | Mac | Trace backbone + dashboards |
| Ollama (inference `llama3.2` + judge `qwen2.5:7b`) | Vast.ai | Needs the GPU |
| `gpu_collector` (NVML) + `network_collector` (psutil) | Vast.ai | NVML/host metrics require the physical GPU node |

**Instrumentation** runs entirely inside the Mac chat app: every agent/LLM/RAG/tool call is wrapped in an OpenTelemetry span, exported via the Pulsar span exporter → Pulsar → `trace_consumer` → `trace_delta_table`. The remote node only serves model tokens; it is not instrumented.

### How GPU / Network Metrics Are Sent

```
Vast.ai node                              Mac (over -R 8000 reverse tunnel)
────────────                              ─────────────────────────────────
gpu_collector  (NVML, every 5s)
  └─ POST /ingest/gpu_metrics ──────────▶ chat_server :8000
network_collector (psutil)                  └─ live_metrics router
  └─ POST /ingest/network_metrics ──────▶       └─ METRICS_MODE=real:
                                                    append raw JSONL to
                                                    data/gpu_metrics_raw/*.jsonl
                                                    data/net_metrics_raw/*.jsonl
                                                         │
                                          stream_routing_infra reads the JSONL
                                          landing zone and MERGEs into the
                                          gpu_metrics / network_metrics Delta tables
```

- Collectors poll locally (`COLLECT_INTERVAL=5s`), set `INGEST_URL=http://localhost:8000`, and POST one JSON row per sample. The reverse tunnel makes the Mac's `:8000` reachable as the node's `localhost:8000`.
- The `/ingest/*` endpoints (`src/live_metrics.py`) only **land raw JSONL** in real mode — they never write Delta directly. This keeps `stream_routing_infra` the **single writer**, avoiding dual-write corruption.
- `/ingest/status` reports `last_real_gpu_ago_s` / `last_real_net_ago_s`; non-null means real pushes are arriving. If pushes stop for longer than the timeout, the app falls back to synthetic metrics.

### Low-Level Design — Job Consume / Produce

| # | Job | Consumes | Produces | MERGE key |
|---|-----|----------|----------|-----------|
| — | `trace_consumer` | Pulsar topic `otlp-traces` | `trace_delta_table` | append |
| 1 | `stream_agent_steps` | `trace_delta_table` | `agent_steps` | trace_id, session_id, span_id |
| 2 | `stream_trajectory` | `agent_steps` | `trajectory_templates` | trace_id, session_id |
| 3 | `stream_quality` | `agent_steps` + Vast judge (`EVAL_MODEL`) | `quality_scores` | trace_id, session_id |
| 4 | `stream_routing_infra` | `agent_steps` (REASON spans) + `gpu_metrics_raw/*.jsonl`, `net_metrics_raw/*.jsonl` | `request_routing`, `gpu_metrics`, `network_metrics` | routing: trace_id, span_id · metrics: timestamp_ms, node_id, gpu_id |
| 5 | `stream_correlated` | `quality_scores` (+ joins trajectory, routing, gpu, network) | `trace_correlated` | trace_id, session_id |

All downstream streams read upstream Delta tables with `skipChangeCommits=true` (upstream uses MERGE/upsert, so change commits must be ignored). GPU/network ingest only runs on micro-batches where `agent_steps` is non-empty — metrics tables populate only after a chat generates spans.

### Table Dependencies

```
trace_delta_table
   └─▶ agent_steps ──┬─▶ trajectory_templates ─┐
                     ├─▶ quality_scores ────────┼─▶ trace_correlated
                     └─▶ request_routing ───────┤
                                                │
   gpu_metrics_raw/*.jsonl ─▶ gpu_metrics ──────┤
   net_metrics_raw/*.jsonl ─▶ network_metrics ──┘
```

- `agent_steps` is the fan-out hub: streams 2, 3, 4 all derive from it.
- `trace_correlated` is the join sink keyed on `(trace_id, session_id)`; it correlates quality with trajectory, routing, GPU, and network on the same trace.
- GPU/network tables are partitioned by `(ingestion_date, ingestion_hour)`, derived from `timestamp_ms` inside the routing job.

### What Each Table Means

| Table | High-level meaning |
|-------|--------------------|
| `trace_delta_table` | Raw OTLP spans landed from Pulsar — the unprocessed firehose |
| `agent_steps` | One row per classified span: **what the agent DID** (which agent, model, tokens, span kind, serving `node_id`) |
| `trajectory_templates` | Per-trace execution shape: **the path the agent took** (step sequence + signature hash, LLM/tool/retrieve/retry counts) |
| `quality_scores` | Per-trace LLM-as-judge verdict: **how GOOD the output was** (5 dimensions + overall) |
| `request_routing` | Per REASON span: **WHERE inference ran** (maps each LLM call to its serving `node_id`/`gpu_id`) |
| `gpu_metrics` | GPU time series keyed by `node_id` + `timestamp_ms`: utilization, contention, temp, throttle |
| `network_metrics` | Network time series keyed by `node_id` + `timestamp_ms`: latency, packet drops, retransmits |
| `trace_correlated` | The unified fact table: **quality joined to the infra conditions that produced it**, one wide row per trace |

### How the Tables Join

```
agent_steps ──(group by trace_id)──▶ trajectory_templates   # what the agent DID
agent_steps ──(LLM-as-judge)───────▶ quality_scores         # how GOOD the output was
agent_steps ──(REASON span → node)─▶ request_routing        # WHERE inference ran
gpu_metrics / network_metrics        (time series, keyed by node_id + timestamp_ms)

trace_correlated = trajectory ⋈ quality        ON trace_id
                             ⋈ routing          ON trace_id  → gives node_id
                             ⋈ gpu/net metrics  ON node_id AND |metric.ts − trace.ts| ≤ ±10s
```

### How Quality Is Scored and Correlated

**Scoring (`stream_quality`, Stream 3).** For each impacted trace it reads all `agent_steps` rows, reconstructs the trace context by span kind — `ENTRY` → user request, `REASON` → reasoning chain + final response, `RETRIEVE`/`TOOL` → evidence — and sends one prompt to the **judge LLM** (`EVAL_MODEL=qwen2.5:7b` on Vast.ai). The judge returns strict JSON scoring 5 dimensions (1–5):

| Dimension | Meaning |
|-----------|---------|
| `completeness` | Addresses all parts of the request |
| `coherence` | Logically structured and consistent |
| `hallucination` | Fabricated claims not in evidence (5 = none, 1 = severe) |
| `groundedness` | Grounded in tool/retrieval results |
| `relevance` | Stays on-topic |

`overall` is the mean of the five. Rows MERGE into `quality_scores` keyed on `(trace_id, session_id)`; a failed eval lands as all-zeros with an `explanation` error string.

**Correlation (`stream_correlated`, Stream 5).** Driven by `trajectory_templates`, it left-joins each trace's quality scores, then time-aligns infra signals: for every routing hop it pulls GPU and network samples within a **±10s window** of the hop timestamp and aggregates them (avg/max contention, temperature, memory pressure, throttle count, inter-node latency, packet drops, TCP retransmits, queue wait). The result is a single wide row per trace carrying the **quality dimensions side-by-side with the infra conditions that produced them** — so a low `quality_overall` or high `hallucination` can be read directly against `gpu_contention_max`, `gpu_throttle_count`, or `inter_node_latency_max` on the same record. Missing quality (eval lag) defaults to `0.0` rather than dropping the trace.

### Deploy Steps (end to end)

**Prerequisites (Mac):** Docker running, Java (for Spark), Python venv, this repo.
**Prerequisites (Vast.ai):** GPU instance reachable via SSH key.

```bash
# 1. Open the dual SSH tunnel (keep this terminal open)
ssh -i ~/.ssh/innovation -p <VAST_SSH_PORT> root@<VAST_IP> -N \
  -L 11435:localhost:11434 \
  -R 8000:localhost:8000

# 2. In a SEPARATE terminal: SSH into the Vast.ai box (interactive shell)
ssh -i ~/.ssh/innovation -p <VAST_SSH_PORT> root@<VAST_IP>

#    Then ON the Vast.ai box: clone repo + start Ollama + collectors
git clone https://github.com/agamjain14/Trajectory-Analytics-Workflow.git /workspace/trajectory 2>/dev/null
cd /workspace/trajectory
ROLE=collector NODE_ID=node-1 \
  INGEST_URL=http://localhost:8000 \
  EVAL_MODEL=qwen2.5:7b \
  bash deploy/vastai_setup.sh

# 3. On Mac: start the full local pipeline (inference + judge → Vast)
OLLAMA_NODES=http://localhost:11435 \
  EVAL_MODEL=qwen2.5:7b \
  bash deploy/hybrid_local.sh start

# 4. Verify
bash deploy/hybrid_local.sh status
curl http://localhost:8000/ingest/status   # last_real_gpu_ago_s / last_real_net_ago_s = real pushes

# 5. Generate traffic, then query analytics (~30-60s lag from 30s triggers)
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Plan a trip to Tokyo for 5 days"}'
curl http://localhost:8000/analytics/correlation/traces

# 6. Stop
bash deploy/hybrid_local.sh stop
```

**Does the single script start everything local?** Yes. `deploy/hybrid_local.sh start` is the only command needed on the Mac — it boots Docker infra (Pulsar/OTel/Jaeger/Prometheus/Grafana), initializes all Delta tables, then launches the MCP server, chat app, `trace_consumer`, and all 5 Spark streaming jobs (detached, logs in `/tmp/trajectory-hybrid/`). `stop` tears all of it down.

**Where does the Vast.ai connection happen?** Two places, both over the SSH tunnel from Step 1 — the script never SSHes itself:
- **Inference + judge (Mac → Vast):** `OLLAMA_NODES=http://localhost:11435` and `EVAL_MODEL` point the app and quality job at `localhost:11435`, which the `-L 11435:localhost:11434` forward tunnels to Vast Ollama.
- **Metrics (Vast → Mac):** the remote collectors `POST` to `localhost:8000`, which the `-R 8000:localhost:8000` reverse tunnels back to the Mac's chat app.

| Service | URL |
|---------|-----|
| Chat UI | http://localhost:8000/static/index.html |
| Analytics | http://localhost:8000/static/analytics.html |
| Status | http://localhost:8000/ingest/status |
| Logs | `/tmp/trajectory-hybrid/` |


## Setup Instructions

---

### Option A: Local (one command)

**Prerequisites:** Docker, Docker Compose, ~6GB free disk (for Ollama model download).

```bash
make local
```

This starts everything: Ollama + model pull + App + OTel Collector + Jaeger + Prometheus + Grafana + Pulsar.

Once ready (~60s for first-time model download):

| Service | URL |
|---------|-----|
| Chat UI | http://localhost:8000/static/index.html |
| Analytics | http://localhost:8000/static/analytics.html |
| Topology | http://localhost:8000/static/topology.html |
| API Docs | http://localhost:8000/docs |
| Jaeger | http://localhost:16686 |
| Grafana | http://localhost:3000 (admin/admin) |

Stop: `make down`

---

### Option B: Live GPU Cluster (2-node Vast.ai)

Deploys across 2 Vast.ai GPU nodes with real GPU/network metrics and round-robin LLM inference.

**Prerequisites:** 2 Vast.ai GPU instances running, SSH key added to both.

#### Automated (GitHub Actions)

1. Push code to GitHub
2. Go to **Actions → Deploy GPU Cluster → Run workflow**
3. Enter the 4 Vast.ai node details (IPs + SSH ports)
4. Wait for deploy (~5 min)

#### Manual (one command)

```bash
bash run_cluster.sh
```

#### Access (via SSH tunnel)

```bash
ssh -p <NODE1_SSH_PORT> -i ~/.ssh/innovation root@<NODE1_IP> \
  -L 8000:localhost:8000 -L 16686:localhost:16686 -L 3000:localhost:3000
```

| Service | URL |
|---------|-----|
| Chat UI | http://localhost:8000/static/index.html |
| Analytics Dashboard | http://localhost:8000/static/analytics.html |
| Topology View | http://localhost:8000/static/topology.html |
| API Docs | http://localhost:8000/docs |
| Metric Source Status | http://localhost:8000/ingest/status |
| Jaeger (traces) | http://localhost:16686 |
| Grafana (dashboards) | http://localhost:3000 (admin/admin) |

#### API Endpoints

```bash
# Chat with the agent
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Plan a trip to Tokyo for 5 days"}'

# Check real vs synthetic metric source
curl http://localhost:8000/ingest/status

# Analytics queries
curl http://localhost:8000/analytics/trajectories
curl http://localhost:8000/analytics/quality
curl http://localhost:8000/analytics/correlation/traces
curl http://localhost:8000/analytics/gpu
curl http://localhost:8000/analytics/network
```

#### Cluster layout

```
Node 1 (primary): App + Observability + Spark streaming + Ollama + Collectors
Node 2 (collector): Ollama + GPU/Network collectors + Judge model (qwen2.5:7b)
```

Both nodes serve LLM inference (round-robin). Quality evaluation uses the stronger judge model on Node 2. GPU and network metrics are real (NVML + psutil). When Vast.ai is off, the app falls back to synthetic metrics automatically (30s timeout).

See [DEPLOY_GUIDE.md](DEPLOY_GUIDE.md) for the full step-by-step runbook.

---

### Reset All Data

```bash
rm -rf data/checkpoints data/agent_steps data/trajectory_templates \
       data/quality_scores data/gpu_metrics data/network_metrics \
       data/request_routing data/trace_correlated data/trace_delta_table
```

## Dependencies & Credits

| Library | Purpose | License |
|---------|---------|---------|
| [OpenTelemetry Python](https://opentelemetry.io/) | Tracing, metrics, logging SDK & OTLP exporters | Apache 2.0 |
| [Ollama](https://ollama.ai/) + [ollama-python](https://github.com/ollama/ollama-python) | Local LLM inference (llama3.2) | MIT |
| [ChromaDB](https://www.trychroma.com/) | Vector database for RAG | Apache 2.0 |
| [sentence-transformers](https://www.sbert.net/) | Embedding model for RAG retrieval | Apache 2.0 |
| [FastAPI](https://fastapi.tiangolo.com/) | HTTP servers (chat, MCP, analytics) | MIT |
| [Apache Pulsar](https://pulsar.apache.org/) | Streaming message backbone | Apache 2.0 |
| [PySpark](https://spark.apache.org/) + [Delta Lake](https://delta.io/) | Structured Streaming & Delta table storage | Apache 2.0 |
| [Jaeger](https://www.jaegertracing.io/) | Distributed trace backend & UI | Apache 2.0 |
| [Prometheus](https://prometheus.io/) | Metrics collection & querying | Apache 2.0 |
| [Grafana](https://grafana.com/) | Metrics dashboards | AGPL 3.0 |
| [tenacity](https://github.com/jd/tenacity) | Retry logic for LLM calls | Apache 2.0 |
| [structlog](https://www.structlog.org/) | Structured logging | MIT/Apache 2.0 |
| [httpx](https://www.python-httpx.org/) / [requests](https://requests.readthedocs.io/) | HTTP clients | BSD / Apache 2.0 |
| [Pydantic](https://docs.pydantic.dev/) | Data validation | MIT |
| [PyArrow](https://arrow.apache.org/docs/python/) | Columnar data for Delta Lake | Apache 2.0 |
| [python-dotenv](https://github.com/theskumar/python-dotenv) | Environment variable loading | BSD |
