# Trajectory Analytics Workflow

## Team

- **Agam Jain**

## Problem Statement

Modern AI agents (multi-step LLM workflows) are opaque. When an orchestrator delegates to specialist sub-agents, each making LLM calls, RAG retrievals, and tool invocations, there is no way to:

1. **Trace the full execution path** across agents, LLM calls, retrievals, and tool invocations in a single request.
2. **Score response quality** automatically using LLM-as-judge and correlate quality drops with specific trajectory patterns.
3. **Attribute performance degradation to infrastructure** — determine whether a slow or low-quality response was caused by agent logic or GPU contention, network latency, or routing decisions.

This project solves all three by building an end-to-end observable AI agent system: a multi-agent travel planner instrumented with OpenTelemetry, feeding a Spark Structured Streaming pipeline that classifies spans, extracts trajectory templates, scores quality via LLM-as-judge, simulates GPU/network infrastructure, and joins everything into a correlated analytics view.

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

## Components

| Component | Purpose |
|-----------|---------|
| `src/telemetry.py` | OpenTelemetry setup — TracerProvider, MeterProvider, LoggerProvider with OTLP + Pulsar exporters |
| `src/metrics.py` | Custom OTel metrics: LLM latency/tokens, agent handoffs, RAG retrieval, tool calls, retries, workflow duration |
| `src/llm_client.py` | Ollama LLM client with retry logic (tenacity) and per-call span instrumentation |
| `src/rag.py` | RAG retrieval via ChromaDB + sentence-transformers embeddings, traced |
| `src/tools.py` | Builtin travel tools, HTTP tool client, MCP tool client — all traced |
| `src/agents.py` | Multi-agent system: Orchestrator, Research, Flight, Hotel, Itinerary agents |
| `src/chat_server.py` | FastAPI chat server with session persistence (SQLite), intent extraction, multi-turn conversation |
| `src/mcp_server.py` | FastAPI MCP tool server exposing simulated travel APIs (flights, hotels, weather, visa, currency) |
| `src/session_store.py` | SQLite-backed session/conversation persistence |
| `src/pulsar_exporter.py` | Custom OpenTelemetry SpanExporter that writes spans to Apache Pulsar |
| `src/trace_consumer.py` | Pulsar consumer that reads OTLP spans and writes to Delta Lake (`trace_delta_table`) |
| `src/streaming_config.py` | Shared config, schemas, Spark session factory, span classification logic for all streaming jobs |
| `src/stream_agent_steps.py` | Stream 1: raw spans → classified `agent_steps` (Delta MERGE) |
| `src/stream_trajectory.py` | Stream 2: agent_steps → `trajectory_templates` (trajectory signature extraction) |
| `src/stream_quality.py` | Stream 3: agent_steps → `quality_scores` (LLM-as-judge evaluation via Ollama) |
| `src/stream_routing_infra.py` | Stream 4: agent_steps → `gpu_metrics`, `network_metrics`, `request_routing` (simulated infra) |
| `src/stream_correlated.py` | Stream 5: quality_scores → `trace_correlated` (joins trajectory + quality + infra metrics) |
| `src/infra_simulator.py` | GPU contention, network latency, and topology-aware request routing simulator |
| `src/analytics_api.py` | FastAPI analytics server — reads all Delta tables, serves REST API + dashboard UI |
| `src/main.py` | CLI entrypoint with interactive chat session |

## Telemetry Signals Emitted

### Traces (Spans)

| Span Name | Description |
|-----------|-------------|
| `agent.orchestrator.plan_trip` | Root span for the entire multi-agent workflow |
| `agent.research_agent.research_destination` | RAG retrieval + LLM reasoning |
| `agent.flight_agent.search_flights` | Tool calls + LLM analysis for flights |
| `agent.hotel_agent.search_hotels` | Hotel search + weather check |
| `agent.itinerary_agent.create_itinerary` | Final day-by-day synthesis |
| `chat {model}` | Each Ollama inference call (model, tokens, latency) |
| `findNearest {collection}` | ChromaDB vector retrieval (query, doc count) |
| `builtin/{tool_name}` | Builtin tool invocations (search_flights, search_hotels, get_weather, get_visa_info, currency_convert) |
| `HTTP {METHOD}` | External HTTP calls with OTel semantic conventions |
| `travel-tools/{tool_name}` | MCP tool server invocations |

All spans carry `session.id` (injected by a custom `SessionIdSpanProcessor`) and standard OTel resource attributes (`service.name`, `service.version`, `deployment.environment`).

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `llm.call.duration` | Histogram | LLM inference latency (seconds) |
| `llm.token.usage` | Counter | Tokens consumed per call |
| `llm.calls.total` | Counter | Total LLM calls |
| `llm.errors.total` | Counter | LLM errors by type |
| `agent.handoffs.total` | Counter | Agent-to-agent handoffs |
| `agent.active` | UpDownCounter | Currently active agents |
| `rag.retrieval.duration` | Histogram | Vector search latency |
| `rag.documents.retrieved` | Histogram | Docs returned per query |
| `tool.calls.total` | Counter | Tool invocations |
| `tool.call.duration` | Histogram | Tool call latency |
| `tool.errors.total` | Counter | Tool call errors |
| `retry.attempts.total` | Counter | Retry count by operation |
| `workflow.duration` | Histogram | End-to-end workflow time |
| `workflow.requests.total` | Counter | Total workflow requests |

### Logs

Structured JSON logs via OpenTelemetry LoggerProvider + `structlog`, automatically correlated with trace/span context.

## Setup Instructions

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

Deploys across 2 Vast.ai GPU nodes with real GPU/network metrics, round-robin LLM inference, and Azure Arc registration.

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
