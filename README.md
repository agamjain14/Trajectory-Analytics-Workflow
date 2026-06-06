# Trajectory Analytics Workflow - AI Travel Agent

A fully instrumented multi-agent AI travel planning application that demonstrates comprehensive observability with OpenTelemetry. Uses Ollama (local LLM) for reasoning, ChromaDB for RAG, and simulated travel tool APIs.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    User Request: "Plan a trip to Paris"          │
└────────────────────────────────┬────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Orchestrator Agent     │
                    │   (routes & synthesizes) │
                    └──┬──────┬──────┬──────┬─┘
                       │      │      │      │
          ┌────────────▼┐  ┌──▼────┐ ┌▼─────┐ ┌▼──────────┐
          │  Research    │  │Flight │ │Hotel │ │ Itinerary │
          │  Agent       │  │Agent  │ │Agent │ │ Agent     │
          │  (RAG+LLM)  │  │(Tools)│ │(Tools)│ │(Synthesis)│
          └──────┬───────┘  └──┬────┘ └──┬───┘ └───────────┘
                 │             │         │
          ┌──────▼───────┐  ┌──▼─────────▼──┐
          │  ChromaDB    │  │  Travel Tools  │
          │  (Vector DB) │  │  (HTTP / MCP)  │
          └──────────────┘  └───────────────-┘
                 │                   │
          ┌──────▼───────┐  ┌───────▼────────┐
          │  Ollama LLM  │  │  MCP Server    │
          │  (reasoning) │  │  (FastAPI)     │
          └──────────────┘  └────────────────┘
```

## Telemetry Signals Emitted

| Signal | What's Captured | Exporter |
|--------|----------------|----------|
| **Traces** | Full request journey across agents, LLM calls, RAG retrieval, tool invocations | OTLP → Jaeger |
| **Metrics** | LLM latency/tokens, agent handoffs, RAG retrieval time, tool call duration, retry counts, workflow duration | OTLP → Prometheus |
| **Logs** | Structured logs with trace context correlation, agent lifecycle events, errors | OTLP → Collector |

## Components

| Component | Purpose |
|-----------|---------|
| `src/telemetry.py` | OpenTelemetry setup (TracerProvider, MeterProvider, LoggerProvider) |
| `src/metrics.py` | Custom metrics definitions (counters, histograms, gauges) |
| `src/llm_client.py` | Ollama client with retry logic and tracing |
| `src/rag.py` | RAG retrieval with ChromaDB, travel knowledge base |
| `src/tools.py` | HTTP tool calls + MCP tool client + builtin travel tools |
| `src/agents.py` | Multi-agent system (Orchestrator, Research, Flight, Hotel, Itinerary) |
| `src/mcp_server.py` | FastAPI MCP tool server (simulated travel APIs) |
| `src/main.py` | Application entrypoint and demo workflow |

## Quick Start

### Prerequisites

- Python 3.11+
- [Ollama](https://ollama.ai/) installed and running
- Docker (for observability stack)

### 1. Setup

```bash
# Clone and enter the project
cd Trajectory-Analytics-Workflow

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment config
cp .env.example .env
```

### 2. Start Ollama

```bash
# Pull the model
ollama pull llama3.2

# Ollama should be serving on localhost:11434
ollama serve
```

### 3. Start Observability Stack

```bash
docker-compose up -d

# Access UIs:
# Jaeger UI: http://localhost:16686
# Prometheus: http://localhost:9090
# Grafana: http://localhost:3000 (admin/admin)
```

### 4. (Optional) Start MCP Tool Server

```bash
python -m src.mcp_server
# Runs on http://localhost:8001
```

### 5. Run the Travel Agent

```bash
python -m src.main
```

## What Happens When You Run It

1. **Telemetry initializes** — TracerProvider, MeterProvider, LoggerProvider all connect to the OTel Collector
2. **RAG seeds** — Travel knowledge base loads into ChromaDB
3. **HTTP tool demo** — Makes a real HTTP call (traced)
4. **MCP tool demo** — Attempts to call the MCP server (graceful fallback)
5. **Trip planning workflow** — Orchestrator coordinates 4 specialist agents:
   - Research Agent retrieves destination info from RAG + reasons with LLM
   - Flight Agent searches flights via tools + LLM analysis
   - Hotel Agent searches accommodations + checks weather
   - Itinerary Agent synthesizes everything into a day-by-day plan
6. **Telemetry emitted** — All operations produce traces (spans), metrics, and structured logs

## Observability Features

### Traces (Spans)
- `agent.orchestrator.plan_trip` — root span for entire workflow
- `agent.research_agent.research_destination` — RAG + LLM research
- `agent.flight_agent.search_flights` — flight tool calls + LLM analysis
- `agent.hotel_agent.search_hotels` — hotel search + weather check
- `agent.itinerary_agent.create_itinerary` — final synthesis
- `llm.chat` — each Ollama inference call
- `rag.retrieve` — vector retrieval operations
- `tool.builtin.*` — each tool invocation
- `tool.http_call` — external HTTP calls
- `tool.mcp_invoke` — MCP protocol calls

### Metrics
- `llm.call.duration` — histogram of LLM latency
- `llm.token.usage` — counter of tokens consumed
- `llm.errors.total` — LLM error count by type
- `agent.handoffs.total` — inter-agent communication count
- `rag.retrieval.duration` — vector search latency
- `rag.documents.retrieved` — docs per query
- `tool.calls.total` — tool invocation count
- `tool.call.duration` — tool latency histogram
- `retry.attempts.total` — retry count by operation
- `workflow.duration` — end-to-end workflow time

### Logs
- Structured JSON with trace/span context correlation
- Agent lifecycle events
- Error details with stack traces
- Performance annotations

---

## Streaming Analytics Pipeline

Infrastructure-aware agent trajectory & quality observability. Correlates agent behavior, LLM-as-judge quality scores, and simulated GPU contention to identify infrastructure-driven degradation.

### Architecture

```
trace_consumer (OTLP → Delta)
       │
       ▼
┌─────────────────┐
│ trace_delta_table│  ← Raw OTLP spans
└────────┬────────┘
         │
         ▼ [Stream 1]
┌─────────────────┐
│   agent_steps   │  ← Classified & flattened spans
└──┬──────┬─────┬─┘
   │      │     │
   ▼      ▼     ▼
[S2]    [S3]   [S4]
   │      │     │
   ▼      │     ▼
┌──────┐  │  ┌────────────────────────────────────┐
│traj- │  │  │ gpu_metrics + network_metrics +     │
│ectory│  │  │ request_routing                     │
└──────┘  │  └────────────────────────────────────┘
          ▼
   ┌─────────────┐
   │quality_scores│  ← LLM-as-judge (Ollama)
   └──────┬──────┘
          │
          ▼ [Stream 5]
   ┌─────────────────┐
   │ trace_correlated │  ← Final joined table
   └─────────────────┘
          │
          ▼
   ┌─────────────┐
   │analytics_api │  → Dashboard at :8002/analytics
   └─────────────┘
```

### Streaming Jobs — Command Reference

| Seq | Command | Source Table | Target Table(s) | Write Mode | Checkpoint |
|-----|---------|-------------|-----------------|------------|------------|
| 0 | `ollama serve` | — | — | — | — |
| 1 | `python -m src.trace_consumer` | OTLP (port 4318) | `trace_delta_table` | Append | — |
| 2 | `uvicorn src.chat_server:app --port 8000` | — | (generates traces) | — | — |
| 3 | `python -m src.stream_agent_steps` | `trace_delta_table` | `agent_steps` | Delta MERGE (upsert on trace_id, session_id, span_id) | `data/checkpoints/agent_steps` |
| 4 | `python -m src.stream_trajectory` | `agent_steps` | `trajectory_templates` | Delta MERGE (upsert on trace_id, session_id) | `data/checkpoints/trajectory` |
| 5 | `python -m src.stream_quality` | `agent_steps` | `quality_scores` | Delta MERGE (upsert on trace_id, session_id) | `data/checkpoints/quality` |
| 6 | `python -m src.stream_routing_infra` | `agent_steps` | `gpu_metrics`, `network_metrics`, `request_routing` | Delta MERGE (upsert on timestamp+node+gpu / trace_id+span_id) | `data/checkpoints/routing_infra` |
| 7 | `python -m src.stream_correlated` | `quality_scores` | `trace_correlated` | Delta MERGE (upsert on trace_id, session_id) | `data/checkpoints/correlated` |
| 8 | `uvicorn src.analytics_api:app --port 8002` | all Delta tables | — (serves API on :8002) | Read-only | — |

**All streaming jobs (3-7) are independent Spark Structured Streaming processes** with 5-minute trigger intervals. Start them in any order — each will wait for its source table to have data.

### Quick Start (All Terminals)

```bash
# Activate venv in each terminal
source .venv/bin/activate

# T1: LLM backend
ollama serve

# T2: OTLP trace ingestion
python -m src.trace_consumer

# T3: Chat agent (generates traces)
uvicorn src.chat_server:app --reload --port 8000

# T4-T8: Streaming jobs (one per terminal)
python -m src.stream_agent_steps
python -m src.stream_trajectory
python -m src.stream_quality
python -m src.stream_routing_infra
python -m src.stream_correlated

# T9: Analytics dashboard
uvicorn src.analytics_api:app --port 8002
```

### Generate Traces

```bash
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Plan a trip to Tokyo for 5 days"}'
```

### Verify Output

```bash
# Check row counts in all tables
python -c "
import deltalake as dl
for t in ['trace_delta_table','agent_steps','trajectory_templates','quality_scores',
          'gpu_metrics','network_metrics','request_routing','trace_correlated']:
    try:
        print(f'{t}: {dl.DeltaTable(f\"./data/{t}\").to_pyarrow_table().num_rows} rows')
    except Exception as e:
        print(f'{t}: ERROR - {e}')
"

# Or hit the API
curl http://localhost:8002/api/traces | python -m json.tool
curl http://localhost:8002/api/quality | python -m json.tool
curl http://localhost:8002/api/correlation-summary | python -m json.tool
```

### Reset All Data

```bash
rm -rf data/checkpoints data/agent_steps data/trajectory_templates \
       data/quality_scores data/gpu_metrics data/network_metrics \
       data/request_routing data/trace_correlated
```
