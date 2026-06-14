# Trajectory Analytics Workflow

**Infrastructure-aware observability for non-deterministic AI agents.** Trace how an agent's
execution paths drift, score its answer quality with an LLM-as-judge, and **prove** whether a
quality drop was caused by GPU contention or by the application itself.

## Team

- **Agam Jain**

## Problem Statement

AI agents are non-deterministic: the same request can take a different path each run, and answer
quality can quietly collapse. When that happens, teams ask *"was the model dumb?"* — which misses
the real cause. This system answers the sharper question: **did the infrastructure underneath the
agent change how it behaved?** It joins three things nobody usually looks at together — the agent's
**execution path**, its **answer quality**, and the **GPU pressure** at that exact moment.

It does this for a **travel-planner agent**: an orchestrator delegates to research, flight, hotel,
and itinerary sub-agents. Every step is instrumented with OpenTelemetry, streamed through Spark +
Delta Lake, judged for quality, joined with real GPU/network metrics, and surfaced on a dashboard.

### The three questions it answers

1. **How do agent paths change over time?** — drift scoring (Jensen-Shannon divergence vs. a calm baseline).
2. **Does a quality drop co-occur with path change AND GPU contention?** — a correlation engine that
   separates **infra-caused** from **app-caused** degradation.
3. **Which trajectory mutations lead to wrong answers?** — isolates the failing paths and their root cause.

---

## High-Level Design

```
┌──────────────────────── AI AGENT APP (FastAPI :8000) ───────────────────────┐
│  User ──▶ Orchestrator ──┬─ Research (RAG+LLM) ─┬─ Flight ─┬─ Hotel ─┬─ Itinerary │
│                          └── all steps wrapped in OpenTelemetry spans ──────────┘ │
└──────────────────────────────────┬───────────────────────────────────────────────┘
                                    │ OTLP spans
                                    ▼
        OTel Collector ─▶ Jaeger / Prometheus / Grafana   (observability)
        Pulsar exporter ─▶ Apache Pulsar ─▶ trace_consumer ─▶ Delta Lake
                                    │
                                    ▼
┌──────────────── SPARK STRUCTURED STREAMING (5 jobs, Delta MERGE) ────────────┐
│  trace_delta_table → agent_steps →  trajectory_templates                     │
│                                  →  quality_scores  (LLM-as-judge)           │
│                                  →  gpu/network/routing  (real GPU metrics)   │
│                                  →  trace_correlated  (joined fact table)     │
│                      analytics_windows (Q1 drift + Q2 verdicts per window)    │
└──────────────────────────────────┬───────────────────────────────────────────┘
                                    ▼
        Analytics API + Dashboard  (/static/analytics.html)
```

---

## Low-Level Design — Streaming Jobs

| # | Job | Consumes | Produces |
|---|-----|----------|----------|
| — | `trace_consumer` | Pulsar topic `otlp-traces` | `trace_delta_table` (append) |
| 1 | `stream_agent_steps` | `trace_delta_table` | `agent_steps` |
| 2 | `stream_trajectory` | `agent_steps` | `trajectory_templates` |
| 3 | `stream_quality` | `agent_steps` + judge LLM | `quality_scores` |
| 4 | `stream_routing_infra` | `agent_steps` + raw GPU/net JSONL | `request_routing`, `gpu_metrics`, `network_metrics` |
| 5 | `stream_correlated` | `quality_scores` (+ joins) | `trace_correlated` |
| 6 | `stream_windows` | `trace_correlated` | `analytics_windows` (Q1 drift + Q2 verdicts) |

Downstream jobs read upstream Delta tables with `skipChangeCommits=true` (upstream uses MERGE).
GPU/network ingest only runs on micro-batches where `agent_steps` is non-empty.

### Table dependencies

```
trace_delta_table
   └▶ agent_steps ─┬▶ trajectory_templates ─┐
                   ├▶ quality_scores ────────┼▶ trace_correlated ─▶ analytics_windows
                   └▶ request_routing ───────┤
   gpu_metrics_raw/*.jsonl ─▶ gpu_metrics ───┤
   net_metrics_raw/*.jsonl ─▶ network_metrics┘
```

`agent_steps` is the fan-out hub. `trace_correlated` is the join sink keyed on `(trace_id, session_id)`.

### What each table means

| Table | Meaning |
|-------|---------|
| `trace_delta_table` | Raw OTLP spans from Pulsar — the unprocessed firehose. |
| `agent_steps` | One row per classified span: **what the agent DID** (agent, model, tokens, span kind, serving node). |
| `trajectory_templates` | Per-trace **path the agent took** (step sequence + signature hash, LLM/tool/retry counts). |
| `quality_scores` | Per-trace LLM-as-judge verdict: **how GOOD the output was** (5 dimensions + overall, 1–5). |
| `request_routing` | Per LLM call: **WHERE inference ran** (maps each call to its `node_id`/`gpu_id`). |
| `gpu_metrics` | GPU time series per node: utilization, contention, temperature, throttle. |
| `network_metrics` | Network time series per node: latency, packet drops, retransmits. |
| `trace_correlated` | The unified fact table: **quality joined to the infra conditions that produced it**, one wide row per trace. |
| `analytics_windows` | Per time window: drift score (Q1) + correlation verdict (Q2). |

### How correlation works

`trace_correlated` joins each trace to the GPU/network samples on the **same node** whose timestamp
falls **within ±10s** of the trace's execution. That ties a quality drop to the *specific* GPU
conditions present while that trace ran — not a global average.

`stream_windows` then buckets traces into time windows and emits a verdict per window:

| Verdict | Meaning |
|---------|---------|
| `gpu_induced_degradation` | Quality dropped **because** the GPU was under pressure (quality-drop + drift + GPU-pressure all fired). |
| `app_layer_degradation` | Quality dropped but the **GPU was idle** — a prompt/model issue, not infra. |
| `trajectory_drift_no_quality_impact` | Path changed but quality held. |
| `quality_drop_stable_trajectory` | Quality dropped but the path didn't change. |
| `normal` | Healthy window. |

**Quality scoring:** for each trace, `stream_quality` reconstructs context by span kind (`ENTRY` =
request, `REASON` = reasoning + response, `RETRIEVE`/`TOOL` = evidence) and asks the judge LLM to
score 5 dimensions (1–5): `completeness`, `coherence`, `hallucination`, `groundedness`, `relevance`.
`overall` is their mean.

---

## Run It Completely Locally (one command)

> **Note on the demo recording:** for the recorded demo, the full analytics pipeline ran locally
> while **only the model inference and the LLM-as-judge ran on a real remote GPU** (RTX 3060,
> Vast.ai) so the GPU-contention metrics were measured off physical hardware. The steps below run
> **100% on your machine** using local Ollama — no GPU box required. (Hybrid GPU instructions are
> in [DEPLOY_GUIDE.md](DEPLOY_GUIDE.md).)

### Prerequisites

- **Docker Desktop** running (for Pulsar, OTel Collector, Jaeger, Prometheus, Grafana)
- **Python 3.11+** with a virtual env (`python3 -m venv .venv && source .venv/bin/activate`)
- **Java 17+** (required by Spark) — `brew install openjdk@17`
- **Ollama** installed and running (`ollama serve`) — pulls `llama3.2` (~2 GB) on first run
- ~6 GB free disk

### Start everything

```bash
# Option 1 — fully containerized (Ollama + app + observability in Docker):
make local

# Option 2 — run the pipeline natively (Docker only for infra), with live logs:
bash run_local.sh        # Ctrl+C stops cleanly
```

First start takes ~60s while Ollama downloads the model. Then open:

| Service | URL |
|---------|-----|
| Chat UI | <http://localhost:8000/static/index.html> |
| **Analytics dashboard** | <http://localhost:8000/static/analytics.html> |
| Topology | <http://localhost:8000/static/topology.html> |
| API docs | <http://localhost:8000/docs> |
| Metric source status | <http://localhost:8000/ingest/status> |
| Jaeger | <http://localhost:16686> |
| Grafana | <http://localhost:3000> (admin/admin) |

### Generate data and query analytics

```bash
# Send a request to the agent:
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Plan a trip to Tokyo for 5 days"}'

# Query the analytics (~30–60s lag — streams trigger every 30s):
curl http://localhost:8000/analytics/correlation/traces
curl http://localhost:8000/analytics/quality
curl http://localhost:8000/analytics/gpu
```

### Stop / reset

```bash
make down          # stop all services  (or Ctrl+C if you used run_local.sh)

# Wipe all generated data (keeps chroma_db RAG store):
rm -rf data/checkpoints data/agent_steps data/trajectory_templates \
       data/quality_scores data/gpu_metrics data/network_metrics \
       data/request_routing data/trace_correlated data/trace_delta_table \
       data/analytics_windows
```

> For a guided demo walkthrough (what to show, what to say, how to read each chart),
> see [DEMO_NARRATION.md](DEMO_NARRATION.md).

---

## Dependencies & Credits

| Library | Purpose | License |
|---------|---------|---------|
| [OpenTelemetry Python](https://opentelemetry.io/) | Tracing/metrics/logging SDK & OTLP export | Apache 2.0 |
| [Ollama](https://ollama.ai/) + [ollama-python](https://github.com/ollama/ollama-python) | Local LLM inference (`llama3.2`) + judge (`qwen2.5:7b`) | MIT |
| [ChromaDB](https://www.trychroma.com/) | Vector DB for RAG | Apache 2.0 |
| [sentence-transformers](https://www.sbert.net/) | Embeddings for RAG retrieval | Apache 2.0 |
| [FastAPI](https://fastapi.tiangolo.com/) | Chat / MCP / analytics HTTP servers | MIT |
| [Apache Pulsar](https://pulsar.apache.org/) | Streaming message backbone | Apache 2.0 |
| [PySpark](https://spark.apache.org/) + [Delta Lake](https://delta.io/) | Structured Streaming & table storage | Apache 2.0 |
| [Jaeger](https://www.jaegertracing.io/) | Distributed trace backend & UI | Apache 2.0 |
| [Prometheus](https://prometheus.io/) | Metrics collection | Apache 2.0 |
| [Grafana](https://grafana.com/) | Metrics dashboards | AGPL 3.0 |
| [tenacity](https://github.com/jd/tenacity) | LLM retry logic | Apache 2.0 |
| [structlog](https://www.structlog.org/) | Structured logging | MIT/Apache 2.0 |
| [httpx](https://www.python-httpx.org/) / [requests](https://requests.readthedocs.io/) | HTTP clients | BSD / Apache 2.0 |
| [Pydantic](https://docs.pydantic.dev/) | Data validation | MIT |
| [PyArrow](https://arrow.apache.org/docs/python/) | Columnar data for Delta Lake | Apache 2.0 |
| [python-dotenv](https://github.com/theskumar/python-dotenv) | Env var loading | BSD |
