# Copilot Instructions

Tone: Concise, direct, and completely technical. Skip conversational filler or pleasantries.

## Stack

Python 3.11+ · Ollama · OpenTelemetry · Pulsar · PySpark Structured Streaming · Delta Lake · FastAPI · ChromaDB · Pydantic v2 · tenacity · structlog

## Conventions

- Type-hint all function signatures.
- `dataclasses` for internal data containers, `Pydantic` for API boundaries.
- Config via `os.getenv()` with defaults. No hardcoded URLs/paths/models.
- Module-level constants in `UPPER_SNAKE_CASE`.
- Self-documenting names over comments.

## OTel

- Wrap operations in `get_tracer().start_as_current_span(...)`.
- Span names: `agent.<name>.<operation>`.
- Set semantic attributes (`agent.name`, `http.request.method`, `gen_ai.*`).
- Errors: `span.set_status(StatusCode.ERROR)` + `span.record_exception(e)`.
- Session propagation: `set_current_session_id()` → `SessionIdSpanProcessor`.
- Metrics via `AppMetrics`.

## Agents

- Extend `BaseAgent`. Return `AgentMessage`.
- `OrchestratorAgent` → `ResearchAgent`, `FlightAgent`, `HotelAgent`, `ItineraryAgent`.

## Streaming

- One pipeline per `stream_*.py`. Shared config in `streaming_config.py`.
- Sink to Delta Lake, partitioned by `ingestion_date`/`ingestion_hour`.
- Checkpoints: `data/checkpoints/<job>/`.

## Error Handling

- `tenacity` retries for LLM and HTTP calls.
- `structlog` with context fields. Validate at system boundaries only.

## Run

```sh
docker-compose up -d                        # infra
python -m src.main                          # chat agent
uvicorn src.analytics_api:app --reload      # API
python -m src.stream_agent_steps            # streaming jobs
```