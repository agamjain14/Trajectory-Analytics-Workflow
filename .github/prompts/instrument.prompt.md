---
description: Add OTel tracing and metrics to a function
agent: insert
---

Add OpenTelemetry instrumentation to the selected code following project conventions:

**Tracing:**
- Wrap in `get_tracer().start_as_current_span("<component>.<operation>")`.
- Set semantic attributes: `agent.name`, `agent.operation`, `agent.framework`, `agent.parameter.*` for agents. `http.request.method`, `url.full`, `server.address`, `server.port` for HTTP. `gen_ai.*` for LLM calls. `rpc.system`, `rpc.service`, `rpc.method` for MCP/RPC.
- On error: `span.set_status(StatusCode.ERROR, str(e))` + `span.record_exception(e)`.
- On success: `span.set_status(StatusCode.OK)`.

**Metrics:**
- Use `AppMetrics` methods: `record_llm_call`, `record_tool_call`, `record_agent_handoff`, `record_llm_error`, `record_retry`.
- Time operations with `time.time()` around the call.

**Logging:**
- Use `get_logger().info(...)` / `.error(...)` with structured context fields.
- No print statements.

**Imports:**
```python
from opentelemetry.trace import StatusCode
from src.telemetry import get_tracer, get_logger
from src.metrics import AppMetrics
```
