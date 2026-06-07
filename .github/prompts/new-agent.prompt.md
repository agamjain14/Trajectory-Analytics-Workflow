---
description: Add a new specialist agent
agent: insert
---

# New agent: {{name}}

Add a specialist agent to `src/agents.py`.

**Structure:**
```python
class {{Name}}Agent(BaseAgent):
    """One-line description of what this agent does."""

    def __init__(self, llm: LLMClient, ..., app_metrics: AppMetrics):
        super().__init__("{{name}}_agent", llm, app_metrics)

    def {{operation}}(self, ...) -> AgentMessage:
        with get_tracer().start_as_current_span(self._create_span_name("{{operation}}")) as span:
            span.set_attribute("agent.name", self.name)
            span.set_attribute("agent.operation", "{{operation}}")
            span.set_attribute("agent.framework", "custom")
            # span.set_attribute("agent.parameter.<key>", value)

            # 1. Call tools (MCP/HTTP/builtin) or RAG retrieval
            # 2. Build messages list with system + user prompt
            # 3. response = self.llm.chat(messages, agent_name=self.name)

            span.set_status(StatusCode.OK)
            get_logger().info("agent.{{operation}}_complete", agent=self.name, ...)

            return AgentMessage(
                from_agent=self.name,
                to_agent="orchestrator",
                content=response["content"],
                context={...},
                tool_results=[...],
            )
```

**Checklist:**
- Register in `OrchestratorAgent.__init__` and wire into `plan_trip`.
- System prompt as first message in `messages` list — no external files.
- All tool calls via `MCPToolClient.invoke_tool` or `HTTPTool.call`.
- Handle tool errors: `span.set_status(StatusCode.ERROR)` + `raise RuntimeError(...)`.
