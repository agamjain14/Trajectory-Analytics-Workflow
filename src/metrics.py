"""
Custom OpenTelemetry metrics for the AI application.
Tracks LLM call durations, token usage, agent handoffs, RAG retrievals, tool calls, and errors.
"""

from opentelemetry import metrics


class AppMetrics:
    """Centralized application metrics using OpenTelemetry."""

    def __init__(self, meter: metrics.Meter):
        # LLM / Inference metrics
        self.llm_call_duration = meter.create_histogram(
            name="llm.call.duration",
            description="Duration of LLM inference calls in seconds",
            unit="s",
        )
        self.llm_token_usage = meter.create_counter(
            name="llm.token.usage",
            description="Total tokens consumed by LLM calls",
            unit="tokens",
        )
        self.llm_calls_total = meter.create_counter(
            name="llm.calls.total",
            description="Total number of LLM calls made",
        )
        self.llm_errors_total = meter.create_counter(
            name="llm.errors.total",
            description="Total number of LLM call errors",
        )

        # Agent metrics
        self.agent_handoffs_total = meter.create_counter(
            name="agent.handoffs.total",
            description="Total agent-to-agent handoffs",
        )
        self.agent_active = meter.create_up_down_counter(
            name="agent.active",
            description="Currently active agents",
        )

        # RAG metrics
        self.rag_retrieval_duration = meter.create_histogram(
            name="rag.retrieval.duration",
            description="Duration of RAG vector retrieval in seconds",
            unit="s",
        )
        self.rag_documents_retrieved = meter.create_histogram(
            name="rag.documents.retrieved",
            description="Number of documents retrieved per RAG query",
        )

        # Tool call metrics
        self.tool_calls_total = meter.create_counter(
            name="tool.calls.total",
            description="Total tool/function calls made",
        )
        self.tool_call_duration = meter.create_histogram(
            name="tool.call.duration",
            description="Duration of tool calls in seconds",
            unit="s",
        )
        self.tool_errors_total = meter.create_counter(
            name="tool.errors.total",
            description="Total tool call errors",
        )

        # Retry metrics
        self.retry_attempts_total = meter.create_counter(
            name="retry.attempts.total",
            description="Total retry attempts across all operations",
        )

        # Request/workflow metrics
        self.workflow_duration = meter.create_histogram(
            name="workflow.duration",
            description="End-to-end workflow execution duration",
            unit="s",
        )
        self.workflow_requests_total = meter.create_counter(
            name="workflow.requests.total",
            description="Total workflow requests processed",
        )

    def record_llm_call(self, duration: float, tokens: int, model: str, agent: str):
        attributes = {"model": model, "agent": agent}
        self.llm_call_duration.record(duration, attributes)
        self.llm_token_usage.add(tokens, attributes)
        self.llm_calls_total.add(1, attributes)

    def record_llm_error(self, model: str, agent: str, error_type: str):
        self.llm_errors_total.add(
            1, {"model": model, "agent": agent, "error_type": error_type}
        )

    def record_agent_handoff(self, from_agent: str, to_agent: str):
        self.agent_handoffs_total.add(
            1, {"from_agent": from_agent, "to_agent": to_agent}
        )

    def record_rag_retrieval(self, duration: float, num_docs: int, collection: str):
        attrs = {"collection": collection}
        self.rag_retrieval_duration.record(duration, attrs)
        self.rag_documents_retrieved.record(num_docs, attrs)

    def record_tool_call(self, duration: float, tool_name: str, success: bool):
        attrs = {"tool_name": tool_name, "success": str(success)}
        self.tool_calls_total.add(1, attrs)
        self.tool_call_duration.record(duration, attrs)
        if not success:
            self.tool_errors_total.add(1, attrs)

    def record_retry(self, operation: str, attempt: int):
        self.retry_attempts_total.add(1, {"operation": operation, "attempt": str(attempt)})
