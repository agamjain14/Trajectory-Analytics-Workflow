"""
LLM Client module.
Wraps Ollama calls with OpenTelemetry instrumentation and retry logic.
"""

import os
import time
from typing import List, Dict, Any, Optional

import ollama
from opentelemetry import trace
from opentelemetry.trace import StatusCode
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep,
)

from src.telemetry import get_tracer, get_logger
from src.metrics import AppMetrics


class LLMClient:
    """Ollama LLM client with OpenTelemetry tracing, metrics, and retry logic."""

    def __init__(self, app_metrics: AppMetrics):
        self.metrics = app_metrics
        self.base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        self.model = os.getenv("OLLAMA_MODEL", "llama3.2")
        self.client = ollama.Client(host=self.base_url)
        self._retry_count = 0

    def _before_retry(self, retry_state):
        """Log retry attempts and record metrics."""
        self._retry_count += 1
        self.metrics.record_retry(
            operation="llm_inference", attempt=self._retry_count
        )
        get_logger().warning(
            "llm.retry_attempt",
            attempt=self._retry_count,
            model=self.model,
            wait=retry_state.next_action.sleep if hasattr(retry_state.next_action, 'sleep') else 0,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, ollama.ResponseError)),
    )
    def _call_ollama(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        span: Optional[trace.Span] = None,
    ) -> Dict[str, Any]:
        """Internal Ollama call with retry logic."""
        response = self.client.chat(
            model=self.model,
            messages=messages,
            options={"temperature": temperature},
        )
        return response

    def chat(
        self,
        messages: List[Dict[str, str]],
        agent_name: str = "default",
        temperature: float = 0.7,
    ) -> Dict[str, Any]:
        """
        Send a chat completion request to Ollama with full tracing.
        
        Returns dict with 'content', 'model', 'tokens', 'duration_ms'.
        """
        with get_tracer().start_as_current_span("llm.chat") as span:
            span.set_attribute("llm.model", self.model)
            span.set_attribute("llm.agent", agent_name)
            span.set_attribute("llm.temperature", temperature)
            span.set_attribute("llm.messages_count", len(messages))
            span.set_attribute("llm.system_prompt", messages[0]["content"][:200] if messages and messages[0]["role"] == "system" else "")

            self._retry_count = 0
            start_time = time.time()

            try:
                response = self._call_ollama(messages, temperature, span)
                duration = time.time() - start_time

                content = response["message"]["content"]
                # Estimate token count (Ollama provides eval_count)
                tokens = response.get("eval_count", len(content.split()) * 2)

                span.set_attribute("llm.response_length", len(content))
                span.set_attribute("llm.tokens", tokens)
                span.set_attribute("llm.duration_ms", duration * 1000)
                span.set_attribute("llm.retry_count", self._retry_count)
                span.set_status(StatusCode.OK)

                # Record metrics
                self.metrics.record_llm_call(
                    duration=duration,
                    tokens=tokens,
                    model=self.model,
                    agent=agent_name,
                )

                get_logger().info(
                    "llm.chat_complete",
                    agent=agent_name,
                    model=self.model,
                    tokens=tokens,
                    duration_ms=round(duration * 1000, 2),
                    retries=self._retry_count,
                )

                return {
                    "content": content,
                    "model": self.model,
                    "tokens": tokens,
                    "duration_ms": round(duration * 1000, 2),
                    "retries": self._retry_count,
                }

            except Exception as e:
                duration = time.time() - start_time
                span.set_status(StatusCode.ERROR, str(e))
                span.record_exception(e)
                span.set_attribute("llm.retry_count", self._retry_count)

                self.metrics.record_llm_error(
                    model=self.model, agent=agent_name, error_type=type(e).__name__
                )

                get_logger().error(
                    "llm.chat_failed",
                    agent=agent_name,
                    model=self.model,
                    error=str(e),
                    retries=self._retry_count,
                )
                raise

    def generate_embedding(self, text: str) -> List[float]:
        """Generate embeddings using Ollama (for custom RAG if needed)."""
        with get_tracer().start_as_current_span("llm.embedding") as span:
            span.set_attribute("llm.model", self.model)
            span.set_attribute("llm.input_length", len(text))

            try:
                response = self.client.embeddings(model=self.model, prompt=text)
                span.set_status(StatusCode.OK)
                return response["embedding"]
            except Exception as e:
                span.set_status(StatusCode.ERROR, str(e))
                span.record_exception(e)
                raise
