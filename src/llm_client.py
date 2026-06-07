"""
LLM Client module.
Supports both Ollama (local/Vast.ai) and Azure OpenAI (serving phase).
Wraps calls with OpenTelemetry instrumentation and retry logic.
Set LLM_BACKEND=azure to use Azure OpenAI, otherwise defaults to Ollama.
"""

import os
import time
from typing import List, Dict, Any, Optional

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

# Backend selection
LLM_BACKEND = os.getenv("LLM_BACKEND", "ollama")  # "ollama" or "azure"

# Multi-node Ollama: comma-separated URLs for round-robin load balancing
# e.g. OLLAMA_NODES=http://node1:11434,http://node2:11434
OLLAMA_NODES = os.getenv("OLLAMA_NODES", "")


class LLMClient:
    """LLM client supporting Ollama and Azure OpenAI with OpenTelemetry tracing."""

    def __init__(self, app_metrics: AppMetrics):
        self.metrics = app_metrics
        self.backend = LLM_BACKEND
        self._retry_count = 0
        self._node_index = 0

        if self.backend == "azure":
            from openai import AzureOpenAI
            self.base_url = os.getenv("AZURE_OPENAI_ENDPOINT", "")
            self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
            self.client = AzureOpenAI(
                azure_endpoint=self.base_url,
                api_key=os.getenv("AZURE_OPENAI_API_KEY", ""),
                api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
            )
            self._ollama_clients = []
        else:
            import ollama as ollama_lib
            self.base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
            self.model = os.getenv("OLLAMA_MODEL", "llama3.2")

            # Build list of Ollama clients for round-robin
            if OLLAMA_NODES:
                node_urls = [u.strip() for u in OLLAMA_NODES.split(",") if u.strip()]
            else:
                node_urls = [self.base_url]

            self._ollama_clients = [
                {"url": url, "client": ollama_lib.Client(host=url)} for url in node_urls
            ]
            self.client = self._ollama_clients[0]["client"]

    def _next_ollama_client(self):
        """Round-robin select the next Ollama client."""
        if not self._ollama_clients:
            return self.client, self.base_url
        node = self._ollama_clients[self._node_index % len(self._ollama_clients)]
        self._node_index += 1
        return node["client"], node["url"]

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
        retry=retry_if_exception_type((ConnectionError, TimeoutError, Exception)),
    )
    def _call_llm(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        span: Optional[trace.Span] = None,
        model_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Internal LLM call with retry logic. Supports Ollama and Azure OpenAI."""
        use_model = model_override or self.model
        if self.backend == "azure":
            response = self.client.chat.completions.create(
                model=use_model,
                messages=messages,
                temperature=temperature,
            )
            content = response.choices[0].message.content
            tokens = response.usage.completion_tokens if response.usage else len(content.split()) * 2
            input_tokens = response.usage.prompt_tokens if response.usage else 0
            return {
                "message": {"content": content},
                "eval_count": tokens,
                "prompt_eval_count": input_tokens,
                "_node_url": self.base_url,
            }
        else:
            client, node_url = self._next_ollama_client()
            response = client.chat(
                model=use_model,
                messages=messages,
                options={"temperature": temperature, "num_predict": 512, "num_ctx": 2048},
            )
            response["_node_url"] = node_url
            return response

    def chat(
        self,
        messages: List[Dict[str, str]],
        agent_name: str = "default",
        temperature: float = 0.7,
        model_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a chat completion request to Ollama with full tracing.
        
        Returns dict with 'content', 'model', 'tokens', 'duration_ms'.
        """
        use_model = model_override or self.model
        with get_tracer().start_as_current_span(f"chat {use_model}") as span:
            # OTel GenAI Semantic Convention attributes
            span.set_attribute("gen_ai.system", "azure_openai" if self.backend == "azure" else "ollama")
            span.set_attribute("gen_ai.operation.name", "chat")
            span.set_attribute("gen_ai.request.model", use_model)
            span.set_attribute("gen_ai.request.temperature", temperature)
            span.set_attribute("gen_ai.request.max_tokens", 0)  # Ollama default: unlimited
            # Custom attributes (no standard equivalent)
            span.set_attribute("gen_ai.agent.name", agent_name)
            span.set_attribute("gen_ai.request.messages_count", len(messages))
            span.set_attribute("gen_ai.prompt.system", messages[0]["content"][:200] if messages and messages[0]["role"] == "system" else "")

            self._retry_count = 0
            start_time = time.time()

            try:
                response = self._call_llm(messages, temperature, span, model_override=model_override)
                duration = time.time() - start_time

                content = response["message"]["content"]
                # Estimate token count (Ollama provides eval_count)
                tokens = response.get("eval_count", len(content.split()) * 2)

                span.set_attribute("gen_ai.response.model", use_model)
                span.set_attribute("gen_ai.response.node_url", response.get("_node_url", self.base_url))
                span.set_attribute("gen_ai.usage.output_tokens", tokens)
                span.set_attribute("gen_ai.usage.input_tokens", response.get("prompt_eval_count", len(str(messages)) // 4))
                span.set_attribute("gen_ai.response.content_length", len(content))
                span.set_attribute("gen_ai.response.content", content)
                span.set_attribute("gen_ai.prompt.user", messages[-1]["content"] if messages else "")
                span.set_attribute("retry.count", self._retry_count)
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
                span.set_attribute("retry.count", self._retry_count)

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
        """Generate embeddings (Ollama native or Azure OpenAI)."""
        with get_tracer().start_as_current_span(f"embeddings {self.model}") as span:
            span.set_attribute("gen_ai.system", "azure_openai" if self.backend == "azure" else "ollama")
            span.set_attribute("gen_ai.operation.name", "embeddings")
            span.set_attribute("gen_ai.request.model", self.model)
            span.set_attribute("gen_ai.request.input_length", len(text))

            try:
                if self.backend == "azure":
                    embed_model = os.getenv("AZURE_OPENAI_EMBED_DEPLOYMENT", self.model)
                    response = self.client.embeddings.create(
                        model=embed_model, input=text
                    )
                    embedding = response.data[0].embedding
                else:
                    response = self.client.embeddings(model=self.model, prompt=text)
                    embedding = response["embedding"]
                span.set_status(StatusCode.OK)
                return embedding
            except Exception as e:
                span.set_status(StatusCode.ERROR, str(e))
                span.record_exception(e)
                raise
