"""
Telemetry initialization module.
Sets up OpenTelemetry traces, metrics, and structured logging for the entire application.
"""

import os
import logging
from contextvars import ContextVar
from dotenv import load_dotenv

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider, SpanProcessor
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

from src.pulsar_exporter import PulsarSpanExporter

import structlog

load_dotenv()

# --- Session context propagation ---
# This ContextVar holds the current session_id for the request.
# The custom SpanProcessor reads it and stamps every span with session.id.
_current_session_id: ContextVar[str] = ContextVar("current_session_id", default="")


def set_current_session_id(session_id: str) -> None:
    """Set the session_id for the current async/thread context."""
    _current_session_id.set(session_id)


def get_current_session_id() -> str:
    """Get the session_id for the current context."""
    return _current_session_id.get()


class SessionIdSpanProcessor(SpanProcessor):
    """SpanProcessor that automatically sets session.id on every span from context."""

    def on_start(self, span, parent_context=None):
        session_id = _current_session_id.get()
        if session_id:
            span.set_attribute("session.id", session_id)

    def on_end(self, span):
        pass

    def shutdown(self):
        pass

    def force_flush(self, timeout_millis=30000):
        return True


_resource = Resource.create(
    {
        SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", "trajectory-analytics-ai-app"),
        "service.version": os.getenv("OTEL_SERVICE_VERSION", "0.1.0"),
        "deployment.environment": os.getenv("DEPLOYMENT_ENV", "development"),
    }
)

_provider: TracerProvider | None = None


def init_tracer() -> trace.Tracer:
    """Initialize OpenTelemetry TracerProvider with OTLP + Pulsar exporters."""
    global _provider
    _provider = TracerProvider(resource=_resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

    # OTLP gRPC exporter (to collector/Jaeger/Tempo)
    otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    _provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

    # Pulsar exporter (direct to Pulsar for streaming pipeline)
    pulsar_exporter = PulsarSpanExporter(
        service_name=os.getenv("OTEL_SERVICE_NAME", "trajectory-analytics-ai-app"),
    )
    _provider.add_span_processor(BatchSpanProcessor(pulsar_exporter))

    # Session ID propagation processor (must be added first so it runs on_start before export)
    _provider.add_span_processor(SessionIdSpanProcessor())

    trace.set_tracer_provider(_provider)
    return trace.get_tracer("trajectory.ai.app")


def init_metrics() -> metrics.Meter:
    """Initialize OpenTelemetry MeterProvider with OTLP + Console exporters."""
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

    otlp_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True),
        export_interval_millis=10000,
    )

    provider = MeterProvider(
        resource=_resource, metric_readers=[otlp_reader]
    )
    metrics.set_meter_provider(provider)
    return metrics.get_meter("trajectory.ai.app")


def init_logging() -> logging.Logger:
    """Initialize OpenTelemetry LoggerProvider with OTLP export + structured logging."""
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

    logger_provider = LoggerProvider(resource=_resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=otlp_endpoint, insecure=True))
    )
    set_logger_provider(logger_provider)

    # Attach OTel handler to Python logging (INFO level to reduce noise)
    handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)

    # Configure structlog for nice dev output
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()


# --- Singleton instances ---
tracer: trace.Tracer | None = None
meter: metrics.Meter | None = None
logger = None


def get_tracer() -> trace.Tracer:
    """Get the initialized tracer (lazy-safe)."""
    global tracer
    if tracer is None:
        tracer = init_tracer()
    return tracer


def get_logger():
    """Get the initialized logger (lazy-safe)."""
    global logger
    if logger is None:
        logger = init_logging()
    return logger


def init_telemetry():
    """Initialize all telemetry signals."""
    global tracer, meter, logger
    tracer = init_tracer()
    meter = init_metrics()
    logger = init_logging()
    logger.info("telemetry.initialized")
    return tracer, meter, logger


def shutdown_telemetry():
    """Flush all pending telemetry and shut down providers."""
    global _provider
    if _provider:
        _provider.force_flush()
        _provider.shutdown()
    logging.getLogger("telemetry").info("Telemetry shut down. All spans flushed.")
