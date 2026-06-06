"""
Telemetry initialization module.
Sets up OpenTelemetry traces, metrics, and structured logging for the entire application.
"""

import os
import logging
from dotenv import load_dotenv

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
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

import structlog

load_dotenv()

_resource = Resource.create(
    {
        SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", "trajectory-analytics-ai-app"),
        "service.version": os.getenv("OTEL_SERVICE_VERSION", "0.1.0"),
        "deployment.environment": os.getenv("DEPLOYMENT_ENV", "development"),
    }
)


def init_tracer() -> trace.Tracer:
    """Initialize OpenTelemetry TracerProvider with OTLP + Console exporters."""
    provider = TracerProvider(resource=_resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

    # OTLP gRPC exporter (to collector/Jaeger/Tempo)
    otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

    trace.set_tracer_provider(provider)
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
