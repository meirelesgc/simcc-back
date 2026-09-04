import logging
import re
from typing import Optional

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import (
    ReadableSpan,
    SpanProcessor,
    TracerProvider,
)
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.sdk.trace.sampling import (
    ParentBased,
    TraceIdRatioBased,
)
from opentelemetry.util._once import Once  # noqa: PLC2701

from simcc.core.telemetry.config import TelemetryConfig, get_telemetry_config

_tracer_provider: Optional[TracerProvider] = None
_in_memory_exporter: Optional[InMemorySpanExporter] = None
_initialized: bool = False
logger = logging.getLogger('simcc.telemetry')


def create_telemetry_resource(config: TelemetryConfig) -> Resource:
    """Creates an OpenTelemetry Resource using Semantic Conventions."""
    return Resource.create(
        attributes={
            'service.name': config.service_name,
            'service.namespace': config.service_namespace,
            'service.version': config.service_version,
            'deployment.environment.name': config.environment,
        }
    )


class SanitizingSpanProcessor(SpanProcessor):
    """Ensures no sensitive data leaks into recorded spans."""

    def on_start(
        self, span: trace.Span, parent_context: Optional[object] = None
    ) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:  # noqa: PLR6301
        if not hasattr(span, '_attributes'):
            return

        target_dict = getattr(span._attributes, '_dict', None)
        if target_dict is None and isinstance(span._attributes, dict):
            target_dict = span._attributes
        if target_dict is None:
            return

        for key in ('prompt', 'response', 'user_query', 'full_prompt'):
            if key in target_dict:
                target_dict[key] = '[REDACTED]'

        stmt = target_dict.get('db.statement')
        if isinstance(stmt, str):
            sanitized = re.sub(r"'[^']*'", "'?'", stmt)
            sanitized = re.sub(r'\b\d+\b', '?', sanitized)
            target_dict['db.statement'] = sanitized

    def shutdown(self) -> None:
        pass

    def force_flush(  # noqa: PLR6301
        self, timeout_millis: int = 30000
    ) -> bool:
        return True


def setup_tracing(
    config: Optional[TelemetryConfig] = None,
) -> Optional[TracerProvider]:
    """Configures the TracerProvider, Resource, Sampler, and Processors."""
    global _tracer_provider, _in_memory_exporter, _initialized  # noqa: PLW0603

    if config is None:
        config = get_telemetry_config()

    if not config.enabled:
        return None

    resource = create_telemetry_resource(config)
    sampler = ParentBased(root=TraceIdRatioBased(config.sampling_ratio))
    provider = TracerProvider(resource=resource, sampler=sampler)
    provider.add_span_processor(SanitizingSpanProcessor())

    exporter_type = config.exporter_type

    if exporter_type == 'in_memory':
        _in_memory_exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(_in_memory_exporter))
    elif exporter_type == 'console':
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
    elif exporter_type == 'otlp':
        try:
            otlp_exporter = OTLPSpanExporter(
                endpoint=config.otlp_endpoint,
                insecure=config.otlp_insecure,
            )
            provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        except Exception as exc:
            logger.warning(
                f'Falha no OTLPSpanExporter ({exc}). Fallback para Console.'
            )
            provider.add_span_processor(
                SimpleSpanProcessor(ConsoleSpanExporter())
            )

    try:
        trace.set_tracer_provider(provider)
    except Exception:
        pass
    _tracer_provider = provider
    _initialized = True
    return provider


def get_tracer(instrumenting_module_name: str = 'simcc') -> trace.Tracer:
    """Returns an active tracer from configured or global provider."""
    if _tracer_provider is not None:
        return _tracer_provider.get_tracer(instrumenting_module_name)
    return trace.get_tracer(instrumenting_module_name)


def get_in_memory_span_exporter() -> Optional[InMemorySpanExporter]:
    """Returns InMemorySpanExporter when exporter_type is in_memory."""
    return _in_memory_exporter


def reset_tracing_for_tests() -> None:
    """Resets global tracing state for isolated test execution."""
    global _tracer_provider, _in_memory_exporter, _initialized  # noqa: PLW0603
    if _tracer_provider is not None:
        try:
            _tracer_provider.shutdown()
        except Exception:
            pass
    _tracer_provider = None
    _in_memory_exporter = None
    _initialized = False
    try:
        trace._TRACER_PROVIDER = None
        trace._TRACER_PROVIDER_SET_ONCE = Once()
    except Exception:
        pass
