import logging
from typing import Optional

import opentelemetry.metrics._internal as m_internal  # noqa: PLC2701
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    InMemoryMetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.util._once import Once  # noqa: PLC2701

from simcc.core.telemetry.config import TelemetryConfig, get_telemetry_config
from simcc.core.telemetry.tracing import create_telemetry_resource

logger = logging.getLogger('simcc.telemetry.metrics')

_meter_provider: Optional[MeterProvider] = None
_in_memory_metric_reader: Optional[InMemoryMetricReader] = None
_meter: Optional[metrics.Meter] = None

# Metric instruments
_http_request_duration = None
_ai_requests = None
_ai_stage_duration = None
_ai_cache_hits = None
_ai_tokens = None
_ai_errors = None


def setup_metrics(
    config: Optional[TelemetryConfig] = None,
) -> Optional[MeterProvider]:
    """Configures MeterProvider, readers, and metric instruments."""
    global _meter_provider, _in_memory_metric_reader, _meter  # noqa: PLW0603
    global _http_request_duration, _ai_requests, _ai_stage_duration  # noqa: PLW0603
    global _ai_cache_hits, _ai_tokens, _ai_errors  # noqa: PLW0603

    if config is None:
        config = get_telemetry_config()

    if not config.enabled:
        return None

    resource = create_telemetry_resource(config)
    readers = []
    metrics_exporter = getattr(config, 'metrics_exporter_type', None)
    if not metrics_exporter:
        if config.exporter_type == 'in_memory':
            metrics_exporter = 'in_memory'
        else:
            metrics_exporter = 'none'

    if metrics_exporter == 'in_memory':
        _in_memory_metric_reader = InMemoryMetricReader()
        readers.append(_in_memory_metric_reader)
    elif metrics_exporter == 'console':
        readers.append(
            PeriodicExportingMetricReader(
                ConsoleMetricExporter(), export_interval_millis=60000
            )
        )
    elif metrics_exporter == 'otlp':
        try:
            readers.append(
                PeriodicExportingMetricReader(
                    OTLPMetricExporter(
                        endpoint=config.otlp_endpoint,
                        insecure=config.otlp_insecure,
                    ),
                    export_interval_millis=30000,
                )
            )
        except Exception as exc:
            logger.warning(
                f'Falha no OTLPMetricExporter ({exc}). Fallback para console.'
            )
            readers.append(
                PeriodicExportingMetricReader(
                    ConsoleMetricExporter(), export_interval_millis=60000
                )
            )

    provider = MeterProvider(resource=resource, metric_readers=readers)
    try:
        metrics.set_meter_provider(provider)
    except Exception:
        pass
    _meter_provider = provider

    _meter = provider.get_meter('simcc.metrics', '4.5.0')

    # Register instruments adhering to contracts/metrics_contract.md
    _http_request_duration = _meter.create_histogram(
        name='http.server.request.duration',
        unit='ms',
        description='Duração das requisições HTTP tratadas pelo servidor',
    )
    _ai_requests = _meter.create_counter(
        name='simcc.ai.requests',
        unit='1',
        description='Total de interações recebidas pela MarIA',
    )
    _ai_stage_duration = _meter.create_histogram(
        name='simcc.ai.stage.duration',
        unit='ms',
        description='Latência individual de cada estágio da MarIA',
    )
    _ai_cache_hits = _meter.create_counter(
        name='simcc.ai.cache_hits',
        unit='1',
        description='Contagem de requisições atendidas no Redis',
    )
    _ai_tokens = _meter.create_counter(
        name='simcc.ai.tokens',
        unit='{tokens}',
        description='Quantidade de tokens de LLM consumidos',
    )
    _ai_errors = _meter.create_counter(
        name='simcc.ai.errors',
        unit='1',
        description='Total de falhas capturadas na MarIA',
    )

    return provider


def get_in_memory_metric_reader() -> Optional[InMemoryMetricReader]:
    """Returns the InMemoryMetricReader instance for testing."""
    return _in_memory_metric_reader


def record_http_duration(
    method: str,
    route: str,
    status_code: int,
    duration_ms: float,
    environment: str = 'development',
) -> None:
    """Records HTTP request duration histogram with low cardinality attrs."""
    if _http_request_duration:
        _http_request_duration.record(
            duration_ms,
            {
                'http.request.method': method.upper(),
                'http.route': route,
                'http.response.status_code': status_code,
                'environment': environment,
            },
        )


def record_ai_request(
    model: str,
    intent: str,
    cache_hit: bool,
    status: str = 'success',
) -> None:
    """Records a MarIA interaction counter."""
    if _ai_requests:
        _ai_requests.add(
            1,
            {
                'model': model,
                'intent': intent,
                'cache_hit': str(cache_hit).lower(),
                'status': status,
            },
        )


def record_ai_stage_duration(
    stage: str,
    status: str,
    duration_ms: float,
) -> None:
    """Records stage duration histogram for MarIA pipeline."""
    if _ai_stage_duration:
        _ai_stage_duration.record(
            duration_ms,
            {
                'stage': stage,
                'status': status,
            },
        )


def record_ai_cache_hit(endpoint: str = '/ai/chat/ask') -> None:
    """Records cache hit counter for MarIA."""
    if _ai_cache_hits:
        _ai_cache_hits.add(
            1,
            {
                'endpoint': endpoint,
                'status': 'hit',
            },
        )


def record_ai_tokens(model: str, token_type: str, count: int) -> None:
    """Records consumed tokens counter."""
    if _ai_tokens and count > 0:
        _ai_tokens.add(
            count,
            {
                'model': model,
                'type': token_type,
            },
        )


def record_ai_error(stage: str, error_type: str) -> None:
    """Records an error counter in the MarIA pipeline."""
    if _ai_errors:
        _ai_errors.add(
            1,
            {
                'stage': stage,
                'error_type': error_type,
            },
        )


def reset_metrics_for_tests() -> None:
    """Resets global metric state for isolated tests."""
    global _meter_provider, _in_memory_metric_reader, _meter  # noqa: PLW0603
    global _http_request_duration, _ai_requests, _ai_stage_duration  # noqa: PLW0603
    global _ai_cache_hits, _ai_tokens, _ai_errors  # noqa: PLW0603
    if _meter_provider is not None:
        try:
            _meter_provider.shutdown()
        except Exception:
            pass
    _meter_provider = None
    _in_memory_metric_reader = None
    _meter = None
    _http_request_duration = None
    _ai_requests = None
    _ai_stage_duration = None
    _ai_cache_hits = None
    _ai_tokens = None
    _ai_errors = None
    try:
        m_internal._METER_PROVIDER = None
        m_internal._METER_PROVIDER_SET_ONCE = Once()
    except Exception:
        pass
