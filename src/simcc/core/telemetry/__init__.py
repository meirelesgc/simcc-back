import logging
from typing import Any, Optional

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from simcc.core.telemetry.config import TelemetryConfig, get_telemetry_config
from simcc.core.telemetry.metrics import (
    record_ai_cache_hit,
    record_ai_error,
    record_ai_request,
    record_ai_stage_duration,
    record_ai_tokens,
    record_http_duration,
    setup_metrics,
)
from simcc.core.telemetry.tracing import (
    get_in_memory_span_exporter,
    get_tracer,
    setup_tracing,
)

logger = logging.getLogger('simcc.telemetry')
_instrumented = False


def init_telemetry(
    app: Optional[Any] = None,
    config: Optional[TelemetryConfig] = None,
) -> None:
    """Unified entry point to initialize OpenTelemetry."""
    global _instrumented  # noqa: PLW0603

    if config is None:
        config = get_telemetry_config()

    if not config.enabled:
        logger.info('OpenTelemetry está desabilitado por configuração.')
        return

    # 1. Setup Core Tracing and Metrics Providers
    tracer_provider = setup_tracing(config)
    meter_provider = setup_metrics(config)

    # 2. Instrument FastAPI if application instance is provided
    if app is not None and not getattr(app.state, '_otel_instrumented', False):
        try:
            FastAPIInstrumentor.instrument_app(
                app,
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
                excluded_urls='^/docs|^/redoc|^/openapi.json',
            )
            app.state._otel_instrumented = True
            logger.info('FastAPI instrumentado com sucesso no OpenTelemetry.')
        except Exception as exc:
            logger.warning(f'Não foi possível instrumentar o FastAPI: {exc}')

    # 3. Instrument External Libraries (HTTPX, Redis, SQLAlchemy)
    if not _instrumented:
        try:
            HTTPXClientInstrumentor().instrument()
        except Exception as exc:
            logger.debug(f'Instrumentação HTTPX ignorada: {exc}')

        try:
            RedisInstrumentor().instrument()
        except Exception as exc:
            logger.debug(f'Instrumentação Redis ignorada: {exc}')

        try:
            SQLAlchemyInstrumentor().instrument(
                enable_commenter=False,
            )
        except Exception as exc:
            logger.debug(f'Instrumentação SQLAlchemy ignorada: {exc}')

        _instrumented = True


__all__ = [
    'init_telemetry',
    'get_tracer',
    'get_in_memory_span_exporter',
    'record_http_duration',
    'record_ai_request',
    'record_ai_stage_duration',
    'record_ai_cache_hit',
    'record_ai_tokens',
    'record_ai_error',
    'TelemetryConfig',
    'get_telemetry_config',
]
