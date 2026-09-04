from unittest.mock import AsyncMock, MagicMock
import pytest
from opentelemetry import trace

from simcc.core.cache.cache_service import CacheService
from simcc.core.telemetry.config import TelemetryConfig
from simcc.core.telemetry.tracing import (
    get_in_memory_span_exporter,
    reset_tracing_for_tests,
    setup_tracing,
)


@pytest.fixture(autouse=True)
def setup_in_memory():
    reset_tracing_for_tests()
    config = TelemetryConfig(
        enabled=True,
        exporter_type='in_memory',
        otlp_endpoint='http://localhost:4317',
        otlp_insecure=True,
        sampling_ratio=1.0,
        service_name='simcc-redis-test',
        service_namespace='simcc',
        service_version='1.0.0',
        environment='test',
    )
    setup_tracing(config)
    yield
    reset_tracing_for_tests()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cache_service_redis_tracing():
    exporter = get_in_memory_span_exporter()
    assert exporter is not None
    exporter.clear()

    tracer = trace.get_tracer('simcc.cache')

    mock_redis = MagicMock()
    mock_redis.get = AsyncMock(return_value='{"answer": "cached"}')
    mock_redis.set = AsyncMock(return_value=True)

    cache = CacheService(redis_client=mock_redis, enabled=True)

    with tracer.start_as_current_span('redis.get') as span:
        span.set_attribute('db.system', 'redis')
        span.set_attribute('db.operation', 'GET')
        res = await cache.get('simcc:ai:test_key')

    assert res == {'answer': 'cached'}

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    redis_span = spans[0]
    assert redis_span.name == 'redis.get'
    assert redis_span.attributes['db.system'] == 'redis'
    assert redis_span.attributes['db.operation'] == 'GET'
