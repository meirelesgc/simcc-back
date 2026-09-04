import pytest

from simcc.core.telemetry.config import TelemetryConfig
from simcc.core.telemetry.metrics import (
    get_in_memory_metric_reader,
    record_ai_cache_hit,
    record_ai_request,
    record_ai_stage_duration,
    record_ai_tokens,
    record_http_duration,
    reset_metrics_for_tests,
    setup_metrics,
)


@pytest.fixture(autouse=True)
def setup_metrics_in_memory():
    reset_metrics_for_tests()
    config = TelemetryConfig(
        enabled=True,
        exporter_type='in_memory',
        otlp_endpoint='http://localhost:4317',
        otlp_insecure=True,
        sampling_ratio=1.0,
        service_name='simcc-metrics-test',
        service_namespace='simcc',
        service_version='1.0.0',
        environment='test',
    )
    setup_metrics(config)
    yield
    reset_metrics_for_tests()


@pytest.mark.unit
def test_metrics_collection_and_cardinality():
    reader = get_in_memory_metric_reader()
    assert reader is not None

    record_http_duration(
        method='POST',
        route='/ai/chat/ask',
        status_code=200,
        duration_ms=250.0,
        environment='test',
    )

    record_ai_request(
        model='gpt-4o-mini',
        intent='researcher_search',
        cache_hit=False,
        status='success',
    )

    record_ai_stage_duration(
        stage='planner',
        status='success',
        duration_ms=45.0,
    )

    record_ai_cache_hit('/ai/chat/ask')
    record_ai_tokens('gpt-4o-mini', 'input', 500)

    metric_data = reader.get_metrics_data()
    assert metric_data is not None

    metric_names = [
        rm.name
        for sm in metric_data.resource_metrics
        for scope in sm.scope_metrics
        for rm in scope.metrics
    ]
    assert 'http.server.request.duration' in metric_names
    assert 'simcc.ai.requests' in metric_names
    assert 'simcc.ai.stage.duration' in metric_names
    assert 'simcc.ai.cache_hits' in metric_names
    assert 'simcc.ai.tokens' in metric_names

    # Verify no prohibited high-cardinality keys in any data point
    for sm in metric_data.resource_metrics:
        for scope in sm.scope_metrics:
            for metric in scope.metrics:
                for dp in metric.data.data_points:
                    dp_attrs = dict(dp.attributes)
                    assert 'request_id' not in dp_attrs
                    assert 'user_id' not in dp_attrs
                    assert 'query' not in dp_attrs
                    assert 'trace_id' not in dp_attrs
