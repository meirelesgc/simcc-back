import json
from http import HTTPStatus

from simcc import app
from simcc.ai.dependencies import get_llm_provider
from simcc.ai.providers.base import LLMProvider


def test_chat_ask_stream_endpoint_success(client):
    # Arrange
    payload = {
        'query': 'Quais pesquisadores trabalham com inteligência artificial?',
        'session_id': 'msg_custom_id_123',
    }

    # Act
    response = client.post('/ai/chat/ask/stream', json=payload)

    # Assert
    assert response.status_code == HTTPStatus.OK
    assert 'text/event-stream' in response.headers['content-type']
    assert response.headers.get('x-accel-buffering') == 'no'
    assert 'no-cache' in response.headers.get('cache-control', '')

    # Parse SSE events
    lines = response.text.strip().split('\n\n')
    events = []
    for block in lines:
        for line in block.split('\n'):
            if line.startswith('data: '):
                events.append(json.loads(line[6:]))

    assert len(events) >= 3  # metadata + deltas + done

    # 1. Metadata Event
    first_event = events[0]
    assert first_event['type'] == 'metadata'
    assert first_event['message_id'] == 'msg_custom_id_123'
    assert 'data' in first_event
    assert first_event['data']['intent'] == 'researcher_search'
    assert 'filters' in first_event['data']
    assert 'sources' in first_event['data']

    # 2. Delta Events
    delta_events = [e for e in events if e['type'] == 'delta']
    assert len(delta_events) > 0
    concatenated_text = ''.join(e['content'] for e in delta_events)
    assert 'Resposta simulada' in concatenated_text
    for d in delta_events:
        assert d['message_id'] == 'msg_custom_id_123'

    # 3. Done Event
    last_event = events[-1]
    assert last_event['type'] == 'done'
    assert last_event['message_id'] == 'msg_custom_id_123'


def test_chat_ask_stream_error_handling(client):
    # Arrange: LLMProvider que lança exceção no streaming
    class FailingLLMProvider(LLMProvider):
        async def generate(self, prompt: str, **kwargs) -> str:
            raise RuntimeError('Falha simulada no LLM')

        async def generate_stream(self, prompt: str, **kwargs):
            raise RuntimeError('Falha simulada no stream')
            yield 'never'

    app.dependency_overrides[get_llm_provider] = FailingLLMProvider

    try:
        payload = {'query': 'Pergunta que falha'}
        response = client.post('/ai/chat/ask/stream', json=payload)

        assert response.status_code == HTTPStatus.OK
        lines = response.text.strip().split('\n\n')
        events = []
        for block in lines:
            for line in block.split('\n'):
                if line.startswith('data: '):
                    events.append(json.loads(line[6:]))

        error_events = [e for e in events if e['type'] == 'error']
        assert len(error_events) == 1
        assert error_events[0]['code'] == 'generation_failed'
    finally:
        # Restaura overrides
        app.dependency_overrides.pop(get_llm_provider, None)
