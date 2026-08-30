from http import HTTPStatus


def test_chat_ask_endpoint_success(client):
    # Arrange
    payload = {
        'query': 'Quais pesquisadores trabalham com inteligência artificial?',
        'session_id': 'test_session_123',
    }

    # Act
    response = client.post('/ai/chat/ask', json=payload)

    # Assert
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert 'answer' in data
    assert data['intent'] == 'researcher_search'
    assert isinstance(data['researchers'], list)
    assert isinstance(data['sources'], list)


def test_chat_ask_endpoint_invalid_payload(client):
    # Arrange
    invalid_payload = {'session_id': 'missing_query_field'}

    # Act
    response = client.post('/ai/chat/ask', json=invalid_payload)

    # Assert
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
