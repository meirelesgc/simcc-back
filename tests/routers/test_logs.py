import pytest
from starlette.websockets import WebSocketDisconnect
from simcc.routers.logs import FALLBACK_TOKEN, MAX_CONCURRENT_CONNECTIONS
from simcc.core.logging import logger

def test_websocket_logs_unauthorized(client):
    # Connecting without a token must be rejected with 4003
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect('/logs/stream'):
            pass
    assert exc_info.value.code == 4003

    # Connecting with an invalid token must be rejected with 4003
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect('/logs/stream?token=invalid_token'):
            pass
    assert exc_info.value.code == 4003

def test_websocket_logs_authorized_and_stream(client):
    # Connecting with correct token must succeed
    with client.websocket_connect(f'/logs/stream?token={FALLBACK_TOKEN}') as websocket:
        # Trigger a test log dispatch
        logger.info("Test WebSocket Log Message", extra={"test_key": "test_value"})
        
        # Read the message from the WebSocket
        data = websocket.receive_json()
        assert data is not None
        assert data.get("message") == "Test WebSocket Log Message"

def test_websocket_logs_concurrent_limit(client):
    connections = []
    try:
        # Open up to MAX_CONCURRENT_CONNECTIONS connections
        for _ in range(MAX_CONCURRENT_CONNECTIONS):
            conn = client.websocket_connect(f'/logs/stream?token={FALLBACK_TOKEN}')
            conn.__enter__()
            connections.append(conn)
            
        # The next connection attempt must be rejected with 4008
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect(f'/logs/stream?token={FALLBACK_TOKEN}'):
                pass
        assert exc_info.value.code == 4008
    finally:
        # Clean up open connections
        for conn in connections:
            conn.__exit__(None, None, None)
