from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from simcc import app
from simcc.ai.dependencies import get_embeddings_provider, get_llm_provider


@pytest.mark.asyncio
async def test_router_maria_researcher_search(client):
    # Mock providers to avoid OpenAI connection
    app.dependency_overrides[get_llm_provider] = lambda: MagicMock()
    app.dependency_overrides[get_embeddings_provider] = lambda: MagicMock()

    # Mock the service to avoid OpenAI calls
    mock_response = {'query': 'Test comment', 'researchers': []}

    with patch(
        'simcc.services.maria_service.MariaService.search_and_summarize',
        new_callable=AsyncMock,
    ) as mock_method:
        mock_method.return_value = mock_response

        response = client.get(
            '/maria/researcher/abstract', params={'query': 'test'}
        )

        assert response.status_code == 200
        assert response.json()['query'] == 'Test comment'

    app.dependency_overrides.clear()
