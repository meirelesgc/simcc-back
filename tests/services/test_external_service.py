import pytest
from unittest.mock import AsyncMock, patch
from simcc.services import external_service

@pytest.mark.asyncio
async def test_list_words_service():
    session = AsyncMock()
    term = "test"
    mock_results = [{"word": "testing", "freq": 10}]
    
    with patch("simcc.repositories.external_repo.list_words", new_callable=AsyncMock) as mock_repo_list:
        mock_repo_list.return_value = mock_results
        
        results = await external_service.list_words(session, term)
        
        assert results == mock_results
        mock_repo_list.assert_called_once()
        # Verify stopwords were passed
        args, kwargs = mock_repo_list.call_args
        assert isinstance(args[2], list)
        assert len(args[2]) > 0
