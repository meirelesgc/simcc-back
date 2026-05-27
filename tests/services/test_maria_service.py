import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from simcc.services.maria_service import MariaService


@pytest.mark.asyncio
async def test_search_and_summarize():
    # Mock dependencies
    llm = MagicMock()
    llm.generate = AsyncMock(return_value='Maria summary')

    embeddings = MagicMock()
    embeddings.get_embeddings = AsyncMock(return_value=[0.1, 0.2])

    session = AsyncMock()

    u1, u2 = uuid.uuid4(), uuid.uuid4()

    with (
        patch(
            'simcc.repositories.maria_repo.search_by_embeddings',
            new_callable=AsyncMock,
        ) as mock_search,
        patch(
            'simcc.repositories.researcher_repo.search_researchers',
            new_callable=AsyncMock,
        ) as mock_fetch,
    ):
        mock_search.return_value = [u1, u2]
        mock_fetch.return_value = [
            {
                'id': u1,
                'name': 'Researcher 1',
                'lattes_id': '1',
                'lattes_10_id': '10',
            },
            {
                'id': u2,
                'name': 'Researcher 2',
                'lattes_id': '2',
                'lattes_10_id': '20',
            },
        ]

        service = MariaService(llm, embeddings)

        response = await service.search_and_summarize(
            session, 'AI in medicine', 'abstract'
        )

        assert response.query == 'Maria summary'
        assert len(response.researchers) == 2

        llm.generate.assert_called_once()
        embeddings.get_embeddings.assert_called_once_with('AI in medicine')
        mock_search.assert_called_once()
        mock_fetch.assert_called_once()


@pytest.mark.asyncio
async def test_generate_search_summary_researcher_optimized():
    # Mock dependencies
    llm = MagicMock()
    llm.generate = AsyncMock(return_value='Summary result')
    embeddings = MagicMock()
    service = MariaService(llm, embeddings)

    session = AsyncMock()
    from simcc.schemas import DefaultFilters

    filters = DefaultFilters(type='NAME', term='Maria')

    # Mock researcher data (more than 5)
    mock_data = [
        {
            'id': uuid.uuid4(),
            'name': f'Researcher {i}',
            'university': 'UFMG',
            'abstract': 'A very long abstract ' * 50,
            'articles': 10,
            'h_index': 5,
            'lattes_id': f'{i}',
            'lattes_10_id': f'10{i}',
            'extra_field': 'should be removed',
        }
        for i in range(10)
    ]

    with patch(
        'simcc.services.researcher_service.search_researchers',
        new_callable=AsyncMock,
    ) as mock_search:
        mock_search.return_value = mock_data

        result = await service.generate_search_summary(session, filters)

        assert result == 'Summary result'
        assert filters.lenght == 5
        mock_search.assert_called_once()

        # Check what was sent to LLM
        llm.generate.assert_called_once()
        prompt_sent = llm.generate.call_args[0][0]

        # Should contain at most 5 researchers
        assert 'Researcher 0' in prompt_sent
        assert 'Researcher 4' in prompt_sent
        assert 'Researcher 5' not in prompt_sent

        # Should not contain extra_field
        assert 'should be removed' not in prompt_sent

        # Should contain truncated abstract
        assert 'A very long abstract' in prompt_sent
        assert len(prompt_sent) < 5000  # Crude check for size reduction


@pytest.mark.asyncio
async def test_search_and_summarize_optimized():
    # Mock dependencies
    llm = MagicMock()
    llm.generate = AsyncMock(return_value='Summary result')
    embeddings = MagicMock()
    embeddings.get_embeddings = AsyncMock(return_value=[0.1, 0.2])
    service = MariaService(llm, embeddings)

    session = AsyncMock()

    # Mock researcher data (more than 5)
    mock_data = [
        {
            'id': uuid.uuid4(),
            'name': f'Researcher {i}',
            'university': 'UFMG',
            'abstract': 'A very long abstract ' * 50,
            'articles': 10,
            'h_index': 5,
            'lattes_id': f'{i}',
            'lattes_10_id': f'10{i}',
            'extra_field': 'should be removed',
        }
        for i in range(10)
    ]

    with (
        patch(
            'simcc.repositories.maria_repo.search_by_embeddings',
            new_callable=AsyncMock,
        ) as mock_search,
        patch(
            'simcc.repositories.researcher_repo.search_researchers',
            new_callable=AsyncMock,
        ) as mock_fetch,
    ):
        mock_search.return_value = [r['id'] for r in mock_data]
        mock_fetch.return_value = mock_data

        response = await service.search_and_summarize(
            session, 'AI', 'abstract'
        )

        assert response.query == 'Summary result'
        assert (
            len(response.researchers) == 10
        )  # Returns all researchers to user

        # Check what was sent to LLM
        llm.generate.assert_called_once()
        prompt_sent = llm.generate.call_args[0][0]

        # Should contain at most 5 researchers in prompt
        assert 'Researcher 0' in prompt_sent
        assert 'Researcher 4' in prompt_sent
        assert 'Researcher 5' not in prompt_sent

        # Should not contain extra_field
        assert 'should be removed' not in prompt_sent
