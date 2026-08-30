from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from simcc.core.utils import DEFAULT_AVATAR_PATH, download_researcher_image
from simcc.services.researcher_service import get_researcher_image_path


@pytest.mark.asyncio
async def test_get_researcher_image_path_returns_existing(tmp_path):
    researcher_id = 'test-researcher-uuid'
    fake_img = tmp_path / f'{researcher_id}.jpg'
    fake_img.write_text('fake image data')

    session = AsyncMock()

    with patch('simcc.services.researcher_service.Path') as mock_path:
        mock_path.return_value = fake_img
        result = await get_researcher_image_path(session, researcher_id)
        assert result == str(fake_img)


@pytest.mark.asyncio
async def test_get_researcher_image_path_fallback_when_not_found(tmp_path):
    researcher_id = 'test-researcher-uuid'
    fake_img = tmp_path / f'{researcher_id}.jpg'  # doesn't exist

    session = AsyncMock()

    with (
        patch('simcc.services.researcher_service.Path') as mock_path,
        patch(
            'simcc.services.researcher_service.download_researcher_image',
            new_callable=AsyncMock,
        ),
    ):
        mock_path.return_value = fake_img
        result = await get_researcher_image_path(session, researcher_id)
        assert result == str(DEFAULT_AVATAR_PATH)
        assert Path(result).exists()


@pytest.mark.asyncio
async def test_download_researcher_image_ignores_invalid_lattes_10():
    session = AsyncMock()
    # Mock database returning invalid lattes_10_id (e.g. '/erro.jsp' or None)
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = '/erro.jsp'
    session.execute.return_value = mock_result

    with patch('simcc.core.utils.httpx.AsyncClient') as mock_client:
        await download_researcher_image('fake-id', session=session)
        # Should not attempt HTTP call
        mock_client.assert_not_called()
