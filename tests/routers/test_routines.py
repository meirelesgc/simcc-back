from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from fastapi import status

from simcc.core.settings import Settings

SETTINGS = Settings()


@pytest.mark.asyncio
async def test_trigger_researcher_routines_unauthorized(client):
    researcher_id = uuid4()
    response = client.post(
        f'/routines/researcher/{researcher_id}',
        headers={'X-Internal-Key': 'invalid_key'},
    )
    assert response.status_code == status.HTTP_412_PRECONDITION_FAILED or response.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.asyncio
async def test_trigger_researcher_routines_success(client):
    researcher_id = uuid4()
    from simcc.routers.routines import SETTINGS as ROUTER_SETTINGS
    key = ROUTER_SETTINGS.INTERNAL_API_KEY or 'test_internal_key'
    
    # Temporarily set INTERNAL_API_KEY to a known value if not configured
    with patch.object(ROUTER_SETTINGS, 'INTERNAL_API_KEY', key):
        with patch(
            'simcc.services.routines_service.run_researcher_routines',
            new_callable=AsyncMock,
        ) as mock_run:
            response = client.post(
                f'/routines/researcher/{researcher_id}',
                headers={'x-internal-key': key},
            )
            assert response.status_code == status.HTTP_202_ACCEPTED
            assert response.json() == {'researcher_id': str(researcher_id)}
            # Background task calls the service
            # (In testclient, background tasks run synchronously when the response returns)
            mock_run.assert_called_once_with(str(researcher_id))
