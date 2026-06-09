from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_get_researcher_image_not_found(client):
    response = client.get(
        '/researcher/image', params={'researcher_id': str(uuid4())}
    )
    # Since download_researcher_image is a placeholder and doesn't create the file, it should be 404
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_researcher_image_bad_request(client):
    response = client.get('/researcher/image')
    assert response.status_code == 400
    assert response.json()['detail'] == 'Parâmetro obrigatório não informado'
