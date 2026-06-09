import os
from pathlib import Path

import pytest
import respx

from simcc.core.db.model import Researcher
from simcc.core.utils import download_researcher_image


@pytest.mark.asyncio
async def test_download_researcher_image_success(session):
    lattes_10_id = '1234567890'

    # Clean up any existing researcher with this lattes_10_id to avoid IntegrityError in repeated runs
    from sqlalchemy import delete
    await session.execute(
        delete(Researcher).where(Researcher.lattes_10_id == lattes_10_id)
    )

    # Insert a researcher into the test database
    researcher = Researcher(name='Test Researcher', lattes_10_id=lattes_10_id)
    session.add(researcher)
    await session.commit()
    await session.refresh(researcher)

    researcher_id = researcher.id

    # Mock the CNPq response
    url = f'http://servicosweb.cnpq.br/wspessoa/servletrecuperafoto?tipo=1&id={lattes_10_id}'
    with respx.mock:
        respx.get(url).respond(status_code=200, content=b'fake image content')

        # Ensure the file doesn't exist before
        image_path = Path(f'storage/image_researcher/{researcher_id}.jpg')
        if image_path.exists():
            os.remove(image_path)

        await download_researcher_image(str(researcher_id), session=session)

        assert image_path.exists()
        assert image_path.read_bytes() == b'fake image content'

        # Cleanup
        os.remove(image_path)


@pytest.mark.asyncio
async def test_download_researcher_image_no_lattes_id(session):
    # Insert a researcher without lattes_10_id
    researcher = Researcher(
        name='Test Researcher No Lattes', lattes_10_id=None
    )
    session.add(researcher)
    await session.commit()
    await session.refresh(researcher)

    researcher_id = researcher.id

    image_path = Path(f'storage/image_researcher/{researcher_id}.jpg')
    if image_path.exists():
        os.remove(image_path)

    await download_researcher_image(str(researcher_id), session=session)

    assert not image_path.exists()


@pytest.mark.asyncio
async def test_download_researcher_image_http_error(session):
    lattes_10_id = '0987654321'

    # Clean up any existing researcher with this lattes_10_id
    from sqlalchemy import delete
    await session.execute(
        delete(Researcher).where(Researcher.lattes_10_id == lattes_10_id)
    )

    researcher = Researcher(
        name='Test Researcher HTTP Error', lattes_10_id=lattes_10_id
    )
    session.add(researcher)
    await session.commit()
    await session.refresh(researcher)

    researcher_id = researcher.id

    url = f'http://servicosweb.cnpq.br/wspessoa/servletrecuperafoto?tipo=1&id={lattes_10_id}'
    with respx.mock:
        respx.get(url).respond(status_code=404)

        image_path = Path(f'storage/image_researcher/{researcher_id}.jpg')
        if image_path.exists():
            os.remove(image_path)

        await download_researcher_image(str(researcher_id), session=session)

        assert not image_path.exists()
