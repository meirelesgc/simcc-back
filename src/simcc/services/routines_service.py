import asyncio
import sys
from http import HTTPStatus
from pathlib import Path
from urllib.parse import urlparse

import httpx

from simcc.core.logging import get_logger
from simcc.core.settings import Settings

# Ensure project root is in sys.path so we can import from scripts.routines
project_root = str(Path(__file__).resolve().parents[3])
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from scripts.routines import (  # noqa: E402
    abstract_ai,
    get_lattes_10,
    pog,
    researcher_classification,
    researcher_indprod,
    researcher_production,
    soap_lattes,
)

logger = get_logger('routines')
SETTINGS = Settings()


def get_hop_url() -> str:
    # Deduce the host name of the hop container based on the DB host name
    parsed = urlparse(SETTINGS.DATABASE_URL)
    db_host = parsed.hostname or 'db'

    if db_host == 'db':
        hop_host = 'hop'
    elif db_host.endswith('_db'):
        prefix = db_host[:-3]  # Remove '_db'
        hop_host = f'{prefix}_hop'
    else:
        hop_host = 'hop'

    return f'http://{hop_host}:8080/hop/execWorkflow'


def to_jdbc_url(db_url: str) -> str:
    parsed = urlparse(db_url)
    scheme = parsed.scheme.split('+')[0]
    user = parsed.username or 'postgres'
    password = parsed.password or 'postgres'
    host = parsed.hostname or 'db'
    port = parsed.port or 5432
    db_name = parsed.path.lstrip('/')

    return (
        f'jdbc:{scheme}://{host}:{port}/{db_name}'
        f'?user={user}&password={password}'
    )


async def run_hop() -> bool:
    hop_url = get_hop_url()

    params = {
        'workflow': '/files/jade-extrator/workflows/Index.hwf',
        'DATABASE_URL': to_jdbc_url(SETTINGS.DATABASE_URL),
        'ADMIN_DATABASE_URL': to_jdbc_url(SETTINGS.ADMIN_DATABASE_URL),
    }

    logger.info('triggering_hop_workflow', hop_url=hop_url, params=params)

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(hop_url, params=params, timeout=600.0)
    except Exception as e:
        logger.error('hop_workflow_error', error=str(e))
        return False

    if response.status_code == HTTPStatus.OK:
        logger.info('hop_workflow_success')
        return True

    logger.error(
        'hop_workflow_failed',
        status_code=response.status_code,
        response=response.text,
    )
    return False


async def run_researcher_routines(researcher_id: str) -> None:
    logger.info('researcher_routines_started', researcher_id=researcher_id)

    # 1. Download/sync SOAP Lattes XMLs
    await asyncio.to_thread(soap_lattes.main, researcher_ids=[researcher_id])

    # 2. Run Hop docker container
    await run_hop()

    # 3. Apply post-hop DB adjustments / fixes (POG)
    await asyncio.to_thread(pog.main)

    # 4. Process researcher production aggregation
    await asyncio.to_thread(
        researcher_production.main, researcher_ids=[researcher_id]
    )

    # 5. Extract/find Lattes ID 10
    await asyncio.to_thread(get_lattes_10.main, researcher_ids=[researcher_id])

    # 6. Process researcher individual production metrics
    await asyncio.to_thread(
        researcher_indprod.main, researcher_ids=[researcher_id]
    )

    # 7. Generate abstract using AI if configured
    if SETTINGS.OPENAI_API_KEY:
        await asyncio.to_thread(
            abstract_ai.main, researcher_ids=[researcher_id]
        )

    # 8. Re-classify researcher profile
    await asyncio.to_thread(
        researcher_classification.main, researcher_ids=[researcher_id]
    )

    logger.info('researcher_routines_finished', researcher_id=researcher_id)
