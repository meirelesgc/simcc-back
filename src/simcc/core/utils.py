from pathlib import Path

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from simcc.core.db.model import Researcher

logger = structlog.get_logger(__name__)

CNPQ_IMAGE_URL = (
    'http://servicosweb.cnpq.br/wspessoa/servletrecuperafoto?tipo=1&id='
)


async def download_researcher_image(
    researcher_id: str, session: AsyncSession | None = None
):
    path = Path(f'storage/image_researcher/{researcher_id}.jpg')

    if path.exists() or not session:
        return

    # Busca o lattes_10_id do pesquisador
    query = select(Researcher.lattes_10_id).where(
        Researcher.id == researcher_id
    )
    result = await session.execute(query)
    lattes_10_id = result.scalar_one_or_none()

    if not lattes_10_id:
        logger.warning(
            'researcher_lattes_10_id_missing',
            researcher_id=researcher_id,
        )
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    url = f'{CNPQ_IMAGE_URL}{lattes_10_id}'

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)

            if response.status_code == 200:
                # CNPq pode retornar 200 com conteúdo vazio ou erro HTML dependendo do caso
                if (
                    len(response.content) > 0
                    and b'html' not in response.content[:100].lower()
                ):
                    path.write_bytes(response.content)
                    logger.info(
                        'researcher_image_downloaded',
                        researcher_id=researcher_id,
                        lattes_10_id=lattes_10_id,
                    )
                else:
                    logger.warning(
                        'researcher_image_invalid_content',
                        researcher_id=researcher_id,
                        lattes_10_id=lattes_10_id,
                    )
            else:
                logger.warning(
                    'researcher_image_download_failed',
                    researcher_id=researcher_id,
                    status_code=response.status_code,
                )
    except Exception as e:
        logger.error(
            'researcher_image_download_error',
            researcher_id=researcher_id,
            error=str(e),
        )
