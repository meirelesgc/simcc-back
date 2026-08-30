import re
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from simcc.core.db.model import Researcher

CNPQ_IMAGE_URL = (
    'http://servicosweb.cnpq.br/wspessoa/servletrecuperafoto?tipo=1&id='
)

DEFAULT_AVATAR_PATH = (
    Path(__file__).resolve().parent.parent / 'static' / 'images' / 'default_avatar.png'
)

LATTES_10_PATTERN = re.compile(r'^[A-Za-z0-9]{10}$')


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

    if not lattes_10_id or not LATTES_10_PATTERN.match(lattes_10_id):
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
    except Exception:
        pass

