import argparse
import ssl
import time
from http import HTTPStatus

import httpx
from sqlalchemy import text

from simcc.core.db.database import get_sync_session
from simcc.core.logging import get_logger

logger = get_logger('routines')


def create_legacy_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.set_ciphers('DEFAULT@SECLEVEL=1')
    return context


def get_lattes_id_10(lattes_id: str) -> str:
    URL = f'https://buscatextual.cnpq.br/buscatextual/cv?id={lattes_id}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
    }

    ssl_context = create_legacy_ssl_context()

    with httpx.Client(
        follow_redirects=False, headers=headers, verify=ssl_context
    ) as client:
        response = client.get(URL, timeout=30.0)

        if response.status_code == HTTPStatus.FOUND:
            return response.headers.get('Location')[-10:]
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--researcher-id',
        type=str,
        default=None,
        help='UUID do pesquisador a processar (opcional)',
    )
    args = parser.parse_args()

    session = next(get_sync_session())
    start_time = time.perf_counter()
    logger.info('lattes_10_routine_started')

    try:
        if args.researcher_id:
            query_select = text("""
                SELECT id AS researcher_id, lattes_id
                FROM researcher
                WHERE id = :researcher_id;
            """)
            researchers = (
                session
                .execute(query_select, {'researcher_id': args.researcher_id})
                .mappings()
                .all()
            )
        else:
            query_select = text("""
                SELECT id AS researcher_id, lattes_id
                FROM researcher
                WHERE lattes_10_id IS NULL;
            """)
            researchers = session.execute(query_select).mappings().all()

        query_update = text("""
            UPDATE researcher
            SET lattes_10_id = :lattes_10_id
            WHERE id = :id;
        """)

        for researcher in researchers:
            lattes_10_id = get_lattes_id_10(researcher['lattes_id'])

            session.execute(
                query_update,
                {
                    'id': researcher['researcher_id'],
                    'lattes_10_id': lattes_10_id,
                },
            )

        session.commit()
        duration = time.perf_counter() - start_time
        logger.info(
            'lattes_10_routine_finished_successfully',
            duration=f'{duration:.2f}s',
        )
    except Exception as e:
        session.rollback()
        duration = time.perf_counter() - start_time
        logger.error(
            'lattes_10_routine_failed',
            error=str(e),
            duration=f'{duration:.2f}s',
        )


if __name__ == '__main__':
    main()
