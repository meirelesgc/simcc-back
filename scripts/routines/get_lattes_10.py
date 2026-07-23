import argparse
import ssl
from http import HTTPStatus

import httpx
from sqlalchemy import text

from simcc.core.db.database import get_sync_session


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


items_found = 0
items_succeeded = 0
items_failed = 0


def main(researcher_ids=None, lattes_ids=None):
    global items_found, items_succeeded, items_failed
    session = next(get_sync_session())

    try:
        if researcher_ids or lattes_ids:
            base_query = """
                SELECT id AS researcher_id, lattes_id
                FROM researcher
                WHERE 1=1
            """
            params = {}
            if researcher_ids:
                base_query += ' AND id = ANY(:researcher_ids)'
                params['researcher_ids'] = list(researcher_ids)
            if lattes_ids:
                base_query += ' AND lattes_id = ANY(:lattes_ids)'
                params['lattes_ids'] = list(lattes_ids)
            researchers = (
                session.execute(text(base_query), params).mappings().all()
            )
        else:
            query_select = text("""
                SELECT id AS researcher_id, lattes_id
                FROM researcher
                WHERE lattes_10_id IS NULL;
            """)
            researchers = session.execute(query_select).mappings().all()

        items_found = len(researchers)
        success = 0
        failed = 0

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
            if lattes_10_id:
                success += 1
            else:
                failed += 1

        session.commit()
        items_succeeded = success
        items_failed = failed
    except Exception as E:
        items_succeeded = 0
        items_failed = items_found
        print(E)
        session.rollback()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--researcher-ids',
        nargs='+',
        type=str,
        default=None,
    )
    parser.add_argument(
        '--lattes-ids',
        nargs='+',
        type=str,
        default=None,
    )
    args = parser.parse_args()

    main(researcher_ids=args.researcher_ids, lattes_ids=args.lattes_ids)
