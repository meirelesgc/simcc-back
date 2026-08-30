import argparse
import re
import ssl
from http import HTTPStatus
from urllib.parse import parse_qs, urlparse

import httpx
from sqlalchemy import text

from simcc.core.db.database import get_sync_session
from simcc.core.logging import logger
from simcc.core.logging.events import (
    routine_item_error,
    routine_progress,
    routine_step_finished,
    routine_step_started,
)

LATTES_10_PATTERN = re.compile(r'^[A-Za-z0-9]{10}$')


def create_legacy_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.set_ciphers('DEFAULT@SECLEVEL=1')
    return context


def get_lattes_id_10(lattes_id: str) -> str | None:
    if not lattes_id:
        return None

    URL = f'https://buscatextual.cnpq.br/buscatextual/cv?id={lattes_id}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
    }

    ssl_context = create_legacy_ssl_context()

    try:
        with httpx.Client(
            follow_redirects=False, headers=headers, verify=ssl_context
        ) as client:
            response = client.get(URL, timeout=30.0)

            if response.status_code == HTTPStatus.FOUND:
                location = response.headers.get('Location', '')
                if not location or 'erro.jsp' in location.lower():
                    return None

                parsed = urlparse(location)
                query_params = parse_qs(parsed.query)
                candidate_id = query_params.get('id', [None])[0]

                if candidate_id and LATTES_10_PATTERN.match(candidate_id):
                    return candidate_id

                if len(location) >= 10:
                    last_10 = location[-10:]
                    if LATTES_10_PATTERN.match(last_10):
                        return last_10
    except Exception as e:
        logger.warning(f'Error requesting lattes_10_id for {lattes_id}: {e}')
        return None

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

        routine_step_started('fetch_lattes_10', total_items=items_found)

        query_update = text("""
            UPDATE researcher
            SET lattes_10_id = :lattes_10_id
            WHERE id = :id;
        """)

        for i, researcher in enumerate(researchers):
            r_id = researcher['researcher_id']
            l_id = researcher['lattes_id']
            try:
                lattes_10_id = get_lattes_id_10(l_id)
                if lattes_10_id:
                    session.execute(
                        query_update,
                        {
                            'id': r_id,
                            'lattes_10_id': lattes_10_id,
                        },
                    )
                    success += 1
                    logger.debug(
                        f'Fetched lattes_10_id {lattes_10_id} for researcher {r_id}'
                    )
                else:
                    failed += 1
                    routine_item_error(
                        r_id,
                        'Could not resolve lattes_10_id from CNPq redirect',
                        lattes_id=l_id,
                    )
            except Exception as item_err:
                failed += 1
                routine_item_error(r_id, str(item_err), lattes_id=l_id)

            if (i + 1) % 50 == 0 or (i + 1) == items_found:
                routine_progress(
                    'fetch_lattes_10', i + 1, items_found, success, failed
                )

        session.commit()
        items_succeeded = success
        items_failed = failed
        routine_step_finished('fetch_lattes_10')
    except Exception as E:
        items_succeeded = 0
        items_failed = items_found
        logger.error(f'Error in get_lattes_10: {E}')
        session.rollback()
        raise E


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
