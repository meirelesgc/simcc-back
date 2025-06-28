from http import HTTPStatus

import httpx

from routines.logger import logger_researcher_routine, logger_routine
from simcc.repositories import conn


def get_lattes_id_10(lattes_id: str) -> str:
    URL = f'https://buscatextual.cnpq.br/buscatextual/cv?id={lattes_id}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36'
    }
    with httpx.Client(follow_redirects=False, headers=headers) as client:
        response = client.get(URL, timeout=30.0)

        if response.status_code == HTTPStatus.FOUND:
            code = response.headers.get('Location')[-10:]
            print(f'FOUND: {code}')
            return code
    return None


def update_lattes_id_10(researcher):
    lattes_10_id = get_lattes_id_10(researcher['lattes_id'])

    SCRIPT_SQL = """
        UPDATE researcher
        SET lattes_10_id = %(lattes_10_id)s
        WHERE id = %(id)s;
        """

    params = {
        'id': researcher['researcher_id'],
        'lattes_10_id': lattes_10_id,
    }
    conn.exec(SCRIPT_SQL, params)


def list_researchers():
    SCRIPT_SQL = """
        SELECT r.id as researcher_id, r.lattes_id as lattes_id
        FROM researcher r
        LEFT JOIN logs.researcher_routine lrr
            ON r.id = lrr.researcher_id
            AND lrr.type = 'LATTES_10'
        WHERE r.lattes_10_id IS NULL
            OR lrr.created_at < NOW() - INTERVAL '30 days';
        """
    result = conn.select(SCRIPT_SQL)
    return result


if __name__ == '__main__':
    researchers = list_researchers()
    for researcher in researchers:
        print('Researcher ID:', researcher['researcher_id'])
        update_lattes_id_10(researcher)
        logger_researcher_routine(
            researcher['researcher_id'], 'LATTES_10', False
        )
    logger_routine('LATTES_10', False)
