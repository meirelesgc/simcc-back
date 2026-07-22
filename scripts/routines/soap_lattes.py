import argparse
import os
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import httpx
from sqlalchemy import text
from zeep import Client
from zeep.transports import Transport

from simcc.core.db.database import get_admin_sync_session, get_sync_session
from simcc.core.settings import Settings

SETTINGS = Settings()

LOG_PATH = 'logs'
XML_PATH = SETTINGS.XML_PATH
CURRENT_XML_PATH = SETTINGS.CURRENT_XML_PATH
ZIP_XML_PATH = SETTINGS.ZIP_XML_PATH
PROXY = SETTINGS.ALTERNATIVE_CNPQ_SERVICE

MAX_RETRIES = 3
MAX_PARALLEL_DOWNLOADS = 5

transport = Transport(timeout=10)
HTTP_TIMEOUT = httpx.Timeout(
    connect=10.0,
    read=60.0,
    write=10.0,
    pool=10.0,
)

client = None

if not PROXY:
    client = Client(
        'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl',
        transport=Transport(timeout=30, operation_timeout=30),
    )


def list_admin_researchers(session, researcher_ids=None, lattes_ids=None):
    query = """
        SELECT researcher_id, name, lattes_id
        FROM public.researcher
        WHERE 1=1
    """

    params = {}

    if researcher_ids:
        query += ' AND researcher_id IN :researcher_ids'
        params['researcher_ids'] = tuple(researcher_ids)

    if lattes_ids:
        query += ' AND lattes_id IN :lattes_ids'
        params['lattes_ids'] = tuple(lattes_ids)

    result = session.execute(text(query), params)

    return result.mappings().all()


def list_main_researchers(session, researcher_ids=None, lattes_ids=None):
    query = """
        SELECT
            id AS researcher_id,
            name,
            lattes_id
        FROM researcher
        WHERE 1=1
    """

    params = {}

    if researcher_ids:
        query += ' AND id IN :researcher_ids'
        params['researcher_ids'] = tuple(researcher_ids)

    if lattes_ids:
        query += ' AND lattes_id IN :lattes_ids'
        params['lattes_ids'] = tuple(lattes_ids)

    result = session.execute(text(query), params)

    return result.mappings().all()


def cnpq_att_call(lattes_id):
    if PROXY:
        response = httpx.get(
            f'https://simcc.uesc.br/v3/api/getDataAtualizacaoCV?lattes_id={lattes_id}',
            verify=False,
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    return client.service.getDataAtualizacaoCV(lattes_id)


def cnpq_att(lattes_id):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = cnpq_att_call(lattes_id)

            if not data:
                return datetime.min

            return datetime.strptime(data, '%d/%m/%Y %H:%M:%S')

        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(2**attempt)
            else:
                print(
                    f'[ERRO] Falha ao consultar atualização do currículo {lattes_id}: {e}'
                )
                return datetime.min


def database_att(session, lattes_id):
    result = (
        session
        .execute(
            text(
                """
                SELECT last_update
                FROM researcher
                WHERE lattes_id = :lattes_id
                """
            ),
            {'lattes_id': lattes_id},
        )
        .mappings()
        .first()
    )

    if result and result.get('last_update'):
        return result['last_update']

    return datetime.min


def download_xml(lattes_id, researcher_id):
    session = next(get_sync_session())

    try:
        cnpq_date = cnpq_att(lattes_id)
        db_date = database_att(session, lattes_id)

        if cnpq_date <= db_date:
            return None

        try:
            if PROXY:
                response = httpx.get(
                    f'https://simcc.uesc.br/v3/api/getCurriculoCompactado?lattes_id={lattes_id}',
                    verify=False,
                    timeout=HTTP_TIMEOUT,
                )
                response.raise_for_status()
                content = response.content
            else:
                content = client.service.getCurriculoCompactado(lattes_id)

        except Exception as e:
            print(f'[ERRO] Download XML {lattes_id}: {e}')
            return str(e)

        try:
            zip_path = os.path.join(ZIP_XML_PATH, f'{lattes_id}.zip')

            with open(zip_path, 'wb') as f:
                f.write(content)

            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(XML_PATH)
                z.extractall(CURRENT_XML_PATH)

            os.remove(zip_path)

            print(f'[OK] XML baixado: {lattes_id}')

            return None

        except Exception as e:
            print(f'[ERRO] Extração XML {lattes_id}: {e}')
            return str(e)

    finally:
        session.close()


def main(researcher_ids=None, lattes_ids=None):
    start_time = time.perf_counter()

    print('Iniciando rotina...')

    admin_session = None
    using_admin = True

    try:
        try:
            admin_session = next(get_admin_sync_session())

            researchers = list_admin_researchers(
                admin_session,
                researcher_ids,
                lattes_ids,
            )

            print(
                f'Utilizando banco administrativo ({len(researchers)} pesquisadores).'
            )

        except Exception as e:
            print(f'Não foi possível conectar ao banco administrativo: {e}')
            print('Utilizando banco principal.')

            using_admin = False

            admin_session = next(get_sync_session())

            researchers = list_main_researchers(
                admin_session,
                researcher_ids,
                lattes_ids,
            )

        for file in os.listdir(XML_PATH):
            path = os.path.join(XML_PATH, file)
            if os.path.isfile(path) and file.endswith('.xml'):
                os.remove(path)

        if not researchers:
            print('Nenhum pesquisador encontrado.')
            return

        total = len(researchers)

        errors = []

        with ThreadPoolExecutor(
            max_workers=MAX_PARALLEL_DOWNLOADS
        ) as executor:
            futures = {}

            for i, researcher in enumerate(researchers):
                lattes_id = researcher['lattes_id'].zfill(16)
                researcher_id = str(researcher['researcher_id'])

                print(f'[{i + 1}/{total}] {researcher_id} - {lattes_id}')

                future = executor.submit(
                    download_xml,
                    lattes_id,
                    researcher_id,
                )

                futures[future] = (lattes_id, researcher_id)

            completed = 0

            for future in as_completed(futures):
                completed += 1

                lattes_id, researcher_id = futures[future]

                print(f'Concluído {completed}/{total}: {lattes_id}')

                try:
                    error = future.result()

                    if error:
                        errors.append((lattes_id, error))

                except Exception as e:
                    errors.append((lattes_id, str(e)))
    finally:
        if admin_session is not None:
            admin_session.close()


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

    main(
        researcher_ids=args.researcher_ids,
        lattes_ids=args.lattes_ids,
    )
