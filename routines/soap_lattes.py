import argparse
import csv
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from datetime import datetime

import httpx
from zeep import Client

from routines.logger import logger_researcher_routine, logger_routine
from simcc.config import Settings
from simcc.repositories import conn, conn_admin

LOG_PATH = 'logs'
XML_PATH = Settings().XML_PATH
CURRENT_XML_PATH = Settings().CURRENT_XML_PATH
ZIP_XML_PATH = Settings().ZIP_XML_PATH
PROXY = Settings().ALTERNATIVE_CNPQ_SERVICE

HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)

SOAP_TIMEOUT_SECONDS = 20
MAX_RETRIES = 3
MAX_WORKERS = 8

errors = []

print('==== INICIALIZAÇÃO ====')
print(f'Proxy habilitado: {bool(PROXY)}')

client = None
if not PROXY:
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
    print('Cliente SOAP criado')
else:
    print('Usando proxy alternativo')


def list_admin_researchers(limit, offset):
    result = conn_admin.select(
        """
        SELECT researcher_id, name, lattes_id
        FROM public.researcher
        ORDER BY researcher_id
        LIMIT %(limit)s OFFSET %(offset)s;
        """,
        {'limit': limit, 'offset': offset},
    )
    return result


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
        except TimeoutError:
            if attempt < MAX_RETRIES:
                time.sleep(2**attempt)
            else:
                errors.append((lattes_id, 'Timeout CNPQ'))
                return datetime.min
        except Exception as e:
            errors.append((lattes_id, str(e)))
            return datetime.min


def database_att(lattes_id):
    result = conn.select(
        """
        SELECT last_update
        FROM researcher
        WHERE lattes_id = %(lattes_id)s;
        """,
        {'lattes_id': lattes_id},
    )
    if result:
        return result[0].get('last_update')
    return datetime.min


def download_xml(lattes_id, researcher_id, index):
    cnpq_date = cnpq_att(lattes_id)
    db_date = database_att(lattes_id)

    if cnpq_date <= db_date:
        return

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
        errors.append((lattes_id, str(e)))
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(e))
        return

    try:
        zip_path = os.path.join(ZIP_XML_PATH, lattes_id + '.zip')
        with open(zip_path, 'wb') as f:
            f.write(content)

        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(XML_PATH)
            z.extractall(CURRENT_XML_PATH)

        os.remove(zip_path)
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', False)
    except Exception as e:
        errors.append((lattes_id, str(e)))
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(e))


if __name__ == '__main__':
    print('==== INÍCIO ROTINA ====')

    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=500)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--clean-xml', action='store_true')
    args = parser.parse_args()

    for directory in [LOG_PATH, CURRENT_XML_PATH, ZIP_XML_PATH]:
        os.makedirs(directory, exist_ok=True)

    if args.clean_xml:
        for file in os.listdir(XML_PATH):
            path = os.path.join(XML_PATH, file)
            if os.path.isfile(path) and file.endswith('.xml'):
                os.remove(path)

    researchers = list_admin_researchers(args.limit, args.offset)

    if not researchers:
        sys.exit(0)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for idx, researcher in enumerate(researchers, start=args.offset + 1):
            lattes_id = researcher['lattes_id'].zfill(16)
            futures.append(
                executor.submit(
                    download_xml, lattes_id, researcher['researcher_id'], idx
                )
            )

        for future in as_completed(futures):
            future.result()

    if errors:
        error_file = f'errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        with open(error_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['lattes_id', 'erro'])
            writer.writerows(errors)

    logger_routine('SOAP_LATTES', False)
    print('==== FIM ROTINA ====')
