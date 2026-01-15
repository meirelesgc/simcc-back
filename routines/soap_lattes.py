import csv
import os
import sys
import time
import zipfile
from datetime import datetime

import httpx
from requests import Session
from zeep import Client
from zeep.transports import Transport

from routines.logger import logger_researcher_routine, logger_routine
from simcc.config import Settings
from simcc.repositories import conn, conn_admin

session = Session()
session.timeout = 30

transport = Transport(session=session)

LOG_PATH = 'logs'
XML_PATH = Settings().XML_PATH
CURRENT_XML_PATH = Settings().CURRENT_XML_PATH
ZIP_XML_PATH = Settings().ZIP_XML_PATH
PROXY = Settings().ALTERNATIVE_CNPQ_SERVICE

HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)

MAX_RETRIES = 3

errors = []

print('==== INICIALIZAÇÃO ====')
print(f'Proxy habilitado: {bool(PROXY)}')

client = None
if not PROXY:
    client = Client(
        'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl',
        transport=transport,
    )
    print('Cliente SOAP criado')
else:
    print('Usando proxy alternativo')


def list_admin_researchers():
    print('Buscando pesquisadores no banco')
    return conn_admin.select(
        """
        SELECT researcher_id, name, lattes_id
        FROM public.researcher
        ORDER BY researcher_id;
        """
    )


def cnpq_att_call(lattes_id):
    print(f'Consultando data de atualização no CNPQ: {lattes_id}')
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
            print(f'Tentativa {attempt} de obter data CNPQ: {lattes_id}')
            data = cnpq_att_call(lattes_id)
            if not data:
                return datetime.min
            return datetime.strptime(data, '%d/%m/%Y %H:%M:%S')
        except Exception as e:
            print(f'Erro ao obter data CNPQ: {lattes_id} | {e}')
            if attempt < MAX_RETRIES:
                time.sleep(2**attempt)
            else:
                errors.append((lattes_id, str(e)))
                return datetime.min


def database_att(lattes_id):
    print(f'Consultando data no banco: {lattes_id}')
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
    print(f'[{index}] Iniciando processamento: {lattes_id}')

    cnpq_date = cnpq_att(lattes_id)
    db_date = database_att(lattes_id)

    print(f'[{index}] Data CNPQ: {cnpq_date} | Data DB: {db_date}')

    if cnpq_date <= db_date:
        print(f'[{index}] CV já atualizado, pulando')
        return

    try:
        print(f'[{index}] Baixando XML compactado')
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
        print(f'[{index}] Erro no download: {e}')
        errors.append((lattes_id, str(e)))
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(e))
        return

    try:
        zip_path = os.path.join(ZIP_XML_PATH, lattes_id + '.zip')
        print(f'[{index}] Salvando ZIP: {zip_path}')

        with open(zip_path, 'wb') as f:
            f.write(content)

        print(f'[{index}] Extraindo XML')
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(XML_PATH)
            z.extractall(CURRENT_XML_PATH)

        os.remove(zip_path)
        print(f'[{index}] Finalizado com sucesso')
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', False)
    except Exception as e:
        print(f'[{index}] Erro ao extrair XML: {e}')
        errors.append((lattes_id, str(e)))
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(e))


if __name__ == '__main__':
    print('==== INÍCIO ROTINA ====')

    for directory in [LOG_PATH, CURRENT_XML_PATH, ZIP_XML_PATH]:
        os.makedirs(directory, exist_ok=True)

    print('Limpando XMLs antigos')
    for file in os.listdir(XML_PATH):
        path = os.path.join(XML_PATH, file)
        if os.path.isfile(path) and file.endswith('.xml'):
            os.remove(path)

    researchers = list_admin_researchers()

    if not researchers:
        print('Nenhum pesquisador encontrado')
        sys.exit(0)

    for idx, researcher in enumerate(researchers, start=1):
        lattes_id = researcher['lattes_id'].zfill(16)
        download_xml(lattes_id, researcher['researcher_id'], idx)

    if errors:
        error_file = f'errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        print(f'Salvando erros em {error_file}')
        with open(error_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['lattes_id', 'erro'])
            writer.writerows(errors)

    logger_routine('SOAP_LATTES', False)
    print('==== FIM ROTINA ====')
