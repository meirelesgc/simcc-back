import os
import zipfile
from datetime import datetime

import httpx
from zeep import Client
from zeep.exceptions import Fault, TransportError

from routines.logger import logger_researcher_routine, logger_routine
from simcc.config import Settings
from simcc.repositories import conn, conn_admin

LOG_PATH = 'logs'
XML_PATH = Settings().XML_PATH
CURRENT_XML_PATH = Settings().CURRENT_XML_PATH
ZIP_XML_PATH = Settings().ZIP_XML_PATH
PROXY = Settings().ALTERNATIVE_CNPQ_SERVICE

HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)

client = None
if not PROXY:
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')


def list_admin_researchers():
    SCRIPT_SQL = """
        SELECT researcher_id, name, lattes_id
        FROM public.researcher;
    """
    return conn_admin.select(SCRIPT_SQL)


def cnpq_att(lattes_id) -> datetime:
    try:
        print(f'Consultando data de atualização no CNPq [{lattes_id}]')
        if PROXY:
            PROXY_URL = f'https://simcc.uesc.br/v3/api/getDataAtualizacaoCV?lattes_id={lattes_id}'
            response = httpx.get(PROXY_URL, verify=False, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            data = response.json()
        else:
            data = client.service.getDataAtualizacaoCV(lattes_id)

        if not data:
            return datetime.min

        return datetime.strptime(data, '%d/%m/%Y %H:%M:%S')
    except Exception as e:
        print(f'Erro ao obter data de atualização [{lattes_id}]: {e}')
        return datetime.min


def database_att(lattes_id) -> datetime:
    params = {'lattes_id': lattes_id}
    SCRIPT_SQL = """
        SELECT last_update
        FROM researcher
        WHERE lattes_id = %(lattes_id)s;
    """
    result = conn.select(SCRIPT_SQL, params)
    if result:
        return result[0].get('last_update')
    return datetime.min


def download_xml(lattes_id, researcher_id):
    cnpq_date = cnpq_att(lattes_id)
    db_date = database_att(lattes_id)

    if cnpq_date <= db_date:
        print('Curriculo atualizado!')
        return

    print('Baixando curriculo...')
    try:
        if PROXY:
            print('Download via proxy')
            PROXY_URL = f'https://simcc.uesc.br/v3/api/getCurriculoCompactado?lattes_id={lattes_id}'
            response = httpx.get(PROXY_URL, verify=False, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            content = response.content
        else:
            content = client.service.getCurriculoCompactado(lattes_id)
    except (
        httpx.RequestError,
        httpx.HTTPStatusError,
        Fault,
        TransportError,
        Exception,
    ) as e:
        print(f'Erro no download do curriculo [{lattes_id}]: {e}')
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
        print('Download concluído')
    except zipfile.BadZipFile as e:
        print(f'Erro ao extrair zip [{lattes_id}]: {e}')
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(e))
    except Exception as e:
        print(f'Erro ao salvar curriculo [{lattes_id}]: {e}')
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(e))


if __name__ == '__main__':
    for directory in [LOG_PATH, CURRENT_XML_PATH, ZIP_XML_PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    for file in os.listdir(XML_PATH):
        file_path = os.path.join(XML_PATH, file)
        if os.path.isfile(file_path) and file_path.endswith('.xml'):
            os.remove(file_path)

    researchers = list_admin_researchers()
    for i, researcher in enumerate(researchers):
        print(f'Curriculo número: [{i}]')
        print(f'Pesquisador: [{researcher.get("name")}]')
        print(f'Lattes: [{researcher.get("lattes_id")}]')

        lattes_id = researcher.get('lattes_id').zfill(16)
        researcher_id = researcher['researcher_id']
        download_xml(lattes_id, researcher_id)

    logger_routine('SOAP_LATTES', False)
    print('FIM!')
