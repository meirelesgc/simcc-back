import os
import time
import zipfile
from datetime import datetime

import httpx
from requests.exceptions import RequestException
from zeep import Client
from zeep.exceptions import XMLSyntaxError

from routines.logger import logger_researcher_routine, logger_routine
from simcc.config import settings
from simcc.repositories import conn, conn_admin

LOG_PATH = 'logs'
CURRENT_XML_PATH = 'current'
ZIP_XML_PATH = 'zip'
PROXY = settings.ALTERNATIVE_CNPQ_SERVICE


def create_zeep_client_with_retries(wsdl_url: str, max_retries=3, delay=3):
    attempt = 0
    while attempt < max_retries:
        try:
            return Client(wsdl_url)
        except (RequestException, XMLSyntaxError, ConnectionResetError) as e:
            attempt += 1
            print(f'Tentativa {attempt} falhou ao conectar ao WSDL: {e}')
            if attempt >= max_retries:
                raise
            time.sleep(delay)


client = create_zeep_client_with_retries(
    'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl'
)


def list_admin_researchers():
    SCRIPT_SQL = """
        SELECT researcher_id, name, lattes_id
        FROM public.researcher;
    """
    return conn_admin.select(SCRIPT_SQL)


def cnpq_att(lattes_id) -> datetime:
    try:
        if PROXY:
            PROXY_URL = f'https://simcc.uesc.br/v3/api/getDataAtualizacaoCV?lattes_id={lattes_id}'
            response = httpx.get(PROXY_URL, verify=False, timeout=None).json()
            if response:
                return datetime.strptime(response, '%d/%m/%Y %H:%M:%S')
            return datetime.min
        response = client.service.getDataAtualizacaoCV(lattes_id)
        return datetime.strptime(response, '%d/%m/%Y %H:%M:%S')
    except (httpx.TimeoutException, TypeError, ValueError) as e:
        print(f'Erro ao obter data de atualização do Lattes: {e}')
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
    if cnpq_att(lattes_id) <= database_att(lattes_id):
        print('Curriculo atualizado!')
        return

    print('Baixando curriculo...')
    try:
        if PROXY:
            print('Baixando curriculo via proxy')
            PROXY_URL = f'https://simcc.uesc.br/v3/api/getCurriculoCompactado?lattes_id={lattes_id}'
            response = httpx.get(PROXY_URL, verify=False, timeout=None).content
        else:
            response = client.service.getCurriculoCompactado(lattes_id)
    except httpx.Timeout as E:
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(E))
        return

    try:
        zip_path = os.path.join(ZIP_XML_PATH, lattes_id + '.zip')
        with open(zip_path, 'wb') as XML:
            XML.write(response)

        with zipfile.ZipFile(zip_path, 'r') as ZIP:
            ZIP.extractall(PATH)
            ZIP.extractall(os.path.join(CURRENT_XML_PATH))
        os.remove(zip_path)
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', False)
    except zipfile.BadZipFile as E:
        print(f'Erro de arquivo zip: {E}')
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(E))
        return


if __name__ == '__main__':
    PATH = 'storage/xml'
    CURRENT_XML_PATH = 'storage/xml/current'
    ZIP_XML_PATH = 'storage/xml/zip'

    for directory in [LOG_PATH, CURRENT_XML_PATH, ZIP_XML_PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    for file in os.listdir(PATH):
        file_path = os.path.join(PATH, file)
        if os.path.isfile(file_path) and file_path.endswith('.xml'):
            os.remove(file_path)

    researchers = list_admin_researchers()
    for _, researcher in enumerate(researchers):
        print(f'Curriculo número: [{_}]')
        print(f'Pesquisador: [{researcher.get("name")}]')
        print(f'Lattes: [{researcher.get("lattes_id")}]')

        lattes_id = researcher.get('lattes_id').zfill(16)
        researcher_id = researcher['researcher_id']
        download_xml(lattes_id, researcher_id)

    logger_routine('SOAP_LATTES', False)
    print('FIM!')
