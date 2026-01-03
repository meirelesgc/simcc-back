import argparse
import os
import sys
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

print('Inicializando cliente SOAP')
client = None
if not PROXY:
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
    print('Cliente SOAP configurado sem proxy')
else:
    print('Usando proxy alternativo CNPQ')


def list_admin_researchers(limit, offset):
    print(f'Buscando pesquisadores limit={limit} offset={offset}')
    SCRIPT_SQL = """
        SELECT researcher_id, name, lattes_id
        FROM public.researcher
        ORDER BY researcher_id
        LIMIT %(limit)s OFFSET %(offset)s;
    """
    result = conn_admin.select(SCRIPT_SQL, {'limit': limit, 'offset': offset})
    print(f'{len(result) if result else 0} pesquisadores encontrados')
    return result


def cnpq_att(lattes_id) -> datetime:
    print(f'Consultando data de atualização no CNPQ: {lattes_id}')
    try:
        if PROXY:
            response = httpx.get(
                f'https://simcc.uesc.br/v3/api/getDataAtualizacaoCV?lattes_id={lattes_id}',
                verify=False,
                timeout=HTTP_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
        else:
            data = client.service.getDataAtualizacaoCV(lattes_id)

        if not data:
            print('Data não retornada pelo CNPQ')
            return datetime.min

        parsed = datetime.strptime(data, '%d/%m/%Y %H:%M:%S')
        print(f'Data CNPQ: {parsed}')
        return parsed
    except Exception as e:
        print(f'Erro ao consultar CNPQ: {e}')
        return datetime.min


def database_att(lattes_id) -> datetime:
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
        print(f'Data banco: {result[0].get("last_update")}')
        return result[0].get('last_update')
    print('Registro não encontrado no banco')
    return datetime.min


def download_xml(lattes_id, researcher_id):
    print(
        f'Iniciando download XML: researcher_id={researcher_id} lattes_id={lattes_id}'
    )
    cnpq_date = cnpq_att(lattes_id)
    db_date = database_att(lattes_id)

    if cnpq_date <= db_date:
        print('XML já está atualizado, pulando download')
        return

    try:
        print('Baixando currículo compactado')
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
        print('Download concluído')
    except (
        httpx.RequestError,
        httpx.HTTPStatusError,
        Fault,
        TransportError,
        Exception,
    ) as e:
        print(f'Erro no download: {e}')
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(e))
        return

    try:
        zip_path = os.path.join(ZIP_XML_PATH, lattes_id + '.zip')
        print(f'Salvando ZIP em {zip_path}')
        with open(zip_path, 'wb') as f:
            f.write(content)

        print('Extraindo arquivos XML')
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(XML_PATH)
            z.extractall(CURRENT_XML_PATH)

        os.remove(zip_path)
        print('Processo finalizado com sucesso')
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', False)
    except Exception as e:
        print(f'Erro ao processar ZIP/XML: {e}')
        logger_researcher_routine(researcher_id, 'SOAP_LATTES', True, str(e))


if __name__ == '__main__':
    print('Iniciando rotina SOAP_LATTES')
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--clean-xml', action='store_true')
    args = parser.parse_args()

    for directory in [LOG_PATH, CURRENT_XML_PATH, ZIP_XML_PATH]:
        os.makedirs(directory, exist_ok=True)
        print(f'Diretório garantido: {directory}')

    if args.clean_xml:
        print('Limpando XMLs antigos')
        for file in os.listdir(XML_PATH):
            path = os.path.join(XML_PATH, file)
            if os.path.isfile(path) and file.endswith('.xml'):
                os.remove(path)

    researchers = list_admin_researchers(args.limit, args.offset)

    if not researchers:
        print('Nenhum registro retornado')
        sys.exit(0)

    for i, researcher in enumerate(researchers, start=args.offset):
        print(f'Processando índice {i}')
        lattes_id = researcher['lattes_id'].zfill(16)
        download_xml(lattes_id, researcher['researcher_id'])

    print('Finalizando rotina SOAP_LATTES')
    logger_routine('SOAP_LATTES', False)
