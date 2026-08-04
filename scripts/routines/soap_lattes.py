import argparse
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import httpx
from sqlalchemy import text
from zeep import Client
from zeep.transports import Transport

from simcc.core.db.database import get_admin_sync_session, get_sync_session
from simcc.core.logging import logger
from simcc.core.logging.context import routine_name_ctx
from simcc.core.settings import Settings

SETTINGS = Settings()

XML_PATH = SETTINGS.XML_PATH
CURRENT_XML_PATH = SETTINGS.CURRENT_XML_PATH
ZIP_XML_PATH = SETTINGS.ZIP_XML_PATH
PROXY = SETTINGS.ALTERNATIVE_CNPQ_SERVICE

MAX_RETRIES = 3
MAX_PARALLEL_DOWNLOADS = 5

HTTP_TIMEOUT = httpx.Timeout(
    connect=10.0,
    read=60.0,
    write=10.0,
    pool=10.0,
)

client = None


def get_zeep_client():
    global client
    if client is None and not PROXY:
        client = Client(
            'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl',
            transport=Transport(timeout=30, operation_timeout=30),
        )
    return client


def list_admin_researchers(session, researcher_ids=None, lattes_ids=None):
    query = """
        SELECT researcher_id, name, lattes_id
        FROM public.researcher
        WHERE 1=1
    """

    params = {}

    if researcher_ids:
        query += ' AND researcher_id = ANY(:researcher_ids)'
        params['researcher_ids'] = list(researcher_ids)

    if lattes_ids:
        query += ' AND lattes_id = ANY(:lattes_ids)'
        params['lattes_ids'] = list(lattes_ids)

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
        query += ' AND id = ANY(:researcher_ids)'
        params['researcher_ids'] = list(researcher_ids)

    if lattes_ids:
        query += ' AND lattes_id = ANY(:lattes_ids)'
        params['lattes_ids'] = list(lattes_ids)

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

    zeep_client = get_zeep_client()
    return zeep_client.service.getDataAtualizacaoCV(lattes_id)


def cnpq_att(lattes_id):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = cnpq_att_call(lattes_id)

            if not data:
                return datetime.min, None

            return datetime.strptime(data, '%d/%m/%Y %H:%M:%S'), None

        except Exception as e:
            last_err = str(e)
            if attempt < MAX_RETRIES:
                time.sleep(2**attempt)

    return (
        None,
        f'Falha ao consultar data de atualização no CNPq após {MAX_RETRIES} tentativas: {last_err}',
    )


def database_att(session, lattes_id):
    try:
        result = (
            session.execute(
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
            return result['last_update'], None

        return datetime.min, None
    except Exception as e:
        return None, f'Falha ao consultar última atualização no banco de dados: {e}'


def download_xml(lattes_id, researcher_id, name=None):
    session = None
    try:
        session = next(get_sync_session())
    except Exception as e:
        return (
            False,
            f'Não foi possível obter sessão do banco de dados: {e}',
        )

    try:
        cnpq_date, cnpq_err = cnpq_att(lattes_id)
        if cnpq_err:
            return False, cnpq_err

        db_date, db_err = database_att(session, lattes_id)
        if db_err:
            return False, db_err

        if cnpq_date <= db_date:
            cnpq_str = (
                cnpq_date.strftime('%d/%m/%Y %H:%M:%S')
                if cnpq_date != datetime.min
                else 'Sem data'
            )
            db_str = (
                db_date.strftime('%d/%m/%Y %H:%M:%S')
                if db_date != datetime.min
                else 'Sem data'
            )
            return (
                False,
                f'Currículo já está atualizado no banco (Data CNPq: {cnpq_str} <= Data Banco: {db_str})',
            )

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
                zeep_client = get_zeep_client()
                content = zeep_client.service.getCurriculoCompactado(lattes_id)

            if not content:
                return False, 'CNPq/Proxy retornou conteúdo de arquivo vazio'

        except Exception as e:
            return False, f'Falha no download do XML/ZIP: {e}'

        try:
            zip_path = os.path.join(ZIP_XML_PATH, f'{lattes_id}.zip')

            os.makedirs(ZIP_XML_PATH, exist_ok=True)
            os.makedirs(XML_PATH, exist_ok=True)
            os.makedirs(CURRENT_XML_PATH, exist_ok=True)

            with open(zip_path, 'wb') as f:
                f.write(content)

            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(XML_PATH)
                z.extractall(CURRENT_XML_PATH)

            if os.path.exists(zip_path):
                os.remove(zip_path)

            return True, 'XML baixado e extraído com sucesso'

        except Exception as e:
            return False, f'Falha ao salvar ou extrair arquivo XML: {e}'

    finally:
        if session is not None:
            session.close()


items_found = 0
items_succeeded = 0
items_failed = 0


def main(researcher_ids=None, lattes_ids=None):
    global items_found, items_succeeded, items_failed

    if not routine_name_ctx.get():
        routine_name_ctx.set('soap_lattes')
    start_time = datetime.now()
    logger.info(
        f"[INÍCIO] Rotina soap_lattes iniciada em {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    admin_session = None
    researchers = []

    try:
        try:
            admin_session = next(get_admin_sync_session())
            researchers = list_admin_researchers(
                admin_session,
                researcher_ids,
                lattes_ids,
            )
            logger.info(
                f'Utilizando banco administrativo ({len(researchers)} pesquisadores encontrados).'
            )
        except Exception as admin_err:
            logger.warning(
                f'[AVISO] Não foi possível conectar ao banco administrativo: {admin_err}'
            )
            logger.info('Tentando conectar ao banco principal...')

            try:
                admin_session = next(get_sync_session())
                researchers = list_main_researchers(
                    admin_session,
                    researcher_ids,
                    lattes_ids,
                )
                logger.info(
                    f'Utilizando banco principal ({len(researchers)} pesquisadores encontrados).'
                )
            except Exception as main_err:
                logger.error(
                    f'[ERRO] Não foi possível conectar ao banco principal: {main_err}'
                )
                logger.error(
                    f'[INTERROMPIDO] Rotina soap_lattes parou no meio em {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}. Motivo: Não foi possível conectar ao banco principal: {main_err}'
                )
                raise main_err

        if os.path.exists(XML_PATH):
            for file in os.listdir(XML_PATH):
                path = os.path.join(XML_PATH, file)
                if os.path.isfile(path) and file.endswith('.xml'):
                    try:
                        os.remove(path)
                    except Exception:
                        pass
        else:
            os.makedirs(XML_PATH, exist_ok=True)

        if not researchers:
            logger.warning('Nenhum pesquisador encontrado com os parâmetros informados.')
            end_time = datetime.now()
            logger.info(
                f"[FIM] Rotina soap_lattes encerrada em {end_time.strftime('%Y-%m-%d %H:%M:%S')}. Total: 0 | Baixados: 0 | Não baixados: 0"
            )
            return

        total = len(researchers)
        items_found = total

        succeeded_count = 0
        failed_count = 0

        with ThreadPoolExecutor(
            max_workers=MAX_PARALLEL_DOWNLOADS
        ) as executor:
            futures = {}

            for researcher in researchers:
                lattes_id = researcher.get('lattes_id')
                researcher_id = str(researcher.get('researcher_id'))
                name = researcher.get('name', 'Desconhecido')

                if not lattes_id or not str(lattes_id).strip():
                    reason = 'Lattes ID não informado ou em branco'
                    logger.warning(
                        f'[NÃO BAIXADO] Pesquisador {name} (ID: {researcher_id}, Lattes: {lattes_id}): Motivo: {reason}'
                    )
                    failed_count += 1
                    continue

                lattes_id_clean = str(lattes_id).strip().zfill(16)

                future = executor.submit(
                    download_xml,
                    lattes_id_clean,
                    researcher_id,
                    name,
                )

                futures[future] = (lattes_id_clean, researcher_id, name)

            completed = 0
            for future in as_completed(futures):
                completed += 1
                lattes_id_clean, researcher_id, name = futures[future]

                try:
                    success, detail = future.result()

                    if success:
                        succeeded_count += 1
                        logger.info(
                            f'[OK] [{completed}/{total}] Pesquisador {name} (ID: {researcher_id}, Lattes: {lattes_id_clean}): {detail}'
                        )
                    else:
                        failed_count += 1
                        logger.warning(
                            f'[NÃO BAIXADO] Pesquisador {name} (ID: {researcher_id}, Lattes: {lattes_id_clean}): Motivo: {detail}'
                        )

                except Exception as e:
                    failed_count += 1
                    reason = f'Erro inesperado no processamento: {e}'
                    logger.error(
                        f'[NÃO BAIXADO] Pesquisador {name} (ID: {researcher_id}, Lattes: {lattes_id_clean}): Motivo: {reason}'
                    )

        items_succeeded = succeeded_count
        items_failed = failed_count

        end_time = datetime.now()
        duration_str = str(end_time - start_time).split('.')[0]
        logger.info(
            f"[FIM] Rotina soap_lattes encerrada em {end_time.strftime('%Y-%m-%d %H:%M:%S')} (Duração: {duration_str}). Total: {total} | Baixados com sucesso: {succeeded_count} | Não baixados: {failed_count}"
        )

    except Exception as e:
        end_time = datetime.now()
        logger.error(
            f"[INTERROMPIDO] Rotina soap_lattes parou no meio em {end_time.strftime('%Y-%m-%d %H:%M:%S')}. Motivo: {e}"
        )
        raise e
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
