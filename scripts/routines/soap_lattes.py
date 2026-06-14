import csv
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
from simcc.core.logging import get_logger
from simcc.core.settings import Settings

logger = get_logger('routines')
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


def list_admin_researchers(session):
    SCRIPT_SQL = text("""
        SELECT researcher_id, name, lattes_id
        FROM public.researcher;
    """)

    return session.execute(SCRIPT_SQL).mappings().all()


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
                logger.error(
                    'cnpq_att_failed',
                    lattes_id=lattes_id,
                    error=str(e),
                )

                return datetime.min


def database_att(session, lattes_id):
    SCRIPT_SQL = text("""
        SELECT last_update
        FROM researcher
        WHERE lattes_id = :lattes_id;
    """)

    result = (
        session
        .execute(
            SCRIPT_SQL,
            {'lattes_id': lattes_id},
        )
        .mappings()
        .first()
    )

    if result and result.get('last_update'):
        return result.get('last_update')

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
            logger.error(
                'download_xml_failed',
                researcher_id=researcher_id,
                lattes_id=lattes_id,
                error=str(e),
            )

            return str(e)

        try:
            zip_path = os.path.join(
                ZIP_XML_PATH,
                f'{lattes_id}.zip',
            )

            with open(zip_path, 'wb') as f:
                f.write(content)

            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(XML_PATH)
                z.extractall(CURRENT_XML_PATH)

            os.remove(zip_path)

            logger.info(
                'download_xml_success',
                researcher_id=researcher_id,
                lattes_id=lattes_id,
            )

            return None

        except Exception as e:
            logger.error(
                'extract_xml_failed',
                researcher_id=researcher_id,
                lattes_id=lattes_id,
                error=str(e),
            )

            return str(e)

    finally:
        session.close()


def main():
    admin_session = next(get_admin_sync_session())

    start_time = time.perf_counter()

    logger.info('soap_lattes_routine_started')

    try:
        for directory in [LOG_PATH, CURRENT_XML_PATH, ZIP_XML_PATH]:
            os.makedirs(directory, exist_ok=True)

        for file in os.listdir(XML_PATH):
            path = os.path.join(XML_PATH, file)

            if os.path.isfile(path) and file.endswith('.xml'):
                os.remove(path)

        researchers = list_admin_researchers(admin_session)

        if not researchers:
            duration = time.perf_counter() - start_time

            logger.info(
                'soap_lattes_routine_finished_successfully',
                count=0,
                duration=f'{duration:.2f}s',
            )

            return

        total_researchers = len(researchers)

        logger.info(
            'researchers_found',
            count=total_researchers,
        )

        errors = []

        with ThreadPoolExecutor(
            max_workers=MAX_PARALLEL_DOWNLOADS
        ) as executor:
            futures = {}

            for i, researcher in enumerate(researchers):
                lattes_id = researcher['lattes_id'].zfill(16)
                researcher_id = str(researcher['researcher_id'])

                logger.info(
                    'processing_researcher',
                    current=i + 1,
                    total=total_researchers,
                    researcher_id=researcher_id,
                    lattes_id=lattes_id,
                )

                future = executor.submit(
                    download_xml,
                    lattes_id,
                    researcher_id,
                )

                futures[future] = (
                    lattes_id,
                    researcher_id,
                )

            completed = 0

            for future in as_completed(futures):
                completed += 1

                lattes_id, researcher_id = futures[future]

                logger.info(
                    'researcher_completed',
                    current=completed,
                    total=total_researchers,
                    researcher_id=researcher_id,
                    lattes_id=lattes_id,
                )

                try:
                    error = future.result()

                    if error:
                        errors.append((lattes_id, error))

                except Exception as e:
                    logger.error(
                        'parallel_download_failed',
                        current=completed,
                        total=total_researchers,
                        researcher_id=researcher_id,
                        lattes_id=lattes_id,
                        error=str(e),
                    )

                    errors.append((lattes_id, str(e)))

        if errors:
            error_file = (
                f'logs/errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            )

            with open(
                error_file,
                'w',
                newline='',
                encoding='utf-8',
            ) as f:
                writer = csv.writer(f)

                writer.writerow([
                    'lattes_id',
                    'erro',
                ])

                writer.writerows(errors)

        duration = time.perf_counter() - start_time

        logger.info(
            'soap_lattes_routine_finished_successfully',
            duration=f'{duration:.2f}s',
        )

    except Exception as e:
        duration = time.perf_counter() - start_time

        logger.error(
            'soap_lattes_routine_failed',
            error=str(e),
            duration=f'{duration:.2f}s',
        )

    finally:
        admin_session.close()


if __name__ == '__main__':
    main()
