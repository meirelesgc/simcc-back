import asyncio
from http import HTTPStatus
from uuid import UUID

import docker
from fastapi import APIRouter, BackgroundTasks, HTTPException

from simcc.core.dependencies import CurrentUser
from simcc.core.logging import get_logger
from simcc.core.settings import Settings

router = APIRouter(tags=['Routines'])
logger = get_logger('routines')
SETTINGS = Settings()


async def _run_script(script: str, *args: str) -> bool:
    try:
        proc = await asyncio.create_subprocess_exec(
            'python', script, *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode == 0:
            logger.info('routine_script_success', script=script)
            return True
        logger.error('routine_script_failed', script=script, stderr=stderr.decode())
        return False
    except Exception as e:
        logger.error('routine_script_error', script=script, error=str(e))
        return False


def _run_hop_sync() -> None:
    client = docker.from_env()
    client.containers.run(
        image=SETTINGS.HOP_IMAGE,
        environment={
            'HOP_LOG_LEVEL': 'Minimal',
            'HOP_RUN_CONFIG': 'local',
            'HOP_FILE_PATH': '${PROJECT_HOME}/workflows/Index.hwf',
            'HOP_PROJECT_NAME': 'Jade-Extrator',
            'HOP_PROJECT_FOLDER': '/files/jade-extrator',
            'HOP_RUN_PARAMETERS': SETTINGS.HOP_RUN_PARAMETERS,
        },
        volumes=[
            f'{SETTINGS.HOP_XML_VOLUME}:/files/jade-extrator/datasets/lattes_xml'
        ],
        network_mode='host',
        remove=True,
    )


async def _run_hop() -> bool:
    if not SETTINGS.HOP_RUN_PARAMETERS:
        logger.warning('hop_skipped_no_parameters_configured')
        return False
    try:
        await asyncio.to_thread(_run_hop_sync)
        logger.info('hop_container_success')
        return True
    except Exception as e:
        logger.error('hop_container_failed', error=str(e))
        return False


async def _run_routines(researcher_id: str) -> None:
    logger.info('researcher_routines_started', researcher_id=researcher_id)
    rid = ('--researcher-id', researcher_id)

    await _run_script('scripts/routines/soap_lattes.py', *rid)
    await _run_hop()
    await _run_script('scripts/routines/pog.py')
    await _run_script('scripts/routines/researcher_production.py', *rid)
    await _run_script('scripts/routines/get_lattes_10.py', *rid)
    await _run_script('scripts/routines/researcher_indprod.py', *rid)

    if SETTINGS.OPENAI_API_KEY:
        await _run_script('scripts/routines/abstract_ai.py', *rid)

    await _run_script('scripts/routines/researcher_classification.py', *rid)

    logger.info('researcher_routines_finished', researcher_id=researcher_id)


@router.post('/routines/researcher/{researcher_id}', status_code=HTTPStatus.ACCEPTED)
async def trigger_researcher_routines(
    researcher_id: UUID,
    background_tasks: BackgroundTasks,
    current_user: CurrentUser,
) -> dict:
    if not current_user:
        raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED)
    background_tasks.add_task(_run_routines, str(researcher_id))
    return {'researcher_id': str(researcher_id)}
