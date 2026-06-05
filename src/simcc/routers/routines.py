import asyncio
import sys
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks

from simcc.core.logging import get_logger

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.routines.get_lattes_10 import main as lattes_10_main
from scripts.routines.pog import main as pog_main
from scripts.routines.researcher_classification import main as researcher_classification_main
from scripts.routines.researcher_indprod import main as researcher_indprod_main
from scripts.routines.researcher_production import main as researcher_production_main
from scripts.routines.soap_lattes import main as soap_lattes_main

router = APIRouter(tags=['routines'])
logger = get_logger('routines')


async def _run_simcc_hop():
    proc = await asyncio.create_subprocess_exec(
        'docker',
        'compose',
        '--profile',
        'routines',
        'run',
        '--rm',
        'simcc_hop',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.error('simcc_hop_failed', stderr=stderr.decode())
    else:
        logger.info('simcc_hop_finished_successfully')


async def _run_researcher_routines(researcher_id: str):
    await asyncio.to_thread(soap_lattes_main, researcher_id)
    await _run_simcc_hop()
    await asyncio.to_thread(pog_main, researcher_id)
    await asyncio.to_thread(lattes_10_main, researcher_id)
    await asyncio.to_thread(researcher_production_main, researcher_id)
    await asyncio.to_thread(researcher_indprod_main, researcher_id)
    await asyncio.to_thread(researcher_classification_main, researcher_id)


@router.post('/routine/researcher/{researcher_id}', status_code=202)
async def run_researcher_routines(
    researcher_id: UUID,
    background_tasks: BackgroundTasks,
):
    background_tasks.add_task(_run_researcher_routines, str(researcher_id))
    return {'detail': f'Rotinas iniciadas para pesquisador {researcher_id}'}
