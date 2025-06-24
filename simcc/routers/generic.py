import os
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import (
    FileResponse,
    PlainTextResponse,
)
from zeep import Client

from simcc.core.connection import Connection
from simcc.core.database import get_conn
from simcc.schemas import DefaultFilters, ResearcherBarema, YearBarema
from simcc.services import ConecteeService, GenericService

STORAGE_PATH = Path('storage/dictionary')
STORAGE_PATH.mkdir(parents=True, exist_ok=True)

router = APIRouter()


@router.get('/logs_researcher')
def get_researcher_logs(): ...


@router.get('/departament/rt')
def get_departament_rt():
    return ConecteeService.get_departament_rt_data()


@router.get('/logs')
def get_logs():
    return GenericService.get_logs()


@router.get('/foment')
def get_researcher_foment(institution_id: UUID = None):
    return GenericService.get_researcher_foment(institution_id)


@router.get('/dictionary.pdf')
def dim_titulacao_xlsx():
    file_path = os.path.join(STORAGE_PATH, 'dictionary.pdf')
    return FileResponse(file_path, filename='dictionary.pdf')


@router.get('/getIdentificadorCNPq')
def lattes_id(
    cpf: str = None, nomeCompleto: str = None, dataNascimento: str = None
):
    try:
        client = Client(
            'http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl'
        )
        response_content = client.service.getIdentificadorCNPq(
            cpf=cpf, nomeCompleto=nomeCompleto, dataNascimento=dataNascimento
        )

        if response_content:
            return PlainTextResponse(content=str(response_content))
        else:
            return PlainTextResponse(content='', status_code=404)

    except Exception as e:
        print(f'Erro ao chamar serviço CNPq: {e}')
        return PlainTextResponse(
            content=f'Erro interno ao processar a solicitação: {e}',
            status_code=500,
        )


@router.get(
    '/getCurriculoCompactado',
    response_class=FileResponse,
)
def lattes_xml(lattes_id: str):
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
    response = client.service.getCurriculoCompactado(lattes_id)
    if response:
        file_path = f'storage/{lattes_id}.zip'
        with open(file_path, 'wb') as file:
            file.write(response)
        return FileResponse(
            path=file_path,
            filename=f'{lattes_id}.zip',
            media_type='application/zip',
        )
    raise HTTPException(status_code=404, detail='Curriculum not found')


@router.get(
    '/getDataAtualizacaoCV',
    response_model=str,
)
def current_lattes_date(lattes_id: str):
    client = Client('http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl')
    response = client.service.getDataAtualizacaoCV(lattes_id)
    if response:
        return datetime.strptime(response, '%d/%m/%Y %H:%M:%S').strftime(
            '%d/%m/%Y %H:%M:%S'
        )
    raise HTTPException(status_code=404, detail='Curriculum not found')


@router.get(
    '/resarcher_barema',
    response_model=list[ResearcherBarema],
)
def resarcher_barema(
    name: Optional[str] = Query(None),
    lattes_id: Optional[str] = Query(None),
    yarticle: Optional[str] = Query(None),
    ywork_event: Optional[str] = Query(None),
    ybook: Optional[str] = Query(None),
    ychapter_book: Optional[str] = Query(None),
    ypatent: Optional[str] = Query(None),
    ysoftware: Optional[str] = Query(None),
    ybrand: Optional[str] = Query(None),
    yresource_progress: Optional[str] = Query(None),
    yresource_completed: Optional[str] = Query(None),
    yparticipation_events: Optional[str] = Query(None),
):
    year = YearBarema(
        article=yarticle,
        work_event=ywork_event,
        book=ybook,
        chapter_book=ychapter_book,
        patent=ypatent,
        software=ysoftware,
        brand=ybrand,
        resource_progress=yresource_progress,
        resource_completed=yresource_completed,
        participation_events=yparticipation_events,
    )

    return GenericService.barema_production(name, lattes_id, year)


@router.get('/secondWord')
def list_words(term: str):
    return GenericService.list_words(term)


@router.get('/lattes_update')
async def lattes_update(
    default_filters: DefaultFilters = Depends(),
    conn: Connection = Depends(get_conn),
):
    return await GenericService.lattes_update(conn, default_filters)


@router.get('/magazine')
async def get_magazine(
    issn: str | None = None,
    initials: str | None = None,
    page: int | None = None,
    lenght: int | None = None,
    conn: Connection = Depends(get_conn),
):
    return await GenericService.get_magazine(conn, issn, initials, page, lenght)
