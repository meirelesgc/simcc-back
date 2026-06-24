from http import HTTPStatus
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from simcc.core.logging import setup_logging
from simcc.core.middleware.logging import LoggingMiddleware
from simcc.routers import (
    external,
    graduate_program,
    institution,
    maria,
    metrics,
    powerBi,
    research_group,
    researcher,
    routines,
)
from simcc.routers.production import (
    bibliographic,
    events,
    experience,
    intellectual_property,
    projects_guidance,
    summaries,
)

setup_logging()

app = FastAPI()

app.add_middleware(LoggingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
    allow_credentials=True,
)

app.include_router(external.router)
app.include_router(bibliographic.router)
app.include_router(intellectual_property.router)
app.include_router(events.router)
app.include_router(projects_guidance.router)
app.include_router(summaries.router)
app.include_router(experience.router)
app.include_router(researcher.router)
app.include_router(metrics.router)
app.include_router(institution.router)
app.include_router(graduate_program.router)
app.include_router(research_group.router)
app.include_router(maria.router)
app.include_router(routines.router)
app.include_router(powerBi.router)


@app.get('/', status_code=HTTPStatus.OK)
def read_root():
    return {'message': 'Olá Mundo!'}


@app.get('/favicon.ico', status_code=HTTPStatus.OK)
def favicon():
    file_path = Path('storage/static', 'favicon.ico')
    return FileResponse(file_path)
