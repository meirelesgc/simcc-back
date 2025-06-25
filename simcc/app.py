from contextlib import asynccontextmanager
from http import HTTPStatus

import httpx
from fastapi import Depends, FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from simcc.config import Settings
from simcc.core.database import admin_conn, conn
from simcc.routers import (
    generic,
    graduate_program,
    metrics,
    powerBI,
    production,
    researcher,
)
from simcc.routers.conectee import departament
from simcc.security import get_current_key


@asynccontextmanager
async def lifespan(app: FastAPI):
    await admin_conn.connect()
    await conn.connect()
    yield
    await admin_conn.disconnect()
    await conn.disconnect()


app = FastAPI(
    root_path=Settings().ROOT_PATH,
    lifespan=lifespan,
    docs_url='/swagger',
    openapi_url='/openapi.json',
    dependencies=[Depends(get_current_key)],
)


app.include_router(production.router, tags=['Production'])
app.include_router(researcher.router, tags=['Researcher'])
app.include_router(powerBI.router, tags=['PowerBI Data'])
app.include_router(metrics.router, tags=['Metrics'])
app.include_router(departament.router, prefix='/ufmg', tags=['Conectee'])
app.include_router(graduate_program.router, tags=['Graduate Program'])
app.include_router(generic.router, tags=['Generic'])


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
    allow_credentials=True,
)


@app.middleware('http')
async def reverse_proxy(request: Request, call_next):
    response = await call_next(request)
    if response.status_code == HTTPStatus.NOT_FOUND:
        async with httpx.AsyncClient() as client:
            proxy_response = await client.request(
                method=request.method,
                url=f'{Settings().PROXY_URL}{request.url.path}',
                params=request.query_params,
                headers=dict(request.headers),
                content=await request.body(),
                timeout=None,
            )
            return Response(
                content=proxy_response.content,
                status_code=proxy_response.status_code,
                headers=dict(proxy_response.headers),
            )
    return response


@app.get('/')
def read_root():
    return {'message': 'Olá Mundo!'}


@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    return FileResponse('storage/ico.ico')
