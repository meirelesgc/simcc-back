from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from simcc.routers.production import bibliographic

app = FastAPI(docs_url='/swagger')


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
    allow_credentials=True,
)

app.include_router(bibliographic.router)
