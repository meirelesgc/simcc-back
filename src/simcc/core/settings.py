from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings, extra='ignore'):
    DATABASE_URL: str
    ADMIN_DATABASE_URL: str

    CORS_ALLOW_ORIGINS: list[str] = ['*']
    CORS_ALLOW_METHODS: list[str] = ['*']
    CORS_ALLOW_HEADERS: list[str] = ['*']
    CORS_ALLOW_CREDENTIALS: bool = True

    ADMIN_URL: str = 'http://localhost:0000/'
    URL: str = 'http://localhost:0000/'
    OPENAI_API_KEY: Optional[str] = None
    FIREBASE_COLLECTION: str = 'termos_busca'
    INTERNAL_API_KEY: Optional[str] = None
    LOG_STREAM_TOKEN: Optional[str] = None

    FIREBASE_CERT_PATH: str = 'cert.json'

    XML_PATH: str = 'storage/xml'
    CURRENT_XML_PATH: str = 'storage/xml/current'
    ZIP_XML_PATH: str = 'storage/xml/current'

    ALTERNATIVE_CNPQ_SERVICE: bool = False

    HOP_IMAGE: str = 'gleidsoncosta/simcc-extrator:latest'
    HOP_XML_VOLUME: str = 'simcc_xml'
    HOP_NETWORK: str = 'simcc-back_default'
    HOP_RUN_PARAMETERS: Optional[str] = None

    APPLICATION: str = 'simcc'
    ENVIRONMENT: str = 'development'
    LOG_LEVEL: str = 'INFO'
    LOG_DIR: str = 'logs'
    LOG_RETENTION_DAYS: int = 7

    @field_validator(
        'CORS_ALLOW_ORIGINS',
        'CORS_ALLOW_METHODS',
        'CORS_ALLOW_HEADERS',
        mode='before',
    )
    @classmethod
    def assemble_cors_list(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str) and not v.startswith('['):
            return [i.strip() for i in v.split(',') if i.strip()]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
