from typing import Literal, Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings, extra='ignore'):
    DATABASE_URL: str
    ADMIN_DATABASE_URL: str

    ADMIN_URL: str = 'http://localhost:0000'
    INTERNAL_API_KEY: Optional[str] = None
    OPENAI_API_KEY: Optional[str] = None

    FIREBASE_COLLECTION: str = 'termos_busca'
    FIREBASE_CERT_PATH: str = 'cert.json'
    LOG_LEVEL_MIDDLEWARE: Literal['all', 'intermediate', 'error'] = 'all'

    XML_PATH: str = 'storage/xml'
    CURRENT_XML_PATH: str = 'storage/xml/current'
    ZIP_XML_PATH: str = 'storage/xml/current'

    ALTERNATIVE_CNPQ_SERVICE: bool = False

    HOP_IMAGE: str = 'gleidsoncosta/simcc-extrator:latest'
    HOP_XML_VOLUME: str = 'simcc_xml'
    HOP_NETWORK: str = 'simcc-back_default'
    HOP_RUN_PARAMETERS: Optional[str] = None

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
