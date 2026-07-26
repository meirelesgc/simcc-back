from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings, extra='ignore'):
    DATABASE_URL: str
    ADMIN_DATABASE_URL: str

    ADMIN_URL: str = 'http://localhost:0000'
    URL: str = 'http://localhost:0000'
    OPENAI_API_KEY: Optional[str] = None
    FIREBASE_COLLECTION: str = 'termos_busca'
    INTERNAL_API_KEY: Optional[str] = None

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

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

