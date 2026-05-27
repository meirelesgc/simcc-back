from typing import Literal, Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings, extra='ignore'):
    DATABASE_URL: str
    ADMIN_DATABASE_URL: str

    ADMIN_URL: str = 'http://localhost:0000'
    OPENAI_API_KEY: Optional[str] = None

    FIREBASE_COLLECTION: str = 'termos_busca'
    LOG_LEVEL_MIDDLEWARE: Literal['all', 'intermediate', 'error'] = 'all'

    XML_PATH: str = 'storage/xml'
    CURRENT_XML_PATH: str = 'storage/xml/current'
    ZIP_XML_PATH: str = 'storage/xml/current'

    ALTERNATIVE_CNPQ_SERVICE: bool = False

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
