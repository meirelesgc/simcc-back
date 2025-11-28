from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ROOT_PATH: str = ''

    URL: Optional[str] = None

    DATABASE_URL: str
    ADMIN_DATABASE_URL: str

    PROXY_URL: str = 'http://localhost:8080'
    ALTERNATIVE_CNPQ_SERVICE: bool = False
    FIREBASE_COLLECTION: str = 'termos_busca'

    XML_PATH: str = 'storage/xml'
    CURRENT_XML_PATH: str = 'storage/xml/current'
    ZIP_XML_PATH: str = 'storage/xml/current'

    OPENAI_API_KEY: str = None

    JADE_ADMIN_URL: str = 'http://localhost:9090'

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


settings = Settings()
