from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_NAME: str
    DATABASE_USER: str = "postgres"
    DATABASE_PASSWORD: str = "postgres"
    DATABASE_HOST: str = "localhost"
    ADM_DATABASE: str
    PORT: int = 5432

    OPENAI_API_KEY: Optional[str] = None
    ALTERNATIVE_CNPQ_SERVICE: bool = False
    JADE_EXTRATOR_FOLTER: Optional[str] = None

    FIREBASE_COLLECTION: Optional[str] = "termos_busca"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
