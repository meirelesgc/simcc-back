from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str

    OPENAI_API_KEY: Optional[str] = None
    ALTERNATIVE_CNPQ_SERVICE: bool = False
    JADE_EXTRATOR_FOLTER: Optional[str] = None

    FIREBASE_COLLECTION: Optional[str] = "termos_busca"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
