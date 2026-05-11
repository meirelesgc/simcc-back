from pydantic_settings import BaseSettings


class Settings(BaseSettings, extra='ignore'):
    DATABASE_URL: str
    ADMIN_DATABASE_URL: str

    ADMIN_URL: str = 'http://localhost:0000'

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
