from simcc.config import Settings
from simcc.core.connection import Connection

conn = Connection(
    Settings().DATABASE_URL,
    max_size=20,
)
admin_conn = Connection(
    Settings().ADMIN_DATABASE_URL,
    max_size=20,
)


async def get_admin_conn():
    yield admin_conn


async def get_conn():
    yield conn
