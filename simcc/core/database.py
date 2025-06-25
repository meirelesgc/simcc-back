from simcc.config import Settings
from simcc.core.connection import Connection

conn = Connection(
    Settings().get_conn_str(),
    max_size=20,
    timeout=10,
)
admin_conn = Connection(
    Settings().get_admin_conn_str(),
    max_size=20,
    timeout=10,
)


async def get_admin_conn():
    yield admin_conn


async def get_conn():
    yield conn
