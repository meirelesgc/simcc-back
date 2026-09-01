from typing import Optional

import redis.asyncio as aioredis
from redis.asyncio.connection import ConnectionPool

_pool: Optional[ConnectionPool] = None


def get_redis_pool(redis_url: str) -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool.from_url(
            redis_url,
            decode_responses=True,
            max_connections=50,
            socket_timeout=1.0,
            socket_connect_timeout=1.0,
        )
    return _pool


def get_redis_client(redis_url: str) -> aioredis.Redis:
    pool = get_redis_pool(redis_url)
    return aioredis.Redis(connection_pool=pool)


async def close_redis_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.disconnect()
        _pool = None
