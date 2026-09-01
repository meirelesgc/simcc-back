from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError

from simcc.core.cache.cache_service import CacheService


@pytest.mark.unit
def test_cache_service_build_key_and_hash():
    service = CacheService(redis_client=None, enabled=True)
    key = service.build_key('ai', 'chat:batch', 'abc123hash')
    assert key == 'simcc:ai:chat:batch:abc123hash'

    h1 = service.hash_payload({'query': 'inteligência artificial'})
    h2 = service.hash_payload({'query': 'inteligência artificial'})
    h3 = service.hash_payload({'query': 'outra busca'})
    assert h1 == h2
    assert h1 != h3


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cache_service_disabled():
    service = CacheService(redis_client=None, enabled=False)
    assert await service.get('any_key') is None
    assert await service.set('any_key', {'val': 1}) is False
    assert await service.delete('any_key') is False
    assert await service.exists('any_key') is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cache_service_get_set_delete_success():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = '{"answer": "Olá", "cached": true}'
    mock_redis.exists.return_value = 1
    mock_redis.delete.return_value = 1

    service = CacheService(
        redis_client=mock_redis, enabled=True, default_ttl=3600
    )

    val = await service.get('test_key')
    assert val == {'answer': 'Olá', 'cached': True}

    set_ok = await service.set('test_key', {'answer': 'Olá'})
    assert set_ok is True
    mock_redis.set.assert_called_once()

    exists = await service.exists('test_key')
    assert exists is True

    deleted = await service.delete('test_key')
    assert deleted is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cache_service_fallback_on_redis_error():
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = ConnectionError('Redis connection refused')
    mock_redis.set.side_effect = ConnectionError('Redis connection refused')
    mock_redis.delete.side_effect = ConnectionError('Redis connection refused')
    mock_redis.exists.side_effect = ConnectionError('Redis connection refused')

    service = CacheService(redis_client=mock_redis, enabled=True)

    # All operations must catch error and return fallback safely
    assert await service.get('test_key') is None
    assert await service.set('test_key', {'data': 123}) is False
    assert await service.delete('test_key') is False
    assert await service.exists('test_key') is False
