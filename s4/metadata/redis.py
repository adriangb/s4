from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator
from redis.asyncio import BlockingConnectionPool, Redis


@dataclass
class RedisMetadataBackend:
    redis: Redis

    @classmethod
    @asynccontextmanager
    async def connect(cls, dsn: str) -> AsyncIterator[RedisMetadataBackend]:
        pool = BlockingConnectionPool.from_url(dsn)  # type: ignore
        try:
            yield cls(Redis(connection_pool=pool))
        finally:
            await pool.aclose()

    async def get(self, key: str) -> bytes | None:
        return await self.redis.get(key)  # type: ignore

    async def put(self, key: str, value: bytes) -> None:
        await self.redis.set(key, value)  # type: ignore
