from __future__ import annotations

from s4.metadata import MetadataBackend


class NoOpCacheBackend(MetadataBackend):
    async def put(self, key: str, value: bytes) -> None:
        pass

    async def get(self, key: str) -> bytes | None:
        return None
