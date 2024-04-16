from dataclasses import dataclass
from typing import Protocol

@dataclass
class Range:
    start: int
    end: int | None

    def __bool__(self) -> bool:
        return self.start >0 or self.end is not None


@dataclass
class ObjectMetadata:
    etag: str
    total: int


@dataclass
class PartialData:
    data: bytes
    total: int


class StorageBackend(Protocol):
    async def put(self, namespace: str, key: str, body: bytes) -> None: ...

    async def get(self, namespace: str, key: str, range: Range | None = None) -> PartialData: ...

    async def head(self, namespace: str, key: str) -> ObjectMetadata: ...

    async def delete(self, namespace: str, key: str) -> None: ...

    async def list_objects(self, namespace: str, prefix: str) -> list[str]: ...
