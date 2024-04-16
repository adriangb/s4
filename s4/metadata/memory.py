from dataclasses import dataclass, field

from s4.metadata import MetadataBackend


@dataclass
class MemoryCacheBackend(MetadataBackend):
    storage: dict[str, bytes] = field(default_factory=dict)

    async def put(self, key: str, value: bytes) -> None:
        self.storage[key] = value

    async def get(self, key: str) -> bytes | None:
        return self.storage.get(key)
