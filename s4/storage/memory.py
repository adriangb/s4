from collections import defaultdict
from dataclasses import dataclass, field
from hashlib import md5

from s4.storage import ObjectMetadata, PartialData, Range, StorageBackend


@dataclass
class Object:
    body: bytes


@dataclass
class InMemoryBackend(StorageBackend):
    storage: dict[str, dict[str, Object]] = field(
        default_factory=lambda: defaultdict(dict)
    )

    async def put(self, namespace: str, key: str, body: bytes) -> None:
        self.storage[namespace][key] = Object(body=body)

    async def get(self, namespace: str, key: str, range: Range | None = None) -> PartialData:
        data = self.storage[namespace][key].body
        total = len(data)
        if range is not None:
            data = data[range.start : range.end + 1 if range.end else len(data)]
        return PartialData(data=data, total=total)

    async def head(self, namespace: str, key: str) -> ObjectMetadata:
        etag = md5(self.storage[namespace][key].body).hexdigest()
        return ObjectMetadata(total=len(self.storage[namespace][key].body), etag=etag)

    async def delete(self, namespace: str, key: str) -> None:
        del self.storage[namespace][key]

    async def list_objects(self, namespace: str, prefix: str) -> list[str]:
        return [key for key in self.storage[namespace] if key.startswith(prefix)]
