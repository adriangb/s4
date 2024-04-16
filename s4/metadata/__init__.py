from dataclasses import dataclass
from typing import Protocol

from s4.storage import PartialData


class MetadataBackend(Protocol):
    async def put(self, key: str, value: bytes) -> None: ...

    async def get(self, key: str) -> bytes | None: ...


@dataclass
class Cache:
    backend: MetadataBackend

    async def cache_range(self, key: str, value: bytes, start: int, end: int, total: int) -> None:
        # store the range metadata
        await self.backend.put(f"range/{key}", f"{start}-{end}/{total}".encode())
        # store the range data
        await self.backend.put(f"data/{key}/{start}-{end}", value)

    async def get_cached_range(self, key: str, start: int, end: int | None) -> PartialData | None:
        # get the range for this key
        range = await self.backend.get(f"range/{key}")
        if range is None:
            return None
        # parse the range
        range = range.decode()
        bounds, total = range.split("/")
        total = int(total)
        range_start, range_end = bounds.split("-")
        range_start = int(range_start)
        range_end = int(range_end)
        if end is None:
            if range_end != total:
                return None
            else:
                end = total
        # check if the requested range is within the stored range
        if start < range_start or end > range_end:
            return None
        # get the data
        data = await self.backend.get(f"data/{key}/{range_start}-{range_end}")
        if data is None:
            return None
        # slice the data
        if start != range_start or end != range_end:
            data = data[start - range_start : end - range_start]
        return PartialData(data=data, total=total)
