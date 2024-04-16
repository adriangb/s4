from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from aioaws.s3 import S3Client, S3Config
from httpx import AsyncClient
from s4.storage import ObjectMetadata, PartialData, Range
from s4.storage import StorageBackend


@dataclass
class S3Storage(StorageBackend):
    client: AsyncClient
    access_key_id: str
    access_key_secret: str
    region: str
    endpoint: str | None

    @classmethod
    @asynccontextmanager
    async def connect(cls, access_key_id: str, access_key_secret: str, region: str, endpoint: str | None = None) -> AsyncIterator[S3Storage]:
        async with AsyncClient() as client:
            try:
                yield cls(client, access_key_id, access_key_secret, region, endpoint)
            finally:
                await client.aclose()
    
    def _get_client(self, bucket: str) -> S3Client:
        return S3Client(
            self.client,
            S3Config(
                aws_access_key=self.access_key_id,
                aws_secret_key=self.access_key_secret,
                aws_region=self.region,
                aws_s3_bucket=bucket,
                aws_host=self.endpoint,
            ),
        )

    async def put(self, namespace: str, key: str, body: bytes) -> None:
        client = self._get_client(namespace)
        await client.upload(key, body)

    async def get(self, namespace: str, key: str, range: Range | None = None) -> PartialData:
        client = self._get_client(namespace)
        url = client.signed_download_url(key, method='GET')
        headers: dict[str, str] = {}
        if range is not None:
            headers["Range"] = f"bytes={range.start}-{range.end or ''}"
        response = await self.client.get(url, headers=headers)
        response.raise_for_status()
        resp_range = response.headers.get("Content-Range")
        if resp_range:
            total = int(resp_range.split("/")[1])
        else:
            total = len(response.content)
        return PartialData(data=response.content, total=total)

    async def head(self, namespace: str, key: str) -> ObjectMetadata:
        client = self._get_client(namespace)
        url = client.signed_download_url(key, method='HEAD')
        response = await self.client.head(url)
        response.raise_for_status()
        total = int(response.headers["Content-Length"])
        return ObjectMetadata(total=total, etag=response.headers["ETag"])


    async def delete(self, namespace: str, key: str) -> None:
        client = self._get_client(namespace)
        await client.delete(key)
    
    async def list_objects(self, namespace: str, prefix: str) -> list[str]:
        client = self._get_client(namespace)
        return [
            obj.key
            async for obj in client.list(prefix=prefix)
        ]
