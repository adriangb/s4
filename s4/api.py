import base64
from dataclasses import dataclass
from hashlib import md5
from time import time
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Header, Path, Request, Response

from s4.metadata import Cache
from s4.storage import StorageBackend, Range
from s4.depends import Injected

router = APIRouter()


@dataclass
class Config:
    host: str


@router.get("/health")
async def health() -> Response:
    return Response(status_code=200)


@dataclass
class ObjectPath:
    bucket: str
    key: str


def get_bucket(
    host: Annotated[str, Header()],
    key: Annotated[str, Path()],
    config: Injected[Config],
) -> ObjectPath:
    # remove the port
    host = host.split(":")[0]
    if host == config.host:
        bucket = key.split("/")[0]
        key = key[len(bucket) + 1 :]
        return ObjectPath(bucket=bucket, key=key)
    else:
        bucket = host.split(".")[0]
        return ObjectPath(bucket=bucket, key=key)


@dataclass
class Md5Digest:
    etag: str
    content_md5: str


def get_md5_digests(data: bytes) -> Md5Digest:
    hash = md5(data)
    etag = hash.hexdigest()
    content_md5 = base64.b64encode(hash.digest()).decode()
    return Md5Digest(etag=etag, content_md5=content_md5)


def range_from_header(range: Annotated[str| None, Header()] = None) -> Range:
    if range is None:
        return Range(start=0, end=None)
    if not range.startswith("bytes="):
        raise HTTPException(status_code=400, detail="Invalid range header")
    range = range[6:]
    start, end = range.split("-")
    start = start or "0"
    return Range(start=int(start), end=int(end) if end else None)

@router.put("/api/s3/{key:path}")
async def upload_object(
    request: Request,
    object: Annotated[ObjectPath, Depends(get_bucket)],
    cache: Injected[Cache],
    fs: Annotated[StorageBackend, Depends()],
) -> Response:
    body = await request.body()
    digests = get_md5_digests(body)
    content_md5 = request.headers.get("Content-MD5")
    if content_md5 and digests.content_md5 != content_md5:
        return Response(status_code=400, content="MD5 mismatch")
    await fs.put(object.bucket, object.key, body)

    # handle parquet
    # this is just an example, the idea is that by knowing the schema of the file we can (1) validate the data, (2) cache the metadata
    if body[:4] == b"PAR1":
        # cache the metadata
        # get the footer length, which is stored in bytes -8 to -4 (the last 4 are the magic number)
        footer_length = int.from_bytes(body[-8:-4], "little")
        # get the footer
        footer = body[-footer_length:]
        # calculate what byte range the footer is in
        footer_start = len(body) - footer_length
        footer_end = len(body)
        # cache the footer
        await cache.cache_range(
            f'{object.bucket}/{object.key}',
            footer,
            footer_start,
            footer_end,
            len(body),
        )

    return Response(status_code=200, headers={"ETag": digests.etag})


@router.get("/api/s3/{key:path}")
async def download_object(
    object: Annotated[ObjectPath, Depends(get_bucket)],
    fs: Annotated[StorageBackend, Depends()],
    cache: Injected[Cache],
    range: Annotated[Range, Depends(range_from_header)],
) -> Response:
    cached = await cache.get_cached_range(f'{object.bucket}/{object.key}', range.start, range.end + 1 if range.end is not None else None)
    if cached:
        if range:
            return Response(status_code=206, content=cached.data, headers={"Content-Range": f"bytes {range.start}-{range.end}/{cached.total}", "Content-Length": str(len(cached.data))})
        return Response(content=cached, headers={"Content-Length": str(cached.total), "ETag": get_md5_digests(cached.data).etag})
    body = await fs.get(object.bucket, object.key, range=range)
    if range:
        return Response(status_code=206, content=body.data, headers={"Content-Range": f"bytes {range.start}-{range.end}/{body.total}", "Content-Length": str(len(body.data))})
    digest = get_md5_digests(body.data)
    return Response(content=body.data, headers={"ETag": digest.etag, "Content-Length": str(len(body.data))})


@router.head("/api/s3/{key:path}")
async def head_object(
    object: Annotated[ObjectPath, Depends(get_bucket)],
    fs: Annotated[StorageBackend, Depends()],
) -> Response:
    body = await fs.head(object.bucket, object.key)
    return Response(headers={"ETag": body.etag, "Content-Length": str(body.total), "Accept-Ranges": "bytes"})
