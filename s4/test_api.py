import socket
from typing import AsyncIterator

import anyio
import pytest
import uvicorn
from httpx import AsyncClient

from s4.api import Config, make_app
from s4.storage.memory import InMemoryBackend


@pytest.fixture
def fs() -> InMemoryBackend:
    """Fixture to provide the endpoint of the API."""
    return InMemoryBackend()


@pytest.fixture
async def endpoint(fs: InMemoryBackend) -> AsyncIterator[str]:
    """Fixture to provide the endpoint of the API."""
    app = make_app(fs, Config(host="127.0.0.1"))
    # find an open port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]

    host = f"http://127.0.0.1:{port}"

    async with AsyncClient(base_url=host) as client:

        async def is_healthy() -> bool:
            try:
                resp = await client.get("/health")
                return resp.status_code == 200
            except Exception:
                return False

        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port))
        async with anyio.create_task_group() as tg:
            tg.start_soon(server.serve)
            while not (await is_healthy()):
                await anyio.sleep(0.05)

            yield f"{host}/api/s3"
            await server.shutdown()
            tg.cancel_scope.cancel()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_upload(endpoint: str) -> None:
    """Test the API for uploading to an S3-compatible storage service."""
    async with AsyncClient(base_url=endpoint) as client:
        resp = await client.put(
            "/path/to/key",
            content=b"Hello, World!",
            headers={"Host": "bucket.s3.amazonaws.com"},
        )
        assert resp.status_code == 200


@pytest.mark.anyio
async def test_download(endpoint: str) -> None:
    """Test the API for downloading from an S3-compatible storage service."""
    async with AsyncClient(base_url=endpoint) as client:
        resp = await client.put(
            "/path/to/key",
            content=b"Hello, World!",
            headers={"Host": "bucket.s3.amazonaws.com"},
        )
        assert resp.status_code == 200

        resp = await client.get(
            "/path/to/key",
            headers={"Host": "bucket.s3.amazonaws.com"},
        )
        assert resp.status_code == 200
        assert resp.content == b"Hello, World!"
