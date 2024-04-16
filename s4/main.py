from contextlib import AsyncExitStack
import anyio
from fastapi import FastAPI

from s4.api import Config, router
from s4.depends import bind
from s4.metadata import Cache
from s4.storage import StorageBackend


def make_app(
    storage: StorageBackend,
    cache: Cache,
    config: Config,
) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    bind(app, StorageBackend, storage)
    bind(app, Cache, cache)
    bind(app, Config, config)
    return app


async def main() -> None:
    import uvicorn

    from s4.storage.s3 import S3Storage
    from s4.metadata.redis import RedisMetadataBackend
    from s4.metadata.noop import NoOpCacheBackend
    from s4.storage.memory import InMemoryBackend

    async with AsyncExitStack() as stack:
        cache = await stack.enter_async_context(
            RedisMetadataBackend.connect("redis://localhost")
        )
        # TO TEST: uncomment this line to disable caching
        # cache = NoOpCacheBackend()
        fs = await stack.enter_async_context(
            S3Storage.connect(
                access_key_id="<secret>",
                access_key_secret="<secret>",
                region="us-east-1",
            )
        )
        # TO TEST: uncomment this line to use in-memory storage instead of S3
        # fs = InMemoryBackend()
        app = make_app(fs, Cache(cache), Config(host="127.0.0.1"))

        config = uvicorn.Config(app, host="0.0.0.0", port=8000)
        server = uvicorn.Server(config)
        await server.serve()


if __name__ == "__main__":
    anyio.run(main)
