"""Small stream operators used by the workshop examples."""

import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from pathlib import Path
from typing import IO

from aiostream.core import streamcontext

from workshop_stream.support.stream import Flow


def write_all(path: Path) -> Flow[bytes, None]:
    return Flow(lambda source: _write_all(source, path))


async def _write_all(source: AsyncIterable[bytes], path: Path) -> AsyncIterator[None]:
    with path.open("wb") as file:
        async with streamcontext(source) as items:
            async for item in items:
                await asyncio.to_thread(_write, file, item)
                yield None


def _write(file: IO[bytes], item: bytes) -> None:
    file.write(item)
