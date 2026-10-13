"""Low-level input sources used by the workshop examples."""

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

from workshop_stream.support import text
from workshop_stream.support.stream import Stream


async def read_line() -> str:
    return await asyncio.to_thread(input)


def read_all(path: Path) -> Stream[bytes]:
    return Stream._from_factory(lambda: _read_file_bytes(path))


def readUtf8Lines(path: Path) -> Stream[str]:
    return read_all(path).via(text.utf8.decode).via(text.lines)


async def _read_file_bytes(path: Path) -> AsyncIterator[bytes]:
    with path.open("rb") as file:
        while chunk := await asyncio.to_thread(file.read, 8192):
            yield chunk
