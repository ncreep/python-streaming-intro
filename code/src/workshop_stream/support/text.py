"""Reusable text transformation flows for byte and string streams."""

from __future__ import annotations

import codecs
from collections.abc import AsyncIterable, AsyncIterator

from aiostream.core import streamcontext

from workshop_stream.support.stream import Flow


async def _decode(source: AsyncIterable[bytes], encoding: str) -> AsyncIterator[str]:
    decoder = codecs.getincrementaldecoder(encoding)()
    async with streamcontext(source) as items:
        async for chunk in items:
            decoded = decoder.decode(chunk)
            if decoded:
                yield decoded
    final_text = decoder.decode(b"", final=True)
    if final_text:
        yield final_text


async def _encode(source: AsyncIterable[str], encoding: str) -> AsyncIterator[bytes]:
    encoder = codecs.getincrementalencoder(encoding)()
    async with streamcontext(source) as items:
        async for chunk in items:
            encoded = encoder.encode(chunk)
            if encoded:
                yield encoded
    final_bytes = encoder.encode("", final=True)
    if final_bytes:
        yield final_bytes


async def _lines(source: AsyncIterable[str]) -> AsyncIterator[str]:
    pending = ""
    async with streamcontext(source) as items:
        async for chunk in items:
            pending += chunk
            start = 0
            index = 0
            while index < len(pending):
                char = pending[index]
                if char == "\r" and index + 1 == len(pending):
                    break
                if char in "\r\n":
                    yield pending[start:index]
                    index += 1
                    if char == "\r" and index < len(pending) and pending[index] == "\n":
                        index += 1
                    start = index
                else:
                    index += 1
            pending = pending[start:]
    if pending.endswith("\r"):
        yield pending[:-1]
    elif pending:
        yield pending


class _Utf8:
    decode: Flow[bytes, str] = Flow(lambda source: _decode(source, "utf-8"))
    encode: Flow[str, bytes] = Flow(lambda source: _encode(source, "utf-8"))


utf8 = _Utf8()
lines: Flow[str, str] = Flow(_lines)
