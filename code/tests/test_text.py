"""Focused tests for the small file text pipeline operators."""

import asyncio

from workshop_stream.support import text
from workshop_stream.support.stream import Flow, Stream


async def collect[T](source: Stream[T]) -> list[T]:
    return await source.to_list()


def test_decode_handles_multibyte_characters_split_between_chunks() -> None:
    source = Stream.values(b"caf\xc3", b"\xa9")

    decoded = asyncio.run(collect(source.via(text.utf8.decode)))

    assert decoded == ["caf", "é"]


def test_encode_converts_text_chunks_to_bytes() -> None:
    source = Stream.values("café", " ☕")

    result = asyncio.run(collect(source.via(text.utf8.encode)))

    assert result == ["café".encode(), " ☕".encode()]


def test_decode_supports_non_utf8_encodings() -> None:
    source = Stream.values(b"caf\xe9")
    latin_1 = Flow(lambda items: text._decode(items, "latin-1"))

    result = asyncio.run(collect(source.via(latin_1)))

    assert result == ["café"]


def test_encode_supports_non_utf8_encodings() -> None:
    source = Stream.values("café")
    latin_1 = Flow(lambda items: text._encode(items, "latin-1"))

    result = asyncio.run(collect(source.via(latin_1)))

    assert result == [b"caf\xe9"]


def test_lines_handles_lf_cr_and_crlf_across_chunks() -> None:
    source = Stream.values("first\r", "\n\nsecond\r", "third\nlast")

    result = asyncio.run(collect(source.via(text.lines)))

    assert result == ["first", "", "second", "third", "last"]


def test_lines_emits_final_unterminated_line_and_blank_lines() -> None:
    source = Stream.values("\n", "value", "\n", "\n")

    result = asyncio.run(collect(source.via(text.lines)))

    assert result == ["", "value", ""]


def test_lines_treats_a_final_carriage_return_as_a_delimiter() -> None:
    source = Stream.values("value\r")

    result = asyncio.run(collect(source.via(text.lines)))

    assert result == ["value"]
