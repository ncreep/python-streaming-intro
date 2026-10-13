"""Tests for the small custom operators used by the workshop."""

import asyncio

from hypothesis import given
from hypothesis import strategies as st

from workshop_stream.support.stream import Stream


async def collect[T](source: Stream[T]) -> list[T]:
    return await source.to_list()


@given(st.lists(st.one_of(st.none(), st.integers(), st.text())))
def test_drop_none_drops_only_none(values: list[int | str | None]) -> None:
    source = Stream.values(*values)
    actual = asyncio.run(collect(source.drop_none()))

    assert actual == [value for value in values if value is not None]


def test_drop_none_preserves_falsey_values() -> None:
    values: list[object | None] = [None, 0, False, "", None]

    source = Stream.values(*values)
    actual = asyncio.run(collect(source.drop_none()))

    assert actual == [0, False, ""]


def test_intersperse_places_separator_between_values() -> None:
    source = Stream.values(1, 2, 3)
    actual = asyncio.run(collect(source.intersperse(0)))

    assert actual == [1, 0, 2, 0, 3]


def test_stream_operators_compose_with_each_other() -> None:
    async def stringify(value: int) -> str:
        return str(value)

    pipeline = Stream.values(1, 2).spaceout(0.001).mapAsync(stringify, concurrency=10)
    source_pipeline = Stream.values(1, 2, 3).take(2).map(str)
    mapped_pipeline = Stream.values(1, 2, 3).map(str).take(2)

    paced = asyncio.run(collect(pipeline))
    limited = asyncio.run(collect(source_pipeline))
    mapped = asyncio.run(collect(mapped_pipeline))

    assert paced == ["1", "2"]
    assert limited == ["1", "2"]
    assert mapped == ["1", "2"]


def test_flatten_composes_with_stream_operators() -> None:
    source = Stream.values(Stream.values(1, 2), Stream.values(3))

    actual = asyncio.run(collect(source.flatten()))

    assert actual == [1, 2, 3]


def test_drain_consumes_every_value_and_returns_none() -> None:
    seen: list[int] = []

    def record(value: int) -> int:
        seen.append(value)
        return value

    async def run() -> None:
        source = Stream.values(1, 2, 3).map(record)
        result = await source.drain()
        assert result is None

    asyncio.run(run())

    assert seen == [1, 2, 3]
