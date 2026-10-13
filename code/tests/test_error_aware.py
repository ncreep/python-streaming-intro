import asyncio

import pytest

from workshop_stream.support.stream import Stream


async def collect[T](source: Stream[T]) -> list[T]:
    return await source.to_list()


def test_values_emits_each_value_in_order() -> None:
    assert asyncio.run(collect(Stream.values("a", "b", "c"))) == ["a", "b", "c"]


def test_raise_error_is_lazy_and_raises_when_consumed() -> None:
    source = Stream.raise_error(RuntimeError("oh noes!"))

    async def consume() -> None:
        async with source.stream() as streamer:
            with pytest.raises(RuntimeError, match="oh noes!"):
                await anext(streamer)

    asyncio.run(consume())


def test_append_emits_values_then_propagates_error() -> None:
    source = Stream.values("a", "b", "c").append(
        Stream.raise_error(RuntimeError("oh noes!"))
    )

    async def consume() -> None:
        actual: list[str] = []
        async with source.stream() as streamer:
            with pytest.raises(RuntimeError, match="oh noes!"):
                async for item in streamer:
                    actual.append(item)
        assert actual == ["a", "b", "c"]

    asyncio.run(consume())


def test_append_composes_two_value_streams() -> None:
    source = Stream.values("a", "b").append(Stream.values("c", "d"))

    assert asyncio.run(collect(source)) == ["a", "b", "c", "d"]


def test_handle_error_emits_fallback_after_values() -> None:
    source = (
        Stream.values("a", "b", "c")
        .append(Stream.raise_error(RuntimeError("oh noes!")))
        .handle_error(lambda error: Stream.values(f"Got: {error}"))
    )

    assert asyncio.run(collect(source)) == ["a", "b", "c", "Got: oh noes!"]


def test_empty_stream_emits_no_values() -> None:
    empty: Stream[int] = Stream.empty()

    assert asyncio.run(collect(empty)) == []


def test_foreach_prints_each_value_and_drains(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = asyncio.run(Stream.values("a", "b", "c").foreach(print).drain())

    assert result is None
    assert capsys.readouterr().out == "a\nb\nc\n"
