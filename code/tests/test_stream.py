import asyncio
import time
from collections.abc import AsyncIterator

from workshop_stream.intro_examples import non_strict_numbers
from workshop_stream.support.stream import Flow, Stream, flow


def test_stream_is_lazy_and_repeatable_with_explicit_collection() -> None:
    constructed: list[str] = []

    def source() -> Stream[int]:
        constructed.append("source")
        return Stream.values(1, 2, 3)

    stream = Stream._from_factory(source)

    assert constructed == []
    assert asyncio.run(stream.to_list()) == [1, 2, 3]
    assert asyncio.run(stream.to_list()) == [1, 2, 3]
    assert constructed == ["source", "source"]


def test_to_list_consumes_values_and_returns_a_list() -> None:
    consumed: list[int] = []
    source = Stream.values(1, 2, 3).foreach(consumed.append)

    actual = asyncio.run(source.to_list())

    assert actual == [None, None, None]
    assert consumed == [1, 2, 3]


def test_via_and_dot_methods_compose() -> None:
    source = Stream.values(1, 2, 3, 4)
    actual = asyncio.run(
        source.via(flow(int).map(str))
        .filter(lambda item: item != "2")
        .take(2)
        .to_list()
    )

    assert actual == ["1", "3"]


def test_map_applies_synchronous_function_sequentially() -> None:
    visited: list[int] = []

    def stringify(value: int) -> str:
        visited.append(value)
        return str(value)

    actual = asyncio.run(Stream.values(1, 2, 3).map(stringify).to_list())

    assert actual == ["1", "2", "3"]
    assert visited == [1, 2, 3]


def test_map_async_preserves_input_order() -> None:
    async def delay(value: int) -> int:
        await asyncio.sleep({1: 0.03, 2: 0.02, 3: 0.001}[value])
        return value

    actual = asyncio.run(
        Stream.values(1, 2, 3).mapAsync(delay, concurrency=3).to_list()
    )

    assert actual == [1, 2, 3]


def test_map_async_defaults_to_sequential_ordered_mapping() -> None:
    started: list[int] = []
    completed: list[int] = []

    async def record(value: int) -> int:
        started.append(value)
        await asyncio.sleep(0.001)
        completed.append(value)
        return value

    actual = asyncio.run(Stream.values(1, 2, 3).mapAsync(record).to_list())

    assert actual == [1, 2, 3]
    assert started == [1, 2, 3]
    assert completed == [1, 2, 3]


def test_map_async_unordered_emits_completion_order() -> None:
    async def delay(value: int) -> int:
        await asyncio.sleep({1: 0.03, 2: 0.02, 3: 0.001}[value])
        return value

    actual = asyncio.run(
        Stream.values(1, 2, 3).mapAsyncUnordered(delay, concurrency=3).to_list()
    )

    assert actual == [3, 2, 1]


def test_flow_async_mapping_methods() -> None:
    async def stringify(value: int) -> str:
        await asyncio.sleep({1: 0.02, 2: 0.001}[value])
        return str(value)

    source = Stream.values(1, 2)
    ordered = flow(int).mapAsync(stringify, concurrency=2)
    unordered = flow(int).mapAsyncUnordered(stringify, concurrency=2)

    assert asyncio.run(ordered(source).to_list()) == ["1", "2"]
    assert asyncio.run(unordered(source).to_list()) == ["2", "1"]


def test_flow_map_async_defaults_to_one_at_a_time() -> None:
    active = 0
    max_active = 0

    async def measure(value: int) -> int:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.001)
        active -= 1
        return value

    actual = asyncio.run(flow(int).mapAsync(measure)(Stream.values(1, 2, 3)).to_list())

    assert actual == [1, 2, 3]
    assert max_active == 1


def test_typed_flow_starter_and_via_compose() -> None:
    stringify: Flow[int, str] = flow(int).map(str)
    keep_long: Flow[str, str] = flow(str).filter(lambda value: len(value) > 1)

    actual = asyncio.run(
        Stream.values(1, 20, 3).via(stringify.via(keep_long)).to_list()
    )

    assert actual == ["20"]


def test_flatten_handles_custom_streams() -> None:
    nested = Stream.values(Stream.values(1, 2), Stream.values(3))

    assert asyncio.run(nested.flatten().to_list()) == [1, 2, 3]


def test_stream_merge_and_zip() -> None:
    left = Stream.values(1, 2)
    right = Stream.values(10, 20)

    assert asyncio.run(left.zip(right).to_list()) == [(1, 10), (2, 20)]
    assert sorted(asyncio.run(left.merge(right).to_list())) == [1, 2, 10, 20]


def test_stream_zip_combines_typed_values() -> None:
    assert asyncio.run(Stream.values(1, 2).zip(Stream.values("a", "b")).to_list()) == [
        (1, "a"),
        (2, "b"),
    ]


def test_flow_zip_combines_transformed_values_with_a_reusable_stream() -> None:
    zipped = flow(int).map(str).zip(Stream.values("a", "b"))
    source = Stream.values(1, 2)

    assert asyncio.run(zipped(source).to_list()) == [("1", "a"), ("2", "b")]
    assert asyncio.run(zipped(source).to_list()) == [("1", "a"), ("2", "b")]


def test_stream_zip_completes_when_either_input_is_shorter() -> None:
    assert asyncio.run(Stream.values(1).zip(Stream.values("a", "b")).to_list()) == [
        (1, "a")
    ]
    assert asyncio.run(Stream.values(1, 2).zip(Stream.values("a")).to_list()) == [
        (1, "a")
    ]


def test_sleep_repeat_and_fixed_delay_emit_none() -> None:
    started = time.monotonic()
    repeated_sleep = asyncio.run(Stream.sleep(0.01).repeat().take(5).to_list())
    elapsed = time.monotonic() - started
    fixed_delay = asyncio.run(Stream.fixed_delay(0).take(3).to_list())

    assert repeated_sleep == [None, None, None, None, None]
    assert elapsed >= 0.05
    assert fixed_delay == [None, None, None]


def test_repeat_recreates_source_for_each_cycle() -> None:
    creations: list[int] = []

    def source() -> Stream[int]:
        async def values() -> AsyncIterator[int]:
            creations.append(len(creations))
            yield len(creations)

        return Stream._from_factory(values)

    repeated = source().repeat().take(4)

    assert asyncio.run(repeated.to_list()) == [1, 2, 3, 4]
    assert creations == [0, 1, 2, 3]


def test_repeat_emits_the_finite_first_cycle_of_a_one_shot_source() -> None:
    iterator = iter((1, 2, 3))

    async def one_shot():
        for value in iterator:
            yield value

    source = Stream._from_factory(lambda: one_shot())

    assert asyncio.run(source.repeat().take(3).to_list()) == [1, 2, 3]


def test_foreach_supports_sync_and_async_callbacks() -> None:
    seen_sync: list[int] = []
    seen_async: list[int] = []

    async def record_async(value: int) -> None:
        seen_async.append(value)

    asyncio.run(Stream.values(1, 2, 3).foreach(seen_sync.append).drain())
    asyncio.run(Stream.values(4, 5).foreach(record_async).drain())

    assert seen_sync == [1, 2, 3]
    assert seen_async == [4, 5]


def test_non_strict_intro_stream_repeats_to_fifty_values() -> None:
    actual = asyncio.run(non_strict_numbers.to_list())

    assert len(actual) == 50
    assert actual[:9] == [1, 2, 3, 1, 2, 3, 1, 2, 3]
