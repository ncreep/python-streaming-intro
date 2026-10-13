import asyncio
from collections.abc import AsyncIterator

import pytest
from aiostream import stream as aio_stream
from hypothesis import given, settings
from hypothesis import strategies as st

from workshop_stream.support.stream import Stream


async def collect[T](
    source: Stream[T], max_items: int, duration: float = 60.0
) -> list[tuple[T, ...]]:
    return await source.group_within(max_items=max_items, duration=duration).to_list()


async def consume_one[T](grouped: Stream[tuple[T, ...]]) -> tuple[T, ...]:
    groups = await grouped.to_list()
    if not groups:
        raise AssertionError("expected a group, but the stream was empty")
    return groups[0]


def from_async_iterator[T](source: AsyncIterator[T]) -> Stream[T]:
    return Stream._from_factory(lambda: aio_stream.iterate(source))


def assert_groups_preserve_sequence[T](
    groups: list[tuple[T, ...]], expected: list[T], max_items: int
) -> None:
    flattened = [item for group in groups for item in group]
    assert len(flattened) == len(expected)
    for actual, wanted in zip(flattened, expected, strict=True):
        assert actual == wanted

    for group in groups:
        assert len(group) > 0
        assert len(group) <= max_items


def assert_group_contains[T](group: tuple[T, ...], expected: list[T]) -> None:
    assert len(group) == len(expected)
    for actual, wanted in zip(group, expected, strict=True):
        assert actual == wanted


@settings(max_examples=50, deadline=None)
@given(
    items=st.lists(st.integers(), max_size=80),
    max_items=st.integers(min_value=1, max_value=20),
)
def test_groups_preserve_values_and_flush_on_completion(
    items: list[int], max_items: int
) -> None:
    # This covers FS2's no-loss, nonempty, bounded-group, and completion
    # properties for element streams. FS2 Chunk-boundary cases are inapplicable:
    # AsyncIterable carries values, not the original chunk boundaries.
    source = Stream.values(*items)
    groups = asyncio.run(collect(source, max_items))

    assert_groups_preserve_sequence(groups, items, max_items)


@pytest.mark.asyncio
async def test_count_limit_emits_without_waiting_for_upstream_completion() -> None:
    completed = asyncio.Event()
    release = asyncio.Event()

    async def source() -> AsyncIterator[int]:
        yield 1
        yield 2
        await release.wait()
        completed.set()

    grouped = from_async_iterator(source()).group_within(max_items=2, duration=5.0)
    async with grouped.stream() as iterator:
        first_group = await asyncio.wait_for(anext(iterator), timeout=0.5)
        assert_group_contains(first_group, [1, 2])
        assert completed.is_set() is False
        release.set()


@pytest.mark.asyncio
async def test_duration_emits_buffered_values_before_upstream_completion() -> None:
    completed = asyncio.Event()
    release = asyncio.Event()

    async def source() -> AsyncIterator[int]:
        yield 1
        await release.wait()
        completed.set()

    grouped = from_async_iterator(source()).group_within(max_items=10, duration=0.1)
    async with grouped.stream() as iterator:
        timed_group = await asyncio.wait_for(anext(iterator), timeout=1.0)
        assert_group_contains(timed_group, [1])
        assert completed.is_set() is False
        release.set()


@pytest.mark.asyncio
async def test_count_emission_resets_the_deadline() -> None:
    duration = 0.3
    first_sent = asyncio.Event()
    send_second = asyncio.Event()
    third_pulled = asyncio.Event()
    keep_source_open = asyncio.Event()

    async def source() -> AsyncIterator[int]:
        yield 1
        first_sent.set()
        await send_second.wait()
        yield 2
        yield 3
        third_pulled.set()
        await keep_source_open.wait()

    grouped = from_async_iterator(source()).group_within(max_items=2, duration=duration)
    async with grouped.stream() as iterator:
        first_group_task = asyncio.ensure_future(anext(iterator))
        await asyncio.wait_for(first_sent.wait(), timeout=0.5)
        # Let most of the first timer elapse before the count flush. The next
        # partial group must receive a fresh full deadline after that flush.
        await asyncio.sleep(duration * 0.65)
        send_second.set()
        first_group = await asyncio.wait_for(first_group_task, timeout=duration)
        assert_group_contains(first_group, [1, 2])

        second_group_task = asyncio.ensure_future(anext(iterator))
        await asyncio.wait_for(third_pulled.wait(), timeout=0.5)
        done, _ = await asyncio.wait({second_group_task}, timeout=duration * 0.65)
        assert len(done) == 0

        second_group = await asyncio.wait_for(second_group_task, timeout=duration * 0.6)
        assert_group_contains(second_group, [3])
        keep_source_open.set()


@pytest.mark.asyncio
async def test_timeout_partial_emission_resets_the_deadline() -> None:
    duration = 0.3
    release_second = asyncio.Event()
    second_pulled = asyncio.Event()
    keep_source_open = asyncio.Event()

    async def source() -> AsyncIterator[int]:
        yield 1
        await release_second.wait()
        yield 2
        second_pulled.set()
        await keep_source_open.wait()

    grouped = from_async_iterator(source()).group_within(
        max_items=10, duration=duration
    )
    async with grouped.stream() as iterator:
        first_group = await asyncio.wait_for(anext(iterator), timeout=1.0)
        assert_group_contains(first_group, [1])

        release_second.set()
        second_group_task = asyncio.ensure_future(anext(iterator))
        await second_pulled.wait()
        done, _ = await asyncio.wait({second_group_task}, timeout=duration * 0.6)
        assert len(done) == 0

        second_group = await asyncio.wait_for(second_group_task, timeout=duration * 0.6)
        assert_group_contains(second_group, [2])
        keep_source_open.set()


@pytest.mark.asyncio
async def test_repeated_empty_deadlines_emit_no_groups_and_next_value_is_immediate() -> (
    None
):
    duration = 0.05
    release_source = asyncio.Event()

    async def source() -> AsyncIterator[int]:
        await release_source.wait()
        yield 7
        yield 8

    grouped = from_async_iterator(source()).group_within(
        max_items=10, duration=duration
    )
    async with grouped.stream() as iterator:
        next_group_task = asyncio.ensure_future(anext(iterator))
        done, _ = await asyncio.wait({next_group_task}, timeout=duration * 4)
        assert len(done) == 0

        release_source.set()
        first_group = await asyncio.wait_for(next_group_task, timeout=duration)
        assert_group_contains(first_group, [7])

        final_group = await asyncio.wait_for(anext(iterator), timeout=duration)
        assert_group_contains(final_group, [8])
        with pytest.raises(StopAsyncIteration):
            await anext(iterator)


@pytest.mark.asyncio
async def test_completion_flushes_partial_group_after_idle_timer_cycles() -> None:
    duration = 0.05
    release_source = asyncio.Event()

    async def source() -> AsyncIterator[int]:
        await release_source.wait()
        yield 10
        yield 11
        yield 12

    grouped = from_async_iterator(source()).group_within(
        max_items=10, duration=duration
    )
    async with grouped.stream() as iterator:
        first_group_task = asyncio.ensure_future(anext(iterator))
        done, _ = await asyncio.wait({first_group_task}, timeout=duration * 4)
        assert len(done) == 0
        release_source.set()

        idle_group = await asyncio.wait_for(first_group_task, timeout=duration)
        assert_group_contains(idle_group, [10])
        final_group = await asyncio.wait_for(anext(iterator), timeout=duration)
        assert_group_contains(final_group, [11, 12])
        with pytest.raises(StopAsyncIteration):
            await anext(iterator)


@pytest.mark.asyncio
async def test_upstream_failure_propagates_after_previously_emitted_group() -> None:
    async def source() -> AsyncIterator[int]:
        yield 1
        yield 2
        raise RuntimeError("source failed")

    grouped = from_async_iterator(source()).group_within(max_items=2, duration=5.0)
    async with grouped.stream() as iterator:
        emitted_group = await asyncio.wait_for(anext(iterator), timeout=0.5)
        assert_group_contains(emitted_group, [1, 2])
        with pytest.raises(RuntimeError, match="source failed"):
            await anext(iterator)


@pytest.mark.asyncio
async def test_repeated_cancellation_closes_each_upstream() -> None:
    closed_count = 0

    for expected_closures in range(1, 4):
        source_started = asyncio.Event()
        never = asyncio.Event()

        async def source() -> AsyncIterator[int]:
            nonlocal closed_count
            try:
                source_started.set()
                await never.wait()
                yield 1
            finally:
                closed_count += 1

        grouped = from_async_iterator(source()).group_within(max_items=10, duration=5.0)
        async with grouped.stream() as iterator:
            read_task = asyncio.ensure_future(anext(iterator))
            await source_started.wait()
            read_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await read_task

        assert closed_count == expected_closures


@pytest.mark.parametrize("max_items", [0, -1])
@pytest.mark.asyncio
async def test_rejects_non_positive_max_items(max_items: int) -> None:
    with pytest.raises(ValueError, match="max_items must be greater than 0"):
        empty: Stream[int] = Stream.empty()
        await consume_one(empty.group_within(max_items=max_items, duration=1.0))


@pytest.mark.parametrize("duration", [-1.0, float("inf"), float("nan")])
@pytest.mark.asyncio
async def test_rejects_invalid_duration(duration: float) -> None:
    with pytest.raises(ValueError, match="duration must be a finite"):
        empty: Stream[int] = Stream.empty()
        await consume_one(empty.group_within(max_items=1, duration=duration))


def test_zero_duration_is_accepted() -> None:
    items = [1, 2, 3]
    source = Stream.values(*items)
    groups = asyncio.run(collect(source, max_items=2, duration=0.0))

    assert_groups_preserve_sequence(groups, items, max_items=2)
