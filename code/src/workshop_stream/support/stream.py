from __future__ import annotations

import asyncio
import math
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
)
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Never, overload

from aiostream import stream as _stream
from aiostream.core import streamcontext as _streamcontext

if TYPE_CHECKING:

    def _filter[Item](
        source: AsyncIterable[Item],
        predicate: Callable[[Item], bool | Awaitable[bool]],
    ) -> AsyncIterator[Item]: ...

    def _spaceout[Item](
        source: AsyncIterable[Item], interval: float
    ) -> AsyncIterator[Item]: ...

    def _take[Item](source: AsyncIterable[Item], count: int) -> AsyncIterator[Item]: ...

else:
    _filter = _stream.filter.raw
    _spaceout = _stream.spaceout.raw
    _take = _stream.take.raw


class Stream[T](AsyncIterable[T]):
    """A lazy, repeatable stream based on aiostream, but exposing a more type-safe and fluent
    interface."""

    __slots__ = ("_factory",)
    _factory: Callable[[], AsyncIterable[T]]

    def __init__(self) -> None:
        self._factory = _stream.empty.raw

    @classmethod
    def _from_factory[Item](
        cls: type[Stream[Item]], factory: Callable[[], AsyncIterable[Item]]
    ) -> Stream[Item]:
        stream = cls.__new__(cls)
        stream._factory = factory
        return stream

    @staticmethod
    def values[Item](*items: Item) -> Stream[Item]:
        return Stream._from_factory(lambda: _stream.iterate.raw(items))

    @staticmethod
    def empty[Item]() -> Stream[Item]:
        return Stream._from_factory(_stream.empty.raw)

    @staticmethod
    def range(start: int, stop: int) -> Stream[int]:
        return Stream._from_factory(lambda: _stream.range.raw(start, stop))

    @staticmethod
    def repeat_eval[Item](action: Callable[[], Awaitable[Item]]) -> Stream[Item]:
        return Stream._from_factory(lambda: _repeat_eval(action))

    @staticmethod
    def raise_error(exception: Exception) -> Stream[Never]:
        return Stream._from_factory(lambda: _stream.throw.raw(exception))

    def via[Output](self, flow: Flow[T, Output]) -> Stream[Output]:
        return flow(self)

    @asynccontextmanager
    async def stream(self) -> AsyncGenerator[AsyncIterator[T]]:
        async with _streamcontext(self._factory()) as items:
            yield items

    def __aiter__(self) -> AsyncIterator[T]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[T]:
        async with self.stream() as items:
            async for item in items:
                yield item

    async def to_list(self) -> list[T]:
        result: list[T] = []
        async with self.stream() as items:
            async for item in items:
                result.append(item)
        return result

    async def drain(self) -> None:
        async with self.stream() as items:
            async for _ in items:
                pass

    def map[Output](
        self,
        function: Callable[[T], Output],
    ) -> Stream[Output]:
        return Stream._from_factory(lambda: _stream.map.raw(self._factory(), function))

    def mapAsync[Output](
        self,
        function: Callable[[T], Awaitable[Output]],
        *,
        concurrency: int = 1,
    ) -> Stream[Output]:
        return Stream._from_factory(
            lambda: _stream.map.raw(
                self._factory(), function, task_limit=concurrency, ordered=True
            )
        )

    def mapAsyncUnordered[Output](
        self,
        function: Callable[[T], Awaitable[Output]],
        *,
        concurrency: int = 1,
    ) -> Stream[Output]:
        return Stream._from_factory(
            lambda: _stream.map.raw(
                self._factory(), function, task_limit=concurrency, ordered=False
            )
        )

    def foreach(
        self,
        action: Callable[[T], None] | Callable[[T], Awaitable[None]],
        *,
        task_limit: int | None = None,
        ordered: bool = True,
    ) -> Stream[None]:
        return Stream._from_factory(
            lambda: _stream.map.raw(
                self._factory(), action, task_limit=task_limit, ordered=ordered
            )
        )

    def filter(self, predicate: Callable[[T], bool]) -> Stream[T]:
        return Stream._from_factory(lambda: _filter(self._factory(), predicate))

    def spaceout(self, interval: float) -> Stream[T]:
        return Stream._from_factory(lambda: _spaceout(self._factory(), interval))

    def cycle(self) -> Stream[T]:
        return Stream._from_factory(lambda: _cycle(self._factory()))

    def repeat(self) -> Stream[T]:
        return Stream._from_factory(lambda: _repeat_source(self._factory))

    @staticmethod
    def sleep(duration: float) -> Stream[None]:
        return Stream.repeat_eval(lambda: asyncio.sleep(duration))

    @staticmethod
    def fixed_delay(duration: float) -> Stream[None]:
        return Stream.sleep(duration)

    def take(self, count: int) -> Stream[T]:
        return Stream._from_factory(lambda: _take(self._factory(), count))

    @overload
    def flatten[Output](
        self: Stream[Stream[Output]], *, task_limit: int | None = None
    ) -> Stream[Output]: ...

    @overload
    def flatten[Output](
        self: Stream[Iterable[Output]], *, task_limit: int | None = None
    ) -> Stream[Output]: ...

    @overload
    def flatten[Output](
        self: Stream[Stream[Output] | list[Output]], *, task_limit: int | None = None
    ) -> Stream[Output]: ...

    @overload
    def flatten[Output](
        self: Stream[AsyncIterable[Output] | Iterable[Output]],
        *,
        task_limit: int | None = None,
    ) -> Stream[Output]: ...

    def flatten[Output](
        self: Stream[AsyncIterable[Output] | Iterable[Output]],
        *,
        task_limit: int | None = None,
    ) -> Stream[Output]:
        return self.via(
            Flow(lambda source: _stream.flatten.raw(source, task_limit=task_limit))
        )

    def group_within(self, *, max_items: int, duration: float) -> Stream[tuple[T, ...]]:
        if max_items <= 0:
            raise ValueError("max_items must be greater than 0")
        if not math.isfinite(duration) or duration < 0:
            raise ValueError(
                "duration must be a finite number greater than or equal to 0"
            )
        return self.via(Flow(lambda source: _group_within(source, max_items, duration)))

    def intersperse[Separator](self, separator: Separator) -> Stream[T | Separator]:
        return self.via(Flow(lambda source: _intersperse(source, separator)))

    def drop_none[Output](self: Stream[Output | None]) -> Stream[Output]:
        return Stream._from_factory(lambda: _drop_none(self._factory()))

    def append[Following](self, following: Stream[Following]) -> Stream[T | Following]:
        return Stream._from_factory(
            lambda: _stream.chain.raw(self._factory(), following._factory())
        )

    def handle_error(self, handler: Callable[[Exception], Stream[T]]) -> Stream[T]:
        return self.via(Flow(lambda source: _handle_error(source, handler)))

    def merge(self, other: Stream[T]) -> Stream[T]:
        return Stream._from_factory(
            lambda: _stream.merge.raw(self._factory(), other._factory())
        )

    def zip[Other](self, other: Stream[Other]) -> Stream[tuple[T, Other]]:
        return flow().zip(other)(self)


class Flow[Input, Output = Input]:
    """A reusable transformation from one custom Stream to another."""

    @overload
    def __init__[Item](self: Flow[Item, Item], transform: None = None) -> None: ...

    @overload
    def __init__(
        self,
        transform: Callable[[AsyncIterable[Input]], AsyncIterable[Output]],
    ) -> None: ...

    def __init__(
        self,
        transform: Callable[[AsyncIterable[Input]], AsyncIterable[Output]]
        | None = None,
    ) -> None:
        self._transform = transform

    def __call__(self, source: Stream[Input]) -> Stream[Output]:
        transform = self._transform
        if transform is None:
            return Stream._from_factory(source._factory)
        return Stream._from_factory(lambda: transform(source._factory()))

    @classmethod
    def _from_stream_transform(
        cls, transform: Callable[[Stream[Input]], Stream[Output]]
    ) -> Flow[Input, Output]:
        return Flow[Input, Output](
            lambda source: transform(Stream._from_factory(lambda: source))
        )

    def via[Following](
        self, following: Flow[Output, Following]
    ) -> Flow[Input, Following]:
        return Flow._from_stream_transform(lambda source: following(self(source)))

    def zip[Other](self, other: Stream[Other]) -> Flow[Input, tuple[Output, Other]]:
        return Flow._from_stream_transform(
            lambda source: _zip_streams(self(source), other)
        )

    def map[Following](
        self,
        function: Callable[[Output], Following],
    ) -> Flow[Input, Following]:
        return Flow._from_stream_transform(lambda source: self(source).map(function))

    def mapAsync[Following](
        self,
        function: Callable[[Output], Awaitable[Following]],
        *,
        concurrency: int = 1,
    ) -> Flow[Input, Following]:
        return Flow._from_stream_transform(
            lambda source: self(source).mapAsync(function, concurrency=concurrency)
        )

    def mapAsyncUnordered[Following](
        self,
        function: Callable[[Output], Awaitable[Following]],
        *,
        concurrency: int = 1,
    ) -> Flow[Input, Following]:
        return Flow._from_stream_transform(
            lambda source: self(source).mapAsyncUnordered(
                function, concurrency=concurrency
            )
        )

    def foreach(
        self,
        action: Callable[[Output], None] | Callable[[Output], Awaitable[None]],
        *,
        task_limit: int | None = None,
        ordered: bool = True,
    ) -> Flow[Input, None]:
        return Flow._from_stream_transform(
            lambda source: self(source).foreach(
                action, task_limit=task_limit, ordered=ordered
            )
        )

    def filter(self, predicate: Callable[[Output], bool]) -> Flow[Input, Output]:
        return Flow._from_stream_transform(
            lambda source: self(source).filter(predicate)
        )

    def spaceout(self, interval: float) -> Flow[Input, Output]:
        return Flow._from_stream_transform(
            lambda source: self(source).spaceout(interval)
        )

    def cycle(self) -> Flow[Input, Output]:
        return Flow._from_stream_transform(lambda source: self(source).cycle())

    def repeat(self) -> Flow[Input, Output]:
        return Flow._from_stream_transform(lambda source: self(source).repeat())

    def take(self, count: int) -> Flow[Input, Output]:
        return Flow._from_stream_transform(lambda source: self(source).take(count))

    @overload
    def flatten[Item](
        self: Flow[Input, AsyncIterable[Item]], *, task_limit: int | None = None
    ) -> Flow[Input, Item]: ...

    @overload
    def flatten[Item](
        self: Flow[Input, Iterable[Item]], *, task_limit: int | None = None
    ) -> Flow[Input, Item]: ...

    def flatten[Item](
        self: Flow[Input, AsyncIterable[Item] | Iterable[Item]],
        *,
        task_limit: int | None = None,
    ) -> Flow[Input, Item]:
        return Flow._from_stream_transform(
            lambda source: self(source).flatten(task_limit=task_limit)
        )

    def group_within(
        self, *, max_items: int, duration: float
    ) -> Flow[Input, tuple[Output, ...]]:
        return Flow._from_stream_transform(
            lambda source: self(source).group_within(
                max_items=max_items, duration=duration
            )
        )

    def intersperse[Separator](
        self, separator: Separator
    ) -> Flow[Input, Output | Separator]:
        return Flow._from_stream_transform(
            lambda source: self(source).intersperse(separator)
        )

    def drop_none[Item](self: Flow[Input, Item | None]) -> Flow[Input, Item]:
        return Flow._from_stream_transform(lambda source: self(source).drop_none())

    def append[Following](
        self, following: Stream[Following]
    ) -> Flow[Input, Output | Following]:
        return Flow._from_stream_transform(
            lambda source: self(source).append(following)
        )

    def handle_error(
        self, handler: Callable[[Exception], Stream[Output]]
    ) -> Flow[Input, Output]:
        return Flow._from_stream_transform(
            lambda source: self(source).handle_error(handler)
        )


@overload
def flow[Input]() -> Flow[Input, Input]: ...


@overload
def flow[Input](_input_type: type[Input]) -> Flow[Input, Input]: ...


def flow[Input](_input_type: type[Input] | None = None) -> Flow[Input, Input]:
    return Flow()


def _zip_streams[Left, Right](
    left: Stream[Left], right: Stream[Right]
) -> Stream[tuple[Left, Right]]:
    return Stream._from_factory(
        lambda: _stream.zip.raw(left._factory(), right._factory())
    )


async def _repeat_eval[Item](
    action: Callable[[], Awaitable[Item]],
) -> AsyncIterator[Item]:
    while True:
        yield await action()


async def _repeat_source[Item](
    factory: Callable[[], AsyncIterable[Item]],
) -> AsyncIterator[Item]:
    while True:
        async with _streamcontext(factory()) as items:
            async for item in items:
                yield item


async def _cycle[Item](source: AsyncIterable[Item]) -> AsyncIterator[Item]:
    cached: list[Item] = []
    async with _streamcontext(source) as items:
        async for item in items:
            cached.append(item)
            yield item
    while cached:
        for item in cached:
            yield item


async def _group_within[Item](
    source: AsyncIterable[Item], max_items: int, duration: float
) -> AsyncIterator[tuple[Item, ...]]:
    buffer: list[Item] = []
    idle = False

    async def next_item(iterator: AsyncIterator[Item]) -> Item:
        return await anext(iterator)

    async with _streamcontext(source) as iterator:
        pending_item: asyncio.Task[Item] | None = None
        timer: asyncio.Task[None] | None = asyncio.create_task(asyncio.sleep(duration))
        try:
            while True:
                if pending_item is None:
                    pending_item = asyncio.create_task(next_item(iterator))

                waiting = (pending_item,) if timer is None else (pending_item, timer)
                await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)

                if timer is not None and timer.done():
                    timer = None
                    if buffer:
                        group, buffer = tuple(buffer), []
                        timer = asyncio.create_task(asyncio.sleep(duration))
                        yield group
                        continue
                    idle = True

                if pending_item.done():
                    try:
                        item = pending_item.result()
                    except StopAsyncIteration:
                        pending_item = None
                        if buffer:
                            yield tuple(buffer)
                        return
                    pending_item = None

                    if idle:
                        idle = False
                        timer = asyncio.create_task(asyncio.sleep(duration))
                        yield (item,)
                        continue

                    buffer.append(item)
                    if len(buffer) == max_items:
                        group, buffer = tuple(buffer), []
                        if timer is not None:
                            timer.cancel()
                            await asyncio.gather(timer, return_exceptions=True)
                        timer = asyncio.create_task(asyncio.sleep(duration))
                        yield group
        finally:
            pending = [task for task in (pending_item, timer) if task is not None]
            for task in pending:
                if not task.done():
                    task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)


async def _intersperse[Item, Separator](
    source: AsyncIterable[Item], separator: Separator
) -> AsyncIterator[Item | Separator]:
    first = True
    async with _streamcontext(source) as items:
        async for item in items:
            if not first:
                yield separator
            yield item
            first = False


async def _drop_none[Item](source: AsyncIterable[Item | None]) -> AsyncIterator[Item]:
    async with _streamcontext(source) as items:
        async for item in items:
            if item is not None:
                yield item


async def _handle_error[Item](
    source: AsyncIterable[Item], handler: Callable[[Exception], Stream[Item]]
) -> AsyncIterator[Item]:
    try:
        async with _streamcontext(source) as items:
            async for item in items:
                yield item
    except Exception as error:
        async with handler(error).stream() as items:
            async for item in items:
                yield item
