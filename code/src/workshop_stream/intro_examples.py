from workshop_stream.support.stream import Stream

non_strict_numbers: Stream[int] = Stream.values(1, 2, 3).repeat().take(50)


finite_numbers: Stream[None] = Stream.range(1, 5).foreach(
    lambda number: print(f"This is {number}")
)


zipped_numbers: Stream[tuple[int, int]] = Stream.values(1, 2, 3).zip(
    Stream.values(10, 20, 30)
)


bigger_numbers: Stream[None] = Stream.range(10, 15).foreach(
    lambda number: print(f"Slower {number}")
)
longer_delays: Stream[None] = Stream.fixed_delay(2)
repeated_delays: Stream[None] = Stream.sleep(1).repeat()
s1: Stream[tuple[None, None]] = repeated_delays.zip(finite_numbers)
s2: Stream[tuple[None, None]] = longer_delays.zip(bigger_numbers)
merged_numbers: Stream[tuple[None, None]] = s1.merge(s2)
time_aware_numbers: Stream[tuple[None, None]] = repeated_delays.zip(finite_numbers)


def report_error(error: Exception) -> Stream[str]:
    return Stream.values(f"Got: {error}")


error_aware: Stream[str] = (
    Stream.values("a", "b", "c")
    .append(Stream.raise_error(Exception("oh noes!")))
    .handle_error(report_error)
)


async def run_non_strict() -> None:
    print(await non_strict_numbers.to_list())


async def run_finite() -> None:
    print(await Stream.range(1, 5).to_list())


async def run_effectful() -> None:
    await finite_numbers.drain()


async def run_zipped() -> None:
    print(await zipped_numbers.to_list())


async def run_time_aware() -> None:
    await time_aware_numbers.drain()


async def run_merged() -> None:
    await merged_numbers.drain()


async def run_error_aware() -> None:
    print(await error_aware.to_list())


def main() -> None:
    import asyncio

    async def run_all() -> None:
        examples = (
            ("Non-strict numbers (capped at 50)", run_non_strict),
            ("Finite numbers", run_finite),
            ("Effectful finite numbers", run_effectful),
            ("Zipped numbers", run_zipped),
            ("Time-aware numbers", run_time_aware),
            ("Error-aware stream", run_error_aware),
            ("Merged numbers", run_merged),
        )
        for title, action in examples:
            print(f"\n=== {title} ===")
            await action()

    asyncio.run(run_all())


if __name__ == "__main__":
    main()
