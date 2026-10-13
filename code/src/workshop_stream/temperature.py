import asyncio
from pathlib import Path

from workshop_stream.support import text
from workshop_stream.support.input_streams import readUtf8Lines
from workshop_stream.support.stream import Stream
from workshop_stream.support.utilities import write_all


def fahrenheit_to_celsius(fahrenheit: str) -> str:
    return str((float(fahrenheit) - 32) * 5 / 9)


temperatures: Stream[None] = (
    readUtf8Lines(Path("fahrenheit.txt"))
    .map(fahrenheit_to_celsius)
    .intersperse("\n")
    .via(text.utf8.encode)
    .via(write_all(Path("celsius.txt")))
)


async def run() -> None:
    await temperatures.drain()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
