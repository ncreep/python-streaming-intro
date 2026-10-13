from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager

from workshop_stream.vacation_report import Enriched


class DBWriter(ABC):
    @abstractmethod
    async def write(self, value: Enriched) -> None: ...

    @abstractmethod
    async def write_all(self, values: Sequence[Enriched]) -> None: ...


class _ProductionDBWriter(DBWriter):
    async def write(self, value: Enriched) -> None:
        print(f"Writing {value}")

    async def write_all(self, values: Sequence[Enriched]) -> None:
        rendered = ",".join(str(value) for value in values)

        print(f"Writing all {rendered}")


@asynccontextmanager
async def db_writer_resource() -> AsyncGenerator[DBWriter]:
    print("Connected to database")
    try:
        yield _ProductionDBWriter()
    finally:
        print("Disconnected from database")
