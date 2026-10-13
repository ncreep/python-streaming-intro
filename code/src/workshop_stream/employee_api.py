from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from random import randrange

from workshop_stream.vacation_report import (
    Enriched,
    Metadata,
    VacationReport,
)


class EmployeeAPI(ABC):
    @abstractmethod
    async def get_available_vacation_days(self, name: str) -> int: ...

    async def enrich(self, vacation: VacationReport) -> Enriched:
        available_days = await self.get_available_vacation_days(vacation.name)
        metadata = Metadata(available_days=available_days)

        return Enriched(vacation, metadata)


class _ProductionEmployeeAPI(EmployeeAPI):
    def __init__(self, credentials: str) -> None:
        self._credentials = credentials

    async def get_available_vacation_days(self, name: str) -> int:
        print(f"Getting available vacation days for {name}")

        return randrange(0, 10)

    async def shutdown(self) -> None:
        print("Shutting down employee API")


@asynccontextmanager
async def employee_api_resource(
    credentials: str,
) -> AsyncGenerator[EmployeeAPI]:
    print("Starting employee API")

    api = _ProductionEmployeeAPI(credentials)

    try:
        yield api
    finally:
        await api.shutdown()
