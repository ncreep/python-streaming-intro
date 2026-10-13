from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from workshop_stream.db_writer import DBWriter, db_writer_resource
from workshop_stream.employee_api import EmployeeAPI, employee_api_resource


@asynccontextmanager
async def resources() -> AsyncGenerator[tuple[EmployeeAPI, DBWriter]]:
    async with (
        employee_api_resource("secret") as api,
        db_writer_resource() as db,
    ):
        yield api, db
