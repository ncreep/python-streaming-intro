import asyncio

import pytest

from workshop_stream.db_writer import db_writer_resource
from workshop_stream.employee_api import employee_api_resource
from workshop_stream.resources import resources
from workshop_stream.vacation_report import (
    Enriched,
    Metadata,
    VacationReport,
)


def test_employee_api_logs_lookup_and_enriches(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def scenario() -> None:
        async with employee_api_resource("test-secret") as api:
            enriched = await api.enrich(VacationReport("Alice", 2, False))

        assert 0 <= enriched.metadata.available_days < 10

    asyncio.run(scenario())
    assert capsys.readouterr().out == (
        "Starting employee API\n"
        "Getting available vacation days for Alice\n"
        "Shutting down employee API\n"
    )


def test_db_writer_logs_single_and_batch_writes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    value = Enriched(VacationReport("Alice", 2, False), Metadata(4))

    async def scenario() -> None:
        async with db_writer_resource() as db:
            await db.write(value)
            await db.write_all((value,))

    asyncio.run(scenario())
    assert capsys.readouterr().out == (
        "Connected to database\n"
        f"Writing {value}\n"
        f"Writing all {value}\n"
        "Disconnected from database\n"
    )


def test_combined_resources_release_after_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="pipeline failed"):
            async with resources():
                raise RuntimeError("pipeline failed")

    asyncio.run(scenario())
    assert capsys.readouterr().out == (
        "Starting employee API\n"
        "Connected to database\n"
        "Disconnected from database\n"
        "Shutting down employee API\n"
    )


def test_combined_resources_release_after_cancellation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def scenario() -> None:
        entered = asyncio.Event()

        async def use_resources() -> None:
            async with resources():
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(use_resources())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert capsys.readouterr().out == (
        "Starting employee API\n"
        "Connected to database\n"
        "Disconnected from database\n"
        "Shutting down employee API\n"
    )
