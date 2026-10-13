import asyncio
from collections.abc import Sequence

from workshop_stream.better_pipelines import (
    db_pipeline as better_db_pipeline,
)
from workshop_stream.better_pipelines import (
    enrich_pipeline as better_enrich_pipeline,
)
from workshop_stream.db_writer import DBWriter
from workshop_stream.employee_api import EmployeeAPI
from workshop_stream.in_memory import sickly
from workshop_stream.pipelines import db_pipeline, enrich_pipeline
from workshop_stream.support.stream import Stream
from workshop_stream.vacation_report import (
    Enriched,
    Metadata,
    VacationReport,
)


class _EmployeeAPI(EmployeeAPI):
    async def get_available_vacation_days(self, name: str) -> int:
        return len(name)


class _DBWriter(DBWriter):
    def __init__(self) -> None:
        self.writes: list[Enriched] = []
        self.batches: list[Sequence[Enriched]] = []

    async def write(self, value: Enriched) -> None:
        self.writes.append(value)

    async def write_all(self, values: Sequence[Enriched]) -> None:
        self.batches.append(values)


async def _collect[T](source: Stream[T]) -> tuple[T, ...]:
    items: list[T] = []
    async with source.stream() as streamer:
        async for item in streamer:
            items.append(item)
    return tuple(items)


def test_in_memory_sickly_source_matches_workshop_name() -> None:
    names = asyncio.run(_collect(sickly))

    assert names == ("Bob", "David")


def test_vacation_value_representations_are_positional() -> None:
    report = VacationReport("Alice", 1, False)
    metadata = Metadata(42)

    assert repr(report) == "VacationReport(Alice, 1, False)"
    assert repr(metadata) == "Metadata(42)"
    assert repr(Enriched(report, metadata)) == (
        "Enriched(VacationReport(Alice, 1, False), Metadata(42))"
    )


def test_basic_pipelines_match_map_then_effectful_action() -> None:
    api = _EmployeeAPI()
    db = _DBWriter()
    report = VacationReport("Alice", 2, False)
    enriched = Enriched(report, Metadata(5))

    result = asyncio.run(_collect(Stream.values(report).via(enrich_pipeline(api))))
    written = asyncio.run(_collect(Stream.values(enriched).via(db_pipeline(db))))

    assert result == (enriched,)
    assert written == (None,)
    assert db.writes == [enriched]


def test_better_enrichment_pipeline_emits_enriched_reports() -> None:
    api = _EmployeeAPI()
    reports = tuple(VacationReport(name, 1, False) for name in ("Alice", "Bob"))

    metered = asyncio.run(
        _collect(Stream.values(*reports).via(better_enrich_pipeline(api)))
    )
    assert metered == tuple(
        Enriched(report, Metadata(len(report.name))) for report in reports
    )


def test_better_database_pipeline_writes_grouped_batches() -> None:
    db = _DBWriter()
    values = tuple(
        Enriched(VacationReport(name, 1, False), Metadata(4))
        for name in ("Alice", "Bob")
    )

    written = asyncio.run(_collect(Stream.values(*values).via(better_db_pipeline(db))))

    assert written == (None,)
    assert db.batches == [values]
