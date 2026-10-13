import asyncio

from workshop_stream.pipelines import db_pipeline, enrich_pipeline
from workshop_stream.resources import resources
from workshop_stream.support.stream import Stream
from workshop_stream.vacation_report import VacationReport

vacations: Stream[VacationReport] = Stream.values(
    VacationReport("Alice", 1, False),
    VacationReport("Bob", 2, True),
    VacationReport("Charlie", 1, False),
    VacationReport("David", 3, True),
)


def is_sick(report: VacationReport) -> bool:
    return report.sick_leave


def report_name(report: VacationReport) -> str:
    return report.name


sickly: Stream[str] = vacations.filter(is_sick).map(report_name)


async def run_in_memory_demo() -> None:
    async with resources() as (api, db):
        flow = vacations.via(enrich_pipeline(api)).via(db_pipeline(db))
        await flow.drain()


def main() -> None:
    asyncio.run(run_in_memory_demo())


if __name__ == "__main__":
    main()
