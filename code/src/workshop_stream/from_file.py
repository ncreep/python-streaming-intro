import asyncio
from pathlib import Path

from workshop_stream.pipelines import db_pipeline, enrich_pipeline
from workshop_stream.resources import resources
from workshop_stream.support import text
from workshop_stream.support.input_streams import read_all
from workshop_stream.support.stream import Stream
from workshop_stream.vacation_report import VacationReport

from_file: Stream[VacationReport] = (
    read_all(Path("vacations.csv"))
    .via(text.utf8.decode)
    .via(text.lines)
    .map(VacationReport.parse_csv)
    .drop_none()
)


async def run_file_demo() -> None:
    async with resources() as (api, db):
        flow = from_file.via(enrich_pipeline(api)).via(db_pipeline(db))

        await flow.drain()


def main() -> None:
    asyncio.run(run_file_demo())


if __name__ == "__main__":
    main()
