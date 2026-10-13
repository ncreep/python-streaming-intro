import asyncio

from workshop_stream.pipelines import db_pipeline, enrich_pipeline
from workshop_stream.resources import resources
from workshop_stream.support.input_streams import read_line
from workshop_stream.support.stream import Stream
from workshop_stream.vacation_report import VacationReport

from_console: Stream[VacationReport] = (
    Stream.repeat_eval(read_line).map(VacationReport.parse_csv).drop_none()
)


async def run_console_demo() -> None:
    async with resources() as (api, db):
        flow = from_console.via(enrich_pipeline(api)).via(db_pipeline(db))

        await flow.drain()


def main() -> None:
    asyncio.run(run_console_demo())


if __name__ == "__main__":
    main()
