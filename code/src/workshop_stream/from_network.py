import asyncio

from workshop_stream.better_pipelines import db_pipeline, enrich_pipeline
from workshop_stream.resources import resources
from workshop_stream.support.stream import Stream
from workshop_stream.support.tcp_streams import tcp_clients
from workshop_stream.vacation_report import VacationReport

from_network: Stream[VacationReport] = (
    tcp_clients(host="localhost", port=5555)
    .map(
        lambda client: (
            client.reads.map(VacationReport.parse_csv)
            .drop_none()
            .handle_error(lambda _error: Stream.empty())
        )
    )
    .flatten(task_limit=100)
)


async def run_network_demo() -> None:
    async with resources() as (api, db):
        flow = from_network.via(enrich_pipeline(api)).via(db_pipeline(db))
        await flow.drain()


def main() -> None:
    asyncio.run(run_network_demo())


if __name__ == "__main__":
    main()
