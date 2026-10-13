from asyncio import Lock, Task, create_task, get_running_loop, run, sleep
from collections import deque
from pathlib import Path

from workshop_stream.db_writer import DBWriter
from workshop_stream.employee_api import EmployeeAPI
from workshop_stream.resources import resources
from workshop_stream.vacation_report import Enriched, VacationReport


async def enrich_imperatively(api: EmployeeAPI, db: DBWriter) -> None:
    loop = get_running_loop()
    start_lock = Lock()
    next_start = loop.time()

    async def enrich_one(report: VacationReport) -> Enriched:
        nonlocal next_start

        async with start_lock:
            now = loop.time()
            if next_start > now:
                await sleep(next_start - now)
            next_start = loop.time() + 0.05

        return await api.enrich(report)

    pending: deque[Task[Enriched]] = deque()

    with Path("vacations.csv").open() as file:
        eof = False
        while not eof or pending:
            while not eof and len(pending) < 10:
                line = file.readline()
                if line == "":
                    eof = True
                    break
                report = VacationReport.parse_csv(line)
                if report is None:
                    continue
                pending.append(create_task(enrich_one(report)))

            if not pending:
                continue
            enriched = await pending.popleft()
            await db.write(enriched)


async def main() -> None:
    async with resources() as (api, db):
        await enrich_imperatively(api, db)


if __name__ == "__main__":
    run(main())
