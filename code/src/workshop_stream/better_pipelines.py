from workshop_stream.db_writer import DBWriter
from workshop_stream.employee_api import EmployeeAPI
from workshop_stream.support.stream import Flow, flow
from workshop_stream.vacation_report import Enriched, VacationReport


def enrich_pipeline(api: EmployeeAPI) -> Flow[VacationReport, Enriched]:
    return flow(VacationReport).spaceout(0.05).mapAsync(api.enrich, concurrency=10)


def db_pipeline(db: DBWriter) -> Flow[Enriched, None]:
    return (
        flow(Enriched)
        .group_within(max_items=10, duration=1.0)
        .mapAsyncUnordered(db.write_all, concurrency=5)
    )
