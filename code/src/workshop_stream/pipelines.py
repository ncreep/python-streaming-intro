from workshop_stream.db_writer import DBWriter
from workshop_stream.employee_api import EmployeeAPI
from workshop_stream.support.stream import Flow, flow
from workshop_stream.vacation_report import Enriched, VacationReport


def enrich_pipeline(api: EmployeeAPI) -> Flow[VacationReport, Enriched]:
    return flow(VacationReport).mapAsync(api.enrich)


def db_pipeline(db: DBWriter) -> Flow[Enriched, None]:
    return flow(Enriched).mapAsync(db.write)
