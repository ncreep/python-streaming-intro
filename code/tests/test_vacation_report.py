import pytest

from workshop_stream.vacation_report import VacationReport


def test_parse_csv_accepts_quoted_comma_and_trims_fields() -> None:
    report = VacationReport.parse_csv('"Doe, Jane", 12 , TrUe ')

    assert report is not None
    assert report.name == "Doe, Jane"
    assert report.days == 12
    assert report.sick_leave is True


@pytest.mark.parametrize(
    "line",
    [
        "Alice,2",
        "Alice,2,false,extra",
        '"Alice,2,false',
        "Alice,not-a-number,false",
        "Alice,2,yes",
        "Alice,2,on",
        "Alice,2,1",
    ],
)
def test_parse_csv_returns_none_for_malformed_or_unsupported_values(line: str) -> None:
    report = VacationReport.parse_csv(line)

    assert report is None
