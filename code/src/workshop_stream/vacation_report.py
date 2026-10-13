import csv
from dataclasses import dataclass


@dataclass(frozen=True, repr=False)
class VacationReport:
    name: str
    days: int
    sick_leave: bool

    def __repr__(self) -> str:
        return f"VacationReport({self.name}, {self.days}, {self.sick_leave})"

    @classmethod
    def parse_csv(cls, line: str) -> VacationReport | None:
        try:
            rows = list(csv.reader([line], strict=True))
        except csv.Error:
            return None

        if len(rows) != 1 or len(rows[0]) != 3:
            return None

        name, days_text, sick_leave_text = (field.strip() for field in rows[0])
        try:
            days = int(days_text)
            sick_leave = {"true": True, "false": False}[sick_leave_text.lower()]
        except ValueError:
            return None
        except KeyError:
            return None

        return cls(name, days, sick_leave)


@dataclass(frozen=True, repr=False)
class Metadata:
    available_days: int

    def __repr__(self) -> str:
        return f"Metadata({self.available_days})"


@dataclass(frozen=True, repr=False)
class Enriched:
    value: VacationReport
    metadata: Metadata

    def __repr__(self) -> str:
        return f"Enriched({self.value}, {self.metadata})"
