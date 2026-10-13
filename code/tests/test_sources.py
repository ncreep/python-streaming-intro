"""Tests for incremental sources and the resource-scoped source demos."""

import asyncio
import re
from pathlib import Path
from typing import IO, Any

import pytest

from workshop_stream.from_console import (
    from_console,
    run_console_demo,
)
from workshop_stream.from_file import from_file, run_file_demo
from workshop_stream.support.stream import Stream


async def collect[T](source: Stream[T]) -> list[T]:
    return await source.to_list()


def test_from_file_parses_valid_lines_and_skips_invalid_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "vacations.csv"
    path.write_bytes(
        b"Alice,2,false\r\nnot,a,valid,row\r\nBob,3,true\r\nCharlie,no,false\r\n"
    )

    reports = asyncio.run(collect(from_file))

    assert [(report.name, report.days, report.sick_leave) for report in reports] == [
        ("Alice", 2, False),
        ("Bob", 3, True),
    ]


def test_from_file_reopens_for_each_subscription(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "vacations.csv"
    path.write_text("Alice,2,false\n", encoding="utf-8")
    source = from_file

    first = asyncio.run(collect(source))
    second = asyncio.run(collect(source))

    expected = [("Alice", 2, False)]
    assert [
        (report.name, report.days, report.sick_leave) for report in first
    ] == expected
    assert [
        (report.name, report.days, report.sick_leave) for report in second
    ] == expected


def test_from_file_preserves_blank_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "vacations.csv"
    path.write_text("\nAlice,2,false\n", encoding="utf-8")

    reports = asyncio.run(collect(from_file))

    assert [(report.name, report.days, report.sick_leave) for report in reports] == [
        ("Alice", 2, False)
    ]


def test_file_source_closes_handle_when_subscription_is_cancelled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "vacations.csv"
    path.write_text("Alice,2,false\nBob,3,true\n", encoding="utf-8")
    opened: list[IO[Any]] = []
    original_open = Path.open

    def tracked_open(
        file_path: Path,
        mode: str = "r",
        *,
        encoding: str | None = None,
        newline: str | None = None,
    ) -> IO[Any]:
        handle = original_open(file_path, mode, encoding=encoding, newline=newline)
        opened.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", tracked_open)

    async def cancel_subscription() -> None:
        async with from_file.stream() as streamer:
            first = await anext(streamer)
            assert (first.name, first.days, first.sick_leave) == ("Alice", 2, False)

    asyncio.run(cancel_subscription())

    assert len(opened) == 1
    assert opened[0].closed is True


def test_repeat_eval_repeats_action_for_requested_items() -> None:
    reads = iter(["", "value"])

    async def read() -> str:
        return next(reads)

    source = Stream.repeat_eval(read).take(2)
    assert asyncio.run(collect(source)) == ["", "value"]


def test_repeat_eval_propagates_eof_error() -> None:
    async def read() -> str:
        raise EOFError

    async def collect_error() -> None:
        async with Stream.repeat_eval(read).stream() as streamer:
            with pytest.raises(EOFError):
                await anext(streamer)

    asyncio.run(collect_error())


def test_repeat_eval_propagates_non_eof_errors() -> None:
    async def read() -> str:
        raise RuntimeError("read failed")

    async def collect_error() -> None:
        async with Stream.repeat_eval(read).stream() as streamer:
            with pytest.raises(RuntimeError, match="read failed"):
                await anext(streamer)

    asyncio.run(collect_error())


def test_from_console_reads_and_parses_standard_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lines = iter(["Alice,2,false", "bad,row", "Bob,3,true"])

    def read_line() -> str:
        try:
            return next(lines)
        except StopIteration:
            raise EOFError from None

    monkeypatch.setattr("builtins.input", read_line)
    reports = asyncio.run(collect(from_console.take(2)))

    assert [(report.name, report.days, report.sick_leave) for report in reports] == [
        ("Alice", 2, False),
        ("Bob", 3, True),
    ]


def test_file_demo_uses_resources_and_only_injected_side_effects(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "vacations.csv"
    path.write_text("Alice,2,false\ninvalid\nBob,3,true\n", encoding="utf-8")
    asyncio.run(run_file_demo())

    lines = capsys.readouterr().out.splitlines()
    assert lines[0:2] == ["Starting employee API", "Connected to database"]
    assert re.fullmatch(r"Getting available vacation days for Alice", lines[2])
    assert re.fullmatch(
        r"Writing Enriched\(VacationReport\(Alice, 2, False\), Metadata\([0-9]+\)\)",
        lines[3],
    )
    assert re.fullmatch(r"Getting available vacation days for Bob", lines[4])
    assert re.fullmatch(
        r"Writing Enriched\(VacationReport\(Bob, 3, True\), Metadata\([0-9]+\)\)",
        lines[5],
    )
    assert lines[6:] == ["Disconnected from database", "Shutting down employee API"]


def test_console_demo_uses_standard_input_and_resources(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    input_lines = iter(["Alice,2,false", "bad", "Bob,1,true"])

    def read_line() -> str:
        try:
            return next(input_lines)
        except StopIteration:
            raise EOFError from None

    monkeypatch.setattr("builtins.input", read_line)
    with pytest.raises(EOFError):
        asyncio.run(run_console_demo())

    lines = capsys.readouterr().out.splitlines()
    assert lines[0:2] == ["Starting employee API", "Connected to database"]
    assert re.fullmatch(r"Getting available vacation days for Alice", lines[2])
    assert re.fullmatch(
        r"Writing Enriched\(VacationReport\(Alice, 2, False\), Metadata\([0-9]+\)\)",
        lines[3],
    )
    assert re.fullmatch(r"Getting available vacation days for Bob", lines[4])
    assert re.fullmatch(
        r"Writing Enriched\(VacationReport\(Bob, 1, True\), Metadata\([0-9]+\)\)",
        lines[5],
    )
    assert lines[6:] == ["Disconnected from database", "Shutting down employee API"]
