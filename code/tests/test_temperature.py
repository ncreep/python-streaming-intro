import asyncio
import os
from pathlib import Path

from workshop_stream.temperature import fahrenheit_to_celsius, run


def test_fahrenheit_to_celsius() -> None:
    assert fahrenheit_to_celsius("32") == "0.0"
    assert fahrenheit_to_celsius("212") == "100.0"


def test_temperature_pipeline_and_file_output(tmp_path: Path) -> None:
    (tmp_path / "fahrenheit.txt").write_bytes(b"32\n68\n212\n")

    def execute() -> None:
        previous = Path.cwd()
        os.chdir(tmp_path)
        try:
            asyncio.run(run())
        finally:
            os.chdir(previous)

    execute()

    assert (tmp_path / "celsius.txt").read_bytes() == b"0.0\n20.0\n100.0"
