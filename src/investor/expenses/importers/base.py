from __future__ import annotations

import csv
import io
from abc import ABC, abstractmethod
from typing import Iterable


def sniff_dialect(sample: str) -> csv.Dialect:
    try:
        return csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
    except Exception:
        class _D(csv.Dialect):
            delimiter = ","
            quotechar = '"'
            doublequote = True
            skipinitialspace = True
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL

        return _D()


def read_csv_rows(content: str) -> tuple[list[str], list[dict[str, str]]]:
    sample = content[:20000]
    dialect = sniff_dialect(sample)
    f = io.StringIO(content)
    reader = csv.DictReader(f, dialect=dialect)
    headers = [h.strip() for h in (reader.fieldnames or []) if h]
    rows: list[dict[str, str]] = []
    for r in reader:
        rows.append({(k or "").strip(): (v or "").strip() for k, v in r.items() if k is not None})
    return headers, rows


class StatementImporter(ABC):
    format_name: str

    @abstractmethod
    def detect(self, headers: Iterable[str]) -> bool: ...

    @abstractmethod
    def parse_rows(self, *, rows: list[dict[str, str]], default_currency: str) -> list["RawTxn"]: ...

