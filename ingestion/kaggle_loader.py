"""Leitura do dataset do Kaggle "ETL Pipelines | World Bank" (nilaychauhan/world-bank-datasets).

Dois usos: bootstrap do histórico de indicadores (`source=kaggle`) e modo replay do producer. Só biblioteca
padrão (csv, json, sqlite3, xml), para a imagem do producer não precisar de pandas.
"""

from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections.abc import Iterable
from pathlib import Path

# Arquivo baixado da internet: defusedxml bloqueia entidades externas e expansão de entidades
from defusedxml import ElementTree as ET
from worldbank_client import clean_str, normalize_project, parse_float

# Arquivo do Kaggle → indicador (os códigos são os de DEFAULT_INDICATORS)
KAGGLE_INDICATOR_FILES = {
    "population_data.csv": "SP.POP.TOTL",
    "gdp_data.csv": "NY.GDP.MKTP.CD",
    "rural_population_percent.csv": "SP.RUR.TOTL.ZS",
    "electricity_access_percent.csv": "EG.ELC.ACCS.ZS",
}
ID_COLUMNS = ("Country Name", "Country Code", "Indicator Name", "Indicator Code")
SOURCE_KAGGLE = "kaggle"
SOURCE_REPLAY = "kaggle-replay"
_YEAR = re.compile(r"^\d{4}$")


def _wide_row(record: dict) -> dict:
    """Linha larga: colunas de identificação como texto, anos como float | None, colunas sem nome fora."""
    row = {c: clean_str(record.get(c)) for c in ID_COLUMNS}
    row.update({str(k): parse_float(v) for k, v in record.items() if _YEAR.match(str(k))})
    return row


def read_wdi_csv(path: Path) -> list[dict]:
    """CSV do WDI em formato largo, pulando as linhas de metadados antes do cabeçalho `Country Name`."""
    lines = Path(path).read_text(encoding="utf-8-sig").splitlines()
    start = next(
        # Linhas vazias entre os metadados viram [] no csv.reader
        (i for i, line in enumerate(lines) if (next(csv.reader([line]), None) or [""])[0] == "Country Name"),
        None,
    )
    if start is None:
        raise ValueError(f"{path}: cabeçalho 'Country Name' não encontrado")
    return [_wide_row(r) for r in csv.DictReader(lines[start:]) if clean_str(r.get("Country Code"))]


def _read_population_json(path: Path) -> list[dict]:
    return [_wide_row(r) for r in json.loads(Path(path).read_text(encoding="utf-8"))]


def _read_population_sqlite(path: Path) -> list[dict]:
    with sqlite3.connect(path) as con:
        con.row_factory = sqlite3.Row
        return [_wide_row(dict(r)) for r in con.execute("SELECT * FROM population_data")]


def _read_population_xml(path: Path) -> list[dict]:
    """XML no formato da UN (um `<record>` por país × ano) → linhas largas por país."""
    rows: dict[str, dict] = {}
    for record in ET.parse(path).getroot().iter("record"):
        fields = {f.get("name"): f for f in record.iter("field")}
        country, item = fields["Country or Area"], fields["Item"]
        code = country.get("key")
        row = rows.setdefault(
            code,
            {
                "Country Name": clean_str(country.text),
                "Country Code": code,
                "Indicator Name": clean_str(item.text),
                "Indicator Code": item.get("key"),
            },
        )
        row[fields["Year"].text.strip()] = parse_float(fields["Value"].text)
    return list(rows.values())


_POPULATION_READERS = {
    ".csv": read_wdi_csv,
    ".json": _read_population_json,
    ".db": _read_population_sqlite,
    ".xml": _read_population_xml,
}


def read_population(path: Path) -> list[dict]:
    """População em qualquer dos 4 formatos do dataset (CSV, JSON, SQLite, XML) → linhas largas."""
    reader = _POPULATION_READERS.get(Path(path).suffix.lower())
    if reader is None:
        raise ValueError(f"formato de população não suportado: {Path(path).suffix}")
    return reader(Path(path))


def wdi_to_long_records(rows: Iterable[dict], extracted_at: str) -> list[dict]:
    """Largo → longo no formato de registro de indicador; células nulas não viram linha."""
    records = []
    for row in rows:
        for column, value in row.items():
            if not _YEAR.match(column) or value is None:
                continue
            records.append(
                {
                    "country_iso3": row["Country Code"],
                    "country_name": row["Country Name"],
                    "indicator_code": row["Indicator Code"],
                    "indicator_name": row["Indicator Name"],
                    "year": int(column),
                    "value": value,
                    "unit": None,
                    "obs_status": None,
                    "source": SOURCE_KAGGLE,
                    "extracted_at": extracted_at,
                }
            )
    return records


def kaggle_indicator_records(raw_dir: Path, extracted_at: str) -> list[dict]:
    """Registros dos 4 indicadores do Kaggle (bootstrap do histórico)."""
    records = []
    for filename in KAGGLE_INDICATOR_FILES:
        records.extend(wdi_to_long_records(read_wdi_csv(Path(raw_dir) / filename), extracted_at))
    return records


def kaggle_projects(path: Path) -> list[dict]:
    """`projects_data.csv` → projetos normalizados (payload do evento, `source=kaggle-replay`)."""
    with Path(path).open(encoding="utf-8-sig", newline="") as f:
        return [
            normalize_project(r, source=SOURCE_REPLAY) for r in csv.DictReader(f) if clean_str(r.get("id"))
        ]
