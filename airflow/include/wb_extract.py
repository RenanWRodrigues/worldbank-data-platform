"""Extrações do Airflow → landing (Volume) + ops.landing_files (docs/contratos/registros-normalizados.md).

`extract_indicator` e `extract_countries` (DAG semanal) e `bootstrap_kaggle` (DAG manual) recebem landing,
cursor e sessão HTTP por parâmetro: os testes usam fakes e as respostas de exemplo de tests/fixtures/api/.
Cada arquivo gravado vira uma linha em ops.landing_files; o `file_id` devolvido é marcado como processado
depois do job do Databricks.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from kaggle_loader import kaggle_indicator_records
from landing import VolumeLanding, to_ndjson
from worldbank_client import aggregate_codes, fetch_countries, fetch_indicator, now_iso

DATASET_INDICATORS = "indicators"
DATASET_COUNTRIES = "countries"
MIN_COUNTRIES = 180  # coverage_check: países distintos, não agregados, com valor não nulo

# Reexecutar a mesma extração regrava o arquivo e o devolve à fila
REGISTER_SQL = """
INSERT INTO ops.landing_files (path, dataset, records)
VALUES (%s, %s, %s)
ON CONFLICT (path) DO UPDATE SET records = EXCLUDED.records, processed_at = NULL
RETURNING file_id
"""


class ExtractError(RuntimeError):
    """Extração vazia, cobertura insuficiente ou landing sem Volume."""


# ---------------------------------------------------------------- caminhos (puros)


def run_tag(run_id: str) -> str:
    """run_id do Airflow (`manual__2026-10-29T13:58:09+00:00`) → trecho seguro para caminho."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def indicator_path(code: str, run_id: str, source: str = "api") -> str:
    return f"indicators/source={source}/indicator={code}/run={run_tag(run_id)}/part-00000.json"


def countries_path(run_id: str) -> str:
    return f"reference/countries/run={run_tag(run_id)}/part-00000.json"


def volume_host(host: str) -> str:
    """Host da conexão do Airflow com ou sem esquema → URL base da Files API."""
    host = host.strip().rstrip("/")
    return host if host.startswith(("https://", "http://")) else f"https://{host}"


# ---------------------------------------------------------------- landing + registro


def write_records(landing, cur, rel_path: str, records: list[dict], dataset: str) -> dict[str, Any]:
    """Grava NDJSON na landing e registra em ops.landing_files; devolve {file_id, path, records}."""
    path = landing.write(rel_path, to_ndjson(records))
    cur.execute(REGISTER_SQL, (path, dataset, len(records)))
    return {"file_id": cur.fetchone()[0], "path": path, "records": len(records)}


def extract_indicator(
    code: str, run_id: str, landing, cur, session=None, extracted_at: str | None = None
) -> dict[str, Any]:
    """Indicador inteiro da Indicators API → landing. Devolve o registro e os países com valor (coverage)."""
    records = fetch_indicator(code, session=session, extracted_at=extracted_at)
    if not records:
        raise ExtractError(f"{code}: a Indicators API não devolveu linhas")
    landed = write_records(landing, cur, indicator_path(code, run_id), records, DATASET_INDICATORS)
    with_value = sorted({r["country_iso3"] for r in records if r["value"] is not None})
    return {**landed, "indicator": code, "countries_with_value": with_value}


def extract_countries(
    run_id: str, landing, cur, session=None, extracted_at: str | None = None
) -> dict[str, Any]:
    """Metadados de países (`/country`) → landing. Devolve quantos países e quais códigos são agregados."""
    records = fetch_countries(session=session, extracted_at=extracted_at)
    aggregates = aggregate_codes(records)
    landed = write_records(landing, cur, countries_path(run_id), records, DATASET_COUNTRIES)
    countries = len({r["country_iso3"] for r in records} - aggregates)
    return {**landed, "countries": countries, "aggregates": sorted(aggregates)}


def bootstrap_kaggle(
    raw_dir: Path | str, run_id: str, landing, cur, session=None, extracted_at: str | None = None
) -> dict[str, Any]:
    """Histórico do Kaggle (um arquivo por indicador, `source=kaggle`) + metadados de países da API.

    Os metadados entram já no bootstrap: sem eles, agregados do WDI (WLD, ARB, HIC…) iriam para dim_country
    com `is_aggregate = false` até a primeira carga semanal.
    """
    extracted_at = extracted_at or now_iso()
    by_indicator: dict[str, list[dict]] = defaultdict(list)
    try:
        for record in kaggle_indicator_records(Path(raw_dir), extracted_at):
            by_indicator[record["indicator_code"]].append(record)
    except FileNotFoundError as exc:
        missing = exc.filename or exc
        raise ExtractError(f"arquivo do Kaggle ausente em {raw_dir}: {missing} (rode `make data`)") from exc
    if not by_indicator:
        raise ExtractError(f"nenhum indicador do Kaggle em {raw_dir} (rode `make data`)")

    files = [
        write_records(landing, cur, indicator_path(code, run_id, "kaggle"), records, DATASET_INDICATORS)
        for code, records in sorted(by_indicator.items())
    ]
    countries = extract_countries(run_id, landing, cur, session=session, extracted_at=extracted_at)
    return {
        "file_ids": [f["file_id"] for f in files] + [countries["file_id"]],
        "records": {code: len(records) for code, records in sorted(by_indicator.items())},
        "countries": countries["countries"],
    }


# ---------------------------------------------------------------- coverage_check (puro)


def coverage_report(indicators: Iterable[Mapping[str, Any]], countries: Mapping[str, Any]) -> dict[str, int]:
    """{item: países distintos não agregados} para os metadados e cada indicador extraído."""
    aggregates = set(countries["aggregates"])
    report = {"countries": int(countries["countries"])}
    for item in indicators:
        report[item["indicator"]] = len(set(item["countries_with_value"]) - aggregates)
    return report


def check_coverage(report: Mapping[str, int], minimum: int = MIN_COUNTRIES) -> None:
    low = {name: n for name, n in report.items() if n < minimum}
    if low:
        detail = ", ".join(f"{name}: {n}" for name, n in low.items())
        raise ExtractError(f"cobertura abaixo de {minimum} países: {detail}")


# ---------------------------------------------------------------- Volume pela conexão do Airflow


def make_volume_landing(
    conn_id: str = "databricks_default", env: Mapping[str, str] = os.environ
) -> VolumeLanding:
    """Landing no Volume com host e token da conexão do Airflow (sem fallback local: o job só lê o Volume)."""
    from airflow.sdk import BaseHook

    conn = BaseHook.get_connection(conn_id)
    if not conn.host or not conn.password:
        raise ExtractError(f"conexão {conn_id} sem host ou token: confira DATABRICKS_HOST/DATABRICKS_TOKEN")
    return VolumeLanding(
        host=volume_host(conn.host),
        token=conn.password,
        catalog=env.get("DATABRICKS_CATALOG", "worldbank"),
        schema=env.get("DATABRICKS_SCHEMA", "lakehouse"),
        volume=env.get("DATABRICKS_VOLUME", "landing"),
    )
