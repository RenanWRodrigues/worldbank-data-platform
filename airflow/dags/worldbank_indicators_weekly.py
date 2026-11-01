"""Carga semanal de indicadores: Indicators API → landing → job `worldbank_indicators` → dw.

`extract` roda uma tarefa por indicador (mapeamento dinâmico sobre
`WB_INDICATORS`), em paralelo com `countries` (metadados e agregados). `coverage_check` barra a carga se algum
indicador ou os metadados vierem com menos de 180 países não agregados com valor; só então o Databricks liga
(pool `databricks`). Depois: copia as tabelas do job para o dw, marca os arquivos desta execução e checa
faixas e frescor.
"""

from __future__ import annotations

import os
from contextlib import closing
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from dag_common import DW_SYNC_POOL, databricks_job, pg_conn

JOB = "worldbank_indicators"


@dag(
    dag_id="worldbank_indicators_weekly",
    schedule="10 6 * * 1",  # segundas, 06:10 UTC
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["worldbank", "indicators", "batch"],
    doc_md=__doc__,
)
def worldbank_indicators_weekly():
    @task
    def indicator_list() -> list[str]:
        from worldbank_client import indicator_codes

        return indicator_codes(os.getenv("WB_INDICATORS"))

    # API pública instável: mais tentativas, com espera crescente, e no máximo 2 indicadores ao mesmo tempo
    @task(retries=3, retry_exponential_backoff=True, max_active_tis_per_dag=2)
    def extract(code: str, **context) -> dict:
        import wb_extract

        landing = wb_extract.make_volume_landing()
        with closing(pg_conn()) as con, con, con.cursor() as cur:
            return wb_extract.extract_indicator(code, context["run_id"], landing, cur)

    @task(retries=3, retry_exponential_backoff=True)
    def countries(**context) -> dict:
        import wb_extract

        landing = wb_extract.make_volume_landing()
        with closing(pg_conn()) as con, con, con.cursor() as cur:
            return wb_extract.extract_countries(context["run_id"], landing, cur)

    @task
    def coverage_check(extracted: list[dict], meta: dict) -> dict[str, int]:
        import wb_extract

        report = wb_extract.coverage_report(extracted, meta)
        print("cobertura (países não agregados com valor):", report)
        wb_extract.check_coverage(report)
        return report

    databricks_indicators = databricks_job("databricks_indicators", JOB, "DATABRICKS_JOB_ID_INDICATORS")

    @task
    def mark_processed(extracted: list[dict], meta: dict) -> int:
        import landing_control

        file_ids = [item["file_id"] for item in extracted] + [meta["file_id"]]
        with closing(pg_conn()) as con, con, con.cursor() as cur:
            return landing_control.mark_files(cur, file_ids)

    @task(pool=DW_SYNC_POOL)
    def sync_dw(**context) -> dict[str, int]:
        import sync_gold

        return sync_gold.sync_job(JOB, context["run_id"])

    @task
    def data_quality() -> dict[str, int]:
        import dw_quality

        checks = dw_quality.weekly_checks(current_year=datetime.now(UTC).year)
        with closing(pg_conn()) as con, con, con.cursor() as cur:
            return dw_quality.run_checks(cur, checks)

    extracted = extract.expand(code=indicator_list())
    meta = countries()
    coverage_check(extracted, meta) >> databricks_indicators
    databricks_indicators >> sync_dw() >> mark_processed(extracted, meta) >> data_quality()


worldbank_indicators_weekly()
