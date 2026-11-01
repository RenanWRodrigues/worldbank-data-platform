"""Bootstrap manual do histórico de indicadores com o dataset do Kaggle.

Grava os 4 indicadores do Kaggle (`source=kaggle`) e os metadados de países da API na landing, roda o job
`worldbank_indicators` e copia o resultado para o dw. Rodar uma vez, depois de `make data`; o silver dá
prioridade à API, então as cargas semanais posteriores substituem os anos que ela cobrir. Sem `data_quality`:
o Kaggle termina em 2017 e reprovaria o frescor da população (a checagem é da DAG semanal).
"""

from __future__ import annotations

import os
from contextlib import closing
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from dag_common import DW_SYNC_POOL, databricks_job, pg_conn

JOB = "worldbank_indicators"


@dag(
    dag_id="worldbank_bootstrap_kaggle",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["worldbank", "indicators", "bootstrap"],
    doc_md=__doc__,
)
def worldbank_bootstrap_kaggle():
    @task
    def load_kaggle_history(**context) -> dict:
        import wb_extract

        landing = wb_extract.make_volume_landing()
        raw_dir = os.getenv("RAW_DATA_DIR", "/opt/airflow/data/raw")
        with closing(pg_conn()) as con, con, con.cursor() as cur:
            return wb_extract.bootstrap_kaggle(raw_dir, context["run_id"], landing, cur)

    databricks_indicators = databricks_job("databricks_indicators", JOB, "DATABRICKS_JOB_ID_INDICATORS")

    @task
    def mark_processed(loaded: dict) -> int:
        import landing_control

        with closing(pg_conn()) as con, con, con.cursor() as cur:
            return landing_control.mark_files(cur, loaded["file_ids"])

    @task(pool=DW_SYNC_POOL)
    def sync_dw(**context) -> dict[str, int]:
        import sync_gold

        return sync_gold.sync_job(JOB, context["run_id"])

    loaded = load_kaggle_history()
    loaded >> databricks_indicators >> sync_dw() >> mark_processed(loaded)


worldbank_bootstrap_kaggle()
