"""Micro-lotes de eventos de projetos: landing → job `worldbank_projects` → dw.

`kafka_lag` registra o lag dos dois sinks; `has_new_files` encerra a execução (tarefas seguintes puladas)
quando o lake-sink não fechou micro-lote novo — o Databricks só liga quando há dado novo. Com arquivos
pendentes: job do Databricks (pool `databricks`, 1 slot), copia o gold do job para o dw, marca os arquivos e
checa a qualidade (inclusive rt × dw e o lag medido no início).
"""

from __future__ import annotations

import os
from contextlib import closing
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from dag_common import DW_SYNC_POOL, databricks_job, pg_conn

JOB = "worldbank_projects"
DATASET = "projects_events"


@dag(
    dag_id="worldbank_projects_microbatch",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["worldbank", "projects", "stream"],
    doc_md=__doc__,
)
def worldbank_projects_microbatch():
    @task
    def kafka_lag() -> dict[str, int]:
        import kafka_health

        lags = kafka_health.measure_lag(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        with closing(pg_conn()) as con, con, con.cursor() as cur:
            kafka_health.record_lag(cur, lags)
        return lags

    @task.short_circuit
    def has_new_files() -> int | None:
        """Maior file_id pendente (vira o limite do mark_processed) ou None, que pula o resto da DAG."""
        import landing_control

        with closing(pg_conn()) as con, con, con.cursor() as cur:
            landing_control.check_in_volume(landing_control.pending_outside_volume(cur, DATASET))
            max_id, count = landing_control.pending_files(cur, DATASET)
        print(f"{count} micro-lote(s) pendente(s) em ops.landing_files (até file_id {max_id})")
        return max_id if count else None

    databricks_projects = databricks_job("databricks_projects", JOB, "DATABRICKS_JOB_ID_PROJECTS")

    @task
    def mark_processed(up_to: int) -> int:
        import landing_control

        with closing(pg_conn()) as con, con, con.cursor() as cur:
            return landing_control.mark_processed(cur, DATASET, up_to)

    @task(pool=DW_SYNC_POOL)
    def sync_dw(**context) -> dict[str, int]:
        import sync_gold

        return sync_gold.sync_job(JOB, context["run_id"])

    @task
    def data_quality() -> dict[str, int]:
        import dw_quality

        checks = dw_quality.micro_batch_checks(
            max_delay_minutes=int(os.getenv("RT_DW_MAX_DELAY_MINUTES", "60")),
            max_lag=int(os.getenv("KAFKA_MAX_LAG", "20000")),
        )
        with closing(pg_conn()) as con, con, con.cursor() as cur:
            return dw_quality.run_checks(cur, checks)

    pending = has_new_files()
    kafka_lag() >> pending >> databricks_projects
    # Marca só depois do sync: se ele falhar, os arquivos seguem pendentes e a próxima execução refaz tudo
    databricks_projects >> sync_dw() >> mark_processed(pending) >> data_quality()


worldbank_projects_microbatch()
