"""Peças comuns às DAGs: conexão com o Postgres e o operador dos jobs do Databricks."""

from __future__ import annotations

import os

PG_CONN_ID = "postgres_dw"
DATABRICKS_CONN_ID = "databricks_default"
DATABRICKS_POOL = "databricks"  # 1 slot (airflow-init): os dois jobs nunca escrevem o gold ao mesmo tempo
DW_SYNC_POOL = "dw_sync"  # 1 slot (airflow-init): dois sync_dw nunca espelham o mesmo fato em paralelo


def pg_conn():
    """Conexão psycopg2 com o banco da plataforma (AIRFLOW_CONN_POSTGRES_DW)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()


def databricks_job(task_id: str, job: str, job_id_env: str):
    """`DatabricksRunNowOperator` do `job` (worldbank_indicators | worldbank_projects) no pool `databricks`.

    O JOB_ID vem do ambiente (`DATABRICKS_JOB_ID_*` do .env); `run_id` da DAG vira o identificador da carga.
    """
    from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

    return DatabricksRunNowOperator(
        task_id=task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=os.getenv(job_id_env) or None,
        job_parameters={
            "catalog": os.getenv("DATABRICKS_CATALOG", "worldbank"),
            "schema": os.getenv("DATABRICKS_SCHEMA", "lakehouse"),
            "volume": os.getenv("DATABRICKS_VOLUME", "landing"),
            "run_id": "{{ run_id }}",
            "job": job,
        },
        pool=DATABRICKS_POOL,
        deferrable=True,  # espera no triggerer, sem ocupar o scheduler durante a partida do serverless
        retries=1,
    )
