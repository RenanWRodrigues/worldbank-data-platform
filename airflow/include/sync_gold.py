"""Carga gold (Delta) → dw (PostgreSQL).

Para cada tabela: temporária `stg_<tabela>` + COPY das linhas lidas do SQL Warehouse +
`INSERT … ON CONFLICT DO UPDATE` e, nos fatos/agg, `DELETE` do que sumiu da origem (espelho). Todas as
tabelas do job numa transação só, com uma linha em `dw.load_audit` por tabela. O SQL é gerado por funções
puras (testadas sem banco); o acesso ao Databricks e ao Postgres fica em `sync_job`, chamado pela tarefa
`sync_dw` das DAGs.
"""

from __future__ import annotations

import csv
import io
import os
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any


class SyncError(RuntimeError):
    """Carga recusada: a transação é desfeita e o dw fica como estava."""


@dataclass(frozen=True)
class ForeignKey:
    column: str
    parent: str
    parent_column: str


@dataclass(frozen=True)
class TableSpec:
    """Tabela do dw: colunas na ordem do DDL (= ordem do gold), chave e regras de carga."""

    name: str
    columns: tuple[str, ...]
    keys: tuple[str, ...]
    mirror: bool = False  # fatos e agg: apaga do dw o que sumiu da origem
    fk_nullify: tuple[ForeignKey, ...] = ()  # FK opcional: vira NULL se o pai não existir
    fk_filter: tuple[ForeignKey, ...] = ()  # FK obrigatória: a linha é descartada


_COUNTRY = ForeignKey("country_code", "dim_country", "country_code")
_YEAR = ForeignKey("year", "dim_year", "year")

SPECS: dict[str, TableSpec] = {
    spec.name: spec
    for spec in (
        TableSpec(
            "dim_country",
            ("country_code", "country_iso2", "country_name", "region", "income_level", "is_aggregate"),
            ("country_code",),
        ),
        TableSpec("dim_indicator", ("indicator_code", "indicator_name", "unit"), ("indicator_code",)),
        TableSpec("dim_year", ("year", "decade", "year_date"), ("year",)),
        TableSpec(
            "fact_indicator",
            ("country_code", "indicator_code", "year", "value", "source"),
            ("country_code", "indicator_code", "year"),
            mirror=True,
            fk_filter=(_COUNTRY, ForeignKey("indicator_code", "dim_indicator", "indicator_code"), _YEAR),
        ),
        TableSpec(
            "fact_project",
            (
                "project_id",
                "country_code",
                "country_name_raw",
                "region_name",
                "project_name",
                "status",
                "lending_instrument",
                "major_sector",
                "board_approval_date",
                "approval_year",
                "closing_date",
                "total_amount_usd",
                "ibrd_amount_usd",
                "ida_amount_usd",
                "grant_amount_usd",
                "last_event_type",
                "last_event_time",
            ),
            ("project_id",),
            mirror=True,
            fk_nullify=(_COUNTRY,),
        ),
        TableSpec(
            "agg_country_year",
            (
                "country_code",
                "year",
                "population",
                "gdp_usd",
                "gdp_per_capita_usd",
                "rural_population_pct",
                "electricity_access_pct",
                "projects_approved",
                "projects_total_amount_usd",
            ),
            ("country_code", "year"),
            mirror=True,
            fk_filter=(_COUNTRY, _YEAR),
        ),
    )
}

# O que cada job produz e a DAG correspondente sincroniza, na ordem do sync (dimensões antes dos fatos).
# Igual a `JOB_TABLES` de databricks/src/wb_transforms.py (tests/test_sync_specs.py compara).
JOB_TABLES: dict[str, list[str]] = {
    "worldbank_indicators": [
        "dim_country",
        "dim_indicator",
        "dim_year",
        "fact_indicator",
        "agg_country_year",
    ],
    "worldbank_projects": ["dim_year", "fact_project", "agg_country_year"],
}


# ---------------------------------------------------------------- SQL gerado (puro)


def staging_name(spec: TableSpec) -> str:
    return f"stg_{spec.name}"


def create_staging_sql(spec: TableSpec) -> str:
    """Temporária com as colunas e NOT NULL do dw (sem CHECK/FK: quem decide é o upsert)."""
    return f"CREATE TEMP TABLE {staging_name(spec)} (LIKE dw.{spec.name} INCLUDING DEFAULTS) ON COMMIT DROP"


def copy_sql(spec: TableSpec) -> str:
    return f"COPY {staging_name(spec)} ({', '.join(spec.columns)}) FROM STDIN WITH (FORMAT csv)"


def source_query(spec: TableSpec, catalog: str, schema: str) -> str:
    """SELECT no gold com as colunas na ordem do dw."""
    return f"SELECT {', '.join(spec.columns)} FROM `{catalog}`.`{schema}`.`gold_{spec.name}`"


def _exists_parent(fk: ForeignKey) -> str:
    return f"EXISTS (SELECT 1 FROM dw.{fk.parent} p WHERE p.{fk.parent_column} = s.{fk.column})"


def upsert_sql(spec: TableSpec) -> list[str]:
    """Comandos, em ordem, que levam a temporária já carregada para `dw.<tabela>`.

    1. FKs opcionais sem pai viram NULL; 2. linhas com FK obrigatória sem pai saem;
    3. upsert (só reescreve linhas que mudaram); 4. espelho: apaga do dw o que não veio da origem.
    """
    stg, cols = staging_name(spec), ", ".join(spec.columns)
    stmts = [
        f"UPDATE {stg} s SET {fk.column} = NULL WHERE s.{fk.column} IS NOT NULL AND NOT {_exists_parent(fk)}"
        for fk in spec.fk_nullify
    ]
    stmts += [f"DELETE FROM {stg} s WHERE NOT {_exists_parent(fk)}" for fk in spec.fk_filter]

    others = [c for c in spec.columns if c not in spec.keys]
    if others:
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in others)
        old = ", ".join(f"t.{c}" for c in others)
        new = ", ".join(f"EXCLUDED.{c}" for c in others)
        action = f"DO UPDATE SET {sets} WHERE ({old}) IS DISTINCT FROM ({new})"
    else:
        action = "DO NOTHING"
    stmts.append(
        f"INSERT INTO dw.{spec.name} AS t ({cols}) SELECT {cols} FROM {stg} "
        f"ON CONFLICT ({', '.join(spec.keys)}) {action}"
    )

    if spec.mirror:
        match = " AND ".join(f"s.{k} = t.{k}" for k in spec.keys)
        stmts.append(f"DELETE FROM dw.{spec.name} t WHERE NOT EXISTS (SELECT 1 FROM {stg} s WHERE {match})")
    return stmts


def audit_sql(spec: TableSpec) -> str:
    """Linha em dw.load_audit com o que foi carregado (após os filtros de FK); parâmetros: tabela, run_id."""
    return (
        "INSERT INTO dw.load_audit (table_name, row_count, run_id) "
        f"SELECT %s, count(*), %s FROM {staging_name(spec)}"
    )


def check_mirror(spec: TableSpec, staged: int, current: int) -> None:
    """Origem vazia com o dw preenchido não é espelhada: apagaria o fato inteiro.

    Gold vazio nesse caso é sinal de job quebrado ou leitura errada, nunca de dado legítimo.
    """
    if spec.mirror and staged == 0 and current > 0:
        raise SyncError(
            f"gold_{spec.name} veio vazio e dw.{spec.name} tem {current} linhas: espelho recusado "
            "(confira o job do Databricks e o check_quality)"
        )


def to_csv(rows: Iterable[Sequence[Any]]) -> str:
    """CSV para o COPY: None vira campo vazio sem aspas (NULL); o resto vai entre aspas ("" ≠ NULL)."""
    buffer = io.StringIO()
    csv.writer(buffer, quoting=csv.QUOTE_NOTNULL, lineterminator="\n").writerows(rows)
    return buffer.getvalue()


# ---------------------------------------------------------------- execução


Fetch = Callable[[TableSpec], Iterable[Sequence[Sequence[Any]]]]


def sync_tables(tables: Sequence[str], fetch: Fetch, pg_conn, run_id: str) -> dict[str, int]:
    """Carrega `tables` (na ordem dada) numa única transação; `fetch(spec)` devolve lotes de linhas do gold.

    Devolve {tabela: linhas carregadas}. Qualquer erro desfaz tudo (nenhuma tabela fica pela metade).
    """
    counts: dict[str, int] = {}
    with pg_conn, pg_conn.cursor() as cur:  # commit no fim, rollback em exceção
        for name in tables:
            spec = SPECS[name]
            cur.execute(create_staging_sql(spec))
            for chunk in fetch(spec):
                cur.copy_expert(copy_sql(spec), io.StringIO(to_csv(chunk)))
            cur.execute(f"SELECT count(*) FROM {staging_name(spec)}")
            staged = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM dw.{spec.name}")
            check_mirror(spec, staged, cur.fetchone()[0])
            for stmt in upsert_sql(spec):
                cur.execute(stmt)
            cur.execute(f"SELECT count(*) FROM {staging_name(spec)}")
            counts[name] = cur.fetchone()[0]
            cur.execute(audit_sql(spec), (name, run_id))
    return counts


def fetch_gold(dbx_conn, catalog: str, schema: str, batch_rows: int = 10_000) -> Fetch:
    """Lê `gold_<tabela>` pelo SQL Warehouse em lotes de `batch_rows` linhas.

    Lotes pequenos: a tarefa roda dentro do scheduler (LocalExecutor, `mem_limit` de 1 GiB); com 50 mil linhas
    o `sync_dw` da carga semanal chegou a ser morto por falta de memória (SIGKILL).
    """

    def _fetch(spec: TableSpec):
        with dbx_conn.cursor() as cur:
            cur.execute(source_query(spec, catalog, schema))
            while rows := cur.fetchmany(batch_rows):
                yield rows

    return _fetch


def sync_job(
    job: str,
    run_id: str,
    postgres_conn_id: str = "postgres_dw",
    databricks_conn_id: str = "databricks_default",
    env: Mapping[str, str] = os.environ,
) -> dict[str, int]:
    """Tarefa `sync_dw`: copia as tabelas do `job` (JOB_TABLES) do gold para o dw."""
    from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    http_path = env.get("DATABRICKS_HTTP_PATH", "")
    if not http_path:
        raise SyncError("DATABRICKS_HTTP_PATH vazio: defina o HTTP path do SQL Warehouse no .env")
    catalog = env.get("DATABRICKS_CATALOG", "worldbank")
    schema = env.get("DATABRICKS_SCHEMA", "lakehouse")

    dbx = DatabricksSqlHook(databricks_conn_id=databricks_conn_id, http_path=http_path).get_conn()
    pg = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()
    try:
        return sync_tables(JOB_TABLES[job], fetch_gold(dbx, catalog, schema), pg, run_id)
    finally:
        pg.close()
        dbx.close()
