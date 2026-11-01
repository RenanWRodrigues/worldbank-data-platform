"""I/O de include/ com fakes: `measure_lag` (Consumer falso), `fetch_gold` (cursor falso) e `dag_common`
(módulos do Airflow falsos em sys.modules — roda sem o Airflow instalado e também no container)."""

from __future__ import annotations

import sys
import types

import kafka_health as kh
import pytest
import sync_gold as sg

# ---------------------------------------------------------------- measure_lag


class TopicPartition:
    def __init__(self, topic, partition, offset=-1001):
        self.topic, self.partition, self.offset = topic, partition, offset


class FakeBroker:
    """Estado do broker: watermarks por partição e offsets commitados por grupo."""

    def __init__(self, watermarks, committed, topic_error=None):
        self.watermarks = watermarks
        self.committed = committed
        self.topic_error = topic_error
        self.consumers: list = []


@pytest.fixture
def broker(monkeypatch):
    state = FakeBroker(watermarks={0: (0, 100), 1: (10, 50)}, committed={})

    class Consumer:
        def __init__(self, config):
            self.config = config
            self.closed = False
            state.consumers.append(self)

        def list_topics(self, topic, timeout=None):
            metadata = types.SimpleNamespace(
                error=state.topic_error, partitions={p: None for p in state.watermarks}
            )
            return types.SimpleNamespace(topics={topic: metadata})

        def committed(self, partitions, timeout=None):
            offsets = state.committed.get(self.config["group.id"], {})
            return [
                TopicPartition(tp.topic, tp.partition, offsets.get(tp.partition, -1001)) for tp in partitions
            ]

        def get_watermark_offsets(self, tp, timeout=None, cached=True):
            assert cached is False  # watermarks frescos, não os do cache local
            return state.watermarks[tp.partition]

        def close(self):
            self.closed = True

    module = types.ModuleType("confluent_kafka")
    module.Consumer, module.TopicPartition = Consumer, TopicPartition
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    return state


def test_measure_lag_soma_high_menos_commitado_por_grupo(broker):
    broker.committed = {"lakehouse-sink": {0: 90, 1: 50}, "serving-realtime": {0: 100, 1: 50}}
    assert kh.measure_lag("kafka:9092") == {"lakehouse-sink": 10, "serving-realtime": 0}


def test_measure_lag_grupo_sem_commit_conta_desde_low(broker):
    assert kh.measure_lag("kafka:9092", groups=("novo",)) == {"novo": 100 + 40}


def test_measure_lag_nao_entra_no_grupo_nem_faz_auto_commit(broker):
    kh.measure_lag("kafka:9092")
    configs = [c.config for c in broker.consumers]
    assert [c["group.id"] for c in configs] == list(kh.GROUPS)
    assert all(c["enable.auto.commit"] is False and c["bootstrap.servers"] == "kafka:9092" for c in configs)
    assert all(c.closed for c in broker.consumers)


def test_measure_lag_topico_com_erro_falha_e_fecha_o_consumer(broker):
    broker.topic_error = "UNKNOWN_TOPIC_OR_PART"
    with pytest.raises(RuntimeError, match="UNKNOWN_TOPIC_OR_PART"):
        kh.measure_lag("kafka:9092")
    assert len(broker.consumers) == 1 and broker.consumers[0].closed


def test_measure_lag_sem_grupos_devolve_vazio(broker):
    assert kh.measure_lag("kafka:9092", groups=()) == {}


# ---------------------------------------------------------------- fetch_gold


class FakeDbxCursor:
    def __init__(self, rows, log):
        self.rows, self.log = list(rows), log

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.log.append("close")
        return False

    def execute(self, sql):
        self.log.append(sql)

    def fetchmany(self, size):
        self.log.append(("fetchmany", size))
        chunk, self.rows = self.rows[:size], self.rows[size:]
        return chunk


class FakeDbxConn:
    def __init__(self, rows):
        self.rows = rows
        self.log: list = []

    def cursor(self):
        return FakeDbxCursor(self.rows, self.log)


def test_fetch_gold_le_a_tabela_gold_em_lotes():
    conn = FakeDbxConn([(i,) for i in range(5)])
    chunks = list(sg.fetch_gold(conn, "wb", "lake", batch_rows=2)(sg.SPECS["dim_year"]))
    assert chunks == [[(0,), (1,)], [(2,), (3,)], [(4,)]]
    assert conn.log[0] == "SELECT year, decade, year_date FROM `wb`.`lake`.`gold_dim_year`"
    assert conn.log[-1] == "close"


def test_fetch_gold_tabela_vazia_nao_gera_lotes():
    conn = FakeDbxConn([])
    assert list(sg.fetch_gold(conn, "wb", "lake")(sg.SPECS["fact_project"])) == []
    assert ("fetchmany", 10_000) in conn.log


def test_fetch_gold_alimenta_sync_tables_com_copy_por_lote():
    """fetch_gold → sync_tables: um COPY por lote, todos para a temporária da tabela."""

    class PgCursor:
        def __init__(self):
            self.copies: list[tuple[str, str]] = []
            self.counts = iter([3, 0, 3])

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, sql, params=None):
            pass

        def copy_expert(self, sql, buffer):
            self.copies.append((sql, buffer.read()))

        def fetchone(self):
            return (next(self.counts),)

    class PgConn:
        def __init__(self):
            self.cur = PgCursor()

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def cursor(self):
            return self.cur

    rows = [(2019, "2010s", "2019-01-01"), (2020, "2020s", "2020-01-01"), (2021, "2020s", "2021-01-01")]
    pg = PgConn()
    counts = sg.sync_tables(
        ["dim_year"], sg.fetch_gold(FakeDbxConn(rows), "wb", "lake", batch_rows=2), pg, "r1"
    )
    assert counts == {"dim_year": 3}
    assert [sql for sql, _ in pg.cur.copies] == [sg.copy_sql(sg.SPECS["dim_year"])] * 2
    assert pg.cur.copies[1][1] == '"2021","2020s","2021-01-01"\n'


# ---------------------------------------------------------------- dag_common


@pytest.fixture
def fake_airflow(monkeypatch):
    """Registra módulos falsos do provider Postgres e Databricks em sys.modules."""
    created: dict[str, list] = {"hooks": [], "operators": []}

    class PostgresHook:
        def __init__(self, postgres_conn_id):
            self.conn_id = postgres_conn_id
            created["hooks"].append(self)

        def get_conn(self):
            return ("conexao", self.conn_id)

    class DatabricksRunNowOperator:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            created["operators"].append(self)

    leaves = {
        "airflow.providers.postgres.hooks.postgres": {"PostgresHook": PostgresHook},
        "airflow.providers.databricks.operators.databricks": {
            "DatabricksRunNowOperator": DatabricksRunNowOperator
        },
    }
    for name, attrs in leaves.items():
        parts = name.split(".")
        for i in range(2, len(parts)):  # pacotes intermediários (airflow.providers, ...)
            monkeypatch.setitem(sys.modules, ".".join(parts[:i]), types.ModuleType(".".join(parts[:i])))
        module = types.ModuleType(name)
        for attr, value in attrs.items():
            setattr(module, attr, value)
        monkeypatch.setitem(sys.modules, name, module)
    return created


def test_pg_conn_usa_a_conexao_postgres_dw(fake_airflow):
    import dag_common

    assert dag_common.pg_conn() == ("conexao", "postgres_dw")


def test_databricks_job_no_pool_com_parametros_do_job(fake_airflow, monkeypatch):
    import dag_common

    monkeypatch.setenv("DATABRICKS_JOB_ID_PROJECTS", "123")
    monkeypatch.setenv("DATABRICKS_CATALOG", "cat")
    monkeypatch.delenv("DATABRICKS_SCHEMA", raising=False)
    monkeypatch.delenv("DATABRICKS_VOLUME", raising=False)
    op = dag_common.databricks_job("databricks_projects", "worldbank_projects", "DATABRICKS_JOB_ID_PROJECTS")
    kw = op.kwargs
    assert kw["task_id"] == "databricks_projects" and kw["job_id"] == "123"
    assert kw["pool"] == "databricks" and kw["databricks_conn_id"] == "databricks_default"
    assert kw["deferrable"] is True
    assert kw["job_parameters"] == {
        "catalog": "cat",
        "schema": "lakehouse",
        "volume": "landing",
        "run_id": "{{ run_id }}",
        "job": "worldbank_projects",
    }


def test_databricks_job_sem_job_id_no_ambiente_vira_none(fake_airflow, monkeypatch):
    import dag_common

    monkeypatch.setenv("DATABRICKS_JOB_ID_X", "")
    op = dag_common.databricks_job("t", "worldbank_indicators", "DATABRICKS_JOB_ID_X")
    assert op.kwargs["job_id"] is None
