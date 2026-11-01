"""PostgresRepository com pool/conexão/cursor falsos: SQL, parâmetros, paginação e mapeamento das linhas.

Nada de Postgres: o cursor grava (sql, params) e devolve respostas prontas, na ordem das consultas.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg2.extras
import psycopg2.pool
import pytest
from app import repository as repo_mod
from app.repository import PostgresRepository, pg_params


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=()):
        if self.db.error is not None:
            raise self.db.error
        self.db.queries.append((" ".join(sql.split()), params))

    def fetchall(self):
        return self.db.responses.pop(0) if self.db.responses else []


class FakeConn:
    def __init__(self, db):
        self.db = db
        self.autocommit = False
        self.closed = 0

    def cursor(self, cursor_factory=None):
        self.db.factories.append(cursor_factory)
        return FakeCursor(self.db)


class FakePool:
    """Pool com uma conexão; `putconn` grava se a conexão foi descartada."""

    def __init__(self):
        self.queries: list[tuple[str, object]] = []
        self.responses: list[list[dict]] = []
        self.factories: list = []
        self.returned: list[bool] = []
        self.error: Exception | None = None
        self.conn = FakeConn(self)

    def getconn(self):
        return self.conn

    def putconn(self, con, close=False):
        self.returned.append(close)


@pytest.fixture
def db():
    return FakePool()


@pytest.fixture
def repo(db):
    r = PostgresRepository(params={"host": "x"})
    r._pool = db
    return r


def answer(db, *responses):
    db.responses.extend(list(r) for r in responses)


# ---------------------------------------------------------------- conexão


def test_pg_params_somente_leitura_com_timeout():
    params = pg_params({"POSTGRES_HOST": "db", "API_RO_PASSWORD": "s"})
    assert params["user"] == "api_ro" and params["host"] == "db" and params["port"] == 5432
    assert "default_transaction_read_only=on" in params["options"]
    assert "statement_timeout=" in params["options"]


def test_parametros_padrao_vem_do_ambiente(monkeypatch):
    monkeypatch.setenv("API_RO_USER", "leitor")
    assert PostgresRepository().params["user"] == "leitor"


def test_pool_criado_no_primeiro_uso_e_reaproveitado(monkeypatch):
    created = []

    class Pool:
        def __init__(self, minconn, maxconn, **params):
            created.append((minconn, maxconn, params))

    monkeypatch.setattr(psycopg2.pool, "ThreadedConnectionPool", Pool)
    r = PostgresRepository(params={"host": "db"}, maxconn=3)
    assert created == []  # a API sobe mesmo com o banco fora do ar
    assert r._get_pool() is r._get_pool()
    assert created == [(1, 3, {"host": "db"})]


def test_cursor_em_autocommit_com_realdictcursor_e_devolve_conexao(repo, db):
    answer(db, [{"ok": 1}])
    assert repo.ping() is True
    assert db.conn.autocommit is True
    assert db.factories == [psycopg2.extras.RealDictCursor]
    assert db.returned == [False]


def test_erro_com_conexao_quebrada_descarta_a_conexao(repo, db):
    db.error = RuntimeError("server closed the connection")
    db.conn.closed = 2
    with pytest.raises(RuntimeError):
        repo.list_indicators()
    assert db.returned == [True]


def test_erro_de_consulta_com_conexao_viva_devolve_ao_pool(repo, db):
    db.error = ValueError("syntax error")
    with pytest.raises(ValueError):
        repo.list_indicators()
    assert db.returned == [False]


def test_ping_sem_banco_responde_false(repo, db):
    db.error = OSError("connection refused")
    assert repo.ping() is False


def test_ping_sem_linhas_responde_false(repo, db):
    answer(db, [])
    assert repo.ping() is False


# ---------------------------------------------------------------- países


def country(code, total):
    return {"country_code": code, "country_name": code, "is_aggregate": False, "total": total}


def test_list_countries_passa_filtros_e_paginacao_como_parametros(repo, db):
    answer(db, [country("BRA", 2), country("KEN", 2)])
    repo.list_countries("Africa", "Low income", False, 10, 0)
    sql, params = db.queries[0]
    assert params == {"agg": False, "region": "Africa", "income": "Low income", "limit": 10, "offset": 0}
    assert "Africa" not in sql  # valor do usuário nunca vai no texto do SQL
    assert "LIMIT %(limit)s OFFSET %(offset)s" in sql


def test_list_countries_total_da_janela_e_remove_a_coluna_total(repo, db):
    answer(db, [country("BRA", 7), country("KEN", 7)])
    items, total = repo.list_countries(None, None, True, 2, 0)
    assert total == 7
    assert all("total" not in item for item in items)
    assert len(db.queries) == 1  # sem contagem extra


def test_list_countries_vazio_na_primeira_pagina_total_zero(repo, db):
    answer(db, [])
    assert repo.list_countries(None, None, False, 10, 0) == ([], 0)
    assert len(db.queries) == 1


def test_list_countries_pagina_alem_do_fim_conta_separado(repo, db):
    answer(db, [], [{"total": 42}])
    items, total = repo.list_countries("Africa", None, False, 10, 100)
    assert (items, total) == ([], 42)
    sql, params = db.queries[1]
    assert sql.startswith("SELECT count(*) AS total FROM dw.dim_country WHERE")
    assert params["region"] == "Africa"


def test_get_country_por_iso3(repo, db):
    answer(db, [{"country_code": "BRA"}])
    assert repo.get_country("BRA") == {"country_code": "BRA"}
    assert db.queries[0][1] == ("BRA",)


def test_get_country_inexistente_devolve_none(repo, db):
    answer(db, [])
    assert repo.get_country("XXX") is None


def test_latest_country_year_ultimo_ano_com_dado(repo, db):
    answer(db, [{"year": 2022, "population": 1.0}])
    assert repo.latest_country_year("BRA")["year"] == 2022
    sql, params = db.queries[0]
    assert "ORDER BY year DESC LIMIT 1" in sql and params == ("BRA",)


def test_country_indicator_series_filtros_opcionais(repo, db):
    answer(db, [{"indicator_code": "SP.POP.TOTL", "year": 2020, "value": 1.0, "source": "kaggle"}])
    rows = repo.country_indicator_series("BRA", None, 2000, None)
    assert rows[0]["year"] == 2020
    assert db.queries[0][1] == {"iso3": "BRA", "ind": None, "start": 2000, "end": None}


# ---------------------------------------------------------------- indicadores e ranking


def test_list_indicators_e_get_indicator(repo, db):
    answer(db, [{"indicator_code": "A"}, {"indicator_code": "B"}], [{"indicator_code": "A"}])
    assert [r["indicator_code"] for r in repo.list_indicators()] == ["A", "B"]
    assert repo.get_indicator("A") == {"indicator_code": "A"}
    assert db.queries[1][1] == ("A",)


def test_ranking_com_ano_explicito_ordem_descendente(repo, db):
    answer(db, [{"country_code": "CHN", "value": 1.4e9}])
    year, rows = repo.ranking("SP.POP.TOTL", 2020, 5, ascending=False)
    assert year == 2020 and rows[0]["country_code"] == "CHN"
    sql, params = db.queries[0]
    assert "ORDER BY f.value DESC, f.country_code" in sql
    assert params == ("SP.POP.TOTL", 2020, 5)


def test_ranking_ascendente(repo, db):
    answer(db, [])
    repo.ranking("SP.POP.TOTL", 2020, 5, ascending=True)
    assert "ORDER BY f.value ASC" in db.queries[0][0]


def test_ranking_sem_ano_usa_o_mais_recente_com_cobertura(repo, db):
    # O ano mais recente costuma ser parcial (PIB de 2025 com 186 países): o padrão é o último ano com
    # ≥ 180 países não agregados (mesma régua do coverage_check e do Power BI); sem nenhum, o mais recente.
    answer(db, [{"year": 2024}], [{"country_code": "IND", "value": 1.0}])
    year, _ = repo.ranking("SP.POP.TOTL", None, 3, ascending=False)
    assert year == 2024
    sql = " ".join(db.queries[0][0].split())
    assert "NOT c.is_aggregate" in sql and "ORDER BY (count(*) >= %s) DESC, f.year DESC" in sql
    assert db.queries[0][1] == ("SP.POP.TOTL", 180)
    assert db.queries[1][1] == ("SP.POP.TOTL", 2024, 3)


def test_ranking_indicador_sem_dados_devolve_none_e_vazio(repo, db):
    answer(db, [{"year": None}])
    assert repo.ranking("XX", None, 3, ascending=False) == (None, [])
    assert len(db.queries) == 1


# ---------------------------------------------------------------- projetos


def test_list_projects_filtros_paginacao_e_total(repo, db):
    answer(db, [{"project_id": "P1"}], [{"total": 31}])
    rows, total = repo.list_projects("KEN", "Active", 2020, 1e6, 20, 40)
    assert rows == [{"project_id": "P1"}] and total == 31
    params = {"country": "KEN", "status": "Active", "year": 2020, "min": 1e6, "limit": 20, "offset": 40}
    assert db.queries[0][1] == params and db.queries[1][1] == params
    assert "ORDER BY board_approval_date DESC NULLS LAST, project_id" in db.queries[0][0]


def test_list_projects_sem_resultado_total_zero(repo, db):
    answer(db, [], [{"total": 0}])
    assert repo.list_projects(None, None, None, None, 20, 0) == ([], 0)


def test_get_project_live_project_e_eventos(repo, db):
    answer(db, [{"project_id": "P1"}], [], [{"seq": 1}, {"seq": 2}])
    assert repo.get_project("P1") == {"project_id": "P1"}
    assert repo.live_project("P1") is None
    assert [e["seq"] for e in repo.project_events("P1", 50)] == [1, 2]
    assert "FROM dw.fact_project" in db.queries[0][0]
    assert "FROM rt.projects_latest" in db.queries[1][0]
    assert db.queries[2][1] == ("P1", 50)
    assert "ORDER BY event_time, produced_at, seq" in db.queries[2][0]


# ---------------------------------------------------------------- tempo real


def test_live_projects_filtra_status_mais_recentes_primeiro(repo, db):
    answer(db, [{"project_id": "P1"}])
    repo.live_projects("Active", 10)
    sql, params = db.queries[0]
    assert params == {"status": "Active", "limit": 10}
    assert "ORDER BY updated_at DESC" in sql


def test_live_alerts_limite(repo, db):
    answer(db, [{"alert_id": 1}])
    assert repo.live_alerts(5) == [{"alert_id": 1}]
    assert db.queries[0][1] == (5,)


def test_max_seq_e_events_after_para_o_sse(repo, db):
    answer(db, [{"seq": 9}], [{"seq": 10}])
    assert repo.max_seq() == 9
    assert repo.events_after(9, 100) == [{"seq": 10}]
    sql, params = db.queries[1]
    assert "WHERE seq > %s ORDER BY seq" in sql and params == (9, 100)


def test_pipeline_status_junta_lag_e_cargas(repo, db):
    now = datetime(2026, 10, 29, tzinfo=UTC)
    answer(
        db,
        [{"events_last_hour": 3, "pending_landing_files": 1}],
        [{"consumer_group": "lakehouse-sink", "lag": 0, "measured_at": now}],
        [{"table_name": "fact_project", "row_count": 10, "loaded_at": now}],
    )
    status = repo.pipeline_status()
    assert status["events_last_hour"] == 3
    assert status["kafka_lag"][0]["consumer_group"] == "lakehouse-sink"
    assert status["dw_loads"][0]["table_name"] == "fact_project"
    assert "DISTINCT ON (consumer_group)" in db.queries[1][0]


def test_todas_as_consultas_sao_parametrizadas(repo, db):
    """Nenhum valor do usuário aparece no texto do SQL (só em params)."""
    marker = "Z'; DROP TABLE x; --"
    answer(db, [], [{"total": 0}])
    repo.list_projects(marker, marker, None, None, 10, 0)
    repo.get_country(marker)
    repo.live_projects(marker, 1)
    assert all(marker not in sql for sql, _ in db.queries)


def test_repositorio_satisfaz_o_protocolo():
    methods = [m for m in vars(repo_mod.Repository) if not m.startswith("_")]
    assert all(callable(getattr(PostgresRepository, m)) for m in methods)
