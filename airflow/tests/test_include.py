"""Lógica pura de airflow/include (sem Airflow, sem banco, sem Kafka): roda em `make test` e no container."""

import csv
import io
from datetime import date

import dw_quality
import kafka_health
import landing_control
import pytest
import sync_gold as sg

# ---------------------------------------------------------------- sync_gold: SQL gerado


def test_dimensao_so_upsert_sem_delete():
    stmts = sg.upsert_sql(sg.SPECS["dim_country"])
    assert len(stmts) == 1
    assert stmts[0].startswith("INSERT INTO dw.dim_country AS t (country_code, country_iso2,")
    assert (
        "FROM stg_dim_country ON CONFLICT (country_code) DO UPDATE SET country_iso2 = EXCLUDED.country_iso2"
        in (stmts[0])
    )
    assert not any(s.startswith("DELETE") for s in stmts)


def test_upsert_so_reescreve_linha_que_mudou():
    (stmt,) = sg.upsert_sql(sg.SPECS["dim_indicator"])
    assert stmt.endswith(
        "WHERE (t.indicator_name, t.unit) IS DISTINCT FROM (EXCLUDED.indicator_name, EXCLUDED.unit)"
    )


def test_tabela_so_com_chave_faz_nothing():
    spec = sg.TableSpec("x", ("a", "b"), ("a", "b"))
    (stmt,) = sg.upsert_sql(spec)
    assert stmt.endswith("ON CONFLICT (a, b) DO NOTHING")


def test_fato_espelha_com_delete_pela_chave_composta():
    stmts = sg.upsert_sql(sg.SPECS["fact_indicator"])
    assert stmts[-1] == (
        "DELETE FROM dw.fact_indicator t WHERE NOT EXISTS (SELECT 1 FROM stg_fact_indicator s WHERE "
        "s.country_code = t.country_code AND s.indicator_code = t.indicator_code AND s.year = t.year)"
    )
    insert = next(i for i, s in enumerate(stmts) if s.startswith("INSERT"))
    assert insert == len(stmts) - 2  # upsert antes do delete


def test_fk_opcional_vira_nulo_antes_do_upsert():
    stmts = sg.upsert_sql(sg.SPECS["fact_project"])
    assert stmts[0] == (
        "UPDATE stg_fact_project s SET country_code = NULL WHERE s.country_code IS NOT NULL AND NOT "
        "EXISTS (SELECT 1 FROM dw.dim_country p WHERE p.country_code = s.country_code)"
    )
    assert stmts[1].startswith("INSERT INTO dw.fact_project")


def test_fk_obrigatoria_filtra_a_linha():
    stmts = sg.upsert_sql(sg.SPECS["agg_country_year"])
    assert stmts[:2] == [
        "DELETE FROM stg_agg_country_year s WHERE NOT "
        "EXISTS (SELECT 1 FROM dw.dim_country p WHERE p.country_code = s.country_code)",
        "DELETE FROM stg_agg_country_year s WHERE NOT "
        "EXISTS (SELECT 1 FROM dw.dim_year p WHERE p.year = s.year)",
    ]


def test_regras_de_carga_por_tipo_de_tabela():
    assert {n for n, s in sg.SPECS.items() if s.mirror} == {
        "fact_indicator",
        "fact_project",
        "agg_country_year",
    }
    for name in ("dim_country", "dim_indicator", "dim_year"):
        assert not sg.SPECS[name].fk_filter and not sg.SPECS[name].fk_nullify
    for spec in sg.SPECS.values():
        assert set(spec.keys) <= set(spec.columns)


def test_staging_copy_e_select_no_gold():
    spec = sg.SPECS["dim_year"]
    assert sg.create_staging_sql(spec) == (
        "CREATE TEMP TABLE stg_dim_year (LIKE dw.dim_year INCLUDING DEFAULTS) ON COMMIT DROP"
    )
    assert sg.copy_sql(spec) == "COPY stg_dim_year (year, decade, year_date) FROM STDIN WITH (FORMAT csv)"
    assert (
        sg.source_query(spec, "wb", "lh") == "SELECT year, decade, year_date FROM `wb`.`lh`.`gold_dim_year`"
    )


def test_job_tables_dimensoes_antes_dos_fatos():
    for tables in sg.JOB_TABLES.values():
        dims = [i for i, t in enumerate(tables) if t.startswith("dim_")]
        others = [i for i, t in enumerate(tables) if not t.startswith("dim_")]
        assert max(dims) < min(others)
    assert sg.JOB_TABLES["worldbank_projects"] == ["dim_year", "fact_project", "agg_country_year"]


def test_espelho_recusa_origem_vazia_com_dw_preenchido():
    with pytest.raises(sg.SyncError, match="gold_fact_project veio vazio"):
        sg.check_mirror(sg.SPECS["fact_project"], staged=0, current=10)
    sg.check_mirror(sg.SPECS["fact_project"], staged=0, current=0)  # ambiente novo
    sg.check_mirror(sg.SPECS["dim_year"], staged=0, current=10)  # dimensão não apaga nada


def test_csv_distingue_nulo_de_string_vazia():
    text = sg.to_csv([("BRA", None, "", 1.5, True, date(2020, 1, 1), 'aspas " e, vírgula')])
    assert text == '"BRA",,"","1.5","True","2020-01-01","aspas "" e, vírgula"\n'
    assert next(csv.reader(io.StringIO(text))) == [
        "BRA",
        "",
        "",
        "1.5",
        "True",
        "2020-01-01",
        'aspas " e, vírgula',
    ]


# ---------------------------------------------------------------- sync_gold: execução com fakes


class FakeCursor:
    def __init__(self, counts):
        self.executed: list[tuple[str, object]] = []
        self.copied: list[tuple[str, str]] = []
        self.counts = counts  # {"stg_x" | "dw.x": n}
        self._last = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._last = sql

    def copy_expert(self, sql, file):
        self.copied.append((sql, file.read()))

    def fetchone(self):
        table = self._last.rsplit(" ", 1)[-1]
        return (self.counts.get(table, 0),)


class FakeConn:
    def __init__(self, cursor):
        self.cur = cursor
        self.committed = self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *exc):
        if exc_type:
            self.rolled_back = True
        else:
            self.committed = True
        return False

    def cursor(self):
        return self.cur


def fetch_from(data):
    def _fetch(spec):
        rows = data.get(spec.name, [])
        return [rows] if rows else []

    return _fetch


def test_sync_tables_carrega_na_ordem_numa_transacao_e_audita():
    cur = FakeCursor({"stg_dim_year": 2, "stg_fact_project": 1, "stg_agg_country_year": 1})
    con = FakeConn(cur)
    data = {
        "dim_year": [(2020, "2020s", date(2020, 1, 1)), (2021, "2020s", date(2021, 1, 1))],
        "fact_project": [("P1", *([None] * 16))],
        "agg_country_year": [("BRA", 2020, *([None] * 7))],
    }
    counts = sg.sync_tables(sg.JOB_TABLES["worldbank_projects"], fetch_from(data), con, "run-1")

    assert counts == {"dim_year": 2, "fact_project": 1, "agg_country_year": 1}
    assert con.committed and not con.rolled_back
    creates = [s for s, _ in cur.executed if s.startswith("CREATE TEMP")]
    assert [c.split()[3] for c in creates] == ["stg_dim_year", "stg_fact_project", "stg_agg_country_year"]
    audits = [p for s, p in cur.executed if s.startswith("INSERT INTO dw.load_audit")]
    assert audits == [("dim_year", "run-1"), ("fact_project", "run-1"), ("agg_country_year", "run-1")]
    assert cur.copied[0] == (
        sg.copy_sql(sg.SPECS["dim_year"]),
        '"2020","2020s","2020-01-01"\n"2021","2020s","2021-01-01"\n',
    )


def test_sync_tables_desfaz_tudo_se_espelho_for_recusado():
    cur = FakeCursor({"stg_dim_year": 1, "dw.fact_project": 500})  # gold_fact_project vazio
    con = FakeConn(cur)
    with pytest.raises(sg.SyncError):
        sg.sync_tables(
            ["dim_year", "fact_project"], fetch_from({"dim_year": [(2020, "2020s", None)]}), con, "r"
        )
    assert con.rolled_back and not con.committed
    assert not any(s.startswith("DELETE FROM dw.fact_project") for s, _ in cur.executed)


# ---------------------------------------------------------------- kafka_health


def test_lag_soma_particoes():
    assert kafka_health.compute_lag({0: (0, 100), 1: (0, 50)}, {0: 90, 1: 50}) == 10


def test_lag_de_grupo_sem_commit_conta_desde_o_low():
    assert kafka_health.compute_lag({0: (20, 100), 1: (0, 5)}, {0: -1001, 1: None}) == 85


def test_lag_com_commit_ja_expirado_pela_retencao():
    assert kafka_health.compute_lag({0: (500, 600)}, {0: 10}) == 100


def test_lag_de_particao_vazia_e_zero():
    assert kafka_health.compute_lag({0: (0, 0)}, {}) == 0


def test_over_limit():
    assert kafka_health.over_limit({"a": 10, "b": 30}, max_lag=20) == {"b": 30}


def test_record_lag_uma_linha_por_grupo():
    class Cur:
        def executemany(self, sql, rows):
            self.sql, self.rows = sql, rows

    cur = Cur()
    kafka_health.record_lag(cur, {"lakehouse-sink": 3, "serving-realtime": 0})
    assert "ops.kafka_lag" in cur.sql
    assert cur.rows == [
        ("lakehouse-sink", kafka_health.TOPIC, 3),
        ("serving-realtime", kafka_health.TOPIC, 0),
    ]


# ---------------------------------------------------------------- landing_control e dw_quality


class ScriptedCursor:
    def __init__(self, results):
        self.results = list(results)
        self.executed = []
        self.rowcount = 7

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.results.pop(0)


def test_pendentes_e_marcacao_limitada_ao_snapshot():
    cur = ScriptedCursor([(42, 3)])
    assert landing_control.pending_files(cur, "projects_events") == (42, 3)
    assert landing_control.mark_processed(cur, "projects_events", 42) == 7
    sql, params = cur.executed[-1]
    assert "file_id <= %s" in sql and "processed_at IS NULL" in sql
    assert params == ("projects_events", 42)


def test_pendente_fora_do_volume_falha_antes_de_ser_marcado():
    with pytest.raises(landing_control.LandingError, match="2 arquivo"):
        landing_control.check_in_volume(["/data/landing/a.json", "/data/landing/b.json"])
    landing_control.check_in_volume([])  # tudo no Volume (o SQL já filtra pelo prefixo)


def test_consulta_de_pendentes_fora_do_volume():
    class Cur(ScriptedCursor):
        def fetchall(self):
            return [("/data/landing/a.json",)]

    cur = Cur([])
    assert landing_control.pending_outside_volume(cur, "projects_events") == ["/data/landing/a.json"]
    sql, params = cur.executed[0]
    assert "NOT LIKE '/Volumes/%%'" in sql and "processed_at IS NULL" in sql
    assert params == ("projects_events",)


def test_checagens_do_micro_lote_usam_updated_at_e_parametros():
    checks = dw_quality.micro_batch_checks(max_delay_minutes=60, max_lag=20000)
    rt = next(c for c in checks if "rt.projects_latest" in c.sql)
    assert "updated_at" in rt.sql and "event_time" not in rt.sql
    assert rt.params == (60,)
    lag = next(c for c in checks if "ops.kafka_lag" in c.sql)
    assert lag.params == (kafka_health.TOPIC, 20000)


def test_mark_files_marca_so_os_ids_da_execucao():
    cur = ScriptedCursor([])
    assert landing_control.mark_files(cur, (3, 5)) == 7
    sql, params = cur.executed[0]
    assert "file_id = ANY(%s)" in sql and "processed_at IS NULL" in sql
    assert params == ([3, 5],)


def test_checagens_semanais_minimos_e_frescor():
    checks = dw_quality.weekly_checks(current_year=2026)
    by_name = {c.name: c for c in checks}
    assert by_name["menos de 180 países não agregados com indicador"].params == (180,)
    assert "NOT c.is_aggregate" in by_name["menos de 180 países não agregados com indicador"].sql
    assert by_name["menos de 4 indicadores com dados"].params == (4,)
    assert by_name["população sem ano ≥ 2024"].params == (2024,)
    assert "'%%'" in by_name["percentual fora de 0–100"].sql  # % escapado para o psycopg2


def test_run_checks_lista_todas_as_violacoes():
    checks = [dw_quality.Check("a", "q1"), dw_quality.Check("b", "q2"), dw_quality.Check("c", "q3")]
    with pytest.raises(dw_quality.QualityError, match="a: 2; c: 1"):
        dw_quality.run_checks(ScriptedCursor([(2,), (0,), (1,)]), checks)
    assert dw_quality.run_checks(ScriptedCursor([(0,), (0,), (0,)]), checks) == {"a": 0, "b": 0, "c": 0}
