"""O SQL de init do Postgres precisa bater com o contrato de docs/modelo-de-dados.md (sem Docker)."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOC = (ROOT / "docs" / "modelo-de-dados.md").read_text(encoding="utf-8")
INIT = ROOT / "postgres" / "init"
SQL = (INIT / "01_init.sql").read_text(encoding="utf-8")
IDENT = re.compile(r"^`?([a-z][a-z0-9_]*)`?$")


def sql_tables() -> dict[str, set[str]]:
    """{schema.tabela: colunas} a partir dos CREATE TABLE do init."""
    tables = {}
    for name, body in re.findall(r"CREATE TABLE (\w+\.\w+) \((.*?)\n\);", SQL, flags=re.S):
        cols = set()
        for line in body.splitlines():
            m = re.match(r"\s+([a-z_][a-z0-9_]*)\s", line)
            if m and m.group(1) not in {"primary", "unique", "check", "foreign"}:
                cols.add(m.group(1))
        tables[name] = cols
    return tables


def doc_ops_rt() -> dict[str, set[str]]:
    """Linhas das tabelas markdown de ops e rt: | `schema.tabela` | chave | colunas | ... |."""
    tables = {}
    for line in DOC.splitlines():
        m = re.match(r"\| `((?:ops|rt)\.\w+)` \|([^|]*)\|([^|]*)\|", line)
        if not m:
            continue
        name, key, cols = m.groups()
        # Chave: identificadores entre crases (`event_id`) ou numa tupla (measured_at, consumer_group, topic)
        tupla = re.match(r"\s*\(([^)]*)\)", key)
        names = set(re.findall(r"`([a-z][a-z0-9_]*)`", key))
        names |= {p.strip() for p in tupla.group(1).split(",")} if tupla else set()
        for part in re.sub(r"\([^)]*\)", "", cols).split(","):
            word = part.strip().split(" ")[0]
            if IDENT.match(word):
                names.add(IDENT.match(word).group(1))
        tables[name] = names
    return tables


def doc_dw() -> dict[str, set[str]]:
    """Parágrafos **tabela** — PK ...: colunas. (dw) e a tabela de colunas de fact_project."""
    tables = {}
    for name, text in re.findall(r"^\*\*(\w+)\*\* — (.*?)(?:\n\n|\n\|)", DOC, flags=re.S | re.M):
        if name.startswith("vw_"):
            continue
        pk = re.match(r"PK \(([^)]*)\)", text)  # chave composta: PK (`a`, `b`)
        cols = set(re.findall(r"`([a-z][a-z0-9_]*)`", pk.group(1))) if pk else set()
        text = re.sub(r"\([^)]*\)", "", text).replace("PK", "")
        text = text.split(".")[0].replace(":", ",")
        cols |= {IDENT.match(p.strip()).group(1) for p in text.split(",") if IDENT.match(p.strip())}
        tables[f"dw.{name}"] = cols
    fact = re.search(r"\*\*fact_project\*\*.*?\n((?:\|.*\n)+)", DOC).group(1)
    for row in fact.splitlines()[2:]:
        tables["dw.fact_project"] |= {c.strip() for c in row.split("|")[1].split(",")}
    tables["dw.fact_project"].add("project_id")
    return tables


EXPECTED = {**doc_ops_rt(), **doc_dw()}


def test_contrato_lido_do_doc_tem_todas_as_tabelas():
    # Garante que o parser do doc não ficou vazio se o markdown mudar de formato
    assert len(EXPECTED) == 13  # ops 3 + rt 3 + dw 7 (a view é checada à parte)
    assert {"country_code", "year", "population", "projects_total_amount_usd"} <= EXPECTED[
        "dw.agg_country_year"
    ]
    assert {"country_code", "indicator_code", "year", "value"} <= EXPECTED["dw.fact_indicator"]


@pytest.mark.parametrize("table", sorted(EXPECTED))
def test_tabela_do_contrato_existe_no_init_com_as_colunas(table):
    tables = sql_tables()
    assert table in tables, f"{table} está em docs/modelo-de-dados.md mas não em 01_init.sql"
    faltando = EXPECTED[table] - tables[table]
    assert not faltando, f"{table}: colunas do contrato ausentes no SQL: {sorted(faltando)}"


def test_init_nao_tem_tabela_fora_do_contrato():
    assert set(sql_tables()) <= set(EXPECTED)


def test_view_e_permissoes_de_leitura():
    assert "CREATE VIEW dw.vw_country_year" in SQL
    assert "WHERE NOT c.is_aggregate" in SQL
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA dw, rt TO powerbi, api_ro" in SQL
    assert "GRANT SELECT ON ops.landing_files, ops.kafka_lag TO api_ro" in SQL
    assert "ALTER DEFAULT PRIVILEGES IN SCHEMA dw, rt GRANT SELECT ON TABLES TO powerbi, api_ro" in SQL
    # Leitura só: nenhum privilégio de escrita para os usuários de BI/API
    assert not re.search(r"GRANT (INSERT|UPDATE|DELETE|ALL)[^;]*(powerbi|api_ro)", SQL)


def test_nenhuma_senha_literal_em_sql_versionado():
    for path in INIT.glob("*.sql"):
        assert not re.search(r"PASSWORD\s+'", path.read_text(encoding="utf-8"), flags=re.I), path


def test_usuarios_criados_com_senha_do_ambiente():
    script = (INIT / "00_users.sh").read_text(encoding="utf-8")
    assert "$POWERBI_PASSWORD" in script and "$API_RO_PASSWORD" in script
    assert "PASSWORD :'powerbi_pw'" in script and "PASSWORD :'api_ro_pw'" in script
    assert "\r\n" not in script, "00_users.sh precisa de fim de linha LF para rodar no container"


# ---------------------------------------------------------------- migrações (ambiente existente)

MIGRATIONS = sorted((ROOT / "postgres" / "migrations").glob("*.sql"))


def test_migracoes_numeradas_e_transacionais():
    assert MIGRATIONS, "postgres/migrations vazio"
    for path in MIGRATIONS:
        assert re.match(r"\d{3}_[a-z0-9_]+\.sql$", path.name), path.name
        sql = path.read_text(encoding="utf-8")
        assert sql.count("BEGIN;") == 1 and sql.rstrip().endswith("COMMIT;"), path.name
        assert not re.search(r"PASSWORD\s+'", sql, flags=re.I), path.name


def test_migracao_last_source_tem_a_autoridade_do_realtime_sink():
    """A ordem do recálculo (ADR 0009) é a mesma do UPSERT do realtime-sink e o init já tem a coluna."""
    import sys

    sys.path.insert(0, str(ROOT / "ingestion"))
    from events import SOURCE_AUTHORITY

    path = ROOT / "postgres" / "migrations" / "001_projects_latest_last_source.sql"
    sql = path.read_text(encoding="utf-8")
    for source, authority in SOURCE_AUTHORITY.items():
        assert f"WHEN '{source}' THEN {authority}" in sql
    assert "ADD COLUMN IF NOT EXISTS last_source" in sql  # idempotente, inclusive em banco novo
    assert re.search(r"event_time DESC,\s+produced_at DESC,\s+seq\s", sql)
    assert "last_source" in sql_tables()["rt.projects_latest"]


def test_migracao_do_setor_canonico_usa_a_mesma_troca_do_realtime_sink():
    import sys

    sys.path.insert(0, str(ROOT / "ingestion"))
    from worldbank_client import canonical_sector

    path = ROOT / "postgres" / "migrations" / "002_projects_latest_setor_canonico.sql"
    sql = path.read_text(encoding="utf-8")
    assert "replace(major_sector, '; ', ', ')" in sql
    assert canonical_sector("Industry; Trade and Services") == "Industry, Trade and Services"
    assert "project_events" not in sql.split("BEGIN;")[1]  # o payload dos eventos nunca é alterado
