"""Gold, serving e qualidade com Spark local (marcador spark)."""

import re
from datetime import date
from pathlib import Path

import pytest
import wb_transforms as wt
from events import make_event

pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
AT = "2026-10-01T06:10:00+00:00"


def rows(df, *cols):
    return sorted(tuple(r) for r in df.select(*cols).collect())


def ind(iso3, code, year, value, source="worldbank-api"):
    return {
        "country_iso3": iso3,
        "country_name": iso3,
        "indicator_code": code,
        "indicator_name": f"nome de {code}",
        "year": year,
        "value": value,
        "unit": None,
        "obs_status": None,
        "source": source,
        "extracted_at": AT,
    }


def country(iso3, iso2, region="Sub-Saharan Africa"):
    return {
        "country_iso3": iso3,
        "country_iso2": iso2,
        "country_name": f"País {iso3}",
        "region": region,
        "income_level": "Aggregates" if region == "Aggregates" else "Low income",
        "is_aggregate": region == "Aggregates",
        "extracted_at": AT,
    }


INDICATOR_ROWS = [
    ind("BRA", "SP.POP.TOTL", 2017, 2e8),
    ind("BRA", "NY.GDP.MKTP.CD", 2017, 2e12),
    ind("BRA", "SP.RUR.TOTL.ZS", 2017, 13.5),
    ind("BRA", "EG.ELC.ACCS.ZS", 2017, 100.0),
    ind("KEN", "SP.POP.TOTL", 2016, 5e7),  # sem PIB: per capita nulo
    ind("WLD", "SP.POP.TOTL", 2017, 7.5e9),  # agregado continua nos fatos (o Power BI filtra pela view)
    ind("BRA", "SP.DYN.LE00.IN", 2017, 75.0),  # fora de INDICATORS: sem unidade, não entra no gold
    ind("XXX", "SP.POP.TOTL", 2017, 1.0),  # sem metadados: entra em dim_country pelo fallback (região nula)
]
COUNTRY_ROWS = [
    country("BRA", "BR", "Latin America & Caribbean"),
    country("KEN", "KE"),
    country("WLD", "1W", "Aggregates"),
    {**country("X1", "X1"), "country_iso3": "EUROPE"},  # código fora do padrão de 3 caracteres
]


@pytest.fixture
def silver(bronze):
    ind_df = wt.silver_indicators(bronze(INDICATOR_ROWS))
    countries = wt.silver_countries(bronze(COUNTRY_ROWS), ind_df)
    return ind_df, countries


def payload(pid, iso2, approval, amount, name=None, **extra):
    return {
        "project_id": pid,
        "project_name": f"Projeto {pid}",
        "status": "Active",
        "country_name": name,
        "country_iso2": iso2,
        "region_name": "Africa",
        "board_approval_date": approval,
        "closing_date": None,
        "total_commitment_usd": amount,
        "ibrd_commitment_usd": 0.0,
        "ida_commitment_usd": amount,
        "grant_amount_usd": None,
        "lending_instrument": "IPF",
        "major_sector": "Energy",
        "borrower": None,
        "source": "kaggle-replay",
        **extra,
    }


PROJECT_PAYLOADS = [
    payload("P100001", "BR", "2017-03-01", 100e6),
    payload("P100002", "BR", "2017-09-01", 50e6),
    payload("P100003", "KE", "2015-01-01", 10e6),
    payload("P100004", "3A", "2017-05-01", 70e6, name="Western and Central Africa"),  # regional
    payload("P100005", "1W", "2018-01-01", 1e6, name="World"),  # ISO2 de agregado
]


@pytest.fixture
def silver_projects(bronze, silver):
    _, countries = silver
    lines = [
        {**make_event("PROJECT_APPROVED", p, date.fromisoformat(p["board_approval_date"])), "kafka_offset": i}
        for i, p in enumerate(PROJECT_PAYLOADS)
    ]
    return wt.silver_projects(wt.silver_project_events(bronze(lines)), countries)


@pytest.fixture
def gold(silver, silver_projects):
    ind_df, countries = silver
    tables = wt.gold_indicator_tables(ind_df, countries)
    tables["fact_project"] = wt.gold_project_table(silver_projects, tables["dim_country"])
    tables.update(wt.gold_serving_tables(tables["fact_indicator"], tables["fact_project"]))
    return tables


# ---------------------------------------------------------------- dimensões e fatos de indicadores


def test_dim_country_so_com_codigo_de_3_caracteres(gold):
    # EUROPE (fora do padrão) não entra; XXX só aparece nos indicadores e entra com região nula
    assert rows(gold["dim_country"], "country_code", "region", "is_aggregate") == [
        ("BRA", "Latin America & Caribbean", False),
        ("KEN", "Sub-Saharan Africa", False),
        ("WLD", "Aggregates", True),
        ("XXX", None, False),
    ]


def test_dim_indicator_com_unidade_de_indicators(gold):
    assert rows(gold["dim_indicator"], "indicator_code", "unit") == sorted(
        (code, unit) for code, (_, unit) in wt.INDICATORS.items()
    )


def test_fact_indicator_so_indicadores_de_indicators_e_todo_pais_com_dimensao(gold):
    df = gold["fact_indicator"]
    assert df.filter("indicator_code = 'SP.DYN.LE00.IN'").count() == 0
    assert df.count() == 7
    orphans = df.join(gold["dim_country"], "country_code", "left_anti").count()
    assert orphans == 0  # FK obrigatória do DW
    assert dict(df.dtypes)["year"] == "smallint"


# ---------------------------------------------------------------- fact_project


def test_fact_project_fk_de_pais_valida_ou_nula(gold):
    assert rows(gold["fact_project"], "project_id", "country_code") == [
        ("P100001", "BRA"),
        ("P100002", "BRA"),
        ("P100003", "KEN"),
        ("P100004", None),
        ("P100005", None),
    ]


def test_fact_project_valores_em_dolares_e_negativo_vira_nulo(bronze, silver):
    _, countries = silver
    ev = make_event("PROJECT_APPROVED", payload("P100009", "KE", "2017-01-01", 5e6), date(2017, 1, 1))
    # Linha da landing adulterada: valor negativo nunca deveria chegar, mas o gold não confia
    line = {**ev, "payload": {**ev["payload"], "grant_amount_usd": -1.0}}
    sp = wt.silver_projects(wt.silver_project_events(bronze([line])), countries)
    fp = wt.gold_project_table(sp, wt.gold_indicator_tables(*silver)["dim_country"])
    assert rows(fp, "total_amount_usd", "ida_amount_usd", "grant_amount_usd") == [(5e6, 5e6, None)]


# ---------------------------------------------------------------- serving


def test_pib_per_capita_calculado_a_mao(gold):
    agg = {(r["country_code"], r["year"]): r for r in gold["agg_country_year"].collect()}
    bra = agg[("BRA", 2017)]
    assert bra["gdp_per_capita_usd"] == pytest.approx(2e12 / 2e8) == 10_000.0
    assert (bra["population"], bra["rural_population_pct"], bra["electricity_access_pct"]) == (
        2e8,
        13.5,
        100.0,
    )
    assert agg[("KEN", 2016)]["gdp_per_capita_usd"] is None  # sem PIB


def test_agg_soma_projetos_por_pais_e_ano_de_aprovacao(gold):
    agg = {(r["country_code"], r["year"]): r for r in gold["agg_country_year"].collect()}
    assert (agg[("BRA", 2017)]["projects_approved"], agg[("BRA", 2017)]["projects_total_amount_usd"]) == (
        2,
        150e6,
    )
    assert agg[("KEN", 2015)]["projects_approved"] == 1 and agg[("KEN", 2015)]["population"] is None
    assert agg[("KEN", 2016)]["projects_approved"] == 0  # há dados de projetos, nenhum nesse país × ano
    assert ("WLD", 2018) not in agg  # projeto sem país não entra no agg


def test_dim_year_cobre_anos_dos_fatos(gold):
    assert rows(gold["dim_year"], "year", "decade", "year_date") == [
        (2015, "2010s", date(2015, 1, 1)),
        (2016, "2010s", date(2016, 1, 1)),
        (2017, "2010s", date(2017, 1, 1)),
        (2018, "2010s", date(2018, 1, 1)),
    ]


def test_so_indicadores_deixa_colunas_de_projetos_nulas(silver):
    tables = wt.gold_indicator_tables(*silver)
    serving = wt.gold_serving_tables(tables["fact_indicator"], None)
    r = serving["agg_country_year"].filter("country_code = 'BRA'").collect()[0]
    assert r["population"] == 2e8 and r["projects_approved"] is None


def test_so_projetos_e_check_quality_nao_falha(bronze, silver_projects):
    # Job de projetos sem nenhum indicador carregado: fact_indicator vazio não pode derrubar o job
    empty_ind = wt.silver_indicators(bronze([]))
    countries = wt.silver_countries(bronze(COUNTRY_ROWS), empty_ind)
    tables = wt.gold_indicator_tables(empty_ind, countries)
    tables["fact_project"] = wt.gold_project_table(silver_projects, tables["dim_country"])
    tables.update(wt.gold_serving_tables(None, tables["fact_project"]))
    assert tables["fact_indicator"].count() == 0
    counts = wt.check_quality(tables, expected=wt.JOB_TABLES["worldbank_projects"])
    assert counts["fact_project"] == 5 and counts["fact_indicator"] == 0
    assert rows(tables["agg_country_year"], "country_code", "year", "projects_approved", "population") == [
        ("BRA", 2017, 2, None),
        ("KEN", 2015, 1, None),
    ]


def test_serving_sem_nenhum_fato_e_erro():
    with pytest.raises(ValueError, match="pelo menos um"):
        wt.gold_serving_tables(None, None)


# ---------------------------------------------------------------- qualidade


def test_check_quality_passa_no_gold_completo(gold):
    counts = wt.check_quality(gold, expected=wt.JOB_TABLES["worldbank_indicators"])
    assert counts["fact_indicator"] == 7 and counts["dim_country"] == 4


def test_check_quality_aponta_pk_duplicada_nula_e_tabela_vazia(gold):
    bad = dict(gold)
    bad["dim_country"] = gold["dim_country"].unionByName(gold["dim_country"].limit(1))
    bad["fact_project"] = gold["fact_project"].withColumn("project_id", wt.F.lit(None).cast("string"))
    bad["dim_indicator"] = gold["dim_indicator"].limit(0)
    with pytest.raises(wt.QualityError) as exc:
        wt.check_quality(bad, expected=["dim_indicator"])
    msg = str(exc.value)
    assert "dim_country: 1 chave(s) duplicada(s)" in msg
    assert "fact_project: 5 linha(s) com chave nula" in msg
    assert "dim_indicator: vazia" in msg


def test_check_quality_recusa_tabela_sem_chave_conhecida(gold):
    with pytest.raises(wt.QualityError, match="sem chave primária"):
        wt.check_quality({"tabela_x": gold["dim_year"]}, expected=[])


# ---------------------------------------------------------------- contrato com o Postgres


def dw_columns() -> dict[str, list[str]]:
    sql = (ROOT / "postgres" / "init" / "01_init.sql").read_text(encoding="utf-8")
    tables = {}
    for name, body in re.findall(r"CREATE TABLE dw\.(\w+) \((.*?)\n\);", sql, flags=re.S):
        cols = [m.group(1) for line in body.splitlines() if (m := re.match(r"\s+([a-z_][a-z0-9_]*)\s", line))]
        tables[name] = [c for c in cols if c not in {"primary", "unique", "check", "foreign"}]
    return tables


def test_colunas_do_gold_iguais_as_do_dw(gold):
    expected = dw_columns()
    assert set(gold) == set(wt.PRIMARY_KEYS) == set(expected) - {"load_audit"}
    for name, df in gold.items():
        assert df.columns == expected[name], name  # mesma ordem: o COPY do sync_gold usa a ordem do gold
