"""Casos de borda da revisão de dados: dados fora do contrato não podem derrubar o job nem o sync.

A fixture `spark` roda com ANSI ligado, como o serverless.
"""

import json
from datetime import UTC, date, datetime

import pytest
import wb_transforms as wt
from events import make_event

pytestmark = pytest.mark.spark

AT = "2026-10-01T06:10:00+00:00"


def rows(df, *cols):
    return sorted(tuple(r) for r in df.select(*cols).collect())


def ind(iso3="BRA", value=1.0, source="worldbank-api", at=AT, year=2017, code="SP.POP.TOTL"):
    return {
        "country_iso3": iso3,
        "country_name": "Brazil",
        "indicator_code": code,
        "indicator_name": "Population, total",
        "year": year,
        "value": value,
        "unit": None,
        "obs_status": None,
        "source": source,
        "extracted_at": at,
    }


# ---------------------------------------------------------------- silver_indicators


def test_codigo_de_pais_normalizado_antes_do_dedup(bronze):
    # " bra" (Kaggle) e "BRA" (API) são o mesmo país: uma linha só, e a API vence
    df = wt.silver_indicators(bronze([ind(" bra", 1.0, "kaggle")], [ind("BRA", 2.0)]))
    assert rows(df, "country_code", "value", "source") == [("BRA", 2.0, "worldbank-api")]


@pytest.mark.parametrize("source", [None, "outra-fonte", " kaggle"])
def test_fonte_fora_do_check_do_dw_e_descartada(bronze, source):
    assert wt.silver_indicators(bronze([ind(source=source)])).count() == 0


def test_nan_infinito_e_ano_fora_da_faixa_sao_descartados(bronze):
    lines = [
        json.dumps(ind()).replace('"value": 1.0', '"value": NaN'),
        json.dumps(ind()).replace('"value": 1.0', '"value": Infinity'),
        ind(year=40000),  # estouraria o smallint
        ind(year=1850),
        ind(year=2017),
    ]
    assert rows(wt.silver_indicators(bronze(lines)), "year") == [(2017,)]


def test_extracted_at_invalido_nao_derruba_com_ansi(bronze):
    df = wt.silver_indicators(bronze([ind(at="30/06/2023 12:00:00 AM")]))
    assert rows(df, "value", "extracted_at") == [(1.0, None)]


# ---------------------------------------------------------------- eventos e projetos


def payload(**extra):
    base = {
        "project_id": "P100001",
        "status": "Active",
        "country_iso2": "KE",
        "country_name": "Kenya",
        "board_approval_date": "2015-03-21",
        "closing_date": None,
        "total_commitment_usd": 1e6,
        "source": "kaggle-replay",
    }
    return {**base, **extra}


def test_tipo_de_evento_desconhecido_e_descartado(bronze):
    ok = make_event("PROJECT_APPROVED", payload(), date(2015, 3, 21))
    bad = {
        **make_event("PROJECT_APPROVED", payload(status="x"), date(2015, 3, 21)),
        "event_type": "PROJECT_DELETED",
    }
    assert rows(wt.silver_project_events(bronze([ok, bad])), "event_type") == [("PROJECT_APPROVED",)]


def test_datas_invalidas_viram_nulas_com_ansi_e_ignoram_o_fuso(bronze):
    countries = wt.silver_countries(bronze([]), wt.silver_indicators(bronze([])))
    ev = make_event(
        "PROJECT_APPROVED",
        payload(board_approval_date="2018-06-28T00:00:00Z", closing_date="6/30/2023 12:00:00 AM"),
        date(2018, 6, 28),
    )
    r = wt.silver_projects(wt.silver_project_events(bronze([ev])), countries).collect()[0]
    assert r["board_approval_date"] == date(2018, 6, 28)  # nunca 27/06, qualquer que seja o fuso
    assert r["closing_date"] is None and r["approval_year"] == 2018


def test_reentrega_fica_com_a_primeira_chegada(bronze):
    ev = make_event("PROJECT_APPROVED", payload(), date(2015, 3, 21))
    first = {**ev, "kafka_partition": 1, "kafka_offset": 90}
    # Tópico recriado: a mesma mensagem volta com offset menor, mas chegou depois
    later = {**ev, "kafka_partition": 1, "kafka_offset": 3}
    df = wt.silver_project_events(
        bronze([first], [later]).withColumn(
            "ingested_at",
            wt.F.when(wt.F.col("source_file").contains("part-000"), wt.F.lit("2026-10-01"))
            .otherwise(wt.F.lit("2026-10-02"))
            .cast("timestamp"),
        )
    )
    assert rows(df, "kafka_offset") == [(90,)]


def test_empate_total_fica_com_o_primeiro_aplicado_como_no_rt(bronze):
    t = datetime(2026, 10, 28, 10, 0, tzinfo=UTC)
    a = {**make_event("PROJECT_UPDATED", payload(status="A"), t, produced_at=t), "kafka_offset": 5}
    b = {**make_event("PROJECT_UPDATED", payload(status="B"), t, produced_at=t), "kafka_offset": 9}
    countries = wt.silver_countries(bronze([]), wt.silver_indicators(bronze([])))
    df = wt.silver_projects(wt.silver_project_events(bronze([b], [a])), countries)
    assert rows(df, "status") == [("A",)]


# ---------------------------------------------------------------- só projetos sem países


def test_so_projetos_sem_nenhum_pais_conhecido_nao_reprova(bronze):
    # Ambiente novo: replay rodando antes de qualquer carga de metadados de países
    empty_ind = wt.silver_indicators(bronze([]))
    countries = wt.silver_countries(bronze([]), empty_ind)
    tables = wt.gold_indicator_tables(empty_ind, countries)
    ev = make_event("PROJECT_APPROVED", payload(), date(2015, 3, 21))
    sp = wt.silver_projects(wt.silver_project_events(bronze([ev])), countries)
    tables["fact_project"] = wt.gold_project_table(sp, tables["dim_country"])
    tables.update(wt.gold_serving_tables(None, tables["fact_project"]))
    expected = wt.expected_tables("worldbank_projects", tables)
    assert expected == ["dim_year", "fact_project"]
    counts = wt.check_quality(tables, expected)
    assert counts["fact_project"] == 1 and counts["agg_country_year"] == 0


def test_com_pais_resolvido_o_agg_volta_a_ser_exigido(bronze):
    countries = wt.silver_countries(
        bronze(
            [
                {
                    "country_iso3": "KEN",
                    "country_iso2": "KE",
                    "country_name": "Kenya",
                    "region": "SSA",
                    "is_aggregate": False,
                    "extracted_at": AT,
                }
            ]
        ),
        wt.silver_indicators(bronze([])),
    )
    tables = wt.gold_indicator_tables(wt.silver_indicators(bronze([])), countries)
    ev = make_event("PROJECT_APPROVED", payload(), date(2015, 3, 21))
    sp = wt.silver_projects(wt.silver_project_events(bronze([ev])), countries)
    tables["fact_project"] = wt.gold_project_table(sp, tables["dim_country"])
    assert "agg_country_year" in wt.expected_tables("worldbank_projects", tables)


# ---------------------------------------------------------------- check_quality espelha o DDL


def test_check_quality_aponta_violacao_de_ddl_e_fk_orfa(bronze):
    ind_df = wt.silver_indicators(bronze([ind()]))
    countries = wt.silver_countries(bronze([]), ind_df)
    tables = wt.gold_indicator_tables(ind_df, countries)
    tables.update(wt.gold_serving_tables(tables["fact_indicator"], None))
    bad = dict(tables)
    bad["fact_indicator"] = tables["fact_indicator"].withColumn("source", wt.F.lit("outra"))
    bad["dim_year"] = tables["dim_year"].limit(0)
    with pytest.raises(wt.QualityError) as exc:
        wt.check_quality(bad, expected=[])
    msg = str(exc.value)
    assert "fact_indicator: 1 linha(s) violam `source IN ('worldbank-api', 'kaggle')`" in msg
    assert "fact_indicator.year: 1 linha(s) sem dim_year.year" in msg
    assert "agg_country_year.year: 1 linha(s) sem dim_year.year" in msg
