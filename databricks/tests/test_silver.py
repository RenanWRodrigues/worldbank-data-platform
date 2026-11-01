"""Silver do lakehouse com Spark local (marcador spark): bronze montado de registros e eventos de exemplo."""

from datetime import UTC, date, datetime

import pytest
import wb_transforms as wt
from events import make_event

pytestmark = pytest.mark.spark

API_T0 = "2026-09-01T06:10:00+00:00"
API_T1 = "2026-10-01T06:10:00+00:00"
API_T2 = "2026-10-08T06:10:00+00:00"
KAGGLE_T = "2026-10-20T00:00:00+00:00"  # bootstrap depois da API: mesmo assim a API vence


def rows(df, *cols):
    return sorted(tuple(r) for r in df.select(*cols).collect())


def indicator(
    iso3="BRA",
    code="SP.POP.TOTL",
    year=2017,
    value=1.0,
    source="worldbank-api",
    at=API_T1,
    **extra,
):
    return {
        "country_iso3": iso3,
        "country_name": {
            "BRA": "Brazil",
            "KEN": "Kenya",
            "WLD": "World",
            "XKG": "Só no Kaggle",
        }.get(iso3, iso3),
        "indicator_code": code,
        "indicator_name": "Population, total",
        "year": year,
        "value": value,
        "unit": None,
        "obs_status": None,
        "source": source,
        "extracted_at": at,
        **extra,
    }


def country(iso3, iso2, name, region, income="Upper middle income", at=API_T1):
    return {
        "country_iso3": iso3,
        "country_iso2": iso2,
        "country_name": name,
        "region": region,
        "income_level": income,
        "is_aggregate": region == "Aggregates",
        "capital_city": None,
        "longitude": None,
        "latitude": None,
        "extracted_at": at,
    }


COUNTRIES = [
    country("BRA", "BR", "Brazil", "Latin America & Caribbean"),
    country("KEN", "KE", "Kenya", "Sub-Saharan Africa", "Lower middle income"),
    country("ZAF", "ZA", "South Africa", "Sub-Saharan Africa"),
    country("KOR", "KR", "Korea, Rep.", "East Asia & Pacific", "High income"),
    country("WLD", "1W", "World", "Aggregates", "Aggregates"),
]


# ---------------------------------------------------------------- silver_indicators


def test_api_vence_kaggle_mesmo_com_kaggle_mais_recente(bronze):
    df = wt.silver_indicators(
        bronze(
            [indicator(value=200.0, source="worldbank-api", at=API_T1)],
            [indicator(value=100.0, source="kaggle", at=KAGGLE_T)],
        )
    )
    assert rows(df, "country_code", "year", "value", "source") == [("BRA", 2017, 200.0, "worldbank-api")]


def test_entre_extracoes_da_api_vence_a_mais_recente(bronze):
    df = wt.silver_indicators(bronze([indicator(value=1.0, at=API_T1)], [indicator(value=2.0, at=API_T2)]))
    assert rows(df, "value") == [(2.0,)]


def test_valor_nulo_da_api_nao_apaga_o_valor_do_kaggle(bronze):
    df = wt.silver_indicators(
        bronze(
            [indicator(value=None), indicator(iso3="KEN", value=None)],
            [indicator(value=7.0, source="kaggle", at=KAGGLE_T)],
        )
    )
    # BRA: nulo da API descartado, fica o Kaggle; KEN: só nulo → nenhuma linha
    assert rows(df, "country_code", "value", "source") == [("BRA", 7.0, "kaggle")]


def test_linhas_invalidas_sao_descartadas(bronze):
    df = wt.silver_indicators(
        bronze(
            [
                '{"quebrado"',
                indicator(iso3=None),
                indicator(year=None),
                indicator(code=None),
                indicator(),
            ]
        )
    )
    assert df.count() == 1


def test_tipos_do_silver_de_indicadores(bronze):
    df = wt.silver_indicators(bronze([indicator()]))
    types = dict(df.dtypes)
    assert types["year"] == "smallint" and types["value"] == "double" and types["extracted_at"] == "timestamp"
    assert set(df.columns) == {
        "country_code",
        "country_name",
        "indicator_code",
        "indicator_name",
        "year",
        "value",
        "unit",
        "obs_status",
        "source",
        "extracted_at",
    }


# ---------------------------------------------------------------- silver_countries


def test_paises_da_api_com_agregados_e_ultima_extracao(bronze):
    antigo = country("BRA", "BR", "Brasil (antigo)", "Latin America & Caribbean", at=API_T0)
    df = wt.silver_countries(
        bronze([antigo], COUNTRIES[:1] + COUNTRIES[4:]),
        wt.silver_indicators(bronze([])),
    )
    assert rows(df, "country_code", "country_name", "is_aggregate") == [
        ("BRA", "Brazil", False),
        ("WLD", "World", True),
    ]


def test_pais_so_nos_indicadores_entra_com_regiao_nula(bronze):
    ind = wt.silver_indicators(bronze([indicator(iso3="XKG", source="kaggle", at=KAGGLE_T), indicator()]))
    df = wt.silver_countries(bronze(COUNTRIES[:1]), ind)
    assert rows(df, "country_code", "country_iso2", "region", "is_aggregate") == [
        ("BRA", "BR", "Latin America & Caribbean", False),
        ("XKG", None, None, False),
    ]


def test_agregado_so_do_kaggle_nao_vira_pais(bronze):
    # FCS ("Fragile and conflict affected situations") não está no /country da API; XKX (Kosovo) é país
    ind = wt.silver_indicators(
        bronze([indicator(iso3=code, source="kaggle", at=KAGGLE_T) for code in ("FCS", "XKX")])
    )
    df = wt.silver_countries(bronze(COUNTRIES[:1]), ind)
    assert rows(df, "country_code", "is_aggregate") == [("BRA", False), ("FCS", True), ("XKX", False)]


# ---------------------------------------------------------------- eventos e projetos


def payload(pid="P100001", status="Active", country_name="Republic of Kenya", iso2="KE", **extra):
    return {
        "project_id": pid,
        "project_name": "Solar",
        "status": status,
        "country_name": country_name,
        "country_iso2": iso2,
        "region_name": "Eastern and Southern Africa",
        "board_approval_date": "2015-03-21",
        "closing_date": "2020-12-31",
        "total_commitment_usd": 1e8,
        "ibrd_commitment_usd": 0.0,
        "ida_commitment_usd": 1e8,
        "grant_amount_usd": None,
        "lending_instrument": "IPF",
        "major_sector": "Energy",
        "borrower": None,
        "source": "kaggle-replay",
        **extra,
    }


T1 = datetime(2026, 10, 28, 10, 0, tzinfo=UTC)
T2 = datetime(2026, 10, 28, 10, 5, tzinfo=UTC)


def landing(event, offset=0, partition=0):
    """Linha da landing como o lake-sink grava: evento + posição no Kafka."""
    return {**event, "kafka_partition": partition, "kafka_offset": offset}


def test_reentrega_duplicada_vira_um_evento(bronze):
    ev = make_event("PROJECT_APPROVED", payload(), date(2015, 3, 21), produced_at=T1)
    df = wt.silver_project_events(bronze([landing(ev, 10)], [landing(ev, 10)], [landing(ev, 55)]))
    assert rows(df, "event_id", "kafka_offset") == [(ev["event_id"], 10)]


def test_evento_invalido_ou_sem_id_e_descartado(bronze):
    ev = make_event("PROJECT_APPROVED", payload(), date(2015, 3, 21))
    df = wt.silver_project_events(bronze(["não é json", {"event_type": "PROJECT_APPROVED"}, landing(ev)]))
    assert df.count() == 1
    types = dict(df.dtypes)
    assert types["event_time"] == "timestamp" and types["kafka_offset"] == "bigint"


def countries_df(bronze):
    return wt.silver_countries(bronze(COUNTRIES), wt.silver_indicators(bronze([])))


def projects(bronze, *events):
    files = [[landing(e, i)] for i, e in enumerate(events)]
    return wt.silver_projects(wt.silver_project_events(bronze(*files)), countries_df(bronze))


def test_projeto_aprovado_e_depois_encerrado(bronze):
    approved = make_event("PROJECT_APPROVED", payload(), date(2015, 3, 21), produced_at=T1)
    closed = make_event("PROJECT_CLOSED", payload(status="Closed"), date(2020, 12, 31), produced_at=T1)
    # O CLOSED chega primeiro (outro arquivo/partição): a ordem é pelo event_time, não pela chegada
    df = projects(bronze, closed, approved)
    assert rows(df, "project_id", "status", "last_event_type", "last_event_id") == [
        ("P100001", "Closed", "PROJECT_CLOSED", closed["event_id"])
    ]


def test_empate_no_mesmo_dia_desempata_por_produced_at(bronze):
    approved = make_event("PROJECT_APPROVED", payload(), date(2015, 3, 21), produced_at=T2)
    updated = make_event(
        "PROJECT_UPDATED",
        payload(project_name="Novo"),
        date(2015, 3, 21),
        produced_at=T1,
    )
    df = projects(bronze, approved, updated)
    assert rows(df, "last_event_type", "project_name") == [("PROJECT_APPROVED", "Solar")]


def test_evento_da_api_vence_encerramento_previsto_do_replay(bronze):
    # ADR 0009: projeto prorrogado — o CLOSED do replay (data prevista, mais nova) perde para a API
    replay_closed = make_event(
        "PROJECT_CLOSED",
        payload(status="Closed", closing_date="2023-12-31"),
        date(2023, 12, 31),
        produced_at=T1,
    )
    api_approved = make_event(
        "PROJECT_APPROVED",
        payload(source="worldbank-projects-api", closing_date="2025-12-31"),
        date(2015, 3, 21),
        produced_at=T1,
    )
    df = projects(bronze, replay_closed, api_approved)
    assert rows(df, "status", "last_event_type", "closing_date") == [
        ("Active", "PROJECT_APPROVED", date(2025, 12, 31))
    ]


def test_dentro_da_api_vale_o_event_time(bronze):
    api = payload(source="worldbank-projects-api")
    approved = make_event("PROJECT_APPROVED", api, date(2015, 3, 21), produced_at=T1)
    closed = make_event("PROJECT_CLOSED", {**api, "status": "Closed"}, date(2020, 12, 31), produced_at=T1)
    df = projects(bronze, closed, approved)
    assert rows(df, "status", "last_event_type") == [("Closed", "PROJECT_CLOSED")]


def test_setor_do_kaggle_fica_com_o_nome_canonico_da_api(bronze):
    kaggle = make_event("PROJECT_APPROVED", payload(major_sector="Water; Sanitation and Waste Management"),
                        date(2015, 3, 21), produced_at=T1)  # fmt: skip
    api = make_event(
        "PROJECT_APPROVED",
        payload(
            pid="P100002",
            source="worldbank-projects-api",
            major_sector="Water, Sanitation and Waste Management",
        ),
        date(2015, 3, 21),
        produced_at=T1,
    )
    df = projects(bronze, kaggle, api)
    assert rows(df, "project_id", "major_sector") == [
        ("P100001", "Water, Sanitation and Waste Management"),
        ("P100002", "Water, Sanitation and Waste Management"),
    ]


def test_colunas_e_tipos_de_silver_projects(bronze):
    df = projects(bronze, make_event("PROJECT_APPROVED", payload(), date(2015, 3, 21)))
    r = df.collect()[0]
    assert r["country_code"] == "KEN" and r["country_name_raw"] == "Republic of Kenya"
    assert r["approval_year"] == 2015 and r["board_approval_date"] == date(2015, 3, 21)
    assert r["closing_date"] == date(2020, 12, 31)
    assert r["total_commitment_usd"] == 1e8 and r["grant_amount_usd"] is None
    assert dict(df.dtypes)["approval_year"] == "smallint"


@pytest.mark.parametrize(
    ("iso2", "name", "expected"),
    [
        ("KE", "Republic of Kenya", "KEN"),  # ISO2 válido
        ("ke", None, "KEN"),  # ISO2 em minúsculas
        ("3A", "Western and Central Africa", None),  # código regional não é país
        ("1W", "World", None),  # ISO2 de agregado não vale
        (None, "Africa", None),  # não pode virar South Africa
        (None, "Republic of Korea", "KOR"),  # sem ISO2: nome por override
        ("ZZ", "Republic of Kenya", "KEN"),  # ISO2 desconhecido: cai para o nome
        (None, "Norway", None),  # país real, mas ausente de silver_countries
    ],
)
def test_resolucao_de_pais(bronze, iso2, name, expected):
    ev = make_event("PROJECT_APPROVED", payload(iso2=iso2, country_name=name), date(2015, 3, 21))
    assert rows(projects(bronze, ev), "country_code") == [(expected,)]


def test_varios_projetos_resolvem_paises_diferentes(bronze):
    evs = [
        make_event("PROJECT_APPROVED", payload("P100001", iso2="KE"), date(2015, 3, 21)),
        make_event(
            "PROJECT_APPROVED",
            payload(
                "P100002",
                iso2=None,
                country_name="Brazil",
                board_approval_date="2016-01-01",
            ),
            date(2016, 1, 1),
        ),
        make_event(
            "PROJECT_APPROVED",
            payload(
                "P100003",
                iso2="3A",
                country_name="Africa",
                board_approval_date="2017-01-01",
            ),
            date(2017, 1, 1),
        ),
    ]
    assert rows(projects(bronze, *evs), "project_id", "country_code", "approval_year") == [
        ("P100001", "KEN", 2015),
        ("P100002", "BRA", 2016),
        ("P100003", None, 2017),
    ]
