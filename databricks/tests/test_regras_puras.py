"""Regras do lakehouse que não precisam de Spark (rodam em `make test`)."""

import pytest
import wb_transforms as wt
from worldbank_client import DEFAULT_INDICATORS


def test_indicators_e_fonte_unica_e_bate_com_o_extrator():
    # DEFAULT_INDICATORS (o que a DAG extrai) e INDICATORS (o que o lakehouse modela) precisam ser os mesmos
    assert set(wt.INDICATORS) == set(DEFAULT_INDICATORS)
    assert {unit for _, unit in wt.INDICATORS.values()} <= {"%", "US$", "count"}
    assert [col for col, _ in wt.INDICATORS.values()] == [
        "population",
        "gdp_usd",
        "rural_population_pct",
        "electricity_access_pct",
    ]


def test_autoridade_da_fonte_igual_no_silver_e_no_realtime_sink():
    # ADR 0009: rt.projects_latest e silver_projects escolhem o estado atual com a mesma ordem
    from events import SOURCE_AUTHORITY

    assert wt.EVENT_SOURCE_AUTHORITY == SOURCE_AUTHORITY
    assert wt.EVENT_SOURCE_AUTHORITY["worldbank-projects-api"] > wt.EVENT_SOURCE_AUTHORITY["kaggle-replay"]


KNOWN = {"KEN", "BRA", "ZAF", "KOR", "EGY", "IND", "XKX", "PSE", "CIV", "VNM", "TUR", "COD"}
# Países do projects_data.csv real que o pycountry não casa (nomes antigos ou truncados em 40 caracteres)
KNOWN_KAGGLE = {
    "AFG", "RWA", "TUR", "LKA", "NER", "MKD", "TGO", "URY", "PNG", "SOM",
    "STP", "MMR", "GUY", "ERI", "SWZ", "VEN", "KNA", "LBY",
}  # fmt: skip


@pytest.mark.parametrize(
    ("name", "code"),
    [
        ("Republic of Kenya", "KEN"),  # nome oficial no pycountry
        ("Kenya", "KEN"),
        ("federative republic of brazil", "BRA"),  # lookup ignora maiúsculas
        ("Republic of Korea", "KOR"),  # override
        ("Kosovo", "XKX"),  # código do World Bank, não ISO
        ("West Bank and Gaza", "PSE"),
        ("Cote d'Ivoire", "CIV"),
        ("Vietnam", "VNM"),
        ("Turkiye", "TUR"),
        ("Congo, Democratic Republic of", "COD"),
    ],
)
def test_nome_de_pais_resolvido(name, code):
    assert wt.resolve_country_name(name, KNOWN) == code


@pytest.mark.parametrize(
    ("name", "code"),
    [
        ("Islamic  Republic of Afghanistan", "AFG"),  # espaço duplo no CSV: normalizado antes do lookup
        ("Republic of Rwanda", "RWA"),
        ("Republic of Turkey", "TUR"),
        ("Democratic Socialist Republic of Sri Lan", "LKA"),  # truncado em 40 caracteres na origem
        ("Republic of Niger", "NER"),
        ("Macedonia", "MKD"),
        ("Republic of Togo", "TGO"),
        ("Oriental Republic of Uruguay", "URY"),
        ("The Independent State of Papua New Guine", "PNG"),
        ("Somali Democratic Republic", "SOM"),
        ("Democratic Republic of Sao Tome and Prin", "STP"),
        ("Republic of the Union of Myanmar", "MMR"),
        ("Co-operative Republic of Guyana", "GUY"),
        ("State of Eritrea", "ERI"),
        ("Kingdom of Swaziland", "SWZ"),
        ("Republica Bolivariana de Venezuela", "VEN"),
        ("St. Kitts and Nevis", "KNA"),
        ("Socialist People's Libyan Arab Jamahiriy", "LBY"),
    ],
)
def test_nome_do_kaggle_real_resolvido(name, code):
    assert wt.resolve_country_name(name, KNOWN_KAGGLE) == code


@pytest.mark.parametrize(
    "name",
    [
        "Africa",  # busca aproximada viraria South Africa
        "Western and Central Africa",
        "World",
        "Eastern and Southern Africa",
        "South Asia",
        "",
        None,
    ],
)
def test_regiao_ou_vazio_nao_vira_pais(name):
    assert wt.resolve_country_name(name, KNOWN) is None


def test_pais_fora_de_dim_country_nao_e_aceito():
    # pycountry conhece a Noruega, mas sem NOR em silver_countries o FK do gold quebraria
    assert wt.resolve_country_name("Norway", KNOWN) is None


def test_codigo_iso_nao_e_aceito_como_nome():
    # pycountry.lookup também casa códigos ("KE", "KEN"); o campo é um nome
    assert wt.resolve_country_name("KE", KNOWN) is None
    assert wt.resolve_country_name("KEN", KNOWN) is None
