"""Cliente das APIs do World Bank com sessão HTTP falsa — nunca rede real (tests/fixtures/api/)."""

import pytest
import worldbank_client as wb

EXTRACTED_AT = "2026-10-28T06:10:00+00:00"


class FakeResponse:
    def __init__(self, status_code: int = 200, body=None, headers: dict | None = None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def json(self):
        return self._body


class FakeSession:
    """Devolve respostas em sequência e registra cada chamada (url, params)."""

    def __init__(self, *responses: FakeResponse):
        self.responses = list(responses)
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        return self.responses.pop(0)


class NoSleep(list):
    """Substitui time.sleep e guarda as esperas pedidas."""

    def __call__(self, seconds: float) -> None:
        self.append(seconds)


# ---------------------------------------------------------------- Indicators API


def test_fetch_indicator_segue_page_pages_com_per_page_1000(api_sample):
    session = FakeSession(
        FakeResponse(body=api_sample("indicators_page1.json")),
        FakeResponse(body=api_sample("indicators_page2.json")),
    )
    records = wb.fetch_indicator("SP.POP.TOTL", session=session, extracted_at=EXTRACTED_AT)

    assert [r["country_iso3"] for r in records] == ["BRA", "WLD", "KEN"]
    assert [c[1]["page"] for c in session.calls] == [1, 2]
    assert all(c[1]["per_page"] == 1000 for c in session.calls)
    assert session.calls[0][0].endswith("/country/all/indicator/SP.POP.TOTL")


def test_linha_de_indicador_normalizada(api_sample):
    row = api_sample("indicators_page1.json")[1][0]
    assert wb.normalize_indicator_row(row, EXTRACTED_AT) == {
        "country_iso3": "BRA",
        "country_name": "Brazil",
        "indicator_code": "SP.POP.TOTL",
        "indicator_name": "Population, total",
        "year": 2023,
        "value": 216422446.0,
        "unit": None,
        "obs_status": None,
        "source": "worldbank-api",
        "extracted_at": EXTRACTED_AT,
    }


def test_valor_nulo_da_api_vira_linha_com_value_none(api_sample):
    row = api_sample("indicators_page2.json")[1][0]
    assert wb.normalize_indicator_row(row, EXTRACTED_AT)["value"] is None


@pytest.mark.parametrize(("campo", "valor"), [("countryiso3code", ""), ("date", "2023Q1"), ("date", "")])
def test_linha_sem_iso3_ou_com_ano_invalido_e_descartada(api_sample, campo, valor):
    row = {**api_sample("indicators_page1.json")[1][0], campo: valor}
    assert wb.normalize_indicator_row(row, EXTRACTED_AT) is None


def test_erro_message_da_indicators_api_levanta_excecao(api_sample):
    with pytest.raises(wb.WorldBankAPIError, match="Invalid value"):
        wb.parse_indicator_page(api_sample("indicators_error.json"))


def test_pagina_sem_linhas_devolve_lista_vazia():
    assert wb.parse_indicator_page([{"page": 1, "pages": 0}, None]) == ([], 1, 0)


# ---------------------------------------------------------------- Países e agregados


def test_paises_normalizados_marcam_agregados(api_sample):
    session = FakeSession(FakeResponse(body=api_sample("countries.json")))
    countries = wb.fetch_countries(session=session, extracted_at=EXTRACTED_AT)

    bra, wld = countries
    assert bra == {
        "country_iso3": "BRA",
        "country_iso2": "BR",
        "country_name": "Brazil",
        "region": "Latin America & Caribbean",  # espaço final da API removido
        "income_level": "Upper middle income",
        "is_aggregate": False,
        "capital_city": "Brasilia",
        "longitude": -47.9292,
        "latitude": -15.7801,
        "extracted_at": EXTRACTED_AT,
    }
    assert wld["is_aggregate"] is True
    assert wld["capital_city"] is None and wld["longitude"] is None
    assert session.calls[0][1]["per_page"] == 1000


def test_cobertura_conta_paises_distintos_nao_agregados_com_valor(api_sample):
    countries = [wb.normalize_country(r, EXTRACTED_AT) for r in api_sample("countries.json")[1]]
    records = [
        {"country_iso3": "BRA", "value": 1.0},
        {"country_iso3": "BRA", "value": 2.0},  # mesmo país conta uma vez
        {"country_iso3": "WLD", "value": 3.0},  # agregado não conta
        {"country_iso3": "KEN", "value": None},  # valor nulo não conta
    ]
    assert wb.aggregate_codes(countries) == {"WLD"}
    assert wb.country_coverage(records, wb.aggregate_codes(countries)) == 1


# ---------------------------------------------------------------- Projects API


def test_projects_api_aceita_objeto_e_lista(api_sample):
    body = api_sample("projects_page1.json")
    como_objeto = wb.parse_projects_page(body)
    como_lista = wb.parse_projects_page({**body, "projects": list(body["projects"].values())})
    assert [p["id"] for p in como_objeto] == ["P123456", "P654321"]
    assert como_lista == como_objeto
    assert wb.parse_projects_page({"projects": {}}) == []


def test_fetch_projects_pagina_os_rows_ate_pagina_vazia(api_sample):
    # Sem `total` na resposta: pagina até a primeira página vazia
    page = {k: v for k, v in api_sample("projects_page1.json").items() if k != "total"}
    session = FakeSession(FakeResponse(body=page), FakeResponse(body=api_sample("projects_empty.json")))
    projects = wb.fetch_projects(session=session, start_date="2024-01-01", rows=2)

    assert [p["project_id"] for p in projects] == ["P123456", "P654321"]
    assert [c[1]["os"] for c in session.calls] == [0, 2]
    assert all(c[1]["rows"] == 2 and c[1]["strdate"] == "2024-01-01" for c in session.calls)
    assert all(p["source"] == "worldbank-projects-api" for p in projects)


def test_fetch_projects_para_no_total_mesmo_se_a_api_ignorar_os(api_sample):
    # Se a API repetisse a mesma página para qualquer `os`, parar só na página vazia nunca terminaria
    page = api_sample("projects_page1.json")  # total = 2
    session = FakeSession(*(FakeResponse(body=page) for _ in range(5)))
    projects = wb.fetch_projects(session=session, rows=1)

    assert [c[1]["os"] for c in session.calls] == [0, 1]
    assert len(projects) == 4  # repetidos: quem consome deduplica por project_id (collect_live_projects)


def test_fetch_projects_por_status_e_por_ids_com_campos_limitados(api_sample):
    session = FakeSession(
        FakeResponse(body=api_sample("projects_page1.json")),
        FakeResponse(body=api_sample("projects_empty.json")),
    )
    wb.fetch_projects(session=session, status="Active", ids=["P123456", "P654321"], rows=2)
    params = session.calls[0][1]
    assert params["status"] == "Active" and params["id"] == "P123456,P654321"
    assert "strdate" not in params
    assert set(params["fl"].split(",")) >= {
        "id",
        "status",
        "boardapprovaldate",
        "closingdate",
        "totalcommamt",
    }


def test_campos_pedidos_cobrem_tudo_que_a_normalizacao_le(api_sample):
    # Se normalize_project passar a ler um campo novo, ele precisa entrar em PROJECT_FIELDS
    raw = api_sample("projects_page1.json")["projects"]["P123456"]
    assert set(raw) <= set(wb.PROJECT_FIELDS)


def test_projeto_da_api_normalizado(api_sample):
    raw = api_sample("projects_page1.json")["projects"]["P123456"]
    assert wb.normalize_project(raw) == {
        "project_id": "P123456",
        "project_name": "Kenya Off-Grid Solar Access",
        "status": "Active",
        "country_name": "Kenya",
        "country_iso2": "KE",
        "region_name": "Eastern and Southern Africa",
        "board_approval_date": "2024-03-21",
        "closing_date": "2029-12-31",
        "total_commitment_usd": 750_000_000.0,
        "ibrd_commitment_usd": None,
        "ida_commitment_usd": None,
        "grant_amount_usd": None,
        "lending_instrument": "Investment Project Financing",
        "major_sector": "Energy and Extractives",
        "borrower": None,
        "source": "worldbank-projects-api",
    }


def test_curr_total_commitment_em_milhoes_e_iso2_regional(api_sample):
    raw = api_sample("projects_page1.json")["projects"]["P654321"]
    project = wb.normalize_project(raw)
    assert project["total_commitment_usd"] == 120_000_000.0
    assert project["country_iso2"] == "3A"  # copiado como veio; o silver decide que não é país
    assert project["closing_date"] is None


def test_projeto_no_formato_do_kaggle():
    # Colunas do projects_data.csv real: sem `countrycode`; `mjsector1` vazio e o setor em `mjsector` ("X;X")
    raw = {
        "id": "P160001",
        "countryname": "Republic of Kenya;Republic of Kenya",
        "totalamt": "6,060,000",
        "ibrdcommamt": "0",
        "idacommamt": "",
        "grantamt": "-5",
        "mjsector1": "",
        "sector1": "(Historic)Highways!$!100!$!TH",
        "mjsector": "Energy and Extractives;Energy and Extractives",
        "boardapprovaldate": "2012-02-15T00:00:00Z",
    }
    project = wb.normalize_project(raw, source="kaggle-replay")
    assert project["country_name"] == "Republic of Kenya"
    assert project["country_iso2"] is None  # o país do Kaggle sai do nome, no silver
    assert project["total_commitment_usd"] == 6_060_000.0
    assert project["ibrd_commitment_usd"] == 0.0
    assert project["ida_commitment_usd"] is None
    assert project["grant_amount_usd"] is None  # negativo → null
    assert project["major_sector"] == "Energy and Extractives"
    assert project["source"] == "kaggle-replay"


@pytest.mark.parametrize(
    ("raw", "esperado"),
    [
        # Kaggle real: nome com "; " dentro (não é separador) repetido com ";"
        (
            {"mjsector": "Agriculture; Fishing and Forestry;Agriculture; Fishing and Forestry"},
            "Agriculture; Fishing and Forestry",
        ),
        ({"mjsector": "Transportation;Energy and Extractives"}, "Transportation"),
        ({"mjsector": "", "sector1": "!$!0"}, None),  # projeto sem setor no Kaggle
        ({"mjsector1": "Energy and Extractives!$!100!$!LX"}, "Energy and Extractives"),  # formato antigo
        ({"mjsector1Name": ["Health"]}, "Health"),  # Projects API
        # Projects API real (out/2026): sem mjsector1Name; `sector` e `mjsector` são listas paralelas e o
        # grande setor é o do subsetor principal (`sector1`), que nem sempre é o 1º da lista (P158768)
        (
            {
                "sector1": {"Name": "Capital Markets", "Percent": 75},
                "sector": [{"Name": "Central Government (Central Agencies)"}, {"Name": "Capital Markets"}],
                "mjsector": [{"Name": "Public Administration"}, {"Name": "Financial Sector"}],
            },
            "Financial Sector",
        ),
        (  # sector1 fora da lista: 1º grande setor
            {"sector1": {"Name": "Outro"}, "sector": [{"Name": "Health"}], "mjsector": [{"Name": "Health"}]},
            "Health",
        ),
        ({"mjsector": [{"Name": "Health"}, {"Name": "Health"}]}, "Health"),  # sem sector1
        # Só o subsetor não vira grande setor (saía "Rural and Inter-Urban Roads" em vez de "Transportation")
        ({"sector1": {"Name": "Rural and Inter-Urban Roads", "Percent": 100}}, None),
        ({"sector1": {"Name": "", "Percent": 0}}, None),  # P505244: projeto sem setor na API
        ({"mjsector": [{"Name": ""}], "sector1": {"Name": "Capital Markets"}}, None),
    ],
)
def test_setor_principal(raw, esperado):
    assert wb.normalize_project({"id": "P1", **raw})["major_sector"] == esperado


def test_campos_de_setor_da_api_estao_no_fl():
    # Sem `sector` e `mjsector` no `fl`, a API não devolve as listas e o grande setor sairia nulo
    assert {"sector1", "sector", "mjsector"} <= set(wb.PROJECT_FIELDS)


@pytest.mark.parametrize(
    ("texto", "esperado"),
    [
        ("2018-06-28T00:00:00Z", "2018-06-28"),
        ("2018-06-28", "2018-06-28"),
        ("6/30/2023 12:00:00 AM", "2023-06-30"),
        ("12/31/2029", "2029-12-31"),
        ("", None),
        (None, None),
        ("não é data", None),
    ],
)
def test_parse_date(texto, esperado):
    assert wb.parse_date(texto) == esperado


@pytest.mark.parametrize(
    ("texto", "esperado"),
    [
        ("6,060,000", 6_060_000.0),
        ("120.50", 120.5),
        (0, 0.0),
        ("", None),
        (None, None),
        ("-1", None),
        ("abc", None),
    ],
)
def test_parse_amount(texto, esperado):
    assert wb.parse_amount(texto) == esperado


def test_content_hash_estavel_e_sensivel_ao_conteudo():
    a = {"project_id": "P1", "status": "Active", "total_commitment_usd": 1.0}
    assert wb.content_hash(a) == wb.content_hash(dict(reversed(a.items())))  # ordem das chaves não importa
    assert wb.content_hash(a) != wb.content_hash({**a, "status": "Closed"})
    assert len(wb.content_hash(a)) == 64


# ---------------------------------------------------------------- Retries e lista de indicadores


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_retry_com_backoff_em_429_e_5xx(status):
    sleep = NoSleep()
    session = FakeSession(FakeResponse(status), FakeResponse(status), FakeResponse(body={"ok": True}))
    assert wb.get_json(session, "https://x", {}, sleep=sleep, backoff=1.0) == {"ok": True}
    assert sleep == [1.0, 2.0]


def test_retry_respeita_retry_after():
    sleep = NoSleep()
    session = FakeSession(FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(body=[]))
    wb.get_json(session, "https://x", {}, sleep=sleep)
    assert sleep == [7.0]


def test_desiste_depois_das_tentativas():
    session = FakeSession(*[FakeResponse(503) for _ in range(3)])
    with pytest.raises(wb.WorldBankAPIError, match="503"):
        wb.get_json(session, "https://x", {}, retries=3, sleep=NoSleep())
    assert len(session.calls) == 3


def test_erro_4xx_nao_e_repetido():
    session = FakeSession(FakeResponse(404), FakeResponse(body={}))
    with pytest.raises(wb.WorldBankAPIError, match="404"):
        wb.get_json(session, "https://x", {}, sleep=NoSleep())
    assert len(session.calls) == 1


def test_lista_de_indicadores_vazia_usa_o_padrao():
    assert wb.indicator_codes("") == list(wb.DEFAULT_INDICATORS)
    assert wb.indicator_codes(None) == list(wb.DEFAULT_INDICATORS)
    assert wb.indicator_codes(" SP.POP.TOTL , NY.GDP.MKTP.CD ") == ["SP.POP.TOTL", "NY.GDP.MKTP.CD"]
    assert set(wb.DEFAULT_INDICATORS) == {"SP.POP.TOTL", "NY.GDP.MKTP.CD", "SP.RUR.TOTL.ZS", "EG.ELC.ACCS.ZS"}


@pytest.mark.parametrize(
    ("nome", "esperado"),
    [
        ("Agriculture; Fishing and Forestry", "Agriculture, Fishing and Forestry"),  # Kaggle
        ("Water; Sanitation and Waste Management", "Water, Sanitation and Waste Management"),
        ("Industry, Trade and Services", "Industry, Trade and Services"),  # Projects API: já canônico
        ("Health", "Health"),
        ("  ", None),
        (None, None),
    ],
)
def test_nome_canonico_do_setor(nome, esperado):
    # O Kaggle troca a vírgula do nome oficial por "; "; o estado atual usa o nome da API nas duas fontes
    assert wb.canonical_sector(nome) == esperado
