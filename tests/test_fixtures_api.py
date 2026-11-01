"""As respostas de exemplo precisam ter a estrutura que o cliente espera (worldbank_client)."""


def test_indicators_api_e_lista_metadados_mais_linhas(api_sample):
    body = api_sample("indicators_page1.json")
    assert isinstance(body, list) and body[0]["pages"] == 2
    assert {"countryiso3code", "date", "value", "indicator", "country"} <= set(body[1][0])


def test_erro_da_indicators_api_vem_como_message(api_sample):
    assert "message" in api_sample("indicators_error.json")[0]


def test_paises_marcam_agregados_pela_regiao(api_sample):
    rows = api_sample("countries.json")[1]
    assert {r["id"]: r["region"]["value"] for r in rows}["WLD"] == "Aggregates"


def test_projects_api_traz_objeto_indexado_por_id(api_sample):
    body = api_sample("projects_page1.json")
    assert isinstance(body["projects"], dict) and "P123456" in body["projects"]
    assert api_sample("projects_empty.json")["projects"] == {}
