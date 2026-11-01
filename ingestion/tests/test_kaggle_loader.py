"""Leitura do dataset do Kaggle a partir da amostra sintética (fixture raw_dir)."""

import kaggle_loader as kl
import pytest

EXTRACTED_AT = "2026-10-28T06:10:00+00:00"


def test_csv_wdi_pula_metadados_e_le_formato_largo(raw_dir):
    rows = kl.read_wdi_csv(raw_dir / "gdp_data.csv")
    assert len(rows) == 6
    assert rows[0]["Country Code"] == "BRA"
    assert rows[0]["Indicator Code"] == "NY.GDP.MKTP.CD"
    assert {"2010", "2017"} <= set(rows[0])


def test_csv_wdi_ignora_coluna_vazia_do_fim_da_linha(tmp_path):
    # O CSV real do World Bank termina cada linha com vírgula (coluna sem nome)
    path = tmp_path / "x.csv"
    path.write_text(
        '"Data Source","World Development Indicators",\n\n'
        '"Country Name","Country Code","Indicator Name","Indicator Code","2016","2017",\n'
        '"Aruba","ABW","Population, total","SP.POP.TOTL","104822","105264",\n',
        encoding="utf-8",
    )
    (row,) = kl.read_wdi_csv(path)
    assert "" not in row
    assert row["2017"] == 105264.0


@pytest.mark.parametrize("ext", ["json", "xml", "db"])
def test_populacao_igual_nos_quatro_formatos(raw_dir, ext):
    def long(path):
        records = kl.wdi_to_long_records(kl.read_population(path), EXTRACTED_AT)
        return sorted((r["country_iso3"], r["year"], r["value"]) for r in records)

    base = long(raw_dir / "population_data.csv")
    assert base, "a amostra deveria ter valores"
    assert long(raw_dir / f"population_data.{ext}") == base


def test_formato_desconhecido_de_populacao(tmp_path):
    with pytest.raises(ValueError, match="parquet"):
        kl.read_population(tmp_path / "population_data.parquet")


def test_largo_para_longo_descarta_nulos_e_marca_kaggle():
    rows = [
        {
            "Country Name": "Brazil",
            "Country Code": "BRA",
            "Indicator Name": "Population, total",
            "Indicator Code": "SP.POP.TOTL",
            "2016": 1.0,
            "2017": None,
        }
    ]
    assert kl.wdi_to_long_records(rows, EXTRACTED_AT) == [
        {
            "country_iso3": "BRA",
            "country_name": "Brazil",
            "indicator_code": "SP.POP.TOTL",
            "indicator_name": "Population, total",
            "year": 2016,
            "value": 1.0,
            "unit": None,
            "obs_status": None,
            "source": "kaggle",
            "extracted_at": EXTRACTED_AT,
        }
    ]


def test_indicadores_do_kaggle_cobrem_os_quatro_arquivos(raw_dir):
    records = kl.kaggle_indicator_records(raw_dir, EXTRACTED_AT)
    assert {r["indicator_code"] for r in records} == {
        "SP.POP.TOTL",
        "NY.GDP.MKTP.CD",
        "SP.RUR.TOTL.ZS",
        "EG.ELC.ACCS.ZS",
    }
    assert all(r["value"] is not None and r["source"] == "kaggle" for r in records)


def test_projetos_do_kaggle_como_dicts_normalizados(raw_dir):
    projects = kl.kaggle_projects(raw_dir / "projects_data.csv")
    assert len(projects) == 60
    p = projects[0]
    assert p["project_id"].startswith("P")
    assert ";" not in p["country_name"]
    assert isinstance(p["total_commitment_usd"], float)
    assert p["board_approval_date"][4] == "-"
    assert p["source"] == "kaggle-replay"
    # World não tem ISO2 na amostra: vazio vira null
    assert any(x["country_iso2"] is None for x in projects)
