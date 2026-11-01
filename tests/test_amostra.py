"""A amostra sintética precisa espelhar os formatos reais do Kaggle — os demais testes dependem dela."""

import json
import sqlite3

import pandas as pd

ESPERADOS = {
    "projects_data.csv",
    "population_data.csv",
    "population_data.json",
    "population_data.xml",
    "population_data.db",
    "gdp_data.csv",
    "rural_population_percent.csv",
    "electricity_access_percent.csv",
}


def test_amostra_gera_todos_os_arquivos(raw_dir):
    assert {p.name for p in raw_dir.iterdir()} >= ESPERADOS


def test_csv_wdi_tem_linhas_de_metadados_antes_do_cabecalho(raw_dir):
    linhas = (raw_dir / "gdp_data.csv").read_text(encoding="utf-8").splitlines()
    idx = next(i for i, linha in enumerate(linhas) if linha.startswith("Country Name"))
    assert idx >= 3


def test_projetos_tem_valores_com_virgula_e_paises_duplicados(raw_dir):
    df = pd.read_csv(raw_dir / "projects_data.csv", dtype=str)
    assert df["totalamt"].str.contains(",").any()
    assert df["countryname"].str.contains(";").all()


def test_populacao_json_e_sqlite_tem_mesmas_linhas_do_csv(raw_dir):
    assert len(json.loads((raw_dir / "population_data.json").read_text())) == 6
    with sqlite3.connect(raw_dir / "population_data.db") as con:
        assert con.execute("select count(*) from population_data").fetchone()[0] == 6
