"""wb_extract: bootstrap sem dados do Kaggle e `make_volume_landing` com `airflow.sdk` falso (sem Airflow)."""

from __future__ import annotations

import sys
import types

import pytest
import wb_extract as wx
from kaggle_loader import KAGGLE_INDICATOR_FILES
from landing import VolumeLanding
from make_sample_data import main as make_sample


class ExplodingLanding:
    def write(self, *_):
        raise AssertionError("nada deveria ser gravado")


def test_bootstrap_com_csvs_do_kaggle_vazios_falha_antes_de_gravar(tmp_path):
    """CSVs WDI só com metadados e cabeçalho (download incompleto): nada é gravado na landing."""
    make_sample(tmp_path)
    for filename in KAGGLE_INDICATOR_FILES:
        path = tmp_path / filename
        path.write_text("".join(path.read_text("utf-8").splitlines(keepends=True)[:5]), encoding="utf-8")
    with pytest.raises(wx.ExtractError, match="nenhum indicador do Kaggle"):
        wx.bootstrap_kaggle(tmp_path, "manual__1", ExplodingLanding(), cur=None)


def test_bootstrap_sem_arquivos_do_kaggle_falha(tmp_path):
    with pytest.raises(wx.ExtractError, match="rode `make data`"):
        wx.bootstrap_kaggle(tmp_path, "manual__1", ExplodingLanding(), cur=None)


@pytest.fixture
def connection(monkeypatch):
    """`airflow.sdk.BaseHook.get_connection` falso; o teste ajusta host/senha em `conn`."""
    conn = types.SimpleNamespace(host="https://adb-1.azuredatabricks.net/", password="tok")
    asked: list[str] = []

    class BaseHook:
        @staticmethod
        def get_connection(conn_id):
            asked.append(conn_id)
            return conn

    module = types.ModuleType("airflow.sdk")
    module.BaseHook = BaseHook
    monkeypatch.setitem(sys.modules, "airflow.sdk", module)
    conn.asked = asked
    return conn


def test_make_volume_landing_usa_host_e_token_da_conexao(connection):
    env = {"DATABRICKS_CATALOG": "cat", "DATABRICKS_SCHEMA": "sch", "DATABRICKS_VOLUME": "vol"}
    landing = wx.make_volume_landing("databricks_default", env=env)
    assert isinstance(landing, VolumeLanding)
    assert landing.host == wx.volume_host(connection.host)
    assert landing.token == "tok"
    assert landing.base == "/Volumes/cat/sch/vol"
    assert connection.asked == ["databricks_default"]


def test_make_volume_landing_padroes_de_catalogo_schema_e_volume(connection):
    assert wx.make_volume_landing(env={}).base == "/Volumes/worldbank/lakehouse/landing"


@pytest.mark.parametrize("field", ["host", "password"])
def test_make_volume_landing_sem_host_ou_token_falha(connection, field):
    setattr(connection, field, "")
    with pytest.raises(wx.ExtractError, match="sem host ou token"):
        wx.make_volume_landing(env={})
