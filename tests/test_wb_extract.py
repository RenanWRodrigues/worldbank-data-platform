"""airflow/include/wb_extract.py com sessão HTTP falsa, landing em memória e cursor falso (nunca rede real).

Fica em tests/ (e não em airflow/tests/) porque usa tests/fixtures/api/ e a amostra sintética do Kaggle,
que não são montadas no container do Airflow.
"""

import json

import pytest
import wb_extract as wx

RUN_ID = "manual__2026-10-29T13:58:09.674223+00:00"
EXTRACTED_AT = "2026-10-29T06:10:00+00:00"


class FakeResponse:
    def __init__(self, body):
        self.status_code = 200
        self._body = body
        self.headers = {}

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, *bodies):
        self.bodies = list(bodies)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        return FakeResponse(self.bodies.pop(0))


class MemoryLanding:
    def __init__(self):
        self.files: dict[str, bytes] = {}

    def write(self, rel_path, data):
        self.files[rel_path] = data
        return f"/Volumes/wb/lh/landing/{rel_path}"

    def records(self, rel_path):
        return [json.loads(line) for line in self.files[rel_path].decode("utf-8").splitlines()]


class RegisterCursor:
    """Simula o INSERT … RETURNING file_id de ops.landing_files."""

    def __init__(self):
        self.rows = []

    def execute(self, sql, params):
        assert "ops.landing_files" in sql and "RETURNING file_id" in sql
        self.rows.append(params)

    def fetchone(self):
        return (len(self.rows),)


def test_caminhos_seguem_o_contrato_da_landing():
    assert wx.run_tag(RUN_ID) == "manual__2026-10-29T13_58_09.674223_00_00"
    assert wx.indicator_path("SP.POP.TOTL", RUN_ID) == (
        "indicators/source=api/indicator=SP.POP.TOTL/run=manual__2026-10-29T13_58_09.674223_00_00/part-00000.json"
    )
    assert wx.indicator_path("SP.POP.TOTL", "r1", "kaggle").startswith("indicators/source=kaggle/")
    assert wx.countries_path("r1") == "reference/countries/run=r1/part-00000.json"


def test_volume_host_aceita_host_com_ou_sem_esquema():
    assert wx.volume_host("dbc-1.cloud.databricks.com/") == "https://dbc-1.cloud.databricks.com"
    assert wx.volume_host("https://dbc-1.cloud.databricks.com") == "https://dbc-1.cloud.databricks.com"


def test_extract_indicator_grava_registra_e_devolve_paises_com_valor(api_sample):
    session = FakeSession(api_sample("indicators_page1.json"), api_sample("indicators_page2.json"))
    landing, cur = MemoryLanding(), RegisterCursor()

    result = wx.extract_indicator(
        "SP.POP.TOTL", RUN_ID, landing, cur, session=session, extracted_at=EXTRACTED_AT
    )

    rel = wx.indicator_path("SP.POP.TOTL", RUN_ID)
    records = landing.records(rel)
    assert result["file_id"] == 1 and result["records"] == len(records) > 0
    assert result["path"] == f"/Volumes/wb/lh/landing/{rel}"
    assert cur.rows == [(result["path"], "indicators", len(records))]
    assert result["countries_with_value"] == sorted(
        {r["country_iso3"] for r in records if r["value"] is not None}
    )
    assert all(r["source"] == "worldbank-api" and r["extracted_at"] == EXTRACTED_AT for r in records)
    assert session.calls[0][1]["per_page"] == 1000


def test_extract_indicator_vazio_falha(api_sample):
    session = FakeSession([{"page": 1, "pages": 1, "per_page": 1000, "total": 0}, None])
    with pytest.raises(wx.ExtractError, match="não devolveu linhas"):
        wx.extract_indicator("XX.YY", RUN_ID, MemoryLanding(), RegisterCursor(), session=session)


def test_extract_countries_separa_agregados(api_sample):
    landing, cur = MemoryLanding(), RegisterCursor()
    result = wx.extract_countries(
        RUN_ID, landing, cur, FakeSession(api_sample("countries.json")), EXTRACTED_AT
    )

    assert result["aggregates"] == ["WLD"]
    assert result["countries"] == 1  # BRA
    assert cur.rows[0][1] == "countries"
    assert {r["country_iso3"] for r in landing.records(wx.countries_path(RUN_ID))} == {"BRA", "WLD"}


def test_bootstrap_kaggle_um_arquivo_por_indicador_mais_paises(raw_dir, api_sample):
    landing, cur = MemoryLanding(), RegisterCursor()
    result = wx.bootstrap_kaggle(
        raw_dir, RUN_ID, landing, cur, FakeSession(api_sample("countries.json")), EXTRACTED_AT
    )

    assert set(result["records"]) == {"SP.POP.TOTL", "NY.GDP.MKTP.CD", "SP.RUR.TOTL.ZS", "EG.ELC.ACCS.ZS"}
    assert result["file_ids"] == [1, 2, 3, 4, 5]  # 4 indicadores + países
    datasets = [row[1] for row in cur.rows]
    assert datasets == ["indicators"] * 4 + ["countries"]
    for code, n in result["records"].items():
        records = landing.records(wx.indicator_path(code, RUN_ID, "kaggle"))
        assert len(records) == n
        assert all(r["source"] == "kaggle" and r["indicator_code"] == code for r in records)


def test_bootstrap_sem_arquivos_do_kaggle_falha(tmp_path):
    with pytest.raises(wx.ExtractError, match="rode `make data`"):
        wx.bootstrap_kaggle(tmp_path, RUN_ID, MemoryLanding(), RegisterCursor(), FakeSession())


def test_coverage_conta_paises_nao_agregados():
    meta = {"countries": 217, "aggregates": ["WLD", "HIC"]}
    extracted = [
        {"indicator": "SP.POP.TOTL", "countries_with_value": ["BRA", "WLD", "HIC", "ARG"]},
        {"indicator": "EG.ELC.ACCS.ZS", "countries_with_value": []},
    ]
    assert wx.coverage_report(extracted, meta) == {"countries": 217, "SP.POP.TOTL": 2, "EG.ELC.ACCS.ZS": 0}


def test_check_coverage_lista_o_que_ficou_abaixo():
    with pytest.raises(wx.ExtractError, match=r"SP.POP.TOTL: 179, EG.ELC.ACCS.ZS: 0"):
        wx.check_coverage({"countries": 217, "SP.POP.TOTL": 179, "EG.ELC.ACCS.ZS": 0})
    wx.check_coverage({"countries": 180, "SP.POP.TOTL": 200})
