"""Landing: mesmo layout no disco local e no Volume do Databricks (Files API com sessão falsa)."""

import json

import landing
import pytest


def test_ndjson_uma_linha_por_evento():
    data = landing.to_ndjson([{"a": 1}, {"b": "ç"}])
    assert data.decode("utf-8").splitlines() == ['{"a":1}', '{"b":"ç"}']
    assert data.endswith(b"\n")


def test_landing_local_grava_no_layout_e_devolve_o_caminho(tmp_path):
    target = landing.LocalLanding(tmp_path)
    path = target.write("projects/events/date=2026-10-28/part-x.json", b'{"a":1}\n')
    written = tmp_path / "projects/events/date=2026-10-28/part-x.json"
    assert path == str(written.as_posix())
    assert written.read_bytes() == b'{"a":1}\n'
    assert not list(written.parent.glob("*.tmp"))  # escrita atômica não deixa temporário


class FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = ""


class FakeSession:
    def __init__(self, *codes):
        self.codes = list(codes)
        self.calls = []

    def put(self, url, data=None, headers=None, params=None, timeout=None):
        self.calls.append({"url": url, "data": data, "headers": headers, "params": params})
        return FakeResponse(self.codes.pop(0))


def volume(session):
    return landing.VolumeLanding(
        host="https://dbc-1.cloud.databricks.com/",
        token="tkn",
        catalog="worldbank",
        schema="lakehouse",
        volume="landing",
        session=session,
        sleep=lambda _s: None,
    )


def test_volume_envia_pela_files_api_com_token():
    session = FakeSession(204)
    path = volume(session).write("projects/events/date=2026-10-28/part-x.json", b"{}\n")
    assert path == "/Volumes/worldbank/lakehouse/landing/projects/events/date=2026-10-28/part-x.json"
    call = session.calls[0]
    assert call["url"] == (
        "https://dbc-1.cloud.databricks.com/api/2.0/fs/files"
        "/Volumes/worldbank/lakehouse/landing/projects/events/date=2026-10-28/part-x.json"
    )
    assert call["headers"]["Authorization"] == "Bearer tkn"
    assert call["params"] == {"overwrite": "true"}


def test_volume_repete_em_5xx_e_falha_em_4xx():
    session = FakeSession(503, 200)
    volume(session).write("a.json", b"{}\n")
    assert len(session.calls) == 2
    with pytest.raises(landing.LandingError, match="403"):
        volume(FakeSession(403)).write("a.json", b"{}\n")


def test_escolhe_volume_so_com_host_e_token(tmp_path):
    local = landing.make_landing({"LANDING_DIR": str(tmp_path)})
    assert isinstance(local, landing.LocalLanding)
    env = {"DATABRICKS_HOST": "https://x", "DATABRICKS_TOKEN": "t", "LANDING_DIR": str(tmp_path)}
    assert isinstance(landing.make_landing(env, session=FakeSession()), landing.VolumeLanding)


def test_ndjson_preserva_o_evento(make_event):
    ev = make_event()
    assert json.loads(landing.to_ndjson([ev])) == ev
