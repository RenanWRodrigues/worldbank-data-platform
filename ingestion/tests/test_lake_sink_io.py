"""lake-sink com I/O falso: `flush` (gravar → registrar → commit) e o laço `main` (consumer falso)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import kafka_utils
import lake_sink as ls
import landing
import pytest
from events import encode, make_event


def event(pid="P100001", event_type="PROJECT_APPROVED"):
    payload = {"project_id": pid, "status": "Active", "source": "kaggle-replay"}
    return make_event(event_type, payload, date(2020, 1, 1))


class RecordingLanding:
    """Landing em memória que registra a escrita no journal."""

    def __init__(self, journal: list, fail: bool = False):
        self.journal = journal
        self.fail = fail
        self.files: dict[str, bytes] = {}

    def write(self, rel_path: str, data: bytes) -> str:
        if self.fail:
            raise landing.LandingError("Files API HTTP 403")
        self.files[rel_path] = data
        self.journal.append(("landing_write", rel_path))
        return f"/Volumes/wb/lake/landing/{rel_path}"


def batch_with(*items):
    """Batch a partir de (evento ou bytes, partição, offset)."""
    buf = ls.Buffer(max_events=1000, max_seconds=60, clock=lambda: 0.0)
    for raw, partition, offset in items:
        buf.add(raw if isinstance(raw, bytes) else encode(raw), partition, offset)
    return buf.drain()


class FakeKafka:
    def __init__(self, journal):
        self.journal = journal
        self.commits = 0

    def commit(self, asynchronous=True):
        assert asynchronous is False
        self.commits += 1
        self.journal.append(("kafka_commit",))


# ---------------------------------------------------------------- flush


def test_flush_grava_registra_e_so_entao_faz_commit(journal, fakes):
    lake, con = RecordingLanding(journal), fakes.Connection(journal)
    ls.flush(batch_with((event(), 0, 10)), lake, con, FakeKafka(journal))
    assert [j[0] for j in journal] == ["landing_write", "execute", "pg_commit", "kafka_commit"]


def test_flush_registra_caminho_dataset_contagem_e_offsets(journal, fakes):
    lake, con = RecordingLanding(journal), fakes.Connection(journal)
    batch = batch_with((event("P100001"), 0, 10), (event("P100002"), 1, 3), (event("P100003"), 0, 11))
    ls.flush(batch, lake, con, FakeKafka(journal))
    [(sql, params)] = con.cur.executed
    assert sql == ls.REGISTER_SQL
    [rel_path] = lake.files
    assert params == (f"/Volumes/wb/lake/landing/{rel_path}", "projects_events", 3, 3, 11)
    assert rel_path.startswith("projects/events/date=") and "part-p0o10-11_p1o3-3-" in rel_path


def test_flush_grava_ndjson_com_posicao_no_kafka(journal, fakes):
    lake = RecordingLanding(journal)
    ls.flush(batch_with((event(), 2, 42)), lake, fakes.Connection(journal), FakeKafka(journal))
    [data] = lake.files.values()
    [line] = data.decode("utf-8").splitlines()
    record = json.loads(line)
    assert (record["kafka_partition"], record["kafka_offset"]) == (2, 42)
    assert record["event_id"] == event()["event_id"]


def test_flush_lote_so_com_invalidos_nao_grava_mas_faz_commit(journal, fakes):
    lake, con, kafka = RecordingLanding(journal), fakes.Connection(journal), FakeKafka(journal)
    ls.flush(batch_with((b"{quebrado", 0, 1)), lake, con, kafka)
    assert lake.files == {} and con.cur.executed == []
    assert kafka.commits == 1  # o offset avança: mensagem inválida não é relida para sempre


def test_flush_falha_na_landing_nao_registra_nem_faz_commit(journal, fakes):
    con, kafka = fakes.Connection(journal), FakeKafka(journal)
    with pytest.raises(landing.LandingError):
        ls.flush(batch_with((event(), 0, 1)), RecordingLanding(journal, fail=True), con, kafka)
    assert con.cur.executed == [] and kafka.commits == 0


def test_flush_falha_ao_registrar_nao_faz_commit(journal, fakes):
    con = fakes.Connection(journal, fail_on="ops.landing_files")
    kafka = FakeKafka(journal)
    with pytest.raises(RuntimeError):
        ls.flush(batch_with((event(), 0, 1)), RecordingLanding(journal), con, kafka)
    assert con.rollbacks == 1 and kafka.commits == 0


# ---------------------------------------------------------------- main


@pytest.fixture
def run_main(monkeypatch, journal, fakes, tmp_path):
    """Roda `main` com consumer e conexão falsos e landing em disco local (`tmp_path`)."""
    monkeypatch.setattr(ls, "_stop", False)
    monkeypatch.setattr(ls.signal, "signal", lambda *_: None)
    con = fakes.Connection(journal)
    monkeypatch.setattr(kafka_utils, "connect_pg", lambda: con)
    monkeypatch.setattr(landing, "make_landing", lambda: landing.LocalLanding(tmp_path))

    def _run(script, env=None):
        for key, value in (env or {}).items():
            monkeypatch.setenv(key, value)
        consumer = fakes.Consumer(journal, script, on_exhausted=lambda: setattr(ls, "_stop", True))
        monkeypatch.setattr(kafka_utils, "make_consumer", lambda group, topics: consumer)
        _run.consumer, _run.con = consumer, con
        ls.main()
        return consumer, con

    return _run


def landed(tmp_path: Path) -> list[list[str]]:
    """project_id de cada arquivo da landing, em ordem de nome."""
    files = sorted(tmp_path.rglob("*.json"))
    return [[json.loads(line)["project_id"] for line in f.read_text("utf-8").splitlines()] for f in files]


def test_main_fecha_lote_por_quantidade_e_commita_depois(run_main, journal, fakes, tmp_path):
    script = [fakes.Message(encode(event(f"P10000{i}")), offset=i) for i in range(1, 3)]
    consumer, _ = run_main(script, env={"FLUSH_MAX_EVENTS": "2", "FLUSH_MAX_SECONDS": "3600"})
    assert landed(tmp_path) == [["P100001", "P100002"]]
    assert consumer.commits == 1
    order = [j[0] for j in journal if j[0] in {"execute", "kafka_commit"}]
    assert order == ["execute", "kafka_commit"]


def test_main_desligamento_grava_lote_aberto(run_main, fakes, tmp_path):
    consumer, con = run_main([fakes.Message(encode(event()), offset=1)], env={"FLUSH_MAX_EVENTS": "500"})
    assert landed(tmp_path) == [["P100001"]]
    assert consumer.commits == 1 and len(con.cur.executed) == 1


def test_main_ignora_erro_do_kafka_e_poll_vazio(run_main, fakes, tmp_path):
    script = [None, fakes.Message(None, error="_TRANSPORT"), fakes.Message(encode(event()), offset=7)]
    consumer, _ = run_main(script, env={"FLUSH_MAX_EVENTS": "500"})
    assert landed(tmp_path) == [["P100001"]] and consumer.commits == 1


def test_main_sem_mensagens_nao_faz_commit_e_fecha_recursos(run_main, tmp_path):
    consumer, con = run_main([None])
    assert consumer.commits == 0 and landed(tmp_path) == []
    assert consumer.closed and con.closed


def test_main_reentrega_no_mesmo_lote_grava_uma_linha(run_main, fakes, tmp_path):
    raw = encode(event())
    run_main([fakes.Message(raw, offset=1), fakes.Message(raw, offset=2)], env={"FLUSH_MAX_EVENTS": "2"})
    assert landed(tmp_path) == [["P100001"]]


def test_main_falha_na_landing_fecha_recursos_sem_commit(run_main, monkeypatch, fakes):
    monkeypatch.setattr(landing, "make_landing", lambda: RecordingLanding([], fail=True))
    with pytest.raises(landing.LandingError):
        run_main([fakes.Message(encode(event()), offset=1)], env={"FLUSH_MAX_EVENTS": "1"})
    assert run_main.consumer.commits == 0
    assert run_main.consumer.closed and run_main.con.closed
