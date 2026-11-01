"""realtime-sink com I/O falso: `apply_batch` (execute_values capturado) e o laço `main` (consumer falso)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import kafka_utils
import psycopg2.extras
import pytest
import realtime_sink as rs
from events import encode, make_event

ALERT_MIN = 500_000_000
PRODUCED = datetime(2024, 1, 1, tzinfo=UTC)


def event(pid="P100001", event_type="PROJECT_APPROVED", day=date(2020, 1, 15), amount=1e6, produced=PRODUCED):
    payload = {
        "project_id": pid,
        "project_name": f"Projeto {pid}",
        "status": "Closed" if event_type == "PROJECT_CLOSED" else "Active",
        "country_name": "Kenya",
        "country_iso2": "KE",
        "total_commitment_usd": amount,
        "board_approval_date": "2020-01-15",
        "source": "kaggle-replay",
    }
    return make_event(event_type, payload, day, produced_at=produced)


def rows_of(*events, alert_min=ALERT_MIN):
    return [rs.to_rows(e, partition=0, offset=i, alert_min=alert_min) for i, e in enumerate(events)]


class FakeExecuteValues:
    """Substitui psycopg2.extras.execute_values; `in_db` simula os event_id já gravados (ON CONFLICT)."""

    def __init__(self, in_db: set[str] | None = None):
        self.in_db = set(in_db or ())
        self.stale: set[str] = set()
        self.calls: list[dict] = []

    def __call__(self, cur, sql, argslist, template=None, page_size=100, fetch=False):
        argslist = list(argslist)
        self.calls.append({"sql": sql, "args": argslist, "template": template, "fetch": fetch})
        if sql is rs.INSERT_EVENTS_SQL:
            inserted = []
            for args in argslist:  # RETURNING devolve só o que o ON CONFLICT DO NOTHING deixou entrar
                if args[0] not in self.in_db:
                    self.in_db.add(args[0])
                    inserted.append((args[0],))
            return inserted if fetch else None
        if sql is rs.UPSERT_LATEST_SQL and fetch:
            # RETURNING do UPSERT: só os projetos que o WHERE deixou mudar (`stale` simula os recusados)
            return [(args[0],) for args in argslist if args[0] not in self.stale]
        return None

    def sqls(self):
        return [c["sql"] for c in self.calls]

    def call(self, sql):
        return next(c for c in self.calls if c["sql"] is sql)


@pytest.fixture
def ev(monkeypatch):
    fake = FakeExecuteValues()
    monkeypatch.setattr(psycopg2.extras, "execute_values", fake)
    return fake


# ---------------------------------------------------------------- apply_batch


def test_apply_batch_insere_eventos_com_returning_e_fetch(ev, fakes):
    rows = rows_of(event("P100001"), event("P100002"))
    rs.apply_batch(fakes.Connection(), rows)
    call = ev.call(rs.INSERT_EVENTS_SQL)
    assert call["fetch"] is True
    assert "RETURNING event_id::text" in call["sql"]
    assert [a[0] for a in call["args"]] == [r.event["event_id"] for r in rows]


def test_apply_batch_grava_payload_como_json_e_posicao_no_kafka(ev, fakes):
    [row] = rows_of(event("P100001"))
    rs.apply_batch(fakes.Connection(), [row])
    args = ev.call(rs.INSERT_EVENTS_SQL)["args"][0]
    assert isinstance(args[6], psycopg2.extras.Json)
    assert args[6].adapted == row.event["payload"]
    assert args[7:] == (0, 0)  # kafka_partition, kafka_offset


def test_apply_batch_evento_novo_atualiza_estado_atual(ev, fakes):
    rs.apply_batch(fakes.Connection(), rows_of(event("P100001")))
    assert ev.sqls() == [rs.INSERT_EVENTS_SQL, rs.UPSERT_LATEST_SQL]  # sem alerta: valor pequeno
    upsert = ev.call(rs.UPSERT_LATEST_SQL)
    assert upsert["template"] == rs._LATEST_TEMPLATE
    assert upsert["args"][0][0] == "P100001"


def test_apply_batch_grava_alerta_depois_do_evento_e_antes_do_estado(ev, fakes):
    rs.apply_batch(fakes.Connection(), rows_of(event("P100001", amount=900e6)))
    assert ev.sqls() == [rs.INSERT_EVENTS_SQL, rs.INSERT_ALERTS_SQL, rs.UPSERT_LATEST_SQL]
    alert = ev.call(rs.INSERT_ALERTS_SQL)["args"][0]
    assert alert[1:3] == ("P100001", "LARGE_APPROVAL") and alert[4] == 900e6


def test_apply_batch_reentrega_nao_gera_alerta_nem_upsert(ev, fakes):
    rows = rows_of(event("P100001", amount=900e6))
    ev.in_db.add(rows[0].event["event_id"])  # já aplicado numa transação anterior
    rs.apply_batch(fakes.Connection(), rows)
    assert ev.sqls() == [rs.INSERT_EVENTS_SQL]


def test_apply_batch_so_aplica_estado_e_alerta_dos_eventos_inseridos_agora(ev, fakes):
    old = event("P100001", amount=900e6)
    new = event("P100002", amount=800e6)
    ev.in_db.add(old["event_id"])
    rs.apply_batch(fakes.Connection(), rows_of(old, new))
    assert [a[1] for a in ev.call(rs.INSERT_ALERTS_SQL)["args"]] == ["P100002"]
    assert [r[0] for r in ev.call(rs.UPSERT_LATEST_SQL)["args"]] == ["P100002"]


def test_apply_batch_duplicado_no_mesmo_lote_aplica_uma_vez(ev, fakes):
    e = event("P100001", amount=900e6)
    rs.apply_batch(fakes.Connection(), rows_of(e, e))
    assert len(ev.call(rs.INSERT_ALERTS_SQL)["args"]) == 1
    assert len(ev.call(rs.UPSERT_LATEST_SQL)["args"]) == 1


def test_apply_batch_reduz_ao_evento_mais_novo_de_cada_projeto(ev, fakes):
    approved = event("P100001", "PROJECT_APPROVED", day=date(2020, 1, 15))
    closed = event("P100001", "PROJECT_CLOSED", day=date(2025, 12, 31))
    rs.apply_batch(fakes.Connection(), rows_of(closed, approved))  # fora de ordem no lote
    [latest] = ev.call(rs.UPSERT_LATEST_SQL)["args"]
    columns = dict(zip(rs._LATEST_COLUMNS, latest, strict=True))
    assert columns["last_event_type"] == "PROJECT_CLOSED" and columns["status"] == "Closed"


def test_apply_batch_faz_commit_numa_transacao(ev, fakes):
    con = fakes.Connection()
    rs.apply_batch(con, rows_of(event("P100001")))
    assert (con.commits, con.rollbacks) == (1, 0)


def test_apply_batch_erro_no_banco_desfaz_a_transacao(monkeypatch, fakes):
    def boom(*_, **__):
        raise RuntimeError("deadlock")

    monkeypatch.setattr(psycopg2.extras, "execute_values", boom)
    con = fakes.Connection()
    with pytest.raises(RuntimeError, match="deadlock"):
        rs.apply_batch(con, rows_of(event("P100001")))
    assert (con.commits, con.rollbacks) == (0, 1)


def test_apply_batch_devolve_o_que_mudou(ev, fakes):
    a, b = event("P100001", amount=900e6), event("P100002")
    result = rs.apply_batch(fakes.Connection([]), rows_of(a, b))
    assert result == rs.BatchResult(inserted=2, alerts=1, updated=2)
    assert ev.call(rs.UPSERT_LATEST_SQL)["fetch"] is True


def test_apply_batch_reentrega_devolve_zero(ev, fakes):
    a = event("P100001")
    ev.in_db.add(a["event_id"])
    assert rs.apply_batch(fakes.Connection([]), rows_of(a)) == rs.BatchResult()


def test_apply_batch_evento_mais_antigo_que_o_estado_nao_conta_como_atualizado(ev, fakes):
    ev.stale.add("P100001")  # o WHERE do UPSERT recusa: estado atual é mais novo
    result = rs.apply_batch(fakes.Connection([]), rows_of(event("P100001")))
    assert (result.inserted, result.updated) == (1, 0)


# ---------------------------------------------------------------- main


@pytest.fixture
def run_main(monkeypatch, journal, fakes):
    """Roda `main` com consumer e conexão falsos; `apply_batch` vira um gravador no journal."""
    monkeypatch.setattr(rs, "_stop", False)
    monkeypatch.setattr(rs.signal, "signal", lambda *_: None)
    con = fakes.Connection(journal)
    applied: list[list[rs.Rows]] = []

    def fake_apply(c, rows):
        assert c is con
        applied.append(rows)
        journal.append(("apply_batch", len(rows)))
        return rs.BatchResult(inserted=len(rows), updated=len(rows))

    monkeypatch.setattr(rs, "apply_batch", fake_apply)
    monkeypatch.setattr(kafka_utils, "connect_pg", lambda: con)

    def _run(script, env=None):
        for key, value in (env or {}).items():
            monkeypatch.setenv(key, value)
        consumer = fakes.Consumer(journal, script, on_exhausted=lambda: setattr(rs, "_stop", True))
        monkeypatch.setattr(kafka_utils, "make_consumer", lambda group, topics: consumer)
        _run.consumer, _run.con = consumer, con
        rs.main()
        return consumer, con, applied

    return _run


def test_main_aplica_lote_e_so_entao_faz_commit(run_main, journal, fakes):
    batch = [
        fakes.Message(encode(event("P100001")), offset=5),
        fakes.Message(encode(event("P100002")), offset=6),
    ]
    _, _, applied = run_main([batch])
    assert [j for j in journal if j[0] in {"apply_batch", "kafka_commit"}] == [
        ("apply_batch", 2),
        ("kafka_commit",),
    ]
    assert [r.event["kafka_offset"] for r in applied[0]] == [5, 6]


def test_main_descarta_json_invalido_evento_fora_do_contrato_e_erro_do_kafka(run_main, fakes):
    good = event("P100001")
    bad_contract = {**event("P100002"), "event_type": "PROJECT_DELETED"}
    batch = [
        fakes.Message(b"{nao-e-json", offset=1),
        fakes.Message(encode(bad_contract), offset=2),
        fakes.Message(None, offset=3, error="_PARTITION_EOF"),
        fakes.Message(encode(good), offset=4),
    ]
    consumer, _, applied = run_main([batch])
    assert [r.event["project_id"] for r in applied[0]] == ["P100001"]
    assert consumer.commits == 1


def test_main_lote_so_com_invalidos_faz_commit_sem_gravar(run_main, fakes):
    consumer, _, applied = run_main([[fakes.Message(b"lixo", offset=1)]])
    assert applied == []
    assert consumer.commits == 1  # avança o offset: mensagem inválida não trava o consumidor


def test_main_sem_mensagens_nao_faz_commit(run_main):
    consumer, _, applied = run_main([[], []])
    assert applied == [] and consumer.commits == 0


def test_main_usa_lote_e_limite_de_alerta_do_ambiente(run_main, journal, fakes):
    batch = [fakes.Message(encode(event("P100001", amount=2e6)))]
    _, _, applied = run_main([batch], env={"RT_BATCH": "7", "ALERT_MIN_COMMITMENT_USD": "1000000"})
    assert ("consume", 7) in journal
    assert applied[0][0].alert is not None


def test_main_falha_ao_gravar_nao_faz_commit_e_fecha_recursos(run_main, monkeypatch, fakes):
    def boom(*_):
        raise RuntimeError("banco fora")

    monkeypatch.setattr(rs, "apply_batch", boom)
    with pytest.raises(RuntimeError, match="banco fora"):
        run_main([[fakes.Message(encode(event("P100001")))]])
    assert run_main.consumer.commits == 0  # o lote volta a ser entregue (at-least-once)
    assert run_main.consumer.closed and run_main.con.closed


def test_main_loga_recebidos_novos_e_duplicados(run_main, monkeypatch, fakes, caplog):
    monkeypatch.setattr(rs, "apply_batch", lambda c, rows: rs.BatchResult(inserted=0))
    with caplog.at_level("INFO", logger="realtime-sink"):
        run_main([[fakes.Message(encode(event("P100001"))), fakes.Message(encode(event("P100002")))]])
    assert "2 eventos recebidos: 0 novos (2 duplicados)" in caplog.text


def test_main_fecha_consumer_e_conexao_ao_parar(run_main):
    consumer, con, _ = run_main([])
    assert consumer.closed and con.closed
