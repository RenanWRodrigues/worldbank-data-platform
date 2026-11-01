"""projects-producer com I/O falso: Publisher, run_replay, estado no Postgres, run_live e main.

`diff_events`/`_approved_events` têm testes próprios em test_projects_producer.py; aqui o `run_live` usa um
`diff_events` substituto para testar só a orquestração (publicar → flush → salvar estado).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import kafka_utils
import projects_producer as pp
import pytest
import worldbank_client
from events import TOPIC, make_event
from kaggle_loader import kaggle_projects


def payload(pid="P100001", status="Active", **extra):
    return {
        "project_id": pid,
        "status": status,
        "board_approval_date": "2020-01-15",
        "source": "worldbank-projects-api",
        **extra,
    }


def settings(raw_dir=Path("."), **overrides):
    base = {
        "mode": "replay",
        "raw_dir": Path(raw_dir),
        "replay_start": None,
        "days_per_second": 30.0,
        "poll_minutes": 1,
        "lookback_days": 30,
    }
    return pp.Settings(**{**base, **overrides})


@pytest.fixture(autouse=True)
def _reset_stop(monkeypatch):
    monkeypatch.setattr(pp, "_stop", False)


# ---------------------------------------------------------------- Publisher


def test_publisher_publica_com_chave_project_id_no_topico(fakes):
    producer = fakes.Producer()
    publisher = pp.Publisher(producer)
    event = make_event(pp.APPROVED, payload(), date(2020, 1, 15))
    publisher.publish(event)
    [sent] = producer.produced
    assert sent["topic"] == TOPIC and sent["key"] == "P100001"
    assert json.loads(sent["value"]) == event
    assert publisher.sent == 1 and producer.polls == [0]


def test_publisher_descarta_evento_fora_do_contrato_sem_produzir(fakes):
    # Descarta e conta: levantar erro derrubaria o producer, e com restart on-failure o live repetiria o mesmo
    # projeto inválido a cada ciclo, sem nunca publicar os demais
    producer = fakes.Producer()
    publisher = pp.Publisher(producer)
    event = {**make_event(pp.APPROVED, payload(), date(2020, 1, 15)), "event_type": "PROJECT_DELETED"}
    publisher.publish(event)
    assert producer.produced == [] and publisher.sent == 0 and publisher.rejected == 1


def test_publisher_espera_o_broker_quando_a_fila_local_enche(fakes):
    producer = fakes.Producer(buffer_errors=2)
    publisher = pp.Publisher(producer)
    publisher.publish(make_event(pp.APPROVED, payload(), date(2020, 1, 15)))
    assert len(producer.produced) == 1
    assert producer.polls == [1, 1, 0]  # duas esperas de 1 s e o poll(0) depois de enfileirar


def test_publisher_flush_sem_pendencias_nem_falhas_passa(fakes):
    producer = fakes.Producer()
    publisher = pp.Publisher(producer)
    publisher.publish(make_event(pp.APPROVED, payload(), date(2020, 1, 15)))
    publisher.flush()
    assert producer.flushes == [60] and publisher.failures == 0


def test_publisher_flush_falha_com_mensagens_nao_entregues(fakes):
    publisher = pp.Publisher(fakes.Producer(remaining=3))
    with pytest.raises(RuntimeError, match="3 mensagens não entregues"):
        publisher.flush()


def test_publisher_flush_falha_quando_o_broker_recusa_entrega(fakes):
    publisher = pp.Publisher(fakes.Producer(delivery_error="NOT_ENOUGH_REPLICAS"))
    publisher.publish(make_event(pp.APPROVED, payload(), date(2020, 1, 15)))
    with pytest.raises(RuntimeError, match="1 falhas"):
        publisher.flush()
    assert publisher.failures == 1


# ---------------------------------------------------------------- run_replay


@pytest.fixture
def sleeps(monkeypatch):
    calls: list[float] = []
    monkeypatch.setattr(pp.time, "sleep", calls.append)
    return calls


def published(producer) -> list[dict]:
    return [json.loads(p["value"]) for p in producer.produced]


def test_run_replay_publica_o_agendamento_da_amostra_em_ordem(raw_dir, sleeps, fakes):
    producer = fakes.Producer()
    pp.run_replay(settings(raw_dir), pp.Publisher(producer))
    expected = pp.replay_schedule(
        kaggle_projects(raw_dir / "projects_data.csv"), today=datetime.now(UTC).date()
    )
    events = published(producer)
    assert expected and len(events) == len(expected)
    assert [(e["event_type"], e["project_id"]) for e in events] == [
        (t, p["project_id"]) for _, t, p in expected
    ]
    assert [e["event_time"] for e in events] == sorted(e["event_time"] for e in events)
    assert producer.flushes == [60]


def test_run_replay_espera_pelo_relogio_acelerado(raw_dir, sleeps, fakes):
    publisher = pp.Publisher(fakes.Producer())
    pp.run_replay(settings(raw_dir, days_per_second=10.0), publisher)
    schedule = pp.replay_schedule(
        kaggle_projects(raw_dir / "projects_data.csv"), today=datetime.now(UTC).date()
    )
    total_days = (schedule[-1][0] - schedule[0][0]).days
    assert sum(sleeps) == pytest.approx(total_days / 10.0)
    assert all(s > 0 for s in sleeps)  # eventos no mesmo dia não esperam


def test_run_replay_respeita_replay_start(raw_dir, sleeps, fakes):
    producer = fakes.Producer()
    pp.run_replay(settings(raw_dir, replay_start=date(2015, 1, 1)), pp.Publisher(producer))
    approvals = [e for e in published(producer) if e["event_type"] == pp.APPROVED]
    assert approvals and all(e["event_time"] >= "2015-01-01" for e in approvals)


def test_run_replay_para_ao_receber_sinal_e_ainda_faz_flush(raw_dir, monkeypatch, fakes):
    monkeypatch.setattr(pp.time, "sleep", lambda _: setattr(pp, "_stop", True))
    producer = fakes.Producer()
    pp.run_replay(settings(raw_dir), pp.Publisher(producer))
    assert len(producer.produced) >= 1
    expected = pp.replay_schedule(
        kaggle_projects(raw_dir / "projects_data.csv"), today=datetime.now(UTC).date()
    )
    assert len(producer.produced) < len(expected)
    assert producer.flushes == [60]


def test_run_replay_sem_projetos_so_faz_flush(tmp_path, sleeps, fakes):
    (tmp_path / "projects_data.csv").write_text("id,status,boardapprovaldate\n", encoding="utf-8")
    producer = fakes.Producer()
    pp.run_replay(settings(tmp_path), pp.Publisher(producer))
    assert producer.produced == [] and producer.flushes == [60]


# ---------------------------------------------------------------- estado no Postgres


def test_load_state_mapeia_linhas_por_project_id(fakes):
    con = fakes.Connection(
        rows=[("P100001", "h1", "Active", pp.APPROVED), ("P100002", "h2", "Pipeline", None)]
    )
    assert pp.load_state(con) == {
        "P100001": {"content_hash": "h1", "status": "Active", "last_event_type": pp.APPROVED},
        "P100002": {"content_hash": "h2", "status": "Pipeline", "last_event_type": None},
    }
    assert "FROM ops.producer_state" in con.cur.executed[0][0]


def test_load_state_tabela_vazia_devolve_dicionario_vazio(fakes):
    assert pp.load_state(fakes.Connection(rows=[])) == {}


def test_save_state_grava_so_os_projetos_alterados_numa_transacao(fakes):
    state = {
        "P100001": {"content_hash": "h1", "status": "Active", "last_event_type": pp.APPROVED},
        "P100002": {"content_hash": "h2", "status": "Closed", "last_event_type": pp.CLOSED},
    }
    con = fakes.Connection()
    pp.save_state(con, state, ["P100002"])
    [(sql, rows)] = con.cur.executed
    assert "ON CONFLICT (project_id) DO UPDATE" in sql
    assert rows == [("P100002", "h2", "Closed", pp.CLOSED)]
    assert con.commits == 1


def test_save_state_sem_mudancas_nao_abre_transacao(fakes):
    con = fakes.Connection()
    pp.save_state(con, {}, [])
    assert con.cur.executed == [] and con.commits == 0


# ---------------------------------------------------------------- run_live


@pytest.fixture
def live(monkeypatch, journal, fakes):
    """run_live com conexão, busca e diff falsos; o primeiro `sleep` encerra o laço."""
    con = fakes.Connection(journal, rows=[("P100001", "h-old", "Active", pp.APPROVED)])
    monkeypatch.setattr(kafka_utils, "connect_pg", lambda: con)
    fetch_calls: list[dict] = []

    def fake_fetch(**kwargs):
        fetch_calls.append(kwargs)
        if kwargs.get("status") == "Active":
            return [payload("P100001", status="Active", project_name="novo nome")]
        return []

    monkeypatch.setattr(worldbank_client, "fetch_projects", fake_fetch)
    sleeps: list[float] = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        pp._stop = True

    monkeypatch.setattr(pp.time, "sleep", fake_sleep)
    producer = fakes.Producer()
    publisher = pp.Publisher(producer)
    original_flush = publisher.flush

    def flush():
        journal.append(("publisher_flush",))
        original_flush()

    publisher.flush = flush

    class Live:
        pass

    ctx = Live()
    ctx.con, ctx.fetch_calls, ctx.sleeps, ctx.producer, ctx.publisher = (
        con,
        fetch_calls,
        sleeps,
        producer,
        publisher,
    )
    return ctx


def stub_diff(events, new_state, seen=None):
    def _diff(projects, state, detected_at):
        if seen is not None:
            seen.append({"projects": projects, "state": state, "detected_at": detected_at})
        return events, new_state

    return _diff


def test_run_live_publica_eventos_do_diff_e_salva_estado_depois_do_flush(live, monkeypatch, journal):
    new_row = {"content_hash": "h-new", "status": "Active", "last_event_type": pp.UPDATED}
    when = datetime(2026, 10, 29, 12, tzinfo=UTC)
    events = [(pp.UPDATED, when, payload("P100001", project_name="novo nome"))]
    monkeypatch.setattr(pp, "diff_events", stub_diff(events, {"P100001": new_row}))
    pp.run_live(settings(mode="live"), live.publisher)
    [event] = published(live.producer)
    assert (event["event_type"], event["project_id"]) == (pp.UPDATED, "P100001")
    order = [j[0] for j in journal if j[0] in {"publisher_flush", "executemany"}]
    assert order == ["publisher_flush", "executemany"]
    assert journal[-1] == ("pg_close",)


def test_run_live_salva_so_estados_que_mudaram(live, monkeypatch):
    unchanged = {"content_hash": "h-old", "status": "Active", "last_event_type": pp.APPROVED}
    pipeline = {"content_hash": "h-p", "status": "Pipeline", "last_event_type": None}
    monkeypatch.setattr(pp, "diff_events", stub_diff([], {"P100001": unchanged, "P100009": pipeline}))
    pp.run_live(settings(mode="live"), live.publisher)
    [(_, rows)] = [(s, r) for s, r in live.con.cur.executed if "INSERT INTO ops.producer_state" in s]
    assert rows == [("P100009", "h-p", "Pipeline", None)]


def test_run_live_consulta_active_e_janela_de_aprovacao(live, monkeypatch):
    seen: list[dict] = []
    monkeypatch.setattr(pp, "diff_events", stub_diff([], {}, seen))
    pp.run_live(settings(mode="live", lookback_days=30), live.publisher)
    assert live.fetch_calls[0] == {"status": "Active"}
    start = date.fromisoformat(live.fetch_calls[1]["start_date"])
    assert (datetime.now(UTC).date() - start).days == 30
    assert seen[0]["state"] == {
        "P100001": {"content_hash": "h-old", "status": "Active", "last_event_type": pp.APPROVED}
    }
    assert [p["project_id"] for p in seen[0]["projects"]] == ["P100001"]


def test_run_live_evento_descartado_nao_salva_estado_do_projeto(live, monkeypatch):
    # Com o hash novo salvo, o projeto nunca mais seria publicado (nem depois de corrigir o contrato/cliente)
    bad = {"content_hash": "h-bad", "status": "Active", "last_event_type": pp.UPDATED}
    good = {"content_hash": "h-ok", "status": "Active", "last_event_type": pp.APPROVED}
    when = datetime(2026, 10, 29, tzinfo=UTC)
    events = [
        (pp.UPDATED, when, payload("P100001", source="fonte-desconhecida")),
        (pp.APPROVED, date(2020, 1, 15), payload("P100002")),
    ]
    monkeypatch.setattr(pp, "diff_events", stub_diff(events, {"P100001": bad, "P100002": good}))
    pp.run_live(settings(mode="live"), live.publisher)
    assert live.publisher.rejected == 1 and [e["project_id"] for e in published(live.producer)] == ["P100002"]
    [(_, rows)] = [(s, r) for s, r in live.con.cur.executed if "INSERT INTO ops.producer_state" in s]
    assert rows == [("P100002", "h-ok", "Active", pp.APPROVED)]


def test_run_live_falha_de_entrega_nao_salva_estado(live, monkeypatch):
    live.producer.delivery_error = "BROKER_DOWN"
    new_row = {"content_hash": "h-new", "status": "Active", "last_event_type": pp.UPDATED}
    events = [(pp.UPDATED, datetime(2026, 10, 29, tzinfo=UTC), payload())]
    monkeypatch.setattr(pp, "diff_events", stub_diff(events, {"P100001": new_row}))
    with pytest.raises(RuntimeError, match="falhas"):
        pp.run_live(settings(mode="live"), live.publisher)
    assert not any("INSERT INTO ops.producer_state" in s for s, _ in live.con.cur.executed)


def test_run_live_falha_da_api_pula_o_ciclo_sem_derrubar_o_producer(live, monkeypatch, caplog):
    # Queda de rede/API num ciclo: nada publicado, nada salvo, aviso no log e nova tentativa no ciclo seguinte
    new_row = {"content_hash": "h-new", "status": "Active", "last_event_type": pp.UPDATED}
    events = [(pp.UPDATED, datetime(2026, 10, 29, tzinfo=UTC), payload("P100001", project_name="novo nome"))]
    monkeypatch.setattr(pp, "diff_events", stub_diff(events, {"P100001": new_row}))
    calls = {"n": 0}

    def flaky_fetch(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise worldbank_client.WorldBankAPIError(
                "ConnectionError: Max retries exceeded (após 5 tentativas)"
            )
        return [payload("P100001", status="Active", project_name="novo nome")] if kwargs.get("status") else []

    monkeypatch.setattr(worldbank_client, "fetch_projects", flaky_fetch)
    cycles = {"n": 0}

    def sleep_counting(seconds):
        live.sleeps.append(seconds)
        if len(live.sleeps) % 60 == 0:  # poll_minutes=1: 60 passos de 1 s por ciclo
            cycles["n"] += 1
            pp._stop = cycles["n"] == 2

    monkeypatch.setattr(pp.time, "sleep", sleep_counting)
    with caplog.at_level("WARNING", logger="projects-producer"):
        pp.run_live(settings(mode="live", poll_minutes=1), live.publisher)
    assert "Projects API indisponível" in caplog.text and "1 ciclo(s) seguido(s)" in caplog.text
    assert [e["project_id"] for e in published(live.producer)] == ["P100001"]  # publicado no 2º ciclo
    saves = [r for s, r in live.con.cur.executed if "INSERT INTO ops.producer_state" in s]
    assert len(saves) == 1  # só o ciclo que funcionou salva estado
    assert live.con.closed


def test_run_live_erro_inesperado_ainda_derruba_o_producer(live, monkeypatch):
    def broken_fetch(**kwargs):
        raise ValueError("bug no cliente")

    monkeypatch.setattr(worldbank_client, "fetch_projects", broken_fetch)
    with pytest.raises(ValueError, match="bug no cliente"):
        pp.run_live(settings(mode="live"), live.publisher)


def test_run_live_espera_poll_minutes_em_passos_de_um_segundo(live, monkeypatch):
    monkeypatch.setattr(pp, "diff_events", stub_diff([], {}))
    monkeypatch.setattr(pp.time, "sleep", live.sleeps.append)  # não para no 1º sleep
    calls = {"n": 0}
    original = pp.load_state

    def load_then_stop(con):
        calls["n"] += 1
        if calls["n"] == 2:
            pp._stop = True
        return original(con)

    monkeypatch.setattr(pp, "load_state", load_then_stop)
    pp.run_live(settings(mode="live", poll_minutes=2), live.publisher)
    assert live.sleeps[:120] == [1] * 120
    assert live.con.closed


# ---------------------------------------------------------------- main


@pytest.fixture
def main_env(monkeypatch, fakes):
    monkeypatch.setattr(pp.signal, "signal", lambda *_: None)
    producer = fakes.Producer()
    monkeypatch.setattr(kafka_utils, "make_producer", lambda: producer)
    calls: list[tuple[str, pp.Settings, pp.Publisher]] = []
    monkeypatch.setattr(pp, "run_replay", lambda s, p: calls.append(("replay", s, p)))
    monkeypatch.setattr(pp, "run_live", lambda s, p: calls.append(("live", s, p)))
    return producer, calls


def test_main_modo_padrao_e_replay(main_env, monkeypatch):
    producer, calls = main_env
    monkeypatch.delenv("PRODUCER_MODE", raising=False)
    monkeypatch.setenv("RAW_DATA_DIR", "/tmp/raw")
    pp.main()
    [(mode, cfg, publisher)] = calls
    assert mode == "replay" and cfg.raw_dir == Path("/tmp/raw") and publisher.producer is producer


def test_main_modo_live(main_env, monkeypatch):
    _, calls = main_env
    monkeypatch.setenv("PRODUCER_MODE", "live")
    monkeypatch.setenv("POLL_MINUTES", "5")
    pp.main()
    [(mode, cfg, _)] = calls
    assert mode == "live" and cfg.poll_minutes == 5


def test_main_modo_invalido_falha_antes_de_criar_o_producer(main_env, monkeypatch):
    _, calls = main_env
    monkeypatch.setenv("PRODUCER_MODE", "batch")

    def no_producer():
        raise AssertionError("não deveria criar o producer")

    monkeypatch.setattr(kafka_utils, "make_producer", no_producer)
    with pytest.raises(ValueError, match="PRODUCER_MODE inválido"):
        pp.main()
    assert calls == []
