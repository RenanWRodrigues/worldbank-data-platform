"""Semântica dos eventos de projeto: replay_schedule e diff_events são funções puras."""

from datetime import UTC, date, datetime

import projects_producer as pp
import pytest
from worldbank_client import content_hash

TODAY = date(2026, 10, 28)
DETECTED = datetime(2026, 10, 28, 15, 0, tzinfo=UTC)


def project(pid="P100001", status="Active", approval="2010-03-15", closing="2015-06-30", **extra):
    return {
        "project_id": pid,
        "status": status,
        "board_approval_date": approval,
        "closing_date": closing,
        "total_commitment_usd": 1e6,
        "source": "kaggle-replay",
        **extra,
    }


# ---------------------------------------------------------------- replay_schedule


def test_replay_em_ordem_cronologica_com_approved_e_closed():
    projects = [
        project("P2", approval="2012-01-01", closing="2013-01-01"),
        project("P1", approval="2010-01-01", closing="2016-01-01", status="Closed"),
    ]
    schedule = pp.replay_schedule(projects, today=TODAY)
    assert [(d.isoformat(), t, p["project_id"]) for d, t, p in schedule] == [
        ("2010-01-01", "PROJECT_APPROVED", "P1"),
        ("2012-01-01", "PROJECT_APPROVED", "P2"),
        ("2013-01-01", "PROJECT_CLOSED", "P2"),
        ("2016-01-01", "PROJECT_CLOSED", "P1"),
    ]


def test_replay_approved_sai_active_e_closed_sai_closed():
    schedule = pp.replay_schedule([project(status="Closed")], today=TODAY)
    assert [p["status"] for _, _, p in schedule] == ["Active", "Closed"]


@pytest.mark.parametrize("status", ["Pipeline", "Dropped"])
def test_replay_ignora_projetos_nao_aprovados(status):
    assert pp.replay_schedule([project(status=status)], today=TODAY) == []


def test_replay_ignora_projeto_sem_data_de_aprovacao():
    assert pp.replay_schedule([project(approval=None)], today=TODAY) == []


def test_replay_nao_gera_evento_no_futuro():
    schedule = pp.replay_schedule([project(approval="2025-01-01", closing="2030-12-31")], today=TODAY)
    assert [t for _, t, _ in schedule] == ["PROJECT_APPROVED"]
    assert pp.replay_schedule([project(approval="2027-01-01", closing=None)], today=TODAY) == []


def test_closed_no_dia_de_hoje_e_publicado():
    schedule = pp.replay_schedule([project(approval="2020-01-01", closing=TODAY.isoformat())], today=TODAY)
    assert [t for _, t, _ in schedule] == ["PROJECT_APPROVED", "PROJECT_CLOSED"]


def test_replay_start_filtra_projetos_aprovados_antes():
    projects = [project("P1", approval="1999-12-31"), project("P2", approval="2000-01-01")]
    schedule = pp.replay_schedule(projects, today=TODAY, start=date(2000, 1, 1))
    assert {p["project_id"] for _, _, p in schedule} == {"P2"}


def test_mesmo_dia_approved_antes_de_closed():
    schedule = pp.replay_schedule([project(approval="2010-01-01", closing="2010-01-01")], today=TODAY)
    assert [t for _, t, _ in schedule] == ["PROJECT_APPROVED", "PROJECT_CLOSED"]


def test_esperas_do_relogio_acelerado():
    schedule = pp.replay_schedule(
        [
            project("P1", approval="2010-01-01", closing=None),
            project("P2", approval="2010-01-31", closing=None),
        ],
        today=TODAY,
    )
    # 30 dias simulados por segundo: o 1º evento sai na hora, o 2º um segundo depois
    assert pp.replay_delays(schedule, days_per_second=30) == [0.0, 1.0]


# ---------------------------------------------------------------- diff_events (modo live)


def state_of(*projects, last_event_type="PROJECT_APPROVED"):
    return {
        p["project_id"]: {
            "content_hash": content_hash(p),
            "status": p["status"],
            "last_event_type": last_event_type,
        }
        for p in projects
    }


def types(events):
    return [(t, p["project_id"]) for t, _, p in events]


def test_projeto_novo_active_gera_approved_na_data_de_aprovacao():
    events, state = pp.diff_events([project(closing="2030-01-01")], {}, DETECTED)
    assert types(events) == [("PROJECT_APPROVED", "P100001")]
    assert events[0][1] == date(2010, 3, 15)
    assert state["P100001"]["last_event_type"] == "PROJECT_APPROVED"


def test_projeto_novo_ja_encerrado_gera_approved_e_closed():
    events, state = pp.diff_events([project(status="Closed")], {}, DETECTED)
    assert types(events) == [("PROJECT_APPROVED", "P100001"), ("PROJECT_CLOSED", "P100001")]
    assert events[0][2]["status"] == "Active"
    assert events[1][1] == date(2015, 6, 30)
    assert state["P100001"]["last_event_type"] == "PROJECT_CLOSED"


def test_pipeline_nao_publica_mas_guarda_estado():
    p = project(status="Pipeline", approval=None)
    events, state = pp.diff_events([p], {}, DETECTED)
    assert events == []
    assert state["P100001"] == {
        "content_hash": content_hash(p),
        "status": "Pipeline",
        "last_event_type": None,
    }


def test_pipeline_que_foi_aprovado_gera_approved():
    antes = project(status="Pipeline", approval=None)
    depois = project(status="Active", closing="2030-01-01")
    events, _ = pp.diff_events([depois], state_of(antes, last_event_type=None), DETECTED)
    assert types(events) == [("PROJECT_APPROVED", "P100001")]


def test_sem_mudanca_nao_publica():
    p = project()
    events, state = pp.diff_events([p], state_of(p), DETECTED)
    assert events == [] and state == state_of(p)


def test_passou_a_closed_gera_closed():
    antes = project(closing="2026-10-01")
    depois = project(status="Closed", closing="2026-10-01")
    events, state = pp.diff_events([depois], state_of(antes), DETECTED)
    assert types(events) == [("PROJECT_CLOSED", "P100001")]
    assert events[0][1] == date(2026, 10, 1)
    assert state["P100001"]["status"] == "Closed"


def test_closed_sem_data_de_encerramento_usa_hora_da_deteccao():
    antes = project(closing=None)
    events, _ = pp.diff_events([project(status="Closed", closing=None)], state_of(antes), DETECTED)
    assert events[0][1] == DETECTED


def test_outra_mudanca_gera_updated_na_hora_da_deteccao():
    antes = project()
    depois = project(total_commitment_usd=2e6)
    events, state = pp.diff_events([depois], state_of(antes), DETECTED)
    assert types(events) == [("PROJECT_UPDATED", "P100001")]
    assert events[0][1] == DETECTED
    assert state["P100001"]["content_hash"] == content_hash(depois)
    assert state["P100001"]["last_event_type"] == "PROJECT_UPDATED"


def test_mudanca_em_projeto_ja_encerrado_e_updated():
    antes = project(status="Closed")
    depois = project(status="Closed", project_name="novo nome")
    events, _ = pp.diff_events([depois], state_of(antes, last_event_type="PROJECT_CLOSED"), DETECTED)
    assert types(events) == [("PROJECT_UPDATED", "P100001")]


def test_mudanca_em_projeto_ainda_em_pipeline_nao_publica():
    antes = project(status="Pipeline", approval=None)
    depois = project(status="Pipeline", approval=None, project_name="novo nome")
    events, state = pp.diff_events([depois], state_of(antes, last_event_type=None), DETECTED)
    assert events == []
    assert state["P100001"]["content_hash"] == content_hash(depois)


def test_estado_de_projetos_que_nao_vieram_na_consulta_e_mantido():
    antigo = project("P999999")
    _, state = pp.diff_events([project()], state_of(antigo), DETECTED)
    assert set(state) == {"P100001", "P999999"}


def test_rebusca_projetos_active_que_sumiram_da_consulta():
    state = {
        **state_of(project("P1"), project("P2"), project("P3")),
        **state_of(project("P4", status="Closed"), last_event_type="PROJECT_CLOSED"),
        **state_of(project("P5", status="Pipeline", approval=None), last_event_type=None),
    }
    # P1 veio na consulta; P2 e P3 eram Active e sumiram (encerrados?); Closed e Pipeline não são rebuscados
    assert pp.ids_to_recheck(state, seen={"P1"}) == ["P2", "P3"]


def test_coleta_do_live_une_active_janela_e_rebusca_sem_repetir():
    calls = []

    def fetch(**kwargs):
        calls.append(kwargs)
        if kwargs.get("status") == "Active":
            return [project("P1"), project("P2")]
        if kwargs.get("start_date"):
            return [project("P2"), project("P9", status="Closed")]
        return [project(pid, status="Closed") for pid in kwargs["ids"]]

    state = state_of(project("P1"), project("P3"))
    projects = pp.collect_live_projects(fetch, state, start_date="2024-10-28", chunk=50)
    assert [p["project_id"] for p in projects] == ["P1", "P2", "P9", "P3"]
    assert calls[2] == {"ids": ["P3"]}
    assert projects[-1]["status"] == "Closed"  # P3 rebuscado: diff_events vai gerar o CLOSED


def test_rebusca_em_lotes():
    chunks = []
    state = {
        f"P{i:06d}": {"content_hash": "x", "status": "Active", "last_event_type": "PROJECT_APPROVED"}
        for i in range(5)
    }

    def fetch(**kwargs):
        if "ids" in kwargs:
            chunks.append(kwargs["ids"])
        return []

    pp.collect_live_projects(fetch, state, start_date="2024-10-28", chunk=2)
    assert [len(c) for c in chunks] == [2, 2, 1]


# ---------------------------------------------------------------- configuração


def test_settings_le_o_ambiente_com_padroes():
    s = pp.Settings.from_env({})
    assert s.mode == "replay" and s.days_per_second == 30.0 and s.poll_minutes == 15
    s = pp.Settings.from_env({"PRODUCER_MODE": "live", "REPLAY_START": "2005-01-01", "POLL_MINUTES": "5"})
    assert s.mode == "live" and s.replay_start == date(2005, 1, 1) and s.poll_minutes == 5
    with pytest.raises(ValueError, match="PRODUCER_MODE"):
        pp.Settings.from_env({"PRODUCER_MODE": "outro"})


def test_closed_depois_de_updated_usa_hora_da_deteccao():
    # O UPDATED anterior tem event_time = hora da detecção (> closing_date); um CLOSED na data do fato
    # ficaria mais antigo e seria ignorado no rt e no silver, deixando o projeto Active para sempre.
    antes = project(closing="2026-10-01", total_commitment_usd=2e6)
    depois = project(status="Closed", closing="2026-10-01", total_commitment_usd=2e6)
    events, _ = pp.diff_events([depois], state_of(antes, last_event_type="PROJECT_UPDATED"), DETECTED)
    assert types(events) == [("PROJECT_CLOSED", "P100001")]
    assert events[0][1] == DETECTED


def test_closed_com_encerramento_antes_da_aprovacao_usa_a_aprovacao():
    # Como no replay: o CLOSED nunca fica antes do APPROVED do mesmo projeto
    antes = project(approval="2020-05-01", closing="2019-12-31")
    depois = project(status="Closed", approval="2020-05-01", closing="2019-12-31")
    events, _ = pp.diff_events([depois], state_of(antes), DETECTED)
    assert events[0][1] == date(2020, 5, 1)


def test_projeto_novo_encerrado_antes_da_aprovacao_closed_na_aprovacao():
    p = project(status="Closed", approval="2020-05-01", closing="2019-12-31")
    events, _ = pp.diff_events([p], {}, DETECTED)
    assert [(t, when) for t, when, _ in events] == [
        ("PROJECT_APPROVED", date(2020, 5, 1)),
        ("PROJECT_CLOSED", date(2020, 5, 1)),
    ]
