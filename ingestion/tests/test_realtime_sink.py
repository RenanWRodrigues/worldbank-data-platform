"""realtime-sink: evento → linhas do rt, alerta e regra de upsert que nunca regride."""

from datetime import UTC, date, datetime

import pytest
import realtime_sink as rs
from events import make_event

ALERT_MIN = 500_000_000


def event(event_type="PROJECT_APPROVED", day=date(2020, 1, 15), produced=None, **payload):
    base = {
        "project_id": "P100001",
        "project_name": "Solar",
        "status": "Active",
        "country_name": "Kenya",
        "country_iso2": "KE",
        "region_name": "Africa",
        "total_commitment_usd": 1e6,
        "board_approval_date": "2020-01-15",
        "closing_date": "2025-12-31",
        "lending_instrument": "IPF",
        "major_sector": "Energy",
        "source": "kaggle-replay",
    }
    return make_event(event_type, {**base, **payload}, day, produced_at=produced)


def test_to_rows_evento_e_estado_atual():
    ev = event()
    rows = rs.to_rows(ev, partition=2, offset=99, alert_min=ALERT_MIN)
    assert rows.event["event_id"] == ev["event_id"]
    assert rows.event["kafka_partition"] == 2 and rows.event["kafka_offset"] == 99
    assert rows.event["payload"] == ev["payload"]
    latest = rows.latest
    assert latest["project_id"] == "P100001" and latest["country_iso2"] == "KE"
    assert latest["total_commitment_usd"] == 1e6
    assert latest["last_event_id"] == ev["event_id"]
    assert latest["last_event_type"] == "PROJECT_APPROVED"
    assert latest["last_event_time"] == datetime(2020, 1, 15, tzinfo=UTC)
    assert rows.alert is None


def test_estado_atual_usa_o_nome_canonico_do_setor_sem_mudar_o_payload():
    ev = event(major_sector="Agriculture; Fishing and Forestry")
    rows = rs.to_rows(ev, 0, 1, ALERT_MIN)
    assert rows.latest["major_sector"] == "Agriculture, Fishing and Forestry"
    assert rows.event["payload"]["major_sector"] == "Agriculture; Fishing and Forestry"  # event_id intacto


def test_alerta_para_aprovacao_grande():
    rows = rs.to_rows(event(total_commitment_usd=750e6), 0, 1, alert_min=ALERT_MIN)
    assert rows.alert["kind"] == "LARGE_APPROVAL"
    assert rows.alert["event_id"] == rows.event["event_id"]
    assert rows.alert["amount_usd"] == 750e6
    assert "750" in rows.alert["message"] and "Solar" in rows.alert["message"]


@pytest.mark.parametrize(
    ("event_type", "amount"),
    [("PROJECT_APPROVED", 499_999_999), ("PROJECT_APPROVED", None), ("PROJECT_CLOSED", 900e6)],
)
def test_sem_alerta_abaixo_do_limite_sem_valor_ou_que_nao_e_aprovacao(event_type, amount):
    assert rs.to_rows(event(event_type, total_commitment_usd=amount), 0, 1, ALERT_MIN).alert is None


def test_alerta_no_limite_exato():
    assert rs.to_rows(event(total_commitment_usd=ALERT_MIN), 0, 1, ALERT_MIN).alert is not None


# ---------------------------------------------------------------- nunca regride


T1 = datetime(2026, 10, 28, 10, 0, tzinfo=UTC)
T2 = datetime(2026, 10, 28, 10, 5, tzinfo=UTC)


def latest_of(ev):
    return rs.to_rows(ev, 0, 1, ALERT_MIN).latest


def test_evento_mais_novo_substitui():
    atual = latest_of(event(day=date(2020, 1, 15)))
    novo = latest_of(event("PROJECT_CLOSED", day=date(2025, 12, 31), status="Closed"))
    assert rs.should_apply(atual, novo)
    assert not rs.should_apply(novo, atual)  # evento atrasado não regride


def test_empate_no_mesmo_dia_desempata_por_produced_at():
    # APPROVED e CLOSED no mesmo dia: vale o publicado por último
    approved = latest_of(event(day=date(2020, 1, 15), produced=T1))
    closed = latest_of(event("PROJECT_CLOSED", day=date(2020, 1, 15), produced=T2, status="Closed"))
    assert rs.should_apply(approved, closed)
    assert not rs.should_apply(closed, approved)


def test_reentrega_do_mesmo_evento_nao_reaplica():
    atual = latest_of(event(produced=T1))
    assert not rs.should_apply(atual, dict(atual))


def test_sem_estado_atual_sempre_aplica():
    assert rs.should_apply(None, latest_of(event()))


def test_latest_per_project_fica_com_o_maior_do_lote():
    a = latest_of(event(day=date(2020, 1, 15), produced=T1))
    b = latest_of(event("PROJECT_CLOSED", day=date(2025, 12, 31), produced=T1, status="Closed"))
    c = latest_of(event(day=date(2020, 1, 15), produced=T2, project_id="P200002"))
    result = rs.latest_per_project([b, a, c])
    assert {r["project_id"]: r["last_event_type"] for r in result} == {
        "P100001": "PROJECT_CLOSED",
        "P200002": "PROJECT_APPROVED",
    }


def test_to_rows_guarda_a_fonte_do_evento():
    assert latest_of(event()).get("last_source") == "kaggle-replay"
    assert latest_of(event(source="worldbank-projects-api"))["last_source"] == "worldbank-projects-api"


# ---------------------------------------------------------------- ADR 0009: API vence o replay


def test_aprovacao_da_api_vence_encerramento_previsto_do_replay():
    # Projeto prorrogado: o replay encerra na data prevista do Kaggle (2023), a API diz Active e publica
    # APPROVED com event_time = aprovação (2018). Mesmo mais antigo, o evento da API vence.
    replay_closed = latest_of(
        event(
            "PROJECT_CLOSED", day=date(2023, 12, 31), produced=T1, status="Closed", closing_date="2023-12-31"
        )
    )
    api_approved = latest_of(
        event(day=date(2018, 6, 27), produced=T1, source="worldbank-projects-api", closing_date="2025-12-31")
    )
    assert rs.should_apply(replay_closed, api_approved)
    assert not rs.should_apply(api_approved, replay_closed)


def test_replay_depois_do_live_nao_sobrescreve_a_api():
    api_updated = latest_of(
        event("PROJECT_UPDATED", day=date(2026, 10, 1), produced=T1, source="worldbank-projects-api")
    )
    replay_mais_novo = latest_of(
        event("PROJECT_CLOSED", day=date(2026, 10, 20), produced=T2, status="Closed")
    )
    assert not rs.should_apply(api_updated, replay_mais_novo)


def test_dentro_da_api_vale_a_ordem_por_event_time():
    api = {"source": "worldbank-projects-api"}
    approved = latest_of(event(day=date(2018, 6, 27), produced=T1, **api))
    closed = latest_of(event("PROJECT_CLOSED", day=date(2025, 12, 31), produced=T1, status="Closed", **api))
    assert rs.should_apply(approved, closed)
    assert not rs.should_apply(closed, approved)


def test_latest_per_project_prefere_a_api_no_mesmo_lote():
    replay = latest_of(event("PROJECT_CLOSED", day=date(2023, 12, 31), status="Closed"))
    api = latest_of(event(day=date(2018, 6, 27), source="worldbank-projects-api"))
    [best] = rs.latest_per_project([api, replay])
    assert best["last_source"] == "worldbank-projects-api" and best["status"] == "Active"


def test_authority_sql_cobre_todas_as_fontes_do_contrato():
    import json
    from pathlib import Path

    schema = json.loads(
        (Path(__file__).resolve().parents[2] / "docs/contratos/evento-kafka.schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert set(rs.SOURCE_AUTHORITY) == set(schema["properties"]["source"]["enum"])
    sql = rs.authority_sql("x")
    for source, authority in rs.SOURCE_AUTHORITY.items():
        assert f"WHEN '{source}' THEN {authority}" in sql


def test_upsert_sql_tem_a_mesma_regra_do_should_apply():
    sql = " ".join(rs.UPSERT_LATEST_SQL.split())
    assert (
        f"({rs.authority_sql('EXCLUDED.last_source')}, EXCLUDED.last_event_time, "
        "EXCLUDED.last_produced_at) > "
        f"({rs.authority_sql('rt.projects_latest.last_source')}, rt.projects_latest.last_event_time, "
        "rt.projects_latest.last_produced_at)"
    ) in sql
    assert "last_source = EXCLUDED.last_source" in sql
    assert "EXCLUDED.last_event_id <> rt.projects_latest.last_event_id" in sql
    assert "ON CONFLICT (event_id) DO NOTHING" in rs.INSERT_EVENTS_SQL
    assert "ON CONFLICT (event_id) DO NOTHING" in rs.INSERT_ALERTS_SQL


def rows_of(ev, offset=1):
    return rs.to_rows(ev, 0, offset, ALERT_MIN)


def test_reentrega_com_produced_at_novo_nao_entra_no_estado_atual():
    # Replay repetido: mesmo event_id, produced_at novo. O APPROVED reentregue não pode vencer o CLOSED
    # do mesmo dia já aplicado; só eventos inseridos agora em rt.project_events decidem o estado atual.
    reenvio = rows_of(event(day=date(2020, 1, 15), produced=T2))
    assert rs.rows_to_apply([reenvio], inserted_ids=set()) == []


def test_rows_to_apply_fica_com_os_inseridos_sem_repetir_event_id():
    a = rows_of(event(produced=T1), offset=1)
    a_de_novo = rows_of(event(produced=T2), offset=2)  # mesmo event_id duas vezes no lote
    b = rows_of(event("PROJECT_CLOSED", day=date(2025, 12, 31), status="Closed"), offset=3)
    result = rs.rows_to_apply([a, a_de_novo, b], inserted_ids={a.event["event_id"], b.event["event_id"]})
    assert [r.event["kafka_offset"] for r in result] == [1, 3]


def test_insert_events_devolve_os_ids_inseridos():
    assert "RETURNING event_id" in rs.INSERT_EVENTS_SQL
    assert rs.UPSERT_LATEST_SQL.rstrip().endswith("RETURNING project_id")


def test_batch_summary_separa_recebidos_de_gravados():
    replay_repetido = rs.batch_summary(13, rs.BatchResult())
    assert replay_repetido == (
        "13 eventos recebidos: 0 novos (13 duplicados), 0 alertas, 0 projetos com estado atualizado"
    )
    assert rs.batch_summary(5, rs.BatchResult(inserted=4, alerts=1, updated=3)).startswith(
        "5 eventos recebidos: 4 novos (1 duplicados), 1 alertas, 3 projetos"
    )
