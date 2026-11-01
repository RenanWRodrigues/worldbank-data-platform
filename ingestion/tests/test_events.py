"""Contrato do evento: make_event gera eventos válidos no JSON Schema e event_id determinístico."""

import uuid
from datetime import UTC, date, datetime

import events
import jsonschema
import pytest
from kaggle_loader import kaggle_projects

PAYLOAD = {
    "project_id": "P123456",
    "status": "Active",
    "total_commitment_usd": 1e6,
    "board_approval_date": "2020-01-15",
    "source": "kaggle-replay",
}


def test_event_id_e_uuid5_deterministico():
    a = events.make_event("PROJECT_APPROVED", PAYLOAD, date(2020, 1, 15))
    b = events.make_event("PROJECT_APPROVED", dict(reversed(PAYLOAD.items())), date(2020, 1, 15))
    assert a["event_id"] == b["event_id"]  # reentrega/replay → mesmo id
    assert uuid.UUID(a["event_id"]).version == 5


@pytest.mark.parametrize(
    "outro",
    [
        {"event_type": "PROJECT_CLOSED"},
        {"payload": {**PAYLOAD, "status": "Closed"}},
        {"payload": {**PAYLOAD, "project_id": "P654321"}},
    ],
)
def test_event_id_muda_com_tipo_ou_conteudo(outro):
    base = {"event_type": "PROJECT_APPROVED", "payload": PAYLOAD}
    a = events.make_event(base["event_type"], base["payload"], date(2020, 1, 15))
    args = {**base, **outro}
    b = events.make_event(args["event_type"], args["payload"], date(2020, 1, 15))
    assert a["event_id"] != b["event_id"]


def test_event_time_de_data_e_meia_noite_utc_e_produced_at_e_agora():
    ev = events.make_event("PROJECT_APPROVED", PAYLOAD, date(2020, 1, 15))
    assert ev["event_time"] == "2020-01-15T00:00:00+00:00"
    produced = datetime.fromisoformat(ev["produced_at"])
    assert produced.tzinfo is not None and abs((datetime.now(UTC) - produced).total_seconds()) < 60


def test_event_time_de_datetime_mantem_a_hora():
    detected = datetime(2026, 10, 28, 15, 30, tzinfo=UTC)
    ev = events.make_event("PROJECT_UPDATED", PAYLOAD, detected, produced_at=detected)
    assert ev["event_time"] == ev["produced_at"] == "2026-10-28T15:30:00+00:00"


def test_source_e_project_id_vem_do_payload():
    ev = events.make_event("PROJECT_APPROVED", PAYLOAD, date(2020, 1, 15))
    assert ev["source"] == "kaggle-replay" and ev["project_id"] == "P123456"


def test_evento_gerado_valida_no_json_schema(kafka_schema):
    for event_type in events.EVENT_TYPES:
        ev = events.make_event(event_type, PAYLOAD, date(2020, 1, 15))
        jsonschema.validate(ev, kafka_schema)
        assert events.validate_event(ev) == []


def test_eventos_de_projetos_reais_da_amostra_validam(raw_dir, kafka_schema):
    for project in kaggle_projects(raw_dir / "projects_data.csv"):
        ev = events.make_event("PROJECT_APPROVED", project, date(2020, 1, 15))
        jsonschema.validate(ev, kafka_schema)


def test_validate_event_lista_os_erros():
    ev = events.make_event("PROJECT_APPROVED", PAYLOAD, date(2020, 1, 15))
    assert events.validate_event({**ev, "event_type": "PROJECT_DELETED"})
    assert events.validate_event({**ev, "payload": {**PAYLOAD, "total_commitment_usd": -1}})
    assert events.validate_event("não é objeto")


def test_serializacao_ida_e_volta():
    ev = events.make_event("PROJECT_APPROVED", PAYLOAD, date(2020, 1, 15))
    assert events.decode(events.encode(ev)) == ev
    assert events.decode(b"{quebrado") is None
