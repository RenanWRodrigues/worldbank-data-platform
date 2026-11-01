"""O contrato do evento é a interface entre producer, lake-sink, realtime-sink e o silver."""

import pytest

jsonschema = pytest.importorskip("jsonschema")


def test_evento_valido(kafka_schema, make_event):
    for t in ("PROJECT_APPROVED", "PROJECT_UPDATED", "PROJECT_CLOSED"):
        jsonschema.validate(make_event(t), kafka_schema)


@pytest.mark.parametrize("campo", ["event_id", "event_type", "event_time", "project_id", "payload"])
def test_evento_sem_campo_obrigatorio_e_invalido(kafka_schema, make_event, campo):
    ev = make_event()
    ev.pop(campo)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(ev, kafka_schema)


def test_tipo_desconhecido_e_valor_negativo_sao_invalidos(kafka_schema, make_event):
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(make_event("PROJECT_DELETED"), kafka_schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(make_event(total_commitment_usd=-1), kafka_schema)


@pytest.mark.parametrize("campo", ["ibrd_commitment_usd", "ida_commitment_usd", "grant_amount_usd"])
def test_valores_por_fonte_negativos_sao_invalidos(kafka_schema, make_event, campo):
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(make_event(**{campo: -1}), kafka_schema)


def test_valores_nulos_e_codigo_regional_sao_validos(kafka_schema, make_event):
    # Projeto regional: valor ausente e ISO2 "3A" passam no contrato; o silver decide que não é país
    jsonschema.validate(make_event(ibrd_commitment_usd=None, country_iso2="3A"), kafka_schema)


def test_data_fora_do_formato_e_invalida(kafka_schema, make_event):
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(make_event(board_approval_date="15/01/2020"), kafka_schema)


def test_contrato_documenta_os_dois_consumer_groups(kafka_schema):
    assert set(kafka_schema["x-kafka"]["consumer_groups"]) == {"lakehouse-sink", "serving-realtime"}
