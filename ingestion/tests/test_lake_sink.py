"""lake-sink: Buffer puro fecha micro-lotes por quantidade ou idade e descarta inválidos."""

import uuid
from datetime import UTC, date, datetime

import lake_sink as ls
from events import encode, make_event


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def event(pid="P100001", event_type="PROJECT_APPROVED"):
    payload = {"project_id": pid, "status": "Active", "source": "kaggle-replay"}
    return make_event(event_type, payload, date(2020, 1, 1))


def test_fecha_por_quantidade():
    buf = ls.Buffer(max_events=2, max_seconds=60, clock=Clock())
    buf.add(encode(event("P1")), partition=0, offset=10)
    assert not buf.should_flush()
    buf.add(encode(event("P2")), partition=0, offset=11)
    assert buf.should_flush()


def test_fecha_por_idade_do_primeiro_evento():
    clock = Clock()
    buf = ls.Buffer(max_events=500, max_seconds=60, clock=clock)
    assert not buf.should_flush()  # vazio nunca fecha
    buf.add(encode(event()), partition=0, offset=1)
    clock.now = 59
    assert not buf.should_flush()
    clock.now = 60
    assert buf.should_flush()


def test_invalidos_sao_descartados_mas_seus_offsets_contam():
    buf = ls.Buffer(max_events=10, max_seconds=60, clock=Clock())
    buf.add(b"{quebrado", partition=1, offset=5)
    buf.add(encode({**event(), "event_type": "PROJECT_DELETED"}), partition=1, offset=6)
    buf.add(encode(event()), partition=1, offset=7)
    batch = buf.drain()
    assert len(batch.events) == 1 and batch.invalid == 2
    assert batch.offsets == {1: (5, 7)}


def test_mensagens_so_invalidas_tambem_pedem_flush_para_o_commit_avancar():
    buf = ls.Buffer(max_events=2, max_seconds=60, clock=Clock())
    buf.add(b"x", partition=0, offset=1)
    buf.add(b"y", partition=0, offset=2)
    assert buf.should_flush()
    assert buf.drain().events == []


def test_reentrega_no_mesmo_lote_nao_duplica():
    buf = ls.Buffer(max_events=10, max_seconds=60, clock=Clock())
    ev = encode(event())
    buf.add(ev, partition=0, offset=1)
    buf.add(ev, partition=0, offset=2)
    assert len(buf.drain().events) == 1


def test_drain_esvazia_e_reinicia_a_idade():
    clock = Clock()
    buf = ls.Buffer(max_events=10, max_seconds=60, clock=clock)
    buf.add(encode(event()), partition=0, offset=1)
    buf.drain()
    clock.now = 1000
    assert len(buf) == 0 and not buf.should_flush()
    buf.add(encode(event()), partition=0, offset=2)
    assert not buf.should_flush()


def test_offsets_por_particao_e_faixa_geral():
    buf = ls.Buffer(max_events=10, max_seconds=60, clock=Clock())
    for partition, offset in [(3, 40), (0, 7), (3, 41), (0, 9)]:
        buf.add(encode(event(f"P{partition}{offset}")), partition=partition, offset=offset)
    batch = buf.drain()
    assert batch.offsets == {0: (7, 9), 3: (40, 41)}
    assert (batch.first_offset, batch.last_offset) == (7, 41)


def test_linha_da_landing_leva_particao_e_offset_do_kafka():
    # O silver usa o offset como último desempate da ordem dos eventos (event_time, produced_at, offset)
    buf = ls.Buffer(max_events=10, max_seconds=60, clock=Clock())
    ev = event()
    buf.add(encode(ev), partition=4, offset=77)
    (line,) = buf.drain().events
    assert line == {**ev, "kafka_partition": 4, "kafka_offset": 77}


def test_caminho_do_micro_lote():
    batch = ls.Batch(events=[], offsets={3: (40, 41), 0: (7, 9)}, invalid=0)
    uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    path = ls.batch_path(batch, datetime(2026, 10, 28, 23, 59, tzinfo=UTC), uid)
    assert path == "projects/events/date=2026-10-28/part-p0o7-9_p3o40-41-12345678.json"
