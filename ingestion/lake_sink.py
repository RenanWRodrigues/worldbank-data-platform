"""Consumidor `lakehouse-sink`: micro-lotes NDJSON → landing → ops.landing_files → commit (at-least-once).

Se o processo cair entre gravar e fazer commit, o lote é reescrito com outro nome;
o silver deduplica por event_id.
"""

from __future__ import annotations

import logging
import os
import signal
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from events import TOPIC, decode, validate_event

log = logging.getLogger("lake-sink")

GROUP_ID = "lakehouse-sink"
DATASET = "projects_events"


@dataclass
class Batch:
    events: list[dict]
    offsets: dict[int, tuple[int, int]]  # partição → (primeiro, último) offset
    invalid: int

    @property
    def first_offset(self) -> int | None:
        return min((first for first, _ in self.offsets.values()), default=None)

    @property
    def last_offset(self) -> int | None:
        return max((last for _, last in self.offsets.values()), default=None)


@dataclass
class Buffer:
    """Acumula eventos válidos até FLUSH_MAX_EVENTS mensagens ou FLUSH_MAX_SECONDS desde a primeira."""

    max_events: int
    max_seconds: float
    clock: Callable[[], float] = time.monotonic
    _events: dict[str, dict] = field(default_factory=dict)
    _offsets: dict[int, tuple[int, int]] = field(default_factory=dict)
    _messages: int = 0
    _invalid: int = 0
    _started: float | None = None

    def add(self, raw: bytes, partition: int, offset: int) -> None:
        if self._started is None:
            self._started = self.clock()
        self._messages += 1
        first, _ = self._offsets.get(partition, (offset, offset))
        self._offsets[partition] = (min(first, offset), offset)
        event = decode(raw)
        if event is None or validate_event(event):
            self._invalid += 1
            log.warning("mensagem inválida descartada (partição %s, offset %s)", partition, offset)
            return
        # Linha da landing = evento + posição no Kafka (último desempate da ordem no silver)
        record = {**event, "kafka_partition": partition, "kafka_offset": offset}
        self._events.setdefault(event["event_id"], record)  # reentrega no mesmo lote

    def __len__(self) -> int:
        return self._messages

    def should_flush(self) -> bool:
        if self._started is None:
            return False
        return self._messages >= self.max_events or self.clock() - self._started >= self.max_seconds

    def drain(self) -> Batch:
        batch = Batch(events=list(self._events.values()), offsets=dict(self._offsets), invalid=self._invalid)
        self._events, self._offsets = {}, {}
        self._messages = self._invalid = 0
        self._started = None
        return batch


def batch_path(batch: Batch, now: datetime, uid: uuid.UUID) -> str:
    """`projects/events/date=AAAA-MM-DD/part-p<partição>o<primeiro>-<último>_…-<uuid8>.json`."""
    offsets = "_".join(f"p{p}o{first}-{last}" for p, (first, last) in sorted(batch.offsets.items()))
    return f"projects/events/date={now.astimezone(UTC).date().isoformat()}/part-{offsets}-{uid.hex[:8]}.json"


REGISTER_SQL = """
INSERT INTO ops.landing_files (path, dataset, records, first_offset, last_offset)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (path) DO NOTHING
"""


_stop = False


def _request_stop(*_):
    global _stop
    _stop = True


def flush(batch: Batch, landing, con, consumer) -> None:
    """Grava o arquivo, registra em ops.landing_files e só então faz commit do offset."""
    if batch.events:
        from landing import to_ndjson

        path = landing.write(batch_path(batch, datetime.now(UTC), uuid.uuid4()), to_ndjson(batch.events))
        with con, con.cursor() as cur:
            cur.execute(
                REGISTER_SQL, (path, DATASET, len(batch.events), batch.first_offset, batch.last_offset)
            )
        log.info("micro-lote %s: %d eventos (%d inválidos)", path, len(batch.events), batch.invalid)
    consumer.commit(asynchronous=False)


def main() -> None:
    from kafka_utils import connect_pg, make_consumer
    from landing import make_landing

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    buffer = Buffer(
        max_events=int(os.getenv("FLUSH_MAX_EVENTS", "500")),
        max_seconds=float(os.getenv("FLUSH_MAX_SECONDS", "60")),
    )
    landing, con = make_landing(), connect_pg()
    consumer = make_consumer(GROUP_ID, [TOPIC])
    log.info("lake-sink em %s", type(landing).__name__)
    try:
        while not _stop:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    log.error("erro do Kafka: %s", msg.error())
                else:
                    buffer.add(msg.value(), msg.partition(), msg.offset())
            if buffer.should_flush():
                flush(buffer.drain(), landing, con, consumer)
        if len(buffer):  # desligamento limpo: não deixa o lote aberto para trás
            flush(buffer.drain(), landing, con, consumer)
    finally:
        consumer.close()
        con.close()


if __name__ == "__main__":
    main()
