"""Consumidor `serving-realtime`: eventos → rt.project_events, rt.projects_latest e rt.alerts em segundos.

Roda com UMA instância: o SSE da API ordena por rt.project_events.seq, e escritores concorrentes
poderiam tornar `seq` visível fora de ordem. Commit do offset só depois da transação no banco
(at-least-once, idempotente).
"""

from __future__ import annotations

import json
import logging
import os
import signal
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from events import SOURCE_AUTHORITY, TOPIC, decode, validate_event
from worldbank_client import canonical_sector

log = logging.getLogger("realtime-sink")

GROUP_ID = "serving-realtime"
LATEST_FIELDS = (
    "project_name",
    "country_iso2",
    "country_name",
    "region_name",
    "status",
    "total_commitment_usd",
    "board_approval_date",
    "closing_date",
    "lending_instrument",
    "major_sector",
)


@dataclass(frozen=True)
class BatchResult:
    """O que um lote mudou de fato: reentregas e eventos mais antigos que o estado atual não contam."""

    inserted: int = 0  # eventos novos em rt.project_events
    alerts: int = 0  # alertas novos em rt.alerts
    updated: int = 0  # projetos com estado atual inserido ou alterado em rt.projects_latest


def batch_summary(received: int, result: BatchResult) -> str:
    """Linha de log do lote: separa o recebido do gravado (num replay repetido tudo chega e nada entra)."""
    duplicates = received - result.inserted
    return (
        f"{received} eventos recebidos: {result.inserted} novos ({duplicates} duplicados), "
        f"{result.alerts} alertas, {result.updated} projetos com estado atualizado"
    )


@dataclass
class Rows:
    event: dict[str, Any]
    latest: dict[str, Any]
    alert: dict[str, Any] | None


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def to_rows(event: dict, partition: int, offset: int, alert_min: float) -> Rows:
    """Evento validado → linha de rt.project_events, candidata a rt.projects_latest e alerta (se houver)."""
    payload = event["payload"]
    event_time, produced_at = _ts(event["event_time"]), _ts(event["produced_at"])
    row = {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "project_id": event["project_id"],
        "event_time": event_time,
        "produced_at": produced_at,
        "source": event["source"],
        "payload": payload,
        "kafka_partition": partition,
        "kafka_offset": offset,
    }
    latest = {
        "project_id": event["project_id"],
        **{f: payload.get(f) for f in LATEST_FIELDS},
        "major_sector": canonical_sector(payload.get("major_sector")),  # payload intacto: event_id não muda
        "last_event_id": event["event_id"],
        "last_event_type": event["event_type"],
        "last_event_time": event_time,
        "last_produced_at": produced_at,
        "last_source": event["source"],
    }
    amount = payload.get("total_commitment_usd")
    alert = None
    if event["event_type"] == "PROJECT_APPROVED" and amount is not None and amount >= alert_min:
        name = payload.get("project_name") or event["project_id"]
        country = payload.get("country_name") or "regional"
        alert = {
            "event_id": event["event_id"],
            "project_id": event["project_id"],
            "kind": "LARGE_APPROVAL",
            "message": f"Aprovação de US$ {amount / 1e6:,.1f} mi: {name} ({country})",
            "amount_usd": amount,
            "event_time": event_time,
        }
    return Rows(event=row, latest=latest, alert=alert)


def _order(latest: dict) -> tuple[int, datetime, datetime]:
    """(autoridade da fonte, event_time, produced_at): a Projects API vence o replay (ADR 0009)."""
    return SOURCE_AUTHORITY[latest["last_source"]], latest["last_event_time"], latest["last_produced_at"]


def should_apply(current: dict | None, new: dict) -> bool:
    """Mesma regra do UPSERT_LATEST_SQL: só avança a ordem do `_order` e não reaplica o mesmo evento."""
    if current is None:
        return True
    return _order(new) > _order(current) and new["last_event_id"] != current["last_event_id"]


def latest_per_project(rows: Iterable[dict]) -> list[dict]:
    """Reduz o lote ao evento mais novo de cada projeto (o banco ainda compara com o estado gravado)."""
    best: dict[str, dict] = {}
    for row in rows:
        if should_apply(best.get(row["project_id"]), row):
            best[row["project_id"]] = row
    return list(best.values())


def rows_to_apply(rows: Iterable[Rows], inserted_ids: set[str]) -> list[Rows]:
    """Só eventos inseridos agora em rt.project_events (uma vez cada) decidem alertas e estado atual.

    Uma reentrega (mesmo `event_id`) já teve seu efeito aplicado na transação que a inseriu; num replay
    repetido ela volta com `produced_at` novo e, comparada de novo, reverteria um CLOSED do mesmo dia.
    """
    seen: set[str] = set()
    fresh = []
    for row in rows:
        event_id = row.event["event_id"]
        if event_id in inserted_ids and event_id not in seen:
            seen.add(event_id)
            fresh.append(row)
    return fresh


INSERT_EVENTS_SQL = """
INSERT INTO rt.project_events (event_id, event_type, project_id, event_time, produced_at, source, payload,
                               kafka_partition, kafka_offset)
VALUES %s
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id::text
"""

INSERT_ALERTS_SQL = """
INSERT INTO rt.alerts (event_id, project_id, kind, message, amount_usd, event_time)
VALUES %s
ON CONFLICT (event_id) DO NOTHING
"""

_LATEST_COLUMNS = ("project_id", *LATEST_FIELDS, "last_event_id", "last_event_type", "last_event_time",
                   "last_produced_at", "last_source")  # fmt: skip


def authority_sql(column: str) -> str:
    """CASE com SOURCE_AUTHORITY: a mesma autoridade do `_order` em SQL (fonte desconhecida = -1)."""
    whens = " ".join(f"WHEN '{s}' THEN {a}" for s, a in sorted(SOURCE_AUTHORITY.items()))
    return f"(CASE {column} {whens} ELSE -1 END)"


UPSERT_LATEST_SQL = f"""
INSERT INTO rt.projects_latest ({", ".join(_LATEST_COLUMNS)}, updated_at)
VALUES %s
ON CONFLICT (project_id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in _LATEST_COLUMNS[1:])},
    updated_at = now()
WHERE ({authority_sql("EXCLUDED.last_source")}, EXCLUDED.last_event_time, EXCLUDED.last_produced_at) >
      ({authority_sql("rt.projects_latest.last_source")}, rt.projects_latest.last_event_time,
       rt.projects_latest.last_produced_at)
  AND EXCLUDED.last_event_id <> rt.projects_latest.last_event_id
RETURNING project_id
"""
_LATEST_TEMPLATE = f"({', '.join(['%s'] * len(_LATEST_COLUMNS))}, now())"


def apply_batch(con, rows: list[Rows]) -> BatchResult:
    """Uma transação: eventos (antes dos alertas, por causa da FK), alertas e estado atual.

    Alertas e estado atual vêm só dos eventos que o INSERT acabou de gravar (`rows_to_apply`). Devolve as
    contagens do que mudou (o UPSERT com WHERE falso não devolve linha no RETURNING).
    """
    from psycopg2.extras import Json, execute_values

    with con, con.cursor() as cur:
        inserted = execute_values(
            cur,
            INSERT_EVENTS_SQL,
            [
                (
                    r.event["event_id"],
                    r.event["event_type"],
                    r.event["project_id"],
                    r.event["event_time"],
                    r.event["produced_at"],
                    r.event["source"],
                    Json(r.event["payload"]),
                    r.event["kafka_partition"],
                    r.event["kafka_offset"],
                )
                for r in rows
            ],
            fetch=True,
        )
        fresh = rows_to_apply(rows, {event_id for (event_id,) in inserted})
        alerts = [r.alert for r in fresh if r.alert]
        if alerts:
            execute_values(
                cur,
                INSERT_ALERTS_SQL,
                [
                    (
                        a["event_id"],
                        a["project_id"],
                        a["kind"],
                        a["message"],
                        a["amount_usd"],
                        a["event_time"],
                    )
                    for a in alerts
                ],
            )
        latest = latest_per_project(r.latest for r in fresh)
        result = BatchResult(inserted=len(inserted), alerts=len(alerts))
        if not latest:
            return result
        updated = execute_values(
            cur,
            UPSERT_LATEST_SQL,
            [tuple(row[c] for c in _LATEST_COLUMNS) for row in latest],
            _LATEST_TEMPLATE,
            fetch=True,
        )
        return BatchResult(inserted=result.inserted, alerts=result.alerts, updated=len(updated))


_stop = False


def _request_stop(*_):
    global _stop
    _stop = True


def main() -> None:
    from kafka_utils import connect_pg, make_consumer

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    alert_min = float(os.getenv("ALERT_MIN_COMMITMENT_USD", "500000000"))
    batch_size = int(os.getenv("RT_BATCH", "500"))
    con = connect_pg()
    consumer = make_consumer(GROUP_ID, [TOPIC])
    log.info("realtime-sink pronto (lote até %d, alerta a partir de US$ %.0f)", batch_size, alert_min)
    try:
        while not _stop:
            messages = consumer.consume(num_messages=batch_size, timeout=1.0)
            if not messages:
                continue
            rows = []
            for msg in messages:
                if msg.error():
                    log.error("erro do Kafka: %s", msg.error())
                    continue
                event = decode(msg.value())
                errors = validate_event(event) if event is not None else ["JSON inválido"]
                if errors:
                    log.warning(
                        "evento inválido descartado (offset %s): %s", msg.offset(), json.dumps(errors)[:300]
                    )
                    continue
                rows.append(to_rows(event, msg.partition(), msg.offset(), alert_min))
            if rows:
                log.info(batch_summary(len(rows), apply_batch(con, rows)))
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        con.close()


if __name__ == "__main__":
    main()
