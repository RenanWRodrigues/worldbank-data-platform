"""Contrato do evento do tópico worldbank.projects.events (docs/contratos/evento-kafka.schema.json)."""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, date, datetime
from functools import cache
from pathlib import Path
from typing import Any

from worldbank_client import content_hash

TOPIC = "worldbank.projects.events"
EVENT_TYPES = ("PROJECT_APPROVED", "PROJECT_UPDATED", "PROJECT_CLOSED")
# Autoridade da fonte no estado atual do projeto (ADR 0009): maior vence antes de event_time/produced_at.
# Mesmos valores de EVENT_SOURCE_AUTHORITY em databricks/src/wb_transforms.py (teste compara).
SOURCE_AUTHORITY = {"kaggle-replay": 0, "worldbank-projects-api": 1}
# Namespace fixo do uuid5: mudar invalida a deduplicação de tudo que já foi publicado
EVENT_NAMESPACE = uuid.UUID("5b0b6c4e-2f7a-4d55-9a3e-6c1f0e9d2b71")
SCHEMA_PATH = Path(
    os.getenv(
        "EVENT_SCHEMA_PATH", Path(__file__).resolve().parents[1] / "docs/contratos/evento-kafka.schema.json"
    )
)


def event_id(project_id: str, event_type: str, payload: dict[str, Any]) -> str:
    """uuid5(projeto|tipo|hash do payload): reentregas e replays geram o mesmo id."""
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{project_id}|{event_type}|{content_hash(payload)}"))


def to_timestamp(value: date | datetime) -> str:
    """Data → meia-noite UTC; datetime → ISO 8601 em UTC."""
    if not isinstance(value, datetime):
        value = datetime(value.year, value.month, value.day, tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def make_event(
    event_type: str,
    payload: dict[str, Any],
    event_time: date | datetime,
    produced_at: datetime | None = None,
) -> dict[str, Any]:
    """Monta o evento; `source` e `project_id` vêm do payload normalizado."""
    project_id = payload["project_id"]
    return {
        "event_id": event_id(project_id, event_type, payload),
        "event_type": event_type,
        "event_time": to_timestamp(event_time),
        "produced_at": to_timestamp(produced_at or datetime.now(UTC)),
        "source": payload["source"],
        "project_id": project_id,
        "payload": payload,
    }


@cache
def _validator():
    import jsonschema  # import tardio: só quem valida precisa da dependência

    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return jsonschema.Draft202012Validator(schema)


def validate_event(event: Any) -> list[str]:
    """Erros de validação contra o contrato; lista vazia = evento válido."""
    return [
        f"{'/'.join(map(str, e.absolute_path)) or '<raiz>'}: {e.message}"
        for e in _validator().iter_errors(event)
    ]


def encode(event: dict[str, Any]) -> bytes:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def decode(raw: bytes) -> dict[str, Any] | None:
    """Mensagem do Kafka → evento; JSON quebrado → None (o consumidor descarta e registra)."""
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
