"""Producer de eventos de projetos (serviço contínuo): APPROVED, UPDATED e CLOSED por projeto.

* replay: histórico do Kaggle em ordem cronológica com relógio acelerado (REPLAY_DAYS_PER_SECOND).
* live: Projects API a cada POLL_MINUTES; publica só o que mudou em relação a ops.producer_state.

`replay_schedule` e `diff_events` são puras; `main()` só faz I/O.
"""

from __future__ import annotations

import logging
import os
import signal
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from events import TOPIC, encode, make_event, validate_event
from worldbank_client import content_hash

log = logging.getLogger("projects-producer")

APPROVED, UPDATED, CLOSED = "PROJECT_APPROVED", "PROJECT_UPDATED", "PROJECT_CLOSED"
APPROVED_STATUSES = {"Active", "Closed"}
_TYPE_ORDER = {APPROVED: 0, UPDATED: 1, CLOSED: 2}

ScheduledEvent = tuple[date, str, dict[str, Any]]
DiffEvent = tuple[str, date | datetime, dict[str, Any]]


# ---------------------------------------------------------------- replay


def _to_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def replay_schedule(projects: Iterable[dict], today: date, start: date | None = None) -> list[ScheduledEvent]:
    """Eventos do replay em ordem cronológica (data, tipo, payload).

    APPROVED (status Active) na aprovação; CLOSED (status Closed) no encerramento se ≤ hoje. Projetos sem
    aprovação, fora de Active/Closed (Pipeline, Dropped) ou aprovados antes de `start` são ignorados;
    nada é agendado depois de hoje.
    """
    schedule: list[ScheduledEvent] = []
    for project in projects:
        approved = _to_date(project.get("board_approval_date"))
        if approved is None or project.get("status") not in APPROVED_STATUSES or approved > today:
            continue
        if start is not None and approved < start:
            continue
        schedule.append((approved, APPROVED, {**project, "status": "Active"}))
        closing = _to_date(project.get("closing_date"))
        if closing is not None and closing <= today:
            schedule.append((max(closing, approved), CLOSED, {**project, "status": "Closed"}))
    schedule.sort(key=lambda e: (e[0], _TYPE_ORDER[e[1]], e[2]["project_id"]))
    return schedule


def replay_delays(schedule: list[ScheduledEvent], days_per_second: float) -> list[float]:
    """Segundos de espera antes de cada evento, pelo avanço do relógio simulado."""
    delays, previous = [], None
    for day, _, _ in schedule:
        delays.append(0.0 if previous is None else (day - previous).days / days_per_second)
        previous = day
    return delays


# ---------------------------------------------------------------- live


def _state_row(project: dict, last_event_type: str | None) -> dict[str, Any]:
    return {
        "content_hash": content_hash(project),
        "status": project.get("status"),
        "last_event_type": last_event_type,
    }


def _approved_events(project: dict, detected_at: datetime) -> list[DiffEvent]:
    """Projeto publicado pela primeira vez: APPROVED e, se já encerrado, CLOSED em seguida."""
    approved = _to_date(project.get("board_approval_date"))
    if approved is None or project.get("status") not in APPROVED_STATUSES:
        return []  # Pipeline/Dropped ou sem data: espera a aprovação
    events: list[DiffEvent] = [(APPROVED, approved, {**project, "status": "Active"})]
    if project["status"] == "Closed":
        events.append((CLOSED, _closed_time(project, None, detected_at), project))
    return events


def _closed_time(project: dict, previous_type: str | None, detected_at: datetime) -> date | datetime:
    """event_time do CLOSED: nunca anterior ao último evento publicado do projeto.

    Depois de um UPDATED (event_time = hora da detecção) vale a hora da detecção; senão a data de
    encerramento, presa na aprovação como no replay; sem data de encerramento, a hora da detecção.
    """
    closing = _to_date(project.get("closing_date"))
    if previous_type == UPDATED or closing is None:
        return detected_at
    approved = _to_date(project.get("board_approval_date"))
    return max(closing, approved) if approved else closing


def diff_events(
    projects: Iterable[dict], state: Mapping[str, dict], detected_at: datetime
) -> tuple[list[DiffEvent], dict[str, dict]]:
    """Compara a consulta da API com ops.producer_state → (eventos a publicar, novo estado).

    Nunca publicado (novo ou ainda em Pipeline) e agora Active/Closed → APPROVED (+ CLOSED se encerrado);
    passou a Closed → CLOSED; qualquer outra mudança de conteúdo → UPDATED na hora da detecção.
    """
    new_state = {pid: dict(row) for pid, row in state.items()}
    events: list[DiffEvent] = []
    for project in projects:
        pid = project["project_id"]
        previous = state.get(pid)
        if previous is not None and previous["content_hash"] == content_hash(project):
            continue
        if previous is None or previous.get("last_event_type") is None:
            produced = _approved_events(project, detected_at)
        elif project.get("status") == "Closed" and previous.get("status") != "Closed":
            when = _closed_time(project, previous.get("last_event_type"), detected_at)
            produced = [(CLOSED, when, project)]
        else:
            produced = [(UPDATED, detected_at, project)]
        events.extend(produced)
        last_type = produced[-1][0] if produced else (previous or {}).get("last_event_type")
        new_state[pid] = _state_row(project, last_type)
    return events, new_state


def ids_to_recheck(state: Mapping[str, dict], seen: set[str]) -> list[str]:
    """Projetos que estavam Active e não vieram na consulta: provavelmente encerrados, rebuscados por id."""
    return sorted(pid for pid, row in state.items() if row.get("status") == "Active" and pid not in seen)


def collect_live_projects(
    fetch: Callable[..., list[dict]], state: Mapping[str, dict], start_date: str, chunk: int = 50
) -> list[dict]:
    """Consulta do live: todos os Active ∪ aprovados desde `start_date` ∪ rebusca dos que saíram de Active.

    Só a janela de aprovação perderia mudanças em projetos antigos (ex.: o encerramento de um de 2015).
    """
    projects: dict[str, dict] = {}
    for found in (fetch(status="Active"), fetch(start_date=start_date)):
        for project in found:
            projects.setdefault(project["project_id"], project)
    missing = ids_to_recheck(state, set(projects))
    for i in range(0, len(missing), chunk):
        for project in fetch(ids=missing[i : i + chunk]):
            projects.setdefault(project["project_id"], project)
    return list(projects.values())


# ---------------------------------------------------------------- configuração e I/O


@dataclass(frozen=True)
class Settings:
    mode: str
    raw_dir: Path
    replay_start: date | None
    days_per_second: float
    poll_minutes: int
    lookback_days: int

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Settings:
        mode = env.get("PRODUCER_MODE", "replay") or "replay"
        if mode not in {"replay", "live"}:
            raise ValueError(f"PRODUCER_MODE inválido: {mode!r} (use replay ou live)")
        start = env.get("REPLAY_START", "")
        return cls(
            mode=mode,
            raw_dir=Path(env.get("RAW_DATA_DIR", "/data/raw")),
            replay_start=date.fromisoformat(start) if start else None,
            days_per_second=float(env.get("REPLAY_DAYS_PER_SECOND", "30")),
            poll_minutes=int(env.get("POLL_MINUTES", "15")),
            lookback_days=int(env.get("LIVE_LOOKBACK_DAYS", "730")),
        )


class Publisher:
    """Publica eventos validados com chave = project_id e conta falhas de entrega e eventos descartados."""

    def __init__(self, producer):
        self.producer = producer
        self.failures = 0
        self.sent = 0
        self.rejected = 0

    def _on_delivery(self, err, _msg):
        if err is not None:
            self.failures += 1
            log.error("falha na entrega: %s", err)

    def publish(self, event: dict) -> bool:
        """Publica o evento; False se ele foi descartado por estar fora do contrato."""
        errors = validate_event(event)
        if errors:
            # Descarta em vez de levantar: com restart on-failure, um projeto inválido derrubaria o producer
            # a cada ciclo do live e nenhum outro evento sairia
            self.rejected += 1
            log.warning("evento fora do contrato descartado (%s): %s", event.get("project_id"), errors)
            return False
        while True:
            try:
                self.producer.produce(
                    TOPIC, key=event["project_id"], value=encode(event), on_delivery=self._on_delivery
                )
                break
            except BufferError:  # fila local cheia: espera o broker drenar
                self.producer.poll(1)
        self.producer.poll(0)
        self.sent += 1
        return True

    def flush(self) -> None:
        remaining = self.producer.flush(60)
        if remaining or self.failures:
            raise RuntimeError(f"{remaining} mensagens não entregues, {self.failures} falhas")


_stop = False


def _request_stop(*_):
    global _stop
    _stop = True


def run_replay(settings: Settings, publisher: Publisher) -> None:
    from kaggle_loader import kaggle_projects

    projects = kaggle_projects(settings.raw_dir / "projects_data.csv")
    schedule = replay_schedule(projects, today=datetime.now(UTC).date(), start=settings.replay_start)
    log.info("replay: %d projetos, %d eventos", len(projects), len(schedule))
    for (day, event_type, payload), delay in zip(
        schedule, replay_delays(schedule, settings.days_per_second), strict=True
    ):
        if _stop:
            break
        if delay:
            publisher.producer.poll(0)
            time.sleep(delay)
        publisher.publish(make_event(event_type, payload, day))
        if publisher.sent % 500 == 0:
            log.info("replay: %d eventos publicados (relógio em %s)", publisher.sent, day)
    publisher.flush()
    log.info("replay concluído: %d eventos publicados, %d descartados", publisher.sent, publisher.rejected)


def load_state(con) -> dict[str, dict]:
    with con.cursor() as cur:
        cur.execute("SELECT project_id, content_hash, status, last_event_type FROM ops.producer_state")
        return {r[0]: {"content_hash": r[1], "status": r[2], "last_event_type": r[3]} for r in cur.fetchall()}


def save_state(con, state: Mapping[str, dict], changed: Iterable[str]) -> None:
    rows = [
        (pid, state[pid]["content_hash"], state[pid]["status"], state[pid]["last_event_type"])
        for pid in changed
    ]
    if not rows:
        return  # ciclo do live sem mudanças: nada a gravar
    with con, con.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO ops.producer_state (project_id, content_hash, status, last_event_type, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (project_id) DO UPDATE SET content_hash = EXCLUDED.content_hash,
                status = EXCLUDED.status, last_event_type = EXCLUDED.last_event_type, updated_at = now()
            """,
            rows,
        )


def run_live(settings: Settings, publisher: Publisher) -> None:
    from kafka_utils import connect_pg
    from worldbank_client import WorldBankAPIError, fetch_projects

    con = connect_pg()
    api_failures = 0
    while not _stop:
        now = datetime.now(UTC)
        start = (now - timedelta(days=settings.lookback_days)).date().isoformat()
        state = load_state(con)
        try:
            projects = collect_live_projects(fetch_projects, state, start_date=start)
        except WorldBankAPIError as exc:
            # Queda de rede ou da API (já com retries): pula o ciclo sem publicar nem salvar estado. Falha do
            # Kafka continua derrubando o processo, porque o estado só pode ser salvo depois da confirmação
            api_failures += 1
            log.warning(
                "live: Projects API indisponível (%d ciclo(s) seguido(s)); nova tentativa em %d min: %s",
                api_failures,
                settings.poll_minutes,
                str(exc)[:300],
            )
            _wait(settings)
            continue
        api_failures = 0
        events, new_state = diff_events(projects, state, now)
        rejected: set[str] = set()
        for event_type, event_time, payload in events:
            if not publisher.publish(make_event(event_type, payload, event_time)):
                rejected.add(payload["project_id"])
        publisher.flush()  # estado só é salvo depois que o Kafka confirmou
        # Projeto com evento descartado mantém o estado antigo: é retentado a cada ciclo (e sai assim que o
        # contrato/cliente for corrigido); com o hash novo salvo, nunca mais seria publicado
        changed = [pid for pid, row in new_state.items() if state.get(pid) != row and pid not in rejected]
        save_state(con, new_state, changed)
        log.info(
            "live: %d projetos consultados, %d eventos, %d estados alterados, %d descartados (total)",
            len(projects),
            len(events),
            len(changed),
            publisher.rejected,
        )
        _wait(settings)
    con.close()


def _wait(settings: Settings) -> None:
    """Espera POLL_MINUTES em passos de 1 s, para o SIGTERM interromper logo."""
    for _ in range(settings.poll_minutes * 60):
        if _stop:
            break
        time.sleep(1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    settings = Settings.from_env(os.environ)
    from kafka_utils import make_producer

    publisher = Publisher(make_producer())
    log.info("modo %s", settings.mode)
    if settings.mode == "replay":
        run_replay(settings, publisher)
    else:
        run_live(settings, publisher)


if __name__ == "__main__":
    main()
