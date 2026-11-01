"""Aceitação da stack local (Fases 6 e 7).

Rode `make up` e `make replay` (ou `make live`) pelo menos uma vez com os sinks no ar; depois
`make test-integration`. Os testes não dependem de chegar evento novo durante a execução.
"""

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
requests = pytest.importorskip("requests")
psycopg2 = pytest.importorskip("psycopg2")

API = os.getenv("API_URL", "http://localhost:8000")
ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


def setting(name: str, default: str | None = None) -> str | None:
    """Variável do ambiente ou, se ausente, do .env usado pelo docker compose (valor nunca é exibido)."""
    if name in os.environ:
        return os.environ[name]
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            if sep and key.strip() == name:
                return value.strip().strip("'\"")
    return default


def connect(user: str, password: str | None):
    # Do host: localhost e a porta publicada (POSTGRES_HOST/POSTGRES_PORT do .env valem dentro do Docker)
    return psycopg2.connect(
        host="localhost",
        port=int(setting("POSTGRES_HOST_PORT", "5432")),
        dbname=setting("POSTGRES_DB", "worldbank"),
        user=user,
        password=password,
    )


@pytest.fixture(scope="module")
def pg():
    con = connect(setting("POSTGRES_USER", "wb"), setting("POSTGRES_PASSWORD"))
    con.autocommit = True
    yield con
    con.close()


def q(pg, sql):
    with pg.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def test_schemas_ops_rt_dw(pg):
    assert {"ops", "rt", "dw"} <= {r[0] for r in q(pg, "select schema_name from information_schema.schemata")}


@pytest.mark.parametrize(
    ("user", "password_var", "pode_ler"),
    [
        ("powerbi", "POWERBI_PASSWORD", ["dw.dim_country", "rt.projects_latest"]),
        ("api_ro", "API_RO_PASSWORD", ["dw.dim_country", "rt.projects_latest", "ops.kafka_lag"]),
    ],
)
def test_schemas_usuarios_de_leitura_leem_e_nao_escrevem(user, password_var, pode_ler):
    con = connect(user, setting(password_var))
    try:
        for table in pode_ler:
            q(con, f"select count(*) from {table}")
        con.rollback()
        with pytest.raises(psycopg2.Error):
            q(con, "insert into dw.load_audit (table_name, row_count, run_id) values ('x', 0, 'x')")
    finally:
        con.close()


def test_schemas_powerbi_nao_le_ops():
    con = connect("powerbi", setting("POWERBI_PASSWORD"))
    try:
        with pytest.raises(psycopg2.errors.InsufficientPrivilege):
            q(con, "select 1 from ops.landing_files limit 1")
    finally:
        con.close()


def test_rt_upsert_nunca_regride_no_banco(pg):
    """SQL real do realtime-sink: empate no mesmo dia, atraso e reentrega (projeto de teste é removido)."""
    from datetime import UTC, date, datetime

    import realtime_sink as rs
    from events import make_event

    pid = "P9999999"
    t1, t2 = datetime(2026, 1, 1, 10, tzinfo=UTC), datetime(2026, 1, 1, 10, 5, tzinfo=UTC)
    t3 = datetime(2026, 1, 1, 11, tzinfo=UTC)
    payload = {
        "project_id": pid,
        "status": "Active",
        "total_commitment_usd": 900e6,
        "source": "kaggle-replay",
    }
    approved = make_event("PROJECT_APPROVED", payload, date(2020, 1, 15), produced_at=t1)
    closed = make_event("PROJECT_CLOSED", {**payload, "status": "Closed"}, date(2020, 1, 15), produced_at=t2)

    def apply(*events):
        rs.apply_batch(pg, [rs.to_rows(e, 0, 0, alert_min=500e6) for e in events])
        return q(pg, f"select status, last_event_type from rt.projects_latest where project_id = '{pid}'")[0]

    con_autocommit = pg.autocommit
    pg.autocommit = False
    try:
        assert apply(approved) == ("Active", "PROJECT_APPROVED")
        assert apply(closed) == ("Closed", "PROJECT_CLOSED")  # mesmo dia, produced_at maior
        assert apply(approved) == ("Closed", "PROJECT_CLOSED")  # atrasado/reentregue não regride
        # Replay repetido: mesmo event_id com produced_at novo — também não regride
        reenvio = make_event("PROJECT_APPROVED", payload, date(2020, 1, 15), produced_at=t3)
        assert reenvio["event_id"] == approved["event_id"]
        assert apply(reenvio) == ("Closed", "PROJECT_CLOSED")
        assert q(pg, f"select count(*) from rt.project_events where project_id = '{pid}'")[0][0] == 2
        assert q(pg, f"select count(*) from rt.alerts where project_id = '{pid}'")[0][0] == 1
    finally:
        with pg, pg.cursor() as cur:
            for table in ("rt.alerts", "rt.projects_latest", "rt.project_events"):
                cur.execute(f"delete from {table} where project_id = %s", (pid,))
        pg.autocommit = con_autocommit


def test_rt_evento_da_api_vence_replay_no_banco(pg):
    """ADR 0009 com o SQL real: CLOSED previsto do replay perde para o APPROVED (mais antigo) da API."""
    from datetime import UTC, date, datetime

    import realtime_sink as rs
    from events import make_event

    pid = "P9999998"
    t1 = datetime(2026, 1, 1, 10, tzinfo=UTC)
    base = {"project_id": pid, "status": "Active", "total_commitment_usd": 1e6}
    replay_closed = make_event(
        "PROJECT_CLOSED",
        {**base, "status": "Closed", "source": "kaggle-replay"},
        date(2023, 12, 31),
        produced_at=t1,
    )
    api_approved = make_event(
        "PROJECT_APPROVED", {**base, "source": "worldbank-projects-api"}, date(2018, 6, 27), produced_at=t1
    )
    replay_tardio = make_event(
        "PROJECT_CLOSED",
        {**base, "status": "Closed", "source": "kaggle-replay", "closing_date": "2026-01-01"},
        date(2026, 1, 1),
        produced_at=t1,
    )

    def apply(*events):
        rs.apply_batch(pg, [rs.to_rows(e, 0, 0, alert_min=500e6) for e in events])
        sql = f"select status, last_source from rt.projects_latest where project_id = '{pid}'"
        return q(pg, sql)[0]

    con_autocommit = pg.autocommit
    pg.autocommit = False
    try:
        assert apply(replay_closed) == ("Closed", "kaggle-replay")
        assert apply(api_approved) == ("Active", "worldbank-projects-api")
        assert apply(replay_tardio) == ("Active", "worldbank-projects-api")
    finally:
        with pg, pg.cursor() as cur:
            for table in ("rt.alerts", "rt.projects_latest", "rt.project_events"):
                cur.execute(f"delete from {table} where project_id = %s", (pid,))
        pg.autocommit = con_autocommit


def test_api_saudavel():
    assert requests.get(f"{API}/health", timeout=5).json()["status"] == "ok"


def test_eventos_chegam_ao_rt_em_segundos(pg):
    # Latência produzido → gravado nos últimos eventos. Contar linhas novas não serve: replays repetidos
    # geram os mesmos event_id e, por idempotência, não acrescentam nada ao rt.
    rows = q(
        pg,
        "select percentile_cont(0.5) within group (order by extract(epoch from consumed_at - produced_at)) "
        "from (select consumed_at, produced_at from rt.project_events order by seq desc limit 50) t",
    )
    if rows[0][0] is None:
        pytest.skip("rt.project_events vazio — rode `make replay` com os sinks no ar")
    assert rows[0][0] <= 30, f"mediana de {rows[0][0]:.0f} s entre publicação e rt (esperado: segundos)"


def test_sem_eventos_duplicados(pg):
    assert q(pg, "select count(*) - count(distinct event_id) from rt.project_events")[0][0] == 0


def test_lake_sink_registra_micro_lotes(pg):
    assert q(pg, "select count(*) from ops.landing_files where dataset = 'projects_events'")[0][0] > 0


def test_api_live_e_status():
    live = requests.get(f"{API}/v1/live/projects?limit=3", timeout=5).json()
    assert live and {"project_id", "last_event_type", "last_event_time"} <= set(live[0])
    status = requests.get(f"{API}/v1/pipeline/status", timeout=5).json()
    # events_last_hour depende de haver replay/live recente; o que importa aqui é o rt ter recebido eventos
    assert status["events_last_hour"] >= 0 and status["last_event_at"] is not None


def test_sse_entrega_evento():
    # backfill=1: reenvia o último evento do rt, sem depender de chegar evento novo durante o teste
    with requests.get(f"{API}/v1/live/stream?backfill=1&max_events=1", stream=True, timeout=60) as r:
        texto = "".join(chunk.decode() for chunk in r.iter_content(chunk_size=None))
    assert "event: PROJECT_" in texto
