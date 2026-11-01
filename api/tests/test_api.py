"""API de serving com TestClient + FakeRepo em memória (sem Postgres)."""

from datetime import UTC, date, datetime

import pytest
from app import main
from fastapi.testclient import TestClient

T0 = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)

COUNTRIES = [
    {
        "country_code": "BRA",
        "country_iso2": "BR",
        "country_name": "Brazil",
        "region": "Latin America & Caribbean",
        "income_level": "Upper middle income",
        "is_aggregate": False,
    },
    {
        "country_code": "KEN",
        "country_iso2": "KE",
        "country_name": "Kenya",
        "region": "Sub-Saharan Africa",
        "income_level": "Lower middle income",
        "is_aggregate": False,
    },
    {
        "country_code": "WLD",
        "country_iso2": "1W",
        "country_name": "World",
        "region": "Aggregates",
        "income_level": "Aggregates",
        "is_aggregate": True,
    },
]
INDICATORS = [
    {"indicator_code": "SP.POP.TOTL", "indicator_name": "Population, total", "unit": "count"},
    {"indicator_code": "EG.ELC.ACCS.ZS", "indicator_name": "Access to electricity", "unit": "%"},
]
FACTS = [
    ("BRA", "SP.POP.TOTL", 2022, 215e6),
    ("BRA", "SP.POP.TOTL", 2023, 216e6),
    ("KEN", "SP.POP.TOTL", 2023, 55e6),
    ("WLD", "SP.POP.TOTL", 2023, 8e9),
    ("KEN", "EG.ELC.ACCS.ZS", 2023, 76.0),
]
PROJECTS = [
    {
        "project_id": "P123456",
        "country_code": "KEN",
        "country_name_raw": "Kenya",
        "region_name": "Eastern and Southern Africa",
        "project_name": "Solar",
        "status": "Active",
        "lending_instrument": "IPF",
        "major_sector": "Energy",
        "board_approval_date": date(2024, 3, 21),
        "approval_year": 2024,
        "closing_date": date(2029, 12, 31),
        "total_amount_usd": 750e6,
        "ibrd_amount_usd": 0.0,
        "ida_amount_usd": 750e6,
        "grant_amount_usd": None,
        "last_event_type": "PROJECT_APPROVED",
        "last_event_time": datetime(2024, 3, 21, tzinfo=UTC),
    },
    {
        "project_id": "P100001",
        "country_code": "BRA",
        "country_name_raw": "Brazil",
        "region_name": "Latin America and Caribbean",
        "project_name": "Saneamento",
        "status": "Closed",
        "lending_instrument": "IPF",
        "major_sector": "Water",
        "board_approval_date": date(2010, 5, 1),
        "approval_year": 2010,
        "closing_date": date(2016, 6, 30),
        "total_amount_usd": 100e6,
        "ibrd_amount_usd": 100e6,
        "ida_amount_usd": 0.0,
        "grant_amount_usd": 0.0,
        "last_event_type": "PROJECT_CLOSED",
        "last_event_time": datetime(2016, 6, 30, tzinfo=UTC),
    },
]
EVENTS = [
    {
        "seq": i + 1,
        "event_id": f"00000000-0000-0000-0000-00000000000{i}",
        "event_type": t,
        "project_id": "P123456",
        "event_time": datetime(2024, 3, 21, tzinfo=UTC),
        "produced_at": T0,
        "source": "kaggle-replay",
        "payload": {"project_id": "P123456", "status": "Active"},
        "consumed_at": T0,
    }
    for i, t in enumerate(["PROJECT_APPROVED", "PROJECT_UPDATED", "PROJECT_CLOSED"])
]
LIVE = [
    {
        "project_id": "P123456",
        "project_name": "Solar",
        "country_iso2": "KE",
        "country_name": "Kenya",
        "region_name": "Eastern and Southern Africa",
        "status": "Active",
        "total_commitment_usd": 750e6,
        "board_approval_date": date(2024, 3, 21),
        "closing_date": date(2029, 12, 31),
        "lending_instrument": "IPF",
        "major_sector": "Energy",
        "last_event_id": EVENTS[0]["event_id"],
        "last_event_type": "PROJECT_APPROVED",
        "last_event_time": datetime(2024, 3, 21, tzinfo=UTC),
        "last_produced_at": T0,
        "updated_at": T0,
    }
]
ALERTS = [
    {
        "alert_id": 1,
        "event_id": EVENTS[0]["event_id"],
        "project_id": "P123456",
        "kind": "LARGE_APPROVAL",
        "message": "Aprovação de US$ 750,0 mi: Solar (Kenya)",
        "amount_usd": 750e6,
        "event_time": datetime(2024, 3, 21, tzinfo=UTC),
        "created_at": T0,
    }
]


class FakeRepo:
    """Implementação em memória do protocolo Repository."""

    def __init__(self, healthy: bool = True):
        self.healthy = healthy

    def ping(self):
        return self.healthy

    def list_countries(self, region, income_level, include_aggregates, limit, offset):
        rows = [
            c
            for c in COUNTRIES
            if (include_aggregates or not c["is_aggregate"])
            and (region is None or c["region"] == region)
            and (income_level is None or c["income_level"] == income_level)
        ]
        return rows[offset : offset + limit], len(rows)

    def get_country(self, iso3):
        return next((c for c in COUNTRIES if c["country_code"] == iso3), None)

    def latest_country_year(self, iso3):
        if iso3 != "BRA":
            return None
        return {"year": 2023, "population": 216e6, "gdp_usd": 2.1e12, "gdp_per_capita_usd": 9722.2,
                "rural_population_pct": 12.4, "electricity_access_pct": 100.0, "projects_approved": 3,
                "projects_total_amount_usd": 1.2e9}  # fmt: skip

    def country_indicator_series(self, iso3, indicator, start_year, end_year):
        rows = [
            {"indicator_code": i, "year": y, "value": v, "source": "worldbank-api"}
            for c, i, y, v in FACTS
            if c == iso3
            and (indicator is None or i == indicator)
            and (start_year is None or y >= start_year)
            and (end_year is None or y <= end_year)
        ]
        return sorted(rows, key=lambda r: (r["indicator_code"], r["year"]))

    def list_indicators(self):
        return INDICATORS

    def get_indicator(self, code):
        return next((i for i in INDICATORS if i["indicator_code"] == code), None)

    def ranking(self, code, year, top, ascending):
        names = {c["country_code"]: c for c in COUNTRIES}
        facts = [f for f in FACTS if f[1] == code and not names[f[0]]["is_aggregate"]]
        year = year or max((f[2] for f in facts), default=None)
        rows = sorted((f for f in facts if f[2] == year), key=lambda f: f[3], reverse=not ascending)[:top]
        return year, [
            {"country_code": c, "country_name": names[c]["country_name"], "value": v} for c, _, _, v in rows
        ]

    def list_projects(self, country, status, year, min_amount, limit, offset):
        rows = [
            p
            for p in PROJECTS
            if (country is None or p["country_code"] == country)
            and (status is None or p["status"] == status)
            and (year is None or p["approval_year"] == year)
            and (min_amount is None or (p["total_amount_usd"] or 0) >= min_amount)
        ]
        return rows[offset : offset + limit], len(rows)

    def get_project(self, project_id):
        return next((p for p in PROJECTS if p["project_id"] == project_id), None)

    def live_project(self, project_id):
        return next((p for p in LIVE if p["project_id"] == project_id), None)

    def project_events(self, project_id, limit):
        return [e for e in EVENTS if e["project_id"] == project_id][:limit]

    def live_projects(self, status, limit):
        return [p for p in LIVE if status is None or p["status"] == status][:limit]

    def live_alerts(self, limit):
        return ALERTS[:limit]

    def max_seq(self):
        return max(e["seq"] for e in EVENTS)

    def events_after(self, seq, limit):
        return [e for e in EVENTS if e["seq"] > seq][:limit]

    def pipeline_status(self):
        return {
            "events_last_hour": 3,
            "last_event_at": T0,
            "projects_live": 1,
            "alerts_last_24h": 1,
            "pending_landing_files": 2,
            "last_landing_file_at": T0,
            "kafka_lag": [{"consumer_group": "serving-realtime", "lag": 0, "measured_at": T0}],
            "dw_loads": [{"table_name": "fact_project", "row_count": 2, "loaded_at": T0}],
        }


@pytest.fixture
def client():
    main.app.dependency_overrides[main.get_repo] = FakeRepo
    main.app.dependency_overrides[main.api_key_setting] = lambda: None
    yield TestClient(main.app)
    main.app.dependency_overrides.clear()


# ---------------------------------------------------------------- saúde e API key


def test_health(client):
    assert client.get("/health").json() == {"status": "ok", "database": "ok"}


def test_health_sem_banco_responde_503(client):
    main.app.dependency_overrides[main.get_repo] = lambda: FakeRepo(healthy=False)
    r = client.get("/health")
    assert r.status_code == 503 and r.json()["database"] == "indisponível"


def test_api_key_exigida_quando_configurada(client):
    main.app.dependency_overrides[main.api_key_setting] = lambda: "segredo"
    assert client.get("/v1/indicators").status_code == 401
    assert client.get("/v1/indicators", headers={"X-API-Key": "errada"}).status_code == 401
    assert client.get("/v1/indicators", headers={"X-API-Key": "segredo"}).status_code == 200
    assert client.get("/health").status_code == 200  # saúde fica aberta para o healthcheck


# ---------------------------------------------------------------- países e indicadores


def test_lista_paises_paginada_sem_agregados(client):
    body = client.get("/v1/countries?limit=1").json()
    assert body["total"] == 2 and body["limit"] == 1 and body["offset"] == 0
    assert [c["country_code"] for c in body["items"]] == ["BRA"]
    assert client.get("/v1/countries?include_aggregates=true").json()["total"] == 3


def test_filtro_de_regiao(client):
    body = client.get("/v1/countries", params={"region": "Sub-Saharan Africa"}).json()
    assert [c["country_code"] for c in body["items"]] == ["KEN"]


@pytest.mark.parametrize("params", ["limit=0", "limit=501", "offset=-1"])
def test_paginacao_invalida_e_422(client, params):
    assert client.get(f"/v1/countries?{params}").status_code == 422


def test_perfil_do_pais_com_ultimo_ano(client):
    body = client.get("/v1/countries/bra").json()  # aceita minúsculas
    assert body["country_code"] == "BRA" and body["latest"]["year"] == 2023
    assert client.get("/v1/countries/KEN").json()["latest"] is None


def test_pais_inexistente_404_e_codigo_invalido_422(client):
    assert client.get("/v1/countries/XXX").status_code == 404
    assert client.get("/v1/countries/BRAZIL").status_code == 422


def test_serie_de_indicadores_do_pais(client):
    rows = client.get("/v1/countries/BRA/indicators?indicator=SP.POP.TOTL&start_year=2023").json()
    assert rows == [
        {"indicator_code": "SP.POP.TOTL", "year": 2023, "value": 216e6, "source": "worldbank-api"}
    ]
    assert client.get("/v1/countries/XXX/indicators").status_code == 404


def test_lista_indicadores(client):
    assert [i["unit"] for i in client.get("/v1/indicators").json()] == ["count", "%"]


def test_ranking_sem_agregados_ordenado(client):
    body = client.get("/v1/rankings/SP.POP.TOTL?top=5").json()
    assert body["year"] == 2023 and body["unit"] == "count"
    assert [(i["rank"], i["country_code"]) for i in body["items"]] == [(1, "BRA"), (2, "KEN")]
    asc = client.get("/v1/rankings/SP.POP.TOTL?order=asc").json()
    assert asc["items"][0]["country_code"] == "KEN"


def test_ranking_de_indicador_inexistente_ou_sem_dados(client):
    assert client.get("/v1/rankings/NAO.EXISTE").status_code == 404
    assert client.get("/v1/rankings/SP.POP.TOTL?year=1965").status_code == 404  # ano válido, sem dados
    assert client.get("/v1/rankings/SP.POP.TOTL?year=1900").status_code == 422  # fora da faixa
    assert client.get("/v1/rankings/SP.POP.TOTL?top=0").status_code == 422
    assert client.get("/v1/rankings/SP.POP.TOTL?order=lado").status_code == 422


# ---------------------------------------------------------------- projetos


def test_lista_projetos_com_filtros(client):
    body = client.get("/v1/projects?status=Active&min_amount=500000000").json()
    assert body["total"] == 1 and body["items"][0]["project_id"] == "P123456"
    assert client.get("/v1/projects?country=bra").json()["items"][0]["project_id"] == "P100001"
    assert client.get("/v1/projects?year=1800").status_code == 422


def test_detalhe_do_projeto_com_dw_rt_e_eventos(client):
    body = client.get("/v1/projects/P123456").json()
    assert body["project"]["total_amount_usd"] == 750e6
    assert body["live"]["last_event_type"] == "PROJECT_APPROVED"
    assert [e["event_type"] for e in body["events"]] == [
        "PROJECT_APPROVED",
        "PROJECT_UPDATED",
        "PROJECT_CLOSED",
    ]


def test_projeto_so_no_dw_ou_inexistente(client):
    body = client.get("/v1/projects/P100001").json()
    assert body["live"] is None and body["events"] == []
    assert client.get("/v1/projects/P000000").status_code == 404
    assert client.get("/v1/projects/abc").status_code == 422


# ---------------------------------------------------------------- tempo real


def test_live_projects_e_alertas(client):
    live = client.get("/v1/live/projects?limit=3").json()
    assert live and {"project_id", "last_event_type", "last_event_time"} <= set(live[0])
    alerts = client.get("/v1/live/alerts").json()
    assert alerts[0]["kind"] == "LARGE_APPROVAL"


def test_pipeline_status(client):
    body = client.get("/v1/pipeline/status").json()
    assert body["events_last_hour"] == 3 and body["pending_landing_files"] == 2
    assert body["kafka_lag"][0]["consumer_group"] == "serving-realtime"


def parse_sse(text: str) -> list[dict]:
    events = []
    for block in text.strip().split("\n\n"):
        fields = dict(line.split(": ", 1) for line in block.splitlines() if not line.startswith(":"))
        if "event" in fields:  # ignora `retry:` e comentários de heartbeat
            events.append(fields)
    return events


def test_sse_entrega_eventos_em_ordem_de_seq(client):
    with client.stream("GET", "/v1/live/stream?after_seq=0&max_events=2") as r:
        assert r.headers["content-type"].startswith("text/event-stream")
        events = parse_sse(r.read().decode())
    assert [(e["id"], e["event"]) for e in events] == [("1", "PROJECT_APPROVED"), ("2", "PROJECT_UPDATED")]
    assert '"project_id":"P123456"' in events[0]["data"].replace(" ", "")


def test_sse_retoma_pelo_last_event_id(client):
    with client.stream("GET", "/v1/live/stream?max_events=1", headers={"Last-Event-ID": "2"}) as r:
        events = parse_sse(r.read().decode())
    assert [e["id"] for e in events] == ["3"]


def test_sse_backfill_entrega_os_ultimos(client):
    with client.stream("GET", "/v1/live/stream?backfill=1&max_events=1") as r:
        events = parse_sse(r.read().decode())
    assert [e["event"] for e in events] == ["PROJECT_CLOSED"]


def test_sse_exige_api_key(client):
    main.app.dependency_overrides[main.api_key_setting] = lambda: "segredo"
    assert client.get("/v1/live/stream?max_events=1").status_code == 401
