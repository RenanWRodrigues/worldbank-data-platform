"""Rotas da API de serving: dw (DW em estrela) e rt (tempo real). SQL fica em repository.py."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from collections.abc import AsyncIterator
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Path, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from app.repository import PostgresRepository, Repository
from app.schemas import (
    Alert,
    Country,
    CountryProfile,
    Health,
    Indicator,
    IndicatorValue,
    LiveProject,
    Page,
    PipelineStatus,
    Project,
    ProjectDetail,
    Ranking,
    RankingItem,
)

SSE_POLL_SECONDS = float(os.getenv("SSE_POLL_SECONDS", "1"))
SSE_HEARTBEAT_SECONDS = 15
SSE_BATCH = 100

_repo: PostgresRepository | None = None


def get_repo() -> Repository:
    """Repositório injetável (os testes trocam por um FakeRepo)."""
    global _repo
    if _repo is None:
        _repo = PostgresRepository()
    return _repo


def api_key_setting() -> str | None:
    return os.getenv("API_KEY") or None


def require_api_key(
    expected: Annotated[str | None, Depends(api_key_setting)],
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    """Com API_KEY definido, as rotas /v1 exigem o header X-API-Key."""
    if expected and not (x_api_key and secrets.compare_digest(x_api_key, expected)):
        raise HTTPException(status_code=401, detail="X-API-Key ausente ou inválida")


RepoDep = Annotated[Repository, Depends(get_repo)]
Iso3 = Annotated[str, Path(pattern=r"^[A-Za-z0-9]{3}$", description="código ISO3 / WDI, ex.: BRA")]
ProjectId = Annotated[str, Path(pattern=r"^P\d{5,7}$", description="ex.: P123456")]
Limit = Annotated[int, Query(ge=1, le=500)]
Offset = Annotated[int, Query(ge=0)]
Year = Annotated[int | None, Query(ge=1960, le=2100)]

app = FastAPI(
    title="World Bank Data Platform — API de serving",
    version="2.0.0",
    description="Lê o DW (`dw`, carga dos micro-lotes e da carga semanal) e o tempo real (`rt`, segundos).",
)
origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_methods=["GET"], allow_headers=["*"])

v1 = APIRouter(prefix="/v1", dependencies=[Depends(require_api_key)])


def _not_found(what: str) -> HTTPException:
    return HTTPException(status_code=404, detail=f"{what} não encontrado")


# ---------------------------------------------------------------- saúde


@app.get("/health", response_model=Health, tags=["saúde"])
def health(repo: RepoDep):
    if repo.ping():
        return {"status": "ok", "database": "ok"}
    return JSONResponse(status_code=503, content={"status": "degradado", "database": "indisponível"})


# ---------------------------------------------------------------- países e indicadores


@v1.get("/countries", response_model=Page[Country], tags=["países"])
def list_countries(
    repo: RepoDep,
    region: str | None = None,
    income_level: str | None = None,
    include_aggregates: bool = False,
    limit: Limit = 50,
    offset: Offset = 0,
):
    items, total = repo.list_countries(region, income_level, include_aggregates, limit, offset)
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@v1.get("/countries/{iso3}", response_model=CountryProfile, tags=["países"])
def get_country(repo: RepoDep, iso3: Iso3):
    country = repo.get_country(iso3.upper())
    if country is None:
        raise _not_found("país")
    return {**country, "latest": repo.latest_country_year(iso3.upper())}


@v1.get("/countries/{iso3}/indicators", response_model=list[IndicatorValue], tags=["países"])
def country_indicators(
    repo: RepoDep,
    iso3: Iso3,
    indicator: str | None = None,
    start_year: Year = None,
    end_year: Year = None,
):
    if repo.get_country(iso3.upper()) is None:
        raise _not_found("país")
    return repo.country_indicator_series(iso3.upper(), indicator, start_year, end_year)


@v1.get("/indicators", response_model=list[Indicator], tags=["indicadores"])
def list_indicators(repo: RepoDep):
    return repo.list_indicators()


@v1.get("/rankings/{code}", response_model=Ranking, tags=["indicadores"])
def ranking(
    repo: RepoDep,
    code: Annotated[str, Path(max_length=40, description="código WDI, ex.: NY.GDP.MKTP.CD")],
    year: Year = None,
    top: Annotated[int, Query(ge=1, le=100)] = 10,
    order: Literal["desc", "asc"] = "desc",
):
    """Países (sem agregados) ordenados pelo valor. Sem `year`: último ano com ≥ 180 países (o mais recente
    costuma ser parcial), ou o mais recente se nenhum chegar a isso."""
    indicator = repo.get_indicator(code)
    if indicator is None:
        raise _not_found("indicador")
    found_year, rows = repo.ranking(code, year, top, ascending=order == "asc")
    if not rows:
        raise _not_found(f"dado de {code} no ano {year or 'mais recente'}")
    items = [RankingItem(rank=i, **r) for i, r in enumerate(rows, start=1)]
    return {**indicator, "year": found_year, "order": order, "items": items}


# ---------------------------------------------------------------- projetos


@v1.get("/projects", response_model=Page[Project], tags=["projetos"])
def list_projects(
    repo: RepoDep,
    country: Annotated[str | None, Query(pattern=r"^[A-Za-z0-9]{3}$")] = None,
    status: str | None = None,
    year: Year = None,
    min_amount: Annotated[float | None, Query(ge=0)] = None,
    limit: Limit = 50,
    offset: Offset = 0,
):
    items, total = repo.list_projects(
        country.upper() if country else None, status, year, min_amount, limit, offset
    )
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@v1.get("/projects/{project_id}", response_model=ProjectDetail, tags=["projetos"])
def get_project(repo: RepoDep, project_id: ProjectId, events: Annotated[int, Query(ge=0, le=500)] = 100):
    project, live = repo.get_project(project_id), repo.live_project(project_id)
    if project is None and live is None:
        raise _not_found("projeto")
    history = repo.project_events(project_id, events) if events else []
    return {"project_id": project_id, "project": project, "live": live, "events": history}


# ---------------------------------------------------------------- tempo real


@v1.get("/live/projects", response_model=list[LiveProject], tags=["tempo real"])
def live_projects(repo: RepoDep, status: str | None = None, limit: Annotated[int, Query(ge=1, le=200)] = 20):
    return repo.live_projects(status, limit)


@v1.get("/live/alerts", response_model=list[Alert], tags=["tempo real"])
def live_alerts(repo: RepoDep, limit: Annotated[int, Query(ge=1, le=200)] = 20):
    return repo.live_alerts(limit)


def _sse(row: dict) -> str:
    data = json.dumps(
        jsonable_encoder({k: v for k, v in row.items() if k != "consumed_at"}), ensure_ascii=False
    )
    return f"id: {row['seq']}\nevent: {row['event_type']}\ndata: {data}\n\n"


async def _event_stream(
    request: Request, repo: Repository, cursor: int, max_events: int | None
) -> AsyncIterator[str]:
    sent, last_beat = 0, time.monotonic()
    yield "retry: 3000\n\n"
    while not await request.is_disconnected():
        rows = await run_in_threadpool(repo.events_after, cursor, SSE_BATCH)
        for row in rows:
            yield _sse(row)
            cursor, sent = row["seq"], sent + 1
            if max_events and sent >= max_events:
                return
        if rows:
            last_beat = time.monotonic()
            continue
        if time.monotonic() - last_beat >= SSE_HEARTBEAT_SECONDS:
            yield ": heartbeat\n\n"  # comentário SSE: mantém proxies e o cliente conectados
            last_beat = time.monotonic()
        await asyncio.sleep(SSE_POLL_SECONDS)


@v1.get(
    "/live/stream",
    tags=["tempo real"],
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {"text/event-stream": {}},
            "description": "eventos em ordem de rt.project_events.seq",
        }
    },
)
async def live_stream(
    request: Request,
    repo: RepoDep,
    after_seq: Annotated[int | None, Query(ge=0, description="começa depois deste seq")] = None,
    backfill: Annotated[int, Query(ge=0, le=500, description="sem cursor: reenvia os últimos N eventos")] = 0,
    max_events: Annotated[int | None, Query(ge=1, le=10_000, description="encerra após N eventos")] = None,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
):
    """SSE dos eventos de projeto. Reconexão retoma pelo `Last-Event-ID` (= seq) enviado pelo navegador."""
    if after_seq is None and last_event_id and last_event_id.isdigit():
        after_seq = int(last_event_id)
    if after_seq is None:
        after_seq = max(await run_in_threadpool(repo.max_seq) - backfill, 0)
    return StreamingResponse(
        _event_stream(request, repo, after_seq, max_events),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@v1.get("/pipeline/status", response_model=PipelineStatus, tags=["operação"])
def pipeline_status(repo: RepoDep):
    return repo.pipeline_status()


app.include_router(v1)
