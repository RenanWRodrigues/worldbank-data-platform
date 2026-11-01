"""Modelos de resposta (aparecem no Swagger em /docs)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class Page[T](BaseModel):
    items: list[T]
    total: int = Field(examples=[217])
    limit: int = Field(examples=[50])
    offset: int = Field(examples=[0])


class Health(BaseModel):
    status: Literal["ok", "degradado"]
    database: Literal["ok", "indisponível"]


class Country(BaseModel):
    country_code: str = Field(examples=["BRA"])
    country_iso2: str | None = Field(default=None, examples=["BR"])
    country_name: str = Field(examples=["Brazil"])
    region: str | None = Field(default=None, examples=["Latin America & Caribbean"])
    income_level: str | None = Field(default=None, examples=["Upper middle income"])
    is_aggregate: bool = False


class CountryYear(BaseModel):
    year: int = Field(examples=[2023])
    population: float | None = None
    gdp_usd: float | None = None
    gdp_per_capita_usd: float | None = None
    rural_population_pct: float | None = None
    electricity_access_pct: float | None = None
    projects_approved: int | None = None
    projects_total_amount_usd: float | None = None


class CountryProfile(Country):
    latest: CountryYear | None = Field(default=None, description="ano mais recente de dw.agg_country_year")


class IndicatorValue(BaseModel):
    indicator_code: str = Field(examples=["SP.POP.TOTL"])
    year: int = Field(examples=[2023])
    value: float
    source: Literal["worldbank-api", "kaggle"]


class Indicator(BaseModel):
    indicator_code: str = Field(examples=["EG.ELC.ACCS.ZS"])
    indicator_name: str = Field(examples=["Access to electricity (% of population)"])
    unit: Literal["%", "US$", "count"]


class RankingItem(BaseModel):
    rank: int
    country_code: str
    country_name: str
    value: float


class Ranking(BaseModel):
    indicator_code: str
    indicator_name: str
    unit: str
    year: int
    order: Literal["desc", "asc"]
    items: list[RankingItem]


class Project(BaseModel):
    project_id: str = Field(examples=["P123456"])
    country_code: str | None = Field(default=None, description="nulo para projetos regionais/multi-país")
    country_name_raw: str | None = None
    region_name: str | None = None
    project_name: str | None = None
    status: str | None = None
    lending_instrument: str | None = None
    major_sector: str | None = None
    board_approval_date: date | None = None
    approval_year: int | None = None
    closing_date: date | None = None
    total_amount_usd: float | None = None
    ibrd_amount_usd: float | None = None
    ida_amount_usd: float | None = None
    grant_amount_usd: float | None = None
    last_event_type: str | None = None
    last_event_time: datetime | None = None


class LiveProject(BaseModel):
    project_id: str
    project_name: str | None = None
    country_iso2: str | None = None
    country_name: str | None = None
    region_name: str | None = None
    status: str | None = None
    total_commitment_usd: float | None = None
    board_approval_date: date | None = None
    closing_date: date | None = None
    lending_instrument: str | None = None
    major_sector: str | None = None
    last_event_id: str
    last_event_type: str
    last_event_time: datetime
    last_produced_at: datetime
    updated_at: datetime = Field(description="quando o rt recebeu o evento")


class ProjectEvent(BaseModel):
    seq: int
    event_id: str
    event_type: str
    project_id: str
    event_time: datetime
    produced_at: datetime
    source: str
    payload: dict[str, Any]
    consumed_at: datetime


class ProjectDetail(BaseModel):
    project_id: str
    project: Project | None = Field(description="estado no dw (atualizado pelos micro-lotes)")
    live: LiveProject | None = Field(description="estado no rt (segundos)")
    events: list[ProjectEvent]


class Alert(BaseModel):
    alert_id: int
    event_id: str
    project_id: str
    kind: str = Field(examples=["LARGE_APPROVAL"])
    message: str
    amount_usd: float | None = None
    event_time: datetime
    created_at: datetime


class KafkaLag(BaseModel):
    consumer_group: str
    lag: int
    measured_at: datetime


class DwLoad(BaseModel):
    table_name: str
    row_count: int
    loaded_at: datetime


class PipelineStatus(BaseModel):
    events_last_hour: int
    last_event_at: datetime | None
    projects_live: int
    alerts_last_24h: int
    pending_landing_files: int
    last_landing_file_at: datetime | None
    kafka_lag: list[KafkaLag]
    dw_loads: list[DwLoad]
