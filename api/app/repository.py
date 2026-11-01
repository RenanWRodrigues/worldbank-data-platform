"""Acesso a dw e rt. Todo SQL da API fica aqui, parametrizado; só nomes internos (ASC/DESC) vão no texto."""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, Protocol

Row = dict[str, Any]


class Repository(Protocol):
    def ping(self) -> bool: ...
    def list_countries(
        self, region: str | None, income_level: str | None, include_aggregates: bool, limit: int, offset: int
    ) -> tuple[list[Row], int]: ...
    def get_country(self, iso3: str) -> Row | None: ...
    def latest_country_year(self, iso3: str) -> Row | None: ...
    def country_indicator_series(
        self, iso3: str, indicator: str | None, start_year: int | None, end_year: int | None
    ) -> list[Row]: ...
    def list_indicators(self) -> list[Row]: ...
    def get_indicator(self, code: str) -> Row | None: ...
    def ranking(
        self, code: str, year: int | None, top: int, ascending: bool
    ) -> tuple[int | None, list[Row]]: ...
    def list_projects(
        self,
        country: str | None,
        status: str | None,
        year: int | None,
        min_amount: float | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Row], int]: ...
    def get_project(self, project_id: str) -> Row | None: ...
    def live_project(self, project_id: str) -> Row | None: ...
    def project_events(self, project_id: str, limit: int) -> list[Row]: ...
    def live_projects(self, status: str | None, limit: int) -> list[Row]: ...
    def live_alerts(self, limit: int) -> list[Row]: ...
    def max_seq(self) -> int: ...
    def events_after(self, seq: int, limit: int) -> list[Row]: ...
    def pipeline_status(self) -> Row: ...


def pg_params(env: Mapping[str, str] = os.environ) -> dict[str, Any]:
    """Conexão como `api_ro` em sessão somente leitura e com tempo máximo por consulta."""
    return {
        "host": env.get("POSTGRES_HOST", "postgres"),
        "port": int(env.get("POSTGRES_PORT", "5432")),
        "dbname": env.get("POSTGRES_DB", "worldbank"),
        "user": env.get("API_RO_USER", "api_ro"),
        "password": env.get("API_RO_PASSWORD", ""),
        "options": "-c default_transaction_read_only=on -c statement_timeout=10000",
        "application_name": "worldbank-api",
    }


# Ranking sem ?year=: último ano com pelo menos esse nº de países (régua do coverage_check e do Power BI)
RANKING_MIN_COUNTRIES = 180

_EVENT_COLUMNS = (
    "seq, event_id::text, event_type, project_id, event_time, produced_at, source, payload, consumed_at"
)
_LIVE_COLUMNS = """project_id, project_name, country_iso2, country_name, region_name, status,
    total_commitment_usd::float8 AS total_commitment_usd, board_approval_date, closing_date,
    lending_instrument, major_sector, last_event_id::text, last_event_type, last_event_time,
    last_produced_at, updated_at"""
_PROJECT_COLUMNS = """project_id, country_code, country_name_raw, region_name, project_name, status,
    lending_instrument, major_sector, board_approval_date, approval_year, closing_date,
    total_amount_usd::float8 AS total_amount_usd, ibrd_amount_usd::float8 AS ibrd_amount_usd,
    ida_amount_usd::float8 AS ida_amount_usd, grant_amount_usd::float8 AS grant_amount_usd,
    last_event_type, last_event_time"""


class PostgresRepository:
    """Pool criado no primeiro uso: a API sobe mesmo com o banco fora do ar (/health responde 503)."""

    def __init__(self, params: dict[str, Any] | None = None, maxconn: int = 5):
        self.params = params or pg_params()
        self.maxconn = maxconn
        self._pool = None
        self._lock = threading.Lock()

    def _get_pool(self):
        with self._lock:
            if self._pool is None:
                from psycopg2.pool import ThreadedConnectionPool

                self._pool = ThreadedConnectionPool(1, self.maxconn, **self.params)
            return self._pool

    @contextmanager
    def _cursor(self) -> Iterator[Any]:
        from psycopg2.extras import RealDictCursor

        pool = self._get_pool()
        con = pool.getconn()
        broken = False
        try:
            con.autocommit = True
            with con.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
        except Exception:
            broken = con.closed != 0
            raise
        finally:
            pool.putconn(con, close=broken)

    def _all(self, sql: str, params: tuple | dict = ()) -> list[Row]:
        with self._cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def _one(self, sql: str, params: tuple | dict = ()) -> Row | None:
        rows = self._all(sql, params)
        return rows[0] if rows else None

    # ---------------------------------------------------------------- saúde

    def ping(self) -> bool:
        try:
            return self._one("SELECT 1 AS ok") is not None
        except Exception:
            return False

    # ---------------------------------------------------------------- países e indicadores

    def list_countries(self, region, income_level, include_aggregates, limit, offset):
        where = """WHERE (%(agg)s OR NOT is_aggregate)
                     AND (%(region)s::text IS NULL OR region = %(region)s)
                     AND (%(income)s::text IS NULL OR income_level = %(income)s)"""
        params = {"agg": include_aggregates, "region": region, "income": income_level, "limit": limit,
                  "offset": offset}  # fmt: skip
        rows = self._all(
            f"""SELECT country_code, country_iso2, country_name, region, income_level, is_aggregate,
                       count(*) OVER () AS total
                FROM dw.dim_country {where}
                ORDER BY country_name LIMIT %(limit)s OFFSET %(offset)s""",
            params,
        )
        if rows or offset == 0:
            total = rows[0]["total"] if rows else 0
        else:  # página além do fim: o total vem de uma contagem
            total = self._one(f"SELECT count(*) AS total FROM dw.dim_country {where}", params)["total"]
        return [{k: v for k, v in r.items() if k != "total"} for r in rows], total

    def get_country(self, iso3):
        return self._one(
            """SELECT country_code, country_iso2, country_name, region, income_level, is_aggregate
               FROM dw.dim_country WHERE country_code = %s""",
            (iso3,),
        )

    def latest_country_year(self, iso3):
        return self._one(
            """SELECT year, population, gdp_usd, gdp_per_capita_usd, rural_population_pct,
                      electricity_access_pct, projects_approved,
                      projects_total_amount_usd::float8 AS projects_total_amount_usd
               FROM dw.agg_country_year
               WHERE country_code = %s
                 AND (population IS NOT NULL OR gdp_usd IS NOT NULL OR electricity_access_pct IS NOT NULL)
               ORDER BY year DESC LIMIT 1""",
            (iso3,),
        )

    def country_indicator_series(self, iso3, indicator, start_year, end_year):
        return self._all(
            """SELECT indicator_code, year, value, source
               FROM dw.fact_indicator
               WHERE country_code = %(iso3)s
                 AND (%(ind)s::text IS NULL OR indicator_code = %(ind)s)
                 AND (%(start)s::int IS NULL OR year >= %(start)s)
                 AND (%(end)s::int IS NULL OR year <= %(end)s)
               ORDER BY indicator_code, year""",
            {"iso3": iso3, "ind": indicator, "start": start_year, "end": end_year},
        )

    def list_indicators(self):
        return self._all(
            "SELECT indicator_code, indicator_name, unit FROM dw.dim_indicator ORDER BY indicator_code"
        )

    def get_indicator(self, code):
        return self._one(
            "SELECT indicator_code, indicator_name, unit FROM dw.dim_indicator WHERE indicator_code = %s",
            (code,),
        )

    def ranking(self, code, year, top, ascending):
        if year is None:
            # Último ano com cobertura (o mais recente costuma ser parcial); sem nenhum, o mais recente
            found = self._one(
                """SELECT f.year FROM dw.fact_indicator f
                   JOIN dw.dim_country c USING (country_code)
                   WHERE f.indicator_code = %s AND NOT c.is_aggregate
                   GROUP BY f.year
                   ORDER BY (count(*) >= %s) DESC, f.year DESC
                   LIMIT 1""",
                (code, RANKING_MIN_COUNTRIES),
            )
            year = found["year"] if found else None
            if year is None:
                return None, []
        direction = "ASC" if ascending else "DESC"  # nome interno, nunca valor do usuário
        rows = self._all(
            f"""SELECT f.country_code, c.country_name, f.value
                FROM dw.fact_indicator f
                JOIN dw.dim_country c USING (country_code)
                WHERE f.indicator_code = %s AND f.year = %s AND NOT c.is_aggregate
                ORDER BY f.value {direction}, f.country_code
                LIMIT %s""",
            (code, year, top),
        )
        return year, rows

    # ---------------------------------------------------------------- projetos

    def list_projects(self, country, status, year, min_amount, limit, offset):
        where = """WHERE (%(country)s::text IS NULL OR country_code = %(country)s)
                     AND (%(status)s::text IS NULL OR status = %(status)s)
                     AND (%(year)s::int IS NULL OR approval_year = %(year)s)
                     AND (%(min)s::numeric IS NULL OR total_amount_usd >= %(min)s)"""
        params = {"country": country, "status": status, "year": year, "min": min_amount, "limit": limit,
                  "offset": offset}  # fmt: skip
        rows = self._all(
            f"""SELECT {_PROJECT_COLUMNS} FROM dw.fact_project {where}
                ORDER BY board_approval_date DESC NULLS LAST, project_id
                LIMIT %(limit)s OFFSET %(offset)s""",
            params,
        )
        total = self._one(f"SELECT count(*) AS total FROM dw.fact_project {where}", params)["total"]
        return rows, total

    def get_project(self, project_id):
        return self._one(
            f"SELECT {_PROJECT_COLUMNS} FROM dw.fact_project WHERE project_id = %s", (project_id,)
        )

    def live_project(self, project_id):
        return self._one(
            f"SELECT {_LIVE_COLUMNS} FROM rt.projects_latest WHERE project_id = %s", (project_id,)
        )

    def project_events(self, project_id, limit):
        return self._all(
            f"""SELECT {_EVENT_COLUMNS} FROM rt.project_events WHERE project_id = %s
                ORDER BY event_time, produced_at, seq LIMIT %s""",
            (project_id, limit),
        )

    # ---------------------------------------------------------------- tempo real

    def live_projects(self, status, limit):
        return self._all(
            f"""SELECT {_LIVE_COLUMNS} FROM rt.projects_latest
                WHERE (%(status)s::text IS NULL OR status = %(status)s)
                ORDER BY updated_at DESC, last_produced_at DESC LIMIT %(limit)s""",
            {"status": status, "limit": limit},
        )

    def live_alerts(self, limit):
        return self._all(
            """SELECT alert_id, event_id::text, project_id, kind, message, amount_usd::float8 AS amount_usd,
                      event_time, created_at
               FROM rt.alerts ORDER BY created_at DESC, alert_id DESC LIMIT %s""",
            (limit,),
        )

    def max_seq(self):
        return self._one("SELECT coalesce(max(seq), 0) AS seq FROM rt.project_events")["seq"]

    def events_after(self, seq, limit):
        return self._all(
            f"SELECT {_EVENT_COLUMNS} FROM rt.project_events WHERE seq > %s ORDER BY seq LIMIT %s",
            (seq, limit),
        )

    def pipeline_status(self):
        status = self._one(
            """SELECT
                 (SELECT count(*) FROM rt.project_events WHERE consumed_at > now() - interval '1 hour')
                     AS events_last_hour,
                 (SELECT max(consumed_at) FROM rt.project_events) AS last_event_at,
                 (SELECT count(*) FROM rt.projects_latest) AS projects_live,
                 (SELECT count(*) FROM rt.alerts WHERE created_at > now() - interval '24 hours')
                     AS alerts_last_24h,
                 (SELECT count(*) FROM ops.landing_files WHERE processed_at IS NULL) AS pending_landing_files,
                 (SELECT max(created_at) FROM ops.landing_files) AS last_landing_file_at"""
        )
        status["kafka_lag"] = self._all(
            """SELECT DISTINCT ON (consumer_group) consumer_group, lag, measured_at
               FROM ops.kafka_lag ORDER BY consumer_group, measured_at DESC"""
        )
        status["dw_loads"] = self._all(
            """SELECT DISTINCT ON (table_name) table_name, row_count, loaded_at
               FROM dw.load_audit ORDER BY table_name, loaded_at DESC"""
        )
        return status
