-- Esquema do PostgreSQL da World Bank Data Platform: schemas ops, rt e dw.
-- Roda só na primeira inicialização (volume vazio), depois de 00_users.sh (que cria powerbi e api_ro).
-- Em ambiente existente, mudanças vão em postgres/migrations/NNN_descricao.sql.
\set ON_ERROR_STOP on

-- Metadados do Airflow em banco separado; usuários de leitura não conectam nele
CREATE DATABASE airflow;
REVOKE CONNECT ON DATABASE airflow FROM PUBLIC;

CREATE SCHEMA ops;
CREATE SCHEMA rt;
CREATE SCHEMA dw;

-- =====================================================================
-- ops — controle operacional
-- =====================================================================

CREATE TABLE ops.landing_files (
    file_id      BIGSERIAL PRIMARY KEY,
    path         TEXT        NOT NULL UNIQUE,  -- reexecutar uma extração não duplica o registro
    dataset      TEXT        NOT NULL CHECK (dataset IN ('projects_events', 'indicators', 'countries')),
    records      INTEGER     NOT NULL CHECK (records >= 0),
    first_offset BIGINT,
    last_offset  BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ
);
-- has_new_files / mark_processed: só os pendentes
CREATE INDEX landing_files_pending_idx ON ops.landing_files (dataset, created_at) WHERE processed_at IS NULL;

CREATE TABLE ops.producer_state (
    project_id      TEXT PRIMARY KEY,
    content_hash    TEXT        NOT NULL,
    status          TEXT,
    last_event_type TEXT CHECK (last_event_type IN ('PROJECT_APPROVED', 'PROJECT_UPDATED', 'PROJECT_CLOSED')),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ops.kafka_lag (
    measured_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumer_group TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    lag            BIGINT      NOT NULL CHECK (lag >= 0),
    PRIMARY KEY (measured_at, consumer_group, topic)
);
-- Última medição por grupo (/v1/pipeline/status)
CREATE INDEX kafka_lag_group_idx ON ops.kafka_lag (consumer_group, measured_at DESC);

-- =====================================================================
-- rt — tempo real (consumer group serving-realtime)
-- =====================================================================

CREATE TABLE rt.project_events (
    event_id        UUID PRIMARY KEY,           -- ON CONFLICT (event_id) DO NOTHING
    seq             BIGSERIAL   NOT NULL UNIQUE, -- ordem do SSE; exige um único realtime-sink
    event_type      TEXT        NOT NULL CHECK (event_type IN ('PROJECT_APPROVED', 'PROJECT_UPDATED', 'PROJECT_CLOSED')),
    project_id      TEXT        NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    produced_at     TIMESTAMPTZ NOT NULL,
    source          TEXT        NOT NULL CHECK (source IN ('worldbank-projects-api', 'kaggle-replay')),
    payload         JSONB       NOT NULL,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    consumed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX project_events_project_idx ON rt.project_events (project_id, event_time);
CREATE INDEX project_events_consumed_idx ON rt.project_events (consumed_at);

CREATE TABLE rt.projects_latest (
    project_id           TEXT PRIMARY KEY,
    project_name         TEXT,
    country_iso2         TEXT,
    country_name         TEXT,
    region_name          TEXT,
    status               TEXT,
    total_commitment_usd NUMERIC(20, 2) CHECK (total_commitment_usd >= 0),
    board_approval_date  DATE,
    closing_date         DATE,
    lending_instrument   TEXT,
    major_sector         TEXT,
    -- Upsert só se (autoridade da fonte, event_time, produced_at) novo > atual e event_id ≠ last_event_id
    -- (ADR 0009: worldbank-projects-api vence kaggle-replay)
    last_event_id        UUID        NOT NULL,
    last_event_type      TEXT        NOT NULL CHECK (last_event_type IN ('PROJECT_APPROVED', 'PROJECT_UPDATED', 'PROJECT_CLOSED')),
    last_event_time      TIMESTAMPTZ NOT NULL,
    last_produced_at     TIMESTAMPTZ NOT NULL,
    last_source          TEXT        NOT NULL CHECK (last_source IN ('worldbank-projects-api', 'kaggle-replay')),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()  -- chegada ao rt (consistência rt × dw)
);
CREATE INDEX projects_latest_updated_idx ON rt.projects_latest (updated_at);
CREATE INDEX projects_latest_event_time_idx ON rt.projects_latest (last_event_time DESC);

CREATE TABLE rt.alerts (
    alert_id   BIGSERIAL PRIMARY KEY,
    event_id   UUID        NOT NULL UNIQUE REFERENCES rt.project_events (event_id),  -- 1 alerta por evento
    project_id TEXT        NOT NULL,
    kind       TEXT        NOT NULL CHECK (kind IN ('LARGE_APPROVAL')),
    message    TEXT        NOT NULL,
    amount_usd NUMERIC(20, 2),
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX alerts_created_idx ON rt.alerts (created_at DESC);

-- =====================================================================
-- dw — estrela (= gold_* do Databricks, carregado por sync_gold.py)
-- Faixas de valores são checadas pela tarefa data_quality, não por CHECK aqui.
-- =====================================================================

CREATE TABLE dw.dim_country (
    country_code TEXT PRIMARY KEY CHECK (char_length(country_code) = 3),  -- ISO3 / código WDI
    country_iso2 TEXT,
    country_name TEXT    NOT NULL,
    region       TEXT,             -- nula para países só no Kaggle
    income_level TEXT,
    is_aggregate BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE dw.dim_indicator (
    indicator_code TEXT PRIMARY KEY,
    indicator_name TEXT NOT NULL,
    unit           TEXT NOT NULL CHECK (unit IN ('%', 'US$', 'count'))
);

CREATE TABLE dw.dim_year (
    year      SMALLINT PRIMARY KEY,
    decade    TEXT NOT NULL,  -- "2010s"
    year_date DATE NOT NULL
);

CREATE TABLE dw.fact_indicator (
    country_code   TEXT             NOT NULL REFERENCES dw.dim_country (country_code),
    indicator_code TEXT             NOT NULL REFERENCES dw.dim_indicator (indicator_code),
    year           SMALLINT         NOT NULL REFERENCES dw.dim_year (year),
    value          DOUBLE PRECISION NOT NULL,
    source         TEXT             NOT NULL CHECK (source IN ('worldbank-api', 'kaggle')),
    PRIMARY KEY (country_code, indicator_code, year)
);
-- Rankings e séries por indicador
CREATE INDEX fact_indicator_indicator_year_idx ON dw.fact_indicator (indicator_code, year);

CREATE TABLE dw.fact_project (
    project_id          TEXT PRIMARY KEY,
    country_code        TEXT REFERENCES dw.dim_country (country_code),  -- nulo: regional/multi-país
    country_name_raw    TEXT,
    region_name         TEXT,
    project_name        TEXT,
    status              TEXT,
    lending_instrument  TEXT,
    major_sector        TEXT,
    board_approval_date DATE,
    approval_year       SMALLINT,  -- existência em dim_year é checada pelo data_quality
    closing_date        DATE,
    total_amount_usd    NUMERIC(20, 2),
    ibrd_amount_usd     NUMERIC(20, 2),
    ida_amount_usd      NUMERIC(20, 2),
    grant_amount_usd    NUMERIC(20, 2),
    last_event_type     TEXT,
    last_event_time     TIMESTAMPTZ
);
CREATE INDEX fact_project_country_idx ON dw.fact_project (country_code);
CREATE INDEX fact_project_approval_year_idx ON dw.fact_project (approval_year);

CREATE TABLE dw.agg_country_year (
    country_code              TEXT     NOT NULL REFERENCES dw.dim_country (country_code),
    year                      SMALLINT NOT NULL REFERENCES dw.dim_year (year),
    population                DOUBLE PRECISION,
    gdp_usd                   DOUBLE PRECISION,
    gdp_per_capita_usd        DOUBLE PRECISION,  -- = gdp_usd / population
    rural_population_pct      DOUBLE PRECISION,
    electricity_access_pct    DOUBLE PRECISION,
    projects_approved         INTEGER,
    projects_total_amount_usd NUMERIC(20, 2),
    PRIMARY KEY (country_code, year)
);
CREATE INDEX agg_country_year_year_idx ON dw.agg_country_year (year);

CREATE TABLE dw.load_audit (
    load_id    BIGSERIAL PRIMARY KEY,
    table_name TEXT        NOT NULL,
    row_count  INTEGER     NOT NULL CHECK (row_count >= 0),
    run_id     TEXT        NOT NULL,
    loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX load_audit_table_idx ON dw.load_audit (table_name, loaded_at DESC);

-- País × ano com atributos do país, sem agregados (World, regiões, faixas de renda)
CREATE VIEW dw.vw_country_year AS
SELECT a.country_code,
       c.country_name,
       c.region,
       c.income_level,
       a.year,
       a.population,
       a.gdp_usd,
       a.gdp_per_capita_usd,
       a.rural_population_pct,
       a.electricity_access_pct,
       a.projects_approved,
       a.projects_total_amount_usd
FROM dw.agg_country_year a
JOIN dw.dim_country c ON c.country_code = a.country_code
WHERE NOT c.is_aggregate;

-- =====================================================================
-- Permissões: powerbi (dw, rt) e api_ro (dw, rt, ops.landing_files, ops.kafka_lag) — somente leitura
-- =====================================================================

GRANT USAGE ON SCHEMA dw, rt TO powerbi, api_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA dw, rt TO powerbi, api_ro;
-- Tabelas criadas depois (migrações, pelo mesmo dono) já nascem legíveis
ALTER DEFAULT PRIVILEGES IN SCHEMA dw, rt GRANT SELECT ON TABLES TO powerbi, api_ro;

GRANT USAGE ON SCHEMA ops TO api_ro;
GRANT SELECT ON ops.landing_files, ops.kafka_lag TO api_ro;
