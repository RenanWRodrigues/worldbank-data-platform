-- ADR 0009: rt.projects_latest.last_source e estado atual recalculado com a ordem
-- (autoridade da fonte, event_time, produced_at) — worldbank-projects-api vence kaggle-replay.
-- Idempotente: pode rodar de novo (e num banco criado pelo init novo, onde a coluna já existe).
-- Rode com o realtime-sink parado (`make migrate` faz isso): o sink antigo não grava last_source.
BEGIN;

LOCK TABLE rt.projects_latest IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE rt.projects_latest ADD COLUMN IF NOT EXISTS last_source TEXT;

-- Fonte do último evento aplicado (todo last_event_id existe em rt.project_events)
UPDATE rt.projects_latest l
   SET last_source = e.source
  FROM rt.project_events e
 WHERE e.event_id = l.last_event_id
   AND l.last_source IS NULL;

-- Recalcula o estado de cada projeto com a ordem nova; empate total fica com o primeiro gravado (seq),
-- como no realtime-sink (lá a comparação é estritamente "maior")
WITH best AS (
    SELECT DISTINCT ON (project_id) *
      FROM rt.project_events
     ORDER BY project_id,
              CASE source WHEN 'worldbank-projects-api' THEN 1 WHEN 'kaggle-replay' THEN 0 ELSE -1 END DESC,
              event_time DESC,
              produced_at DESC,
              seq
)
UPDATE rt.projects_latest l
   SET project_name         = b.payload ->> 'project_name',
       country_iso2         = b.payload ->> 'country_iso2',
       country_name         = b.payload ->> 'country_name',
       region_name          = b.payload ->> 'region_name',
       status               = b.payload ->> 'status',
       total_commitment_usd = (b.payload ->> 'total_commitment_usd')::numeric,
       board_approval_date  = (b.payload ->> 'board_approval_date')::date,
       closing_date         = (b.payload ->> 'closing_date')::date,
       lending_instrument   = b.payload ->> 'lending_instrument',
       major_sector         = b.payload ->> 'major_sector',
       last_event_id        = b.event_id,
       last_event_type      = b.event_type,
       last_event_time      = b.event_time,
       last_produced_at     = b.produced_at,
       last_source          = b.source,
       updated_at           = now()
  FROM best b
 WHERE b.project_id = l.project_id
   AND b.event_id <> l.last_event_id;

ALTER TABLE rt.projects_latest ALTER COLUMN last_source SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'rt.projects_latest'::regclass AND conname = 'projects_latest_last_source_check'
    ) THEN
        ALTER TABLE rt.projects_latest ADD CONSTRAINT projects_latest_last_source_check
            CHECK (last_source IN ('worldbank-projects-api', 'kaggle-replay'));
    END IF;
END $$;

COMMIT;
