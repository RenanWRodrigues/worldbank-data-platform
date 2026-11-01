-- Grande setor com o nome oficial (o da Projects API) no estado atual: o Kaggle troca a vírgula do nome por "; "
-- ("Agriculture; Fishing and Forestry" → "Agriculture, Fishing and Forestry"). O realtime-sink já grava assim;
-- esta migração acerta as linhas antigas. rt.project_events.payload fica como veio da fonte (event_id intacto).
-- Idempotente; `updated_at` não muda (não é chegada de evento novo).
BEGIN;

UPDATE rt.projects_latest
   SET major_sector = replace(major_sector, '; ', ', ')
 WHERE major_sector LIKE '%; %';

COMMIT;
