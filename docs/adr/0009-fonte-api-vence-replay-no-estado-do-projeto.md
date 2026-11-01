# ADR 0009 — No estado atual do projeto, evento da Projects API vence evento do replay

**Status:** aceita (complementa a 0007 e a 0008, sem revertê-las)

## Contexto
O tópico recebe eventos das duas fontes da ADR 0006: o *replay* do Kaggle (`kaggle-replay`) e o modo *live*
(`worldbank-projects-api`). A ordem `(event_time, produced_at)` das ADRs 0007/0008 não distingue a fonte, e as duas
discordam sobre projetos prorrogados:
* O replay gera CLOSED na data de encerramento **prevista** do Kaggle (`closing_date ≤ hoje`), com `event_time` =
  essa data (ex.: P167000, 2023-12-31).
* O live vê o mesmo projeto Active na API, com `closing_date` novo (2025-12-31), e publica APPROVED com
  `event_time` = data de aprovação (2018-06-27), anterior ao CLOSED sintético.
* O CLOSED do replay vence no `rt.projects_latest` e no silver, e o live nunca corrige: o hash em
  `ops.producer_state` não muda, então nenhum evento novo é publicado.

Na execução de 01/11/2026, 306 projetos Active na API apareciam Closed no `rt` e no `dw` (283 com evento da API).

## Decisão
* O estado atual de um projeto (`rt.projects_latest` e `silver_projects`) é escolhido pela ordem
  **(autoridade da fonte, `event_time`, `produced_at`)**, com `worldbank-projects-api` (1) acima de `kaggle-replay` (0).
  Dentro da mesma fonte valem as regras das ADRs 0007/0008. A autoridade fica em `SOURCE_AUTHORITY`
  (`ingestion/events.py`) e `EVENT_SOURCE_AUTHORITY` (`databricks/src/wb_transforms.py`); um teste compara os dois.
* `rt.projects_latest` ganha `last_source`; `postgres/migrations/001_projects_latest_last_source.sql` cria a coluna
  e recalcula o estado de todos os projetos a partir de `rt.project_events` com a ordem nova (`make migrate`).
  O silver é reconstruído inteiro a cada execução do job `worldbank_projects` e se corrige sozinho.
* O producer, o contrato do evento e o `event_id` não mudam.

## Consequências
+ A API, fonte da verdade sobre projetos em andamento, nunca é sobrescrita por uma data prevista desatualizada do
  Kaggle; `rt` e silver continuam concordando (mesma ordem nos dois).
+ Replays repetidos depois do live não alteram mais o estado de projetos já vistos pela API.
− Projeto Active na API **sem** `boardapprovaldate` não gera evento (ADR 0007) e por isso não tem evento da API: se
  o replay o encerrou, continua Closed (23 casos em 01/11/2026). Fica visível pela divergência entre
  `ops.producer_state.status` e `rt.projects_latest.status`.
− Um CLOSED do replay correto (projeto encerrado de fato) também perde para um evento antigo da API do mesmo
  projeto; na prática o live rebusca por `id` quem sai de Active e publica o CLOSED da própria API.
↻ Para reverter, remover a autoridade da ordem (voltar a `(event_time, produced_at)`) e rodar a mesma migração de
  recálculo; a coluna `last_source` pode ficar como rastreabilidade.
