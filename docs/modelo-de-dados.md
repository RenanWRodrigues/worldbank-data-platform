# Modelo de dados (contrato v2)

Os nomes abaixo são o contrato entre Spark (`gold_<tabela>`), PostgreSQL (`dw.<tabela>`), FastAPI e Power BI.
Mudança aqui exige mudança coordenada em `postgres/init`, `databricks/src`, `airflow/include/sync_gold.py`
(SPECS) e, se usada, nas rotas da API e no DAX.

## ops — controle operacional

| tabela | chave | colunas | escrita por |
|---|---|---|---|
| `ops.landing_files` | `file_id` (serial) | path, dataset (`projects_events` · `indicators` · `countries`), records, first_offset, last_offset, created_at, processed_at | lake-sink, extrações do Airflow; `processed_at` pela DAG de micro-lotes |
| `ops.producer_state` | `project_id` | content_hash, status, last_event_type, updated_at | producer (modo live) |
| `ops.kafka_lag` | (measured_at, consumer_group, topic) | lag | DAG de micro-lotes |

## rt — tempo real (consumidor `serving-realtime`)

| tabela | chave | colunas | regras |
|---|---|---|---|
| `rt.project_events` | `event_id` (uuid) · `seq` serial único | event_type, project_id, event_time, produced_at, source, payload jsonb, kafka_partition, kafka_offset, consumed_at | `ON CONFLICT (event_id) DO NOTHING`; `seq` ordena o SSE da API — exige **um único** `realtime-sink` gravando (com escritores concorrentes, `seq` pode ficar visível fora de ordem e o SSE pularia eventos) |
| `rt.projects_latest` | `project_id` | project_name, country_iso2, country_name, region_name, status, total_commitment_usd, board_approval_date, closing_date, lending_instrument, major_sector, last_event_id, last_event_type, last_event_time, last_produced_at, last_source, updated_at | upsert só se `(autoridade da fonte, event_time, produced_at)` novo > o do evento atual (`worldbank-projects-api` vence `kaggle-replay`, ADR 0009; `last_source` guarda a fonte) e `event_id` ≠ `last_event_id` (empate de `event_time` desempata por `produced_at`); só eventos inseridos agora em `rt.project_events` disputam — reentrega, mesmo com `produced_at` novo, não reaplica (ADR 0008); `updated_at` = hora em que o `rt` recebeu o evento; `major_sector` com o nome oficial do grande setor ("; " do Kaggle → ", ") |
| `rt.alerts` | `alert_id` · `event_id` único | project_id, kind (`LARGE_APPROVAL`), message, amount_usd, event_time, created_at | 1 alerta por evento; `event_id` referencia `rt.project_events` (o sink grava o evento antes do alerta, na mesma transação) |

## Lakehouse (Delta)

| tabela | grão | colunas principais |
|---|---|---|
| bronze_indicators · bronze_countries · bronze_project_events | 1 linha por linha NDJSON | raw_json, source_file, file_modified_at, ingested_at |
| silver_indicators | país × indicador × ano | country_code, country_name, indicator_code, indicator_name, year, value, unit, obs_status, source, extracted_at — só `value` não nulo (um `null` da API não apaga o Kaggle); **API vence Kaggle**, depois o mais recente |
| silver_countries | país/agregado | country_code, country_name, country_iso2, region, income_level, is_aggregate |
| silver_project_events | evento | event_id (único), event_type, event_time, produced_at, source, project_id, payload, kafka_partition, kafka_offset (da linha da landing; reentrega fica com o menor offset) |
| silver_projects | projeto | estado do último evento — ordem (autoridade da fonte, event_time, produced_at, offset): Projects API vence o replay (ADR 0009) — + country_code (ISO3 resolvido) + approval_year; `major_sector` com o nome oficial ("; " do Kaggle → ", ", como no `rt`) |

## dw (PostgreSQL) = gold_* (Delta)

**dim_country** — PK `country_code` (ISO3 / código WDI, sempre 3 caracteres): `country_iso2`, `country_name`,
`region`, `income_level`, `is_aggregate` (região e renda vêm da API; países só no Kaggle entram com região nula).

**dim_indicator** — PK `indicator_code`: `indicator_name`, `unit` (`%` · `US$` · `count`).
A unidade **não** vem da API (o campo `unit` dela costuma vir vazio): sai do dicionário
`INDICATORS = {código: (coluna_agg, unidade)}` em `databricks/src/wb_transforms.py`, a fonte única de
indicadores do lakehouse. `DEFAULT_INDICATORS` do `ingestion/worldbank_client.py` (o que é extraído) deve ter os
mesmos códigos — um teste unitário compara os dois.

**dim_year** — PK `year`: `decade` ("2010s"), `year_date`. Cobre os anos das fatos existentes.

**fact_indicator** — PK (`country_code`, `indicator_code`, `year`): `value`, `source` (`worldbank-api` · `kaggle`).
Só linhas com `value` não nulo: o gold descarta os `value: null` da API (no DW a coluna é `NOT NULL`).

**fact_project** — PK `project_id`
| coluna | regra |
|---|---|
| country_code | ISO3 resolvido; **nulo** para regionais/multi-país ou país ausente de dim_country. ISO2 só vale se existir em dim_country **e** não for agregado (códigos regionais como `3A`, `1W` viram nulo); depois tenta o nome |
| country_name_raw, region_name, project_name, status, lending_instrument, major_sector | do último evento |
| board_approval_date, approval_year, closing_date | datas normalizadas |
| total_amount_usd, ibrd_amount_usd, ida_amount_usd, grant_amount_usd | dólares, ≥ 0 (a API em milhões é convertida); vêm de `total_commitment_usd`, `ibrd_commitment_usd`, `ida_commitment_usd`, `grant_amount_usd` do payload |
| last_event_type, last_event_time | rastreabilidade com o stream |

**agg_country_year** — PK (`country_code`, `year`): population, gdp_usd, gdp_per_capita_usd (= gdp/pop),
rural_population_pct, electricity_access_pct, projects_approved, projects_total_amount_usd.

**load_audit** — load_id, table_name, row_count, run_id, loaded_at.

**vw_country_year** — agg + country_name, region, income_level (sem agregados).

## Carga gold → dw (`sync_gold.py`)
COPY para temporária + `INSERT … ON CONFLICT DO UPDATE` (só reescreve linhas que mudaram) numa transação única
para todas as tabelas do job, com uma linha em `dw.load_audit` por tabela (`row_count` depois dos filtros de FK).
Origem vazia com o dw preenchido num fato/agg falha o sync em vez de espelhar (apagaria a tabela).
* Dimensões: só upsert (fatos antigos podem referenciá-las).
* Fatos e agg: upsert + `DELETE` do que sumiu da origem (espelho).
* FKs opcionais (`fact_project.country_code`) viram NULL se o pai não existir; FKs obrigatórias
  (`fact_indicator`, `agg_country_year`) filtram a linha.

**O que cada job produz e cada DAG sincroniza** — o `30_serving` sempre reconstrói `dim_year` e
`agg_country_year` a partir de **todo** o silver (indicadores e projetos), nunca só do que o job atual processou;
assim o espelho com `DELETE` não apaga colunas do outro fluxo.

| job / DAG | tabelas gold produzidas e sincronizadas (ordem do sync) |
|---|---|
| `worldbank_indicators` · `worldbank_indicators_weekly` · `worldbank_bootstrap_kaggle` | dim_country, dim_indicator, dim_year, fact_indicator, agg_country_year |
| `worldbank_projects` · `worldbank_projects_microbatch` | dim_year, fact_project, agg_country_year |

## Qualidade (falha o pipeline)
- Spark `check_quality(tables, expected)`: PK não nula e única em todas; **não vazia só nas tabelas que o job
  atual produz** (`expected`, conforme a tabela acima) — o job de projetos roda com `gold_fact_indicator` vazio
  (caso "só projetos") sem falhar. `expected_tables(job, tables)` ainda dispensa `agg_country_year` no job de
  projetos quando nenhum projeto tem país resolvido (ambiente novo, sem metadados de países).
  Também espelha o DDL do `dw` para falhar no Spark e não no sync: NOT NULL, domínios (`source`, `unit`),
  `country_code` com 3 caracteres e FKs órfãs (fact/agg → dim; `fact_project.country_code` nulo é válido).
- Silver descarta o que o DW recusaria: `source` fora de `worldbank-api`/`kaggle`, `value` NaN/infinito, ano fora
  de 1900–2100, `event_type` fora da ADR 0007; datas e timestamps inválidos viram nulos (o serverless roda com
  ANSI, em que `to_timestamp` lançaria erro e travaria o job a cada execução). Deduplicações com ordem total.
- Extração: `coverage_check` — cada indicador e os metadados com ≥ 180 países, contando **países distintos
  (não agregados) com pelo menos um valor não nulo**; linhas com `value: null` não contam.
- DW semanal: ≥ 180 países não agregados; ≥ 4 indicadores; faixas (≥ 0; % entre 0 e 100); população com
  ano ≥ ano atual − 2.
- DW micro-lote (`airflow/include/dw_quality.py`): lag de cada consumer group (medido no início da DAG)
  ≤ `KAFKA_MAX_LAG`; ano de aprovação existe em dim_year; valores ≥ 0; todo projeto de `rt.projects_latest` com
  `updated_at` (hora de chegada ao `rt`, **não** o `event_time`, que no replay é histórico) mais antigo que
  `RT_DW_MAX_DELAY_MINUTES` (padrão 60) existe no `dw`. O orçamento: até 30 min de agenda + 1 min de flush +
  5–10 min de partida do serverless + job e sync.
