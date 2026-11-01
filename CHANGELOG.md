# Changelog

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/); versões seguem SemVer.

## [Não lançado]

### Alterado (documentação)

- Link público do dashboard ("Publicar na Web" do Power BI) no README (botão no topo e seção Dashboard) e na
  seção 12 do `powerbi/GUIA_POWERBI.md`.
- Runbook, "Exceções conhecidas da auditoria": setores "(Historic)…" (4 projetos com a classificação
  setorial antiga do Banco Mundial).

## [1.1.1] - 2026-11-01

### Corrigido

- Setor dos projetos da Projects API: a API real não devolve `mjsector1Name`, e o cliente caía em `sector1.Name`,
  que é o **subsetor** ("Rural and Inter-Urban Roads" no lugar de "Transportation"); com a ADR 0009 o erro
  passou a valer também para projetos de 2000–2017. Agora `major_sector` é o grande setor do subsetor principal
  (`mjsector` na posição de `sector1` em `sector`), o mesmo significado do Kaggle; `sector` e `mjsector` entram
  no `fl`. Fixture da API no formato real. No live, os projetos afetados geram `PROJECT_UPDATED`.
- Nome do grande setor igual nas duas fontes: o Kaggle troca a vírgula do nome oficial por "; "
  ("Agriculture; Fishing and Forestry"). `rt.projects_latest` e `silver_projects` usam o nome da API
  (`canonical_sector` / `_canonical_sector`); o payload dos eventos fica intacto, para não mudar o `event_id`.
  `postgres/migrations/002_projects_latest_setor_canonico.sql` acerta as linhas antigas do `rt` (`make migrate`).

### Alterado

- Producer em live tolera queda da Projects API: um `WorldBankAPIError` (rede ou API fora, depois dos retries)
  pula o ciclo com aviso no log (`Projects API indisponível (N ciclo(s) seguido(s))`), sem publicar nem salvar
  estado, em vez de encerrar o processo com traceback. Falha do Kafka continua encerrando o producer.
- Runbook: seção "Exceções conhecidas da auditoria", com os resultados que o `/checar-qualidade` marca
  sem ser erro (PIB per capita de Mônaco e Liechtenstein, `FCS` sem ISO2, projetos regionais sem país, os ~23
  Active sem data de aprovação, entre outros).

### Alterado (documentação)

- Números de projetos atualizados depois da ADR 0009 e da correção do setor: `powerbi/validacao_kpis.sql`
  (resultados esperados de K13–K21b e K28; K22–K27 marcados como foto de 29/10), `README.md` (US$ 589 bi,
  média de US$ 107 mi em 2000–2017) e `powerbi/GUIA_POWERBI.md` (81,0% encerrados, regiões, 656 sem país,
  444 sem setor).
- Textos que descreviam a regra antiga: garantia de ordenação do README com a autoridade da fonte (ADR 0009);
  índice de ADRs do README com a 0008 e a 0009; ressalvas do guia sobre status do replay, regiões da África e
  `updated_at` (já corrigido pela ADR 0008).

## [1.1.0] - 2026-11-01

### Corrigido

- Log do `realtime-sink` dizia "N eventos recebidos no rt" mesmo quando todos eram duplicatas (replay repetido).
  Agora `apply_batch` devolve as contagens reais (eventos novos, alertas, projetos com estado alterado, via
  `RETURNING`) e o log mostra `N eventos recebidos: X novos (Y duplicados), A alertas, P projetos com estado atualizado`.
- Runbook: `make up` recria o producer com `PRODUCER_MODE` do `.env` (padrão `replay`) e desfaz um `make live`;
  para ficar em live, `PRODUCER_MODE=live` no `.env`.
- Projetos prorrogados (Active na Projects API) apareciam Closed no `rt` e no `dw`: o replay gerava CLOSED na data
  de encerramento prevista do Kaggle, mais nova que o APPROVED do live. ADR 0009: no estado atual do projeto, evento
  `worldbank-projects-api` vence `kaggle-replay` (`rt.projects_latest` e `silver_projects`, mesma ordem).
  `rt.projects_latest.last_source` novo; `postgres/migrations/001_projects_latest_last_source.sql` cria a coluna e
  recalcula o estado a partir de `rt.project_events`; `make migrate` aplica as migrações com o `realtime-sink` parado.

### Alterado

- Power BI: página inicial em 1280 × 720, como as demais (era 1920 × 1080), com botões e arte de fundo
  (`powerbi/wordlbank.svg`) realinhados; o relatório abre na Home, não mais na página Ao vivo (oculta no Service).
- README reescrito em torno do infográfico (`docs/img/infografico_animado.gif` no topo; PNG, MP4 e HTML linkados),
  com diagrama Mermaid da arquitetura, destaques, páginas do dashboard em tabela e índice da documentação.
- Imagens antigas removidas (`fluxo_projeto.png`, `fluxo_animado.*`, `arquitetura*.html`, `prototipo/`);
  `docs/img/README.md` e `powerbi/GUIA_POWERBI.md` não apontam mais para elas — o layout das páginas fica só na seção 7 do guia.
- Infográfico movido de `docs/img/linkedin/` para `docs/img/` (a cópia duplicada foi removida).
- Versão do FastAPI em uso (0.115.6) documentada no README (badge e tabela da stack), em `docs/arquitetura.md` e
  Pydantic 2.14.1 e Uvicorn 0.34.0 também no README.
- README com as versões fixadas de todas as ferramentas (badges, tabela da stack e nova tabela "Versões"):
  Airflow 3.1.8, Kafka 3.9.1, PostgreSQL 16.6, PySpark 3.5.3, Kafka UI v0.7.2 e bibliotecas principais.
  `docs/arquitetura.md` traz as mesmas versões em cada componente (imagem e bibliotecas).
  `docs/runbook.md` ganhou a seção "Versões da stack", com o comando que confere cada versão no container.
- Power BI: relatório versionado em `powerbi/relatorio/WorldBank.pbip`, com página inicial de navegação
  (arte em `powerbi/wordlbank.svg`); o
  `.gitignore` ignora `.pbi/localSettings.json` e `.pbi/cache.abf` em qualquer subpasta. Página Ao vivo: tabelas
  `rt_*` sem relacionamento (o conector PostgreSQL não dobra junção por texto em DirectQuery) e `TREATAS` no lugar
  de `CROSSFILTER`; `NOW()` no lugar de `UTCNOW()` (o Desktop converte `timestamptz` para o horário local);
  contagens dos cartões com `+ 0` (`Eventos (24h)`, do gráfico por tipo, fica sem: com ele o visual falhava ao
  filtrar Tipo de evento ou Fonte); `KEEPFILTERS` no `event_type` das medidas de aprovação (24 h), que ignoravam a
  fatia Tipo de evento; no máximo 3 conexões DirectQuery (com 10, o Postgres de 512 MB caiu). Novas
  medidas `País selecionado`, `Região selecionada` e `Valor comprometido (US$ bi)`; formatos com texto nos subtítulos.
- Power BI publicado: página Ao vivo oculta e sem ícone/botão na Home, porque o Power BI Service não alcança o
  Postgres local em DirectQuery. Ela continua no `.pbip` para uso no Desktop. README, `docs/arquitetura.md` e
  `powerbi/GUIA_POWERBI.md` (nova seção 12, "Publicar no Power BI Service") explicam a diferença.

### Corrigido (revisão pós-1.0.0)

- Documentação continua pública: o `.gitignore` não exclui mais `CHANGELOG.md`, `powerbi/` e `docs/` (o README
  aponta para ADRs, runbook, modelo de dados e Power BI). Os testes de contrato (`test_postgres_init.py`,
  `test_sync_specs.py`) não são mais pulados sem `docs/modelo-de-dados.md` e voltam a rodar no CI.
- Producer: evento fora do contrato é descartado e contado (`Publisher.rejected`, no resumo de cada ciclo) em vez de
  derrubar o serviço — com `restart: on-failure`, o live repetiria o mesmo projeto inválido para sempre. No live,
  o projeto descartado mantém o estado antigo em `ops.producer_state` e é retentado a cada ciclo (com o hash novo
  salvo, nunca mais seria publicado, nem depois de corrigido o cliente ou o contrato).
- Bootstrap do Kaggle sem os CSVs: `ExtractError` "rode `make data`" em vez de `FileNotFoundError` cru.
- `save_state` não abre transação em ciclo do live sem mudanças.
- Auto Loader com `cloudFiles.allowOverwrites`: reexecutar uma DAG com o mesmo `run_id` regrava o arquivo de
  indicadores no mesmo caminho e agora ele é relido (antes o job seguia com o dado antigo).
- Pool `dw_sync` (1 slot) no `sync_dw` das três DAGs: dois syncs não espelham mais o mesmo fato em paralelo.
- API: ranking sem `?year=` usa o último ano com ≥ 180 países (`RANKING_MIN_COUNTRIES`), não um ano parcial.
- Portas do compose publicadas só em `127.0.0.1` (o Kafka UI não tem autenticação).
- Teste de integração `test_api_live_e_status` não depende mais de evento na última hora; novo cenário de
  reentrega com `produced_at` novo contra o Postgres real (ADR 0008).
- `fetch_projects` para no `total` da resposta, além da página vazia: se a Projects API ignorasse `os` e repetisse a
  página, o modo live ficaria preso num laço infinito (e economiza uma requisição por consulta).
- `make test-airflow`: a continuação de linha (`\`) perdida no `docker compose run` foi restaurada.

## [1.0.0] - 2026-10-29

Primeira versão completa: indicadores em batch semanal e projetos em stream, do World Bank ao Power BI.

### Adicionado (Fase 16)

- README final de portfólio: problema, arquitetura, "por que cada ferramenta" (honesto sobre a escala), garantias,
  como rodar, exemplos de `curl`, marcadores dos prints do dashboard e insights com números de `validacao_kpis.sql`.
- `docs/runbook.md` com os incidentes reais do projeto (memória do WSL, `.wslconfig` criado como pasta, Docker
  travado, token do Databricks expirado, nomes de país do Kaggle, FCS, DAG em `deferred`, timeout do DagBag).

### Alterado (Fase 16)

- Infográfico do LinkedIn (`docs/img/linkedin/infografico_animado.html`): painel `load_audit` e cartões com valores
  reais do DW (59 mil linhas de indicadores, 10,6 mil projetos, 18 mil eventos, 8,1 bi de pessoas, 91,9% com
  eletricidade).

### Corrigido (Fase 15, revisão)

- `realtime-sink`: reentrega com `produced_at` novo (replay repetido) não reaplica mais o estado atual — com
  APPROVED e CLOSED no mesmo dia, o projeto voltava a Active no `rt`. Só eventos inseridos agora
  (`RETURNING event_id` → `rows_to_apply`) geram alerta e disputam `rt.projects_latest` (ADR 0008).
- Producer live: CLOSED depois de UPDATED usa a hora da detecção e, nos demais casos, `max(closing, aprovação)`
  como no replay — antes podia nascer mais antigo que o UPDATED e o projeto ficava Active para sempre (ADR 0008).
- FCS (agregado só do Kaggle) entrava em `dim_country` como país: `KAGGLE_ONLY_AGGREGATES` no silver; o dw passou
  a 217 países e o Power BI deixou de excluí-lo à mão.
- DAGs: `mark_processed` roda depois do `sync_dw` — falha no sync deixa os arquivos pendentes e a próxima
  execução refaz job e sync (antes o dw ficava desatualizado sem alarme).
- `LIVE_LOOKBACK_DAYS` com o mesmo padrão (730) no código, no compose e no `.env.example`.

### Adicionado (Fase 15)

- 106 testes de I/O com fakes (sinks, producer, repositório da API, `kafka_health`, `dag_common`, `fetch_gold`,
  `make_volume_landing`): cobertura de `make coverage` de 79% para ~97%; `fail_under = 80` no `pyproject.toml`.
- CI: job `pre-commit` (inclui gitleaks) e job de unitários + Spark com `--cov-fail-under=80`.
- `make test-airflow` com `DAGBAG_IMPORT_TIMEOUT=120` (importar as DAGs passava de 30 s com a stack de pé).

### Adicionado (Fase 14)

- `powerbi/GUIA_POWERBI.md`: 8 perguntas de negócio e KPIs, conexão (usuário `powerbi`, somente leitura),
  modelo composto (`dw` em Importar, `rt` em DirectQuery), relacionamentos, tema, layout das 5 páginas
  (Perfil do País por drill-through; Ao vivo com atualização automática de 30 s), validação dos cartões,
  fuso horário, ressalvas dos dados e como salvar em `.pbip`.
- `powerbi/medidas.dax`: medidas — ano de referência (último ano com ≥ 180 países nos
  4 indicadores; hoje 2024), estoque × fluxo, agregados excluídos, % ponderados pela população, projetos,
  ranking de PIB e medidas DirectQuery da página Ao vivo (janelas por `consumed_at`/`created_at` em UTC).
- `powerbi/validacao_kpis.sql`: uma consulta por KPI (mundo, Brasil e Ao vivo) com os resultados reais de
  29/10/2026 colados como comentário.
- Ressalvas registradas no guia: FCS gravado como país (resolvido na Fase 15),
  histórico de projetos completo só em 2000–2017, `rt.projects_latest.updated_at` atualizado por replay
  repetido em projetos com APPROVED e CLOSED no mesmo `event_time`, lag do Kafka fora do alcance do `powerbi`.

### Corrigido (Fase 13, execução real)

- `projects_data.csv` real não tem `countrycode` e traz o setor em `mjsector` ("Setor;Setor", com nomes como
  "Agriculture; Fishing and Forestry"): `normalize_project` lê `mjsector` (separador `;` sem espaço). Sem isso,
  todos os projetos do replay ficariam com `major_sector` nulo. Amostra sintética, testes e contrato
  (`registros-normalizados.md`) passaram a refletir as colunas reais.
- `sync_dw` da carga semanal foi morto por falta de memória (SIGKILL) no scheduler (LocalExecutor, 1 GiB):
  `fetch_gold` lê o gold em lotes de 10 mil linhas (antes 50 mil).
- `make data` usa o CLI do Kaggle do `.venv` (`KAGGLE`); `make replay`/`make live` reconstroem a imagem do
  producer (`--build`), que copia o código no build; `make test-airflow` não deixa `.pyc` nas pastas montadas.
- Resposta real da Indicators API (indicador, países e erro) conferida com `tests/fixtures/api/`: mesmo formato.
- País dos projetos do Kaggle: o `countryname` real vem truncado em 40 caracteres ("…Sri Lan"), com nomes oficiais
  antigos ("Republic of Turkey", "Kingdom of Swaziland") e espaços duplos ("Islamic Republic of Afghanistan").
  `resolve_country_name` normaliza os espaços e `COUNTRY_OVERRIDES` ganhou 18 países (719 projetos de
  um país só que ficavam sem `country_code`).

### Adicionado (Fase 12)

- `airflow/include/wb_extract.py`: `extract_indicator`, `extract_countries` e `bootstrap_kaggle` gravam NDJSON no
  Volume (`make_volume_landing`, com host e token da conexão `databricks_default`, sem fallback local) e registram
  cada arquivo em `ops.landing_files` (`RETURNING file_id`; reexecução regrava e devolve à fila). Caminhos do
  contrato (`indicators/source=…/indicator=…/run=…`, `reference/countries/run=…`); `coverage_report` e
  `check_coverage` (≥ 180 países distintos não agregados com valor, por indicador e nos metadados).
- DAG `worldbank_indicators_weekly` (`10 6 * * 1`): `indicator_list` → `extract` (mapeado, 2 em paralelo, 3
  tentativas com espera crescente) + `countries` → `coverage_check` → `databricks_indicators` → `mark_processed`
  → `sync_dw` → `data_quality` (`dw_quality.weekly_checks`: ≥ 180 países, ≥ 4 indicadores, valores ≥ 0, % em
  0–100, população com ano ≥ atual − 2).
- DAG `worldbank_bootstrap_kaggle` (manual): `load_kaggle_history` → `databricks_indicators` → `mark_processed` →
  `sync_dw`. Resolve a pendência da Fase 9: o bootstrap também extrai `/country`, então WLD, ARB, HIC etc. já
  entram em dim_country como agregados.
- `dag_common.py` (conexão Postgres e `databricks_job` no pool), usado pelas três DAGs;
  `landing_control.mark_files` (marca só os `file_id` da execução).
- Testes: `tests/test_wb_extract.py` (sessão falsa + fixtures da API + amostra do Kaggle), checagens semanais e
  `mark_files` em `test_include.py`, integridade das três DAGs (36 testes em `make test-airflow`).

### Alterado (Fase 12)

- As DAGs semanal e de bootstrap ganharam `mark_processed` (não previsto no plano): sem ele os arquivos de
  indicadores ficariam pendentes para sempre em `ops.landing_files` e em `/v1/pipeline/status`.
- Imagem do Airflow com `defusedxml` (kaggle_loader); `WB_INDICATORS` e `RAW_DATA_DIR` no ambiente do Airflow.

### Adicionado (Fase 11)

- Fase 11: `airflow/include/sync_gold.py` — `SPECS` (colunas na ordem do DDL, chave, espelho, `fk_nullify`,
  `fk_filter`), `JOB_TABLES`, SQL gerado por funções puras (`create_staging_sql`, `copy_sql`, `upsert_sql` com
  `IS DISTINCT FROM` para não reescrever linhas iguais, `audit_sql`), `to_csv` (NULL ≠ string vazia),
  `check_mirror` (recusa espelhar fato vazio sobre dw preenchido) e `sync_tables` (COPY em temporárias, todas as
  tabelas do job numa transação, `dw.load_audit`); leitura do gold pelo SQL Warehouse (`DATABRICKS_HTTP_PATH`).
  `kafka_health.py` (`compute_lag` puro, lido do broker sem entrar no grupo, grava em `ops.kafka_lag`),
  `landing_control.py` (pendentes e `mark_processed` limitados ao maior `file_id` visto antes do job) e
  `dw_quality.py` (checagens do micro-lote, incluindo rt × dw por `updated_at` e o lag ≤ `KAFKA_MAX_LAG`).
- DAG `worldbank_projects_microbatch` (`*/30`, sem catchup, 1 execução por vez): `kafka_lag` → `has_new_files`
  (short-circuit) → `databricks_projects` (`DatabricksRunNowOperator` deferrable no pool `databricks`) →
  `mark_processed` → `sync_dw` → `data_quality`.
- Testes: `airflow/tests/test_include.py` (22, também em `make test`), `airflow/tests/test_dag_integrity.py`
  (no container) e `tests/test_sync_specs.py` (SPECS × DDL: colunas, PKs e FKs; `JOB_TABLES` × docs × lakehouse).

### Corrigido (Fase 11)

- `mark_processed` marcava como processados os micro-lotes gravados em disco local (lake-sink sem credenciais),
  que o Databricks nunca leu: o dw ficou com 37 de 2239 projetos. Agora o `has_new_files` falha se houver
  pendente fora de `/Volumes/` (`landing_control.check_in_volume`). Os 16 arquivos locais (2498 eventos) foram
  reenviados ao Volume e devolvidos à fila.

### Alterado (Fase 11)

- Airflow recebe `DATABRICKS_HTTP_PATH`, `DATABRICKS_JOB_ID_*`, catálogo/schema/volume, `KAFKA_BOOTSTRAP_SERVERS`,
  `KAFKA_MAX_LAG` e `RT_DW_MAX_DELAY_MINUTES`; `pytest` na imagem do Airflow.
- `make test-airflow` roda num container avulso (`docker compose run --no-deps`), sem precisar do Airflow de pé,
  e com `MSYS_NO_PATHCONV=1` para o Git Bash do Windows.
- O limite de lag sai da tarefa `kafka_lag` e vai para o `data_quality`: o micro-lote é processado mesmo com lag
  alto e a DAG fica vermelha no fim.

### Adicionado (Fase 10)

- Fase 10: notebooks `databricks/notebooks/` — `_common.py` (widgets catalog/schema/volume/run_id/job,
  `autoload_text` com Auto Loader text + `availableNow`, `save` que regrava e relê do Delta, `table_or_none`,
  `bronze` com fallback vazio), `00_setup.py` (catálogo, schema, Volume e pastas da landing), `10_indicators.py`,
  `20_projects.py` e `30_serving.py` (dim_year/agg de todos os fatos + `check_quality` sobre as tabelas relidas);
  `databricks/databricks.yml` com os jobs serverless `worldbank_indicators` (00 → 10 → 30) e
  `worldbank_projects` (00 → 20 → 30).

### Alterado (Fase 10)

- `make bundle-validate`/`bundle-deploy` aceitam `DBX_PROFILE=<perfil>` do Databricks CLI (o `DEFAULT` pode estar
  com token vencido enquanto o login OAuth cria outro perfil).
- `pycountry` instalado por `%pip install` nos notebooks em vez de `environments` no bundle: segundo o schema do
  Databricks CLI, `environment_key` só vale para tarefas Python script/wheel/dbt no serverless.
- Fase 9: gold em `wb_transforms.py` — `gold_indicator_tables` (dim_country com código de 3 caracteres,
  dim_indicator com unidade de `INDICATORS`, fact_indicator só de `INDICATORS` e com país em dim_country),
  `gold_project_table` (FK de país válida ou nula, valores ≥ 0), `gold_serving_tables` (dim_year e
  agg_country_year de todos os fatos que existirem; PIB per capita = PIB / população), `check_quality` com
  `PRIMARY_KEYS`, `DDL_RULES` e `FOREIGN_KEYS`, `JOB_TABLES` e `expected_tables`. Teste de contrato: colunas e
  ordem do gold iguais às do `postgres/init/01_init.sql`.
- Revisão de engenharia de dados (sem bloqueadores) aplicada: parsing tolerante ao ANSI do serverless
  (`try_to_timestamp`, datas pelos 10 primeiros caracteres, sem depender do fuso), chave de país normalizada antes
  do dedup, `source`/`event_type` fora do domínio, NaN/infinito e ano fora de 1900–2100 descartados, ordem total
  nos `row_number` (eventos ficam com a primeira chegada, como o rt), caso "só projetos" sem países. Fixture Spark
  com ANSI ligado e testes de borda em `databricks/tests/test_revisao_dados.py`.

### Adicionado (Fases 7 e 8)

- Fase 8: `databricks/src/wb_transforms.py` — `INDICATORS` (fonte única, comparada com `DEFAULT_INDICATORS`),
  schemas explícitos, `silver_indicators` (API > Kaggle > mais recente; nulos descartados antes da prioridade),
  `silver_countries` (metadados da API + países só nos indicadores com região nula), `silver_project_events`
  (dedup por `event_id`), `silver_projects` (último evento por event_time/produced_at/offset; país por ISO2 só se
  existir e não for agregado, depois nome via `COUNTRY_OVERRIDES` + `pycountry.lookup` exato) e
  `resolve_country_name`. Tudo só pela JVM (sem UDF nem `createDataFrame`): o mapa de nomes vira literais.
  21 testes Spark (`make test-spark`) com bronze lido de arquivos NDJSON + 20 testes puros em `make test`.
- `lake-sink` grava `kafka_partition` e `kafka_offset` em cada linha da landing (desempate do silver).
- Fase 7: FastAPI de serving em `api/` — `schemas.py` (Pydantic, `Page[T]`), `repository.py` (protocolo
  `Repository` + `PostgresRepository` com pool criado no primeiro uso, usuário `api_ro`, sessão read-only e
  `statement_timeout`, SQL parametrizado) e `main.py` com `/health` (503 sem banco), países, indicadores, rankings
  (sem agregados), projetos (dw + rt + eventos), `/v1/live/projects|alerts`, `/v1/live/stream` (SSE por `seq`,
  `Last-Event-ID`, `backfill`, heartbeat) e `/v1/pipeline/status`; API key opcional (`X-API-Key`). 23 testes com
  `TestClient` + `FakeRepo` (incluindo SSE e API key). `api/Dockerfile` não-root com healthcheck e serviço `api`
  no compose. Exemplos de `curl` no README.

### Corrigido (pós-Fase 7)

- Modo live só consultava aprovações dentro de `LIVE_LOOKBACK_DAYS`: mudanças em projetos antigos (inclusive o
  encerramento) nunca eram detectadas. Agora `collect_live_projects` une todos os Active, a janela de aprovação e
  a rebusca por `id` dos que estavam Active no estado e sumiram (`ids_to_recheck`). `fetch_projects` ganhou os
  filtros `status` e `ids` e pede só os campos usados (`PROJECT_FIELDS` via `fl`); páginas de 1000.
- Runbook: SSE mudo após replay (idempotência), live vazio por atraso da API, producer parado após o replay,
  Postgres local na 5432.

### Alterado (Fase 7)

- `LIVE_LOOKBACK_DAYS` padrão 365 → 730: a Projects API publica aprovações com atraso (em out/2026 a mais recente
  era de dez/2024) e uma janela de 1 ano voltava vazia.
- Teste de integração do SSE usa `backfill=1`: replays repetidos não geram eventos novos (idempotência), então
  esperar um evento inédito durante o teste o tornava instável.
- Fase 6: `ingestion/landing.py` (Volume via Files API com retries ou disco local em `LANDING_DIR`, mesmo layout,
  escrita atômica), `ingestion/lake_sink.py` (`Buffer` puro por `FLUSH_MAX_EVENTS`/`FLUSH_MAX_SECONDS`, descarta
  inválidos sem travar o offset, NDJSON em `projects/events/date=…/part-p<partição>o<offsets>-<uuid8>.json`,
  registro em `ops.landing_files` e só então commit) e `ingestion/realtime_sink.py` (`to_rows`, `should_apply`,
  `latest_per_project` puros; uma transação com eventos → alertas → upsert que nunca regride; commit após o banco;
  `RT_BATCH`). Serviços `lake-sink` e `realtime-sink` no compose. Teste de integração do SQL real do upsert
  (empate no mesmo dia, atraso, reentrega).

### Corrigido (Fase 6)

- `test_eventos_chegam_ao_rt_em_segundos` contava linhas novas no `rt`, o que replays repetidos (mesmos
  `event_id`) nunca produzem; agora mede a latência mediana publicação → `rt` dos últimos 50 eventos (≤ 30 s).
- Fase 5: `ingestion/events.py` (`make_event` com `event_id` uuid5 de projeto|tipo|hash, `validate_event` contra o
  JSON Schema, `encode`/`decode`), `ingestion/projects_producer.py` (`replay_schedule`, `replay_delays` e
  `diff_events` puras conforme a ADR 0007; modos replay e live, estado em `ops.producer_state` salvo só após o
  `flush`) e `ingestion/kafka_utils.py` (configs puras + imports tardios). Compose: Kafka 3.9.1 KRaft
  (`kafka:9092` / `localhost:9094`, auto-create desligado), `kafka-init` (6 partições, 30 dias), Kafka UI v0.7.2 em
  `:8085` e `projects-producer` (imagem `ingestion/Dockerfile`, não-root; replay termina com código 0 e
  `restart: on-failure`). `.dockerignore` na raiz deixa `.env`, dados e `.venv` fora do contexto de build.
- Fase 4: `ingestion/worldbank_client.py` (Indicators API com `page/pages` e `per_page=1000`, `/country` com
  agregados, Projects API com `os/rows` e `projects` como objeto ou lista; retries com backoff e `Retry-After` em
  429/5xx; normalização pura: datas ISO e m/d/Y, "6,060,000", `curr_*` em milhões, negativos e `""` → null,
  `content_hash`, `country_coverage`, `indicator_codes`) e `ingestion/kaggle_loader.py` (CSV WDI pulando
  metadados, população em CSV/JSON/SQLite/XML, largo → longo com `source=kaggle`, projetos normalizados), só com
  biblioteca padrão + `requests` e `defusedxml` (fixados em `ingestion/requirements.txt`). Testes com sessão
  HTTP falsa e a amostra sintética.
- Fase 3: `docker-compose.yml` (projeto `worldbank-platform`, `postgres:16.6` com healthcheck, volume `pgdata`,
  `mem_limit`), `postgres/init/00_users.sh` (usuários `powerbi`/`api_ro` com senhas do ambiente e sessão
  read-only) e `postgres/init/01_init.sql` (banco `airflow`; schemas ops, rt, dw; view `dw.vw_country_year`;
  GRANTs somente leitura).
- `tests/test_postgres_init.py`: confere o SQL de init contra `docs/modelo-de-dados.md` (tabelas, colunas,
  permissões, nenhuma senha literal) sem Docker.
- Testes de integração dos usuários de leitura (leem o que devem, não escrevem, `powerbi` não lê `ops`);
  credenciais, banco e porta lidos do ambiente ou do `.env`.
- Contrato (`docs/modelo-de-dados.md`): `dim_country.country_code` com 3 caracteres, `fact_indicator` só com
  `value` não nulo e `rt.alerts.event_id` referenciando `rt.project_events` — como já está no `01_init.sql`.
- Estrutura inicial: specs, ADRs e CI.
- ADR 0007 (semântica dos eventos de projeto: quando nasce cada evento, `event_time`, desempate por `produced_at`).
- `make up-stream` (fluxo de projetos sem Airflow) e variáveis `POWERBI_PASSWORD`, `API_RO_PASSWORD`,
  `KAFKA_HEAP_OPTS` e `RT_DW_MAX_DELAY_MINUTES` no `.env.example`.
- Testes de contrato: valores IBRD/IDA/grant negativos são inválidos; ISO2 regional e valores nulos são válidos.

- Airflow 3.1.8 no compose (parte de infraestrutura da Fase 11): imagem `airflow/Dockerfile` com providers
  databricks, postgres e fab, `confluent-kafka` e `databricks-sql-connector` (constraints oficiais);
  `airflow-init` (migração, admin, pool `databricks` com 1 slot), apiserver em `:8080`, scheduler, dag-processor e
  triggerer com healthcheck e `mem_limit`; conexões `postgres_dw` e `databricks_default` por `AIRFLOW_CONN_*`.

### Alterado

- `make psql` usa `POSTGRES_USER`/`POSTGRES_DB` do container em vez de `wb`/`worldbank` fixos.
- `make` usa o Python e o ruff do `.venv` quando ele existe (o `python` do PATH pode não ter as dependências).
- Porta do Postgres no host configurável por `POSTGRES_HOST_PORT` (padrão 5432), para conviver com um Postgres
  local; os testes de integração usam essa variável.
- Healthcheck do Postgres via TCP: não fica "saudável" durante os scripts de init (servidor temporário só em socket).

### Removido

- `postgres/init/.gitkeep` (a pasta já tem os scripts de init).

### Alterado (specs, revisão da Fase 1)

- `check_quality` exige "não vazia" só nas tabelas que o job produz (caso "só projetos").
- Consistência rt × dw medida por `rt.projects_latest.updated_at` com limite configurável (padrão 60 min).
- Usuários `powerbi`/`api_ro` criados por `postgres/init/00_users.sh` com senhas do ambiente; `api_ro` lê `ops.kafka_lag`.
- Tabelas sincronizadas por job/DAG documentadas; `30_serving` reconstrói `dim_year`/`agg_country_year` de todo o silver.
- Replay sem eventos no futuro e sem projetos não aprovados; live não publica Pipeline.
- `rt.projects_latest` ganha `last_event_id` e `last_produced_at`; upsert desempata por `produced_at`.
- Contrato: IBRD/IDA/grant com `minimum: 0`; mapeamento completo do payload em `registros-normalizados.md`.
- `INDICATORS` (código → coluna, unidade) como fonte única de indicadores do lakehouse.
- `coverage_check` conta países distintos com valor não nulo; Indicators API com `per_page=1000`.
- `pycountry` declarado em `environments` do bundle; `mem_limit` e heap do Kafka limitados para caber em 8 GB.

### Corrigido

- `Makefile` chamava `python3`, inexistente no Windows nativo.
- CI: job Spark quebrava com "nenhum teste coletado" (código 5) antes da Fase 8.
- Formatação ruff em 4 arquivos (o `ruff format --check` do CI falhava).
- `make test-spark` quebrava com código 5 antes da Fase 8 (mesma proteção do CI).

### Atualizado

- Hooks do pre-commit (`pre-commit autoupdate`): pre-commit-hooks v6.0.0, ruff v0.17.3 (id `ruff-check`),
  gitleaks v8.30.0; ruff alinhado em 0.17.3 no `requirements-dev.txt` e no CI.
