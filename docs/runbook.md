# Runbook (v2)

## Versões da stack

Versões fixadas (a lista completa, com o arquivo de cada uma, está na seção "Versões" do README). Se um container
mostrar outra versão, a imagem está desatualizada. Troque `<serviço>` pelo nome do serviço no compose:

* Imagens construídas pelo projeto (`api`, `projects-producer`, `lake-sink`, `realtime-sink` e os `airflow-*`):
  `docker compose build api` (ou `docker compose build` para todas) e depois `docker compose up -d`.
* Imagens oficiais (`postgres`, `kafka`, `kafka-ui`): `docker compose pull postgres` e depois `docker compose up -d`.

| Serviço | Versão esperada | Como conferir |
|---|---|---|
| Airflow | 3.1.8 | `docker compose exec airflow-scheduler airflow version` |
| Kafka (KRaft) · Kafka UI | 3.9.1 · v0.7.2 | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --version` |
| PostgreSQL | 16.6 | `docker compose exec postgres postgres --version` |
| FastAPI · Pydantic · Uvicorn | 0.115.6 · 2.14.1 · 0.34.0 | `docker compose exec api python -c "import fastapi,pydantic,uvicorn;print(fastapi.__version__,pydantic.__version__,uvicorn.__version__)"` |
| confluent-kafka (producer e sinks) | 2.13.2 | `docker compose exec realtime-sink python -c "import confluent_kafka;print(confluent_kafka.version())"` |
| Python das imagens · local | 3.12.12 · 3.12 | `docker compose exec api python --version` · `.venv/Scripts/python --version` |
| PySpark (testes locais) · Java | 3.5.3 · 17 ou 21 | `.venv/Scripts/python -c "import pyspark;print(pyspark.__version__)"` · `java -version` |
| Databricks | serverless (runtime gerenciado) | Jobs → execução → Compute; `pycountry==24.6.1` vem do `%pip install` dos notebooks |

No Git Bash do Windows, prefixe com `MSYS_NO_PATHCONV=1` os comandos com caminho absoluto dentro do container
(ex.: `/opt/kafka/...`); sem isso o caminho é convertido para `C:/Program Files/Git/opt/...` e o `exec` falha.

## Incidentes

| Sintoma | Onde olhar | Ação |
|---|---|---|
| `kafka_lag` falha | log da tarefa; `docker compose ps kafka` | Broker fora do ar ou tópico inexistente (`kafka-init`) |
| `data_quality`: "consumer group com lag acima de …" | `docker compose logs lake-sink realtime-sink`; `ops.kafka_lag` | O lag é medido no início da DAG e checado no fim (o micro-lote roda mesmo assim). Consumidor parado/travado: `docker compose restart <serviço>`. Lag alto contínuo: aumentar `RT_BATCH`/`FLUSH_MAX_EVENTS` ou `KAFKA_MAX_LAG` |
| `has_new_files`: "arquivo(s) pendente(s) fora de /Volumes/" | `ops.landing_files` (path começando com `/data/landing`) | O `lake-sink` rodou sem `DATABRICKS_HOST`/`TOKEN` e gravou em disco local. Com as credenciais no `.env` e o `lake-sink` recriado: envie os arquivos ao Volume no mesmo caminho relativo (dentro do `lake-sink`: `landing.make_landing().write(rel, bytes)`, ou `databricks fs cp -r data/landing/projects dbfs:/Volumes/<catálogo>/<schema>/<volume>/projects`) e troque o `path` deles em `ops.landing_files` para `/Volumes/...`. O silver deduplica por `event_id` |
| `has_new_files` sempre pula | logs do `lake-sink`; `ops.landing_files` | Sem eventos (producer parado? `make logs`) ou landing local por falta de credenciais Databricks |
| Página "Ao vivo"/`/v1/live` vazia | `curl localhost:8000/v1/pipeline/status` | `make replay`; conferir `realtime-sink` |
| `coverage_check` falha | log da tarefa (`cobertura abaixo de 180 países: …`) e de `extract[...]` | API do World Bank instável ou indicador com poucos países: a DAG tenta de novo; se persistir, executar mais tarde. Indicador novo em `WB_INDICATORS` com cobertura baixa por natureza: tire-o da lista |
| `extract`/`countries`/`load_kaggle_history`: "conexão databricks_default sem host ou token" | `.env` | Preencha `DATABRICKS_HOST`/`DATABRICKS_TOKEN` e recrie os containers do Airflow (`docker compose up -d`) |
| `load_kaggle_history`: "arquivo do Kaggle ausente … (rode `make data`)" | `data/raw` | Rode `make data` (dataset real do Kaggle) antes do bootstrap |
| `data_quality` semanal: "população sem ano ≥ …" | `dw.fact_indicator` (SP.POP.TOTL) | A API ainda não publicou anos recentes ou só o Kaggle (até 2017) foi carregado: rode a DAG semanal |
| `databricks_*` falha | Databricks → Jobs → execução | `check_quality` (ler mensagem); `job not found` → `DATABRICKS_JOB_ID_*` |
| `sync_dw` falha por FK/coluna | log da tarefa | Coluna nova no gold sem migração no Postgres ou sem SPEC em `sync_gold.py` |
| `sync_dw`: "DATABRICKS_HTTP_PATH vazio" | `.env` | SQL Warehouses → (warehouse) → Connection details → HTTP path; depois `docker compose up -d` para recriar os containers do Airflow |
| `sync_dw`: "gold_… veio vazio … espelho recusado" | Databricks → Catalog → `gold_<tabela>` | Proteção do espelho: origem vazia com o dw preenchido apagaria o fato inteiro. Rode o job de novo e confira o `check_quality`; nada foi alterado no dw (transação desfeita) |
| `databricks_projects` falha com "job_id" ausente | `.env` | `DATABRICKS_JOB_ID_PROJECTS` vazio: `databricks jobs list \| grep worldbank` (o bundle em modo dev prefixa o nome com `[dev <usuário>]`) |
| `data_quality` (micro-lote): "projeto no rt há mais de N min e ausente do dw" | `ops.landing_files` pendentes; log do `sync_dw` | Job de projetos ou sync atrasado/falhando; rodar a DAG manualmente. Logo depois de ligar o Airflow com o rt já cheio, a 1ª execução pode falhar até o dw alcançar o rt |
| `make test-airflow` não acha `/opt/airflow/tests` (Windows) | — | O Git Bash converte caminhos; o alvo já usa `MSYS_NO_PATHCONV=1`. Fora do make: `MSYS_NO_PATHCONV=1 docker compose run …` |
| Projetos sem país em excesso | `/checar-qualidade` (15 `country_name_raw` mais frequentes) | Regionais/multi-país são esperados. O `projects_data.csv` real corta o nome em 40 caracteres ("…Sri Lan"), usa nomes oficiais antigos ("Kingdom of Swaziland") e espaços duplos: nomes novos → `COUNTRY_OVERRIDES` em `wb_transforms.py` (com teste), `make bundle-deploy` e rodar o job de projetos + sync |
| Replay duplicou eventos? | `rt.project_events` | Não duplica: `event_id` determinístico. Reiniciar o replay é seguro |
| Producer voltou para replay depois de `make up` (estava em live) | `docker compose logs --tail=5 projects-producer` ("modo live" × "replay:") | `make live` só vale até o próximo `make up`/`docker compose up`, que recria o producer com `PRODUCER_MODE` do `.env` (padrão `replay`). Para ficar em live: `PRODUCER_MODE=live` no `.env`. Voltar para replay não perde dado (duplicatas são descartadas), mas republica o histórico inteiro |
| SSE/`/v1/live` mudos depois de `make replay` | `rt.project_events` (contagem não muda) | Esperado: replay repetido gera os mesmos `event_id`, descartados como duplicatas (o log do `realtime-sink` mostra `N eventos recebidos: 0 novos (N duplicados)`). Para ver eventos novos: `make live`, ou `curl -N "localhost:8000/v1/live/stream?backfill=5"` |
| Modo live: "0 projetos consultados" | `make logs` (producer) | A Projects API publica aprovações com atraso; aumente `LIVE_LOOKBACK_DAYS` (padrão 730). Os projetos Active vêm sempre, independentemente da janela |
| Producer em live: "Projects API indisponível (N ciclo(s) seguido(s))" | `make logs` (producer) | Rede ou API do World Bank fora do ar (já depois dos retries do cliente). O ciclo é pulado sem publicar nem salvar estado e o producer tenta de novo em `POLL_MINUTES`; nada se perde. Se N só cresce, confira a internet do host e o status da API. Falha do Kafka, ao contrário, derruba o producer (o estado só é salvo depois da confirmação) e o `restart: on-failure` o traz de volta |
| `projects-producer` parado (Exited 0) | `docker compose ps -a` | Normal no replay: ele termina e não reinicia (`restart: on-failure`) |
| Testes de integração caem num Postgres local | erro de autenticação/UTF-8 no psycopg2 | Outro Postgres na 5432 do host: `POSTGRES_HOST_PORT=5433` no `.env` e `make up` |
| Tarefa do Airflow morta com `SIGKILL` / heartbeat do scheduler atrasado | `docker compose logs airflow-scheduler \| grep SIGKILL`; `docker stats` | Falta de memória: as tarefas rodam dentro do scheduler (1 GiB). Confira a memória do Docker (`docker info --format '{{.MemTotal}}'`); no Windows, ajuste em `%USERPROFILE%\.wslconfig` (`[wsl2]` `memory=…` `swap=4GB`) e rode `wsl --shutdown`. Num notebook de 8 GB, `memory=4GB` (com 5 GB o Windows ficou sem folga e o Docker travou). Com pouca memória: `docker compose stop kafka-ui` ou `make up-stream` |
| Ativar uma DAG agendada gerou uma execução extra | Grid da DAG | Normal com `catchup=False`: o scheduler cria a execução do intervalo mais recente. As cargas são idempotentes |
| Reprocessar o lakehouse do zero | — | Apagar tabelas bronze/silver e checkpoints; Kafka tem 30 dias de retenção e a landing guarda os arquivos |
| Código só do Kaggle aparece como país (região nula) | `dw.dim_country` com `region IS NULL AND NOT is_aggregate` | Agregado do World Bank fora do `/country` da API (ex.: FCS): inclua em `KAGGLE_ONLY_AGGREGATES` (`wb_transforms.py`), deploy e rode o job de indicadores + sync. Países de verdade sem metadados (XKX, CHI) ficam como estão |
| Docker responde `500 Internal Server Error` / comandos `docker` travam | Gerenciador de Tarefas → Memória | Host sem memória (aconteceu no fim do replay real com o WSL em 5 GB num notebook de 8 GB). Feche programas, reinicie o Docker Desktop (ou `wsl --shutdown` e reabra) e baixe `memory` no `.wslconfig`. Nada se perde: os sinks retomam do último offset confirmado |
| `.wslconfig` sem efeito (Docker continua com a mesma memória) | `ls -la %USERPROFILE%\.wslconfig` | Foi criado como **pasta** ou com extensão `.txt`: precisa ser um arquivo sem extensão. Depois: sair do Docker Desktop, `wsl --shutdown`, reabrir |
| DAG "running" para sempre depois de um travamento do Docker | Grid → tarefa `databricks_*` em `deferred` | Normal: o operador é deferrable e o triggerer retoma a espera do job ao voltar. Só intervenha se o job no Databricks tiver falhado |
| `make test-airflow`: `DagBag import timeout … after 30.0s` | saída do pytest | Importação lenta com a stack de pé e pouca memória; o alvo já usa `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120`. Se persistir, pare o Airflow (`docker compose stop airflow-scheduler …`) antes |
| Databricks CLI: `Invalid access token` | `databricks auth profiles` (coluna Valid) | Token expirado: `databricks auth login --profile DEFAULT --host https://<workspace>`. Sem `--profile`, versões novas do CLI falham com "profile name must not be empty" |
| `make data`: `kaggle: command not found` | `.venv/Scripts/kaggle.exe` | O Makefile usa o CLI do `.venv` (`KAGGLE`); rode `make setup`. Credencial em `~/.kaggle/kaggle.json` (ou `KAGGLE_API_TOKEN`), nunca no repositório |
| Modo live: "0 eventos" por horas | `make logs` (producer) e `ops.producer_state` | Normal sem mudanças na API: só publica o que mudou. Projetos Active **sem** `boardapprovaldate` (~460) nunca geram evento até a data aparecer (ADR 0007) |
| Projeto Active na Projects API aparece Closed no `rt`/`dw` | `ops.producer_state.status` × `rt.projects_latest.status` | ADR 0009: a API vence o replay. Ambiente anterior à 0009: `make migrate` (recalcula o `rt`) e `make bundle-deploy` (o silver se corrige na próxima micro-batch). Sobra só projeto Active sem `boardapprovaldate` na API, que não gera evento (ADR 0007) |
| Projeto volta a Active depois de encerrado no `rt` | `rt.project_events` do projeto | Corrigido na Fase 15 (ADR 0008): reentregas não reaplicam estado e CLOSED nunca fica antes do último evento. Se aparecer, confira se o `realtime-sink`/producer foram recriados com o código novo (`docker compose up -d --build`) |
| Producer: "evento fora do contrato descartado" | `make logs` (producer); contagem "descartados" no resumo de cada ciclo | Um projeto da API gerou evento inválido (ex.: `project_id` fora de `^P\d{5,7}$`). O producer descarta, conta e segue com os demais; no live o estado do projeto não é salvo, então ele é retentado a cada ciclo e sai sozinho depois da correção. Se for formato novo da API, ajuste `worldbank_client.py` e o contrato juntos e recrie o producer |
| Power BI/Kafka UI não conectam de outra máquina | `docker compose ps` (portas `127.0.0.1:`) | Intencional: a stack só escuta no próprio host. Para expor, troque o bind no `docker-compose.yml` sabendo que o Kafka UI não tem autenticação |

## Exceções conhecidas da auditoria (`/checar-qualidade`)

Resultados que a auditoria marca, mas que **não são erro**: não corrija nem afrouxe checagens por causa deles.
Valores da auditoria de 01/11/2026; se um número sair muito destas faixas, aí sim investigue.

| Check | O que aparece | Por que é esperado |
|---|---|---|
| PIB per capita > 200 000 | Mônaco (`MCO`, até ~288 mil em 2024) e Liechtenstein (`LIE`, até ~220 mil): 8 linhas em `dw.agg_country_year` | Valores reais da API (PIB alto, população muito pequena). Outro país acima do limite → conferir PIB e população na fonte |
| Sem `country_iso2` | 1 linha: `FCS` ("Fragile and conflict affected situations") | Agregado do World Bank, sem ISO2. País de verdade sem ISO2 seria problema |
| Setores "(Historic)…" em `major_sector` | 4 projetos, 1 em cada: `(Historic)Electric Power & Other Energy` e `(Historic)Environment` (Kaggle), `(Historic)Economic Policy` e `(Historic)Multisector` (Projects API) | Classificação setorial antiga do Banco Mundial, anterior à taxonomia atual de 12 grandes setores; vem assim das duas fontes. Não mapeie para um setor atual (não há correspondência oficial). Muitos projetos novos com "(Historic)" → conferir a extração do setor (`_major_sector`) |
| Projetos sem `country_code` | ~6 % de `dw.fact_project`, quase todos regionais ("Africa", "World", "Western Africa"…) | Regra do modelo: regional/multi-país fica nulo. Grupos como `OECS Countries`, `Caucasus` e `Mercosur` também — **não** entram no `COUNTRY_OVERRIDES` (só nomes de um único país) |
| Active na Projects API e Closed no `rt` | ~23 projetos (`ops.producer_state` × `rt.projects_latest`) | Projetos Active sem `boardapprovaldate` na API não geram evento (ADR 0007); se o replay os encerrou, ficam Closed (limite da ADR 0009). Crescimento forte → rever a ADR 0007 |
| Eletricidade sem dados antes de 1990 | 0 países nas décadas 1960–1980 (`EG.ELC.ACCS.ZS`) | A série do WDI começa em 1990 |
| Último ano do PIB com menos países | 2025 com ~186 países (contra ~214 nos anos completos) | Ano corrente ainda incompleto na API. O ranking sem `?year=` já usa o último ano com ≥ 180 países (`RANKING_MIN_COUNTRIES`) |
| Linhas `source = kaggle` | só em agregados (nenhum país não agregado) | A API vence o Kaggle no silver; sobram do Kaggle só agregados que a API não devolve |
| Lag máximo alto em 7 dias | milhares de mensagens em `ops.kafka_lag` | Medição feita durante um `make replay`. O que importa é a medição mais recente (deve ser ~0) |
| Nenhum evento novo nem alerta nas últimas 24 h | `rt.project_events` e `rt.alerts` parados | Replay repetido só gera duplicatas; o live só publica o que mudou na API. Se `ops.producer_state.updated_at` ficar parado por dias **com o producer em live**, investigue os campos do hash |
