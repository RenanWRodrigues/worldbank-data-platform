# ADR 0001 — Eventos chegam ao lakehouse por micro-lotes num Volume

**Status:** aceita (revisada na v2)

## Contexto
Kafka roda localmente em Docker; o Databricks roda na nuvem e não alcança `localhost`. O compute serverless
(Free Edition) só aceita Structured Streaming com `Trigger.AvailableNow`, sem streaming contínuo.

## Decisão
O consumer group `lakehouse-sink` fecha micro-lotes NDJSON (por quantidade ou tempo) e os envia ao Volume via
Files API (conexão de saída). O bronze lê com Auto Loader `availableNow`, disparado pelo Airflow a cada 30 min
só quando `ops.landing_files` tem arquivos pendentes.

## Consequências
+ Funciona atrás de NAT; sem expor o Kafka; bronze imutável e reprocessável.
+ Compute só liga quando há dado novo.
− Latência de minutos no lakehouse — compensada pelo caminho `rt` (ADR 0004).
↻ Com Kafka gerenciado e acessível, o bronze pode ler `format("kafka")` direto (Lakeflow em modo contínuo).
