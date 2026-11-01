# ADR 0004 — Batch para indicadores, stream com fan-out para projetos

**Status:** aceita

## Contexto
Indicadores do World Bank são anuais e revisados poucas vezes por ano. Projetos mudam continuamente
(aprovação, alteração, encerramento) e interessam a consumidores com necessidades diferentes:
histórico analítico completo e visão operacional imediata.

## Decisão
* Indicadores: extração semanal da API pelo Airflow, direto para a landing (sem Kafka — seria artificial).
* Projetos: eventos no tópico `worldbank.projects.events` (chave `project_id`, 6 partições, 30 dias) com dois
  consumer groups independentes: `lakehouse-sink` (micro-lotes) e `serving-realtime` (Postgres `rt` em segundos,
  alertas). `event_id` determinístico garante dedup a jusante (at-least-once).

## Alternativas consideradas
- Tudo em batch: perde a visão em tempo real e o fan-out.
- Tudo em stream (inclusive indicadores): complexidade sem ganho; o dado é anual.
- Fila simples (Redis/SQS): sem retenção/replay nem consumer groups com offsets independentes.

## Consequências
+ Cada ferramenta tem um motivo; novos consumidores (ex.: notificações) entram sem tocar no producer.
− Mais serviços para operar; mitigado com medição de lag no Airflow e `/v1/pipeline/status`.
