# ADR 0003 — Airflow 3.1 com LocalExecutor

**Status:** aceita

## Contexto
A linha 2.x do Airflow saiu do suporte; a 3.x traz Task SDK, API server e dag-processor separado.

## Decisão
Airflow 3.1 em Docker com LocalExecutor (sem Celery/Redis), autenticação FAB, segredo JWT compartilhado
entre os containers, DAGs escritas com `airflow.sdk`.

## Consequências
+ Stack menor (sem Redis/worker) adequada para uma máquina local.
− Não escala horizontalmente; para produção, trocar por CeleryExecutor ou KubernetesExecutor.
