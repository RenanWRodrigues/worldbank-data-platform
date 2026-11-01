# ADR 0002 — Gold copiado para o PostgreSQL por upsert

**Status:** aceita (revisada na v2)

## Contexto
Power BI e FastAPI precisam de um banco sempre disponível, barato e local. Ler o SQL Warehouse a cada
consulta exigiria o warehouse ligado e credenciais em mais lugares.

## Decisão
Após cada job, o Airflow lê as tabelas `gold_*` pelo SQL Warehouse e aplica no schema `dw` com COPY em
temporária + `INSERT … ON CONFLICT` numa única transação (dimensões: só upsert; fatos: upsert + remoção do
que sumiu), registrando em `dw.load_audit`.

## Consequências
+ Sem janelas de tabela vazia; reexecução idempotente; carga de projetos não apaga indicadores.
+ Postgres vira o *serving store* de BI e API (com usuários somente leitura).
− Duas cópias do gold; volume grande pediria CDC/MERGE incremental por partição.
