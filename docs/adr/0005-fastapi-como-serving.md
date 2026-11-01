# ADR 0005 — FastAPI como camada de serving (não como fonte)

**Status:** aceita

## Contexto
Na v1 a FastAPI simulava um sistema de origem servindo CSVs que poderiam ser lidos direto — uma camada
artificial. Já os dados tratados (DW e tempo real) não tinham interface para aplicações.

## Decisão
A FastAPI passa a ler `dw` e `rt` com usuário somente leitura e expor perfis, séries, rankings, projetos,
eventos ao vivo (lista e SSE), alertas e status do pipeline. Rotas dependem de um `Repository` injetável.

## Consequências
+ Padrão comum no mercado (BI para analistas, API para sistemas); testável sem banco.
+ SSE demonstra o caminho de baixa latência ponta a ponta (Kafka → rt → cliente).
− A API não substitui a fonte: extração fica no Airflow/producer.
