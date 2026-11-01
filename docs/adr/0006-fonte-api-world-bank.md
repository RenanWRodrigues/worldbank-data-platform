# ADR 0006 — API do World Bank como fonte; Kaggle como histórico

**Status:** aceita

## Contexto
O dataset do Kaggle é estático (dados até ~2018). Uma extração agendada só faz sentido se a fonte muda.

## Decisão
Indicadores e metadados de países vêm da Indicators API v2; projetos, da Projects API v2 (modo live).
O Kaggle continua com dois papéis: bootstrap do histórico (DAG manual, `source=kaggle`, API prevalece em
conflito) e modo *replay* do producer para demonstração reprodutível.

## Consequências
+ Dados atuais; região, renda e agregados oficiais (fim das listas fixas).
+ O projeto continua usando o dataset do tutorial do Kaggle.
− Dependência de API externa: retries, `coverage_check` e testes com respostas gravadas mitigam.
