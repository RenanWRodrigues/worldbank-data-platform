# ADR 0007 — Semântica dos eventos de projeto

**Status:** aceita

## Contexto
A ADR 0004 definiu o stream de projetos, mas não fixou quando cada evento nasce nem o que é `event_time`.
Sem isso, o replay geraria eventos no futuro (`closingdate` posterior a hoje), projetos sem aprovação
(Pipeline/Dropped) virariam APPROVED no modo live, e o `rt` não teria como desempatar dois eventos do mesmo dia.

## Decisão
* **APPROVED** só para projetos com data de aprovação e status Active ou Closed; `event_time` = data de aprovação
  (meia-noite UTC). Projetos em Pipeline não geram evento até serem aprovados.
* **CLOSED** quando o status passa a Closed (live) ou na data de encerramento se `closing_date ≤ hoje` (replay);
  `event_time` = data de encerramento. O replay termina em "hoje".
* **UPDATED** para qualquer outra mudança de conteúdo detectada no live; `event_time` = hora da detecção.
* `produced_at` = hora da publicação e desempata `event_time` igual, no `rt` e no silver
  (ordem: `event_time`, `produced_at`, offset).
* `event_id` = uuid5(`project_id` | `event_type` | hash do payload) — determinístico, base da deduplicação.

## Consequências
+ Replays e reentregas não duplicam; o estado atual é reprodutível no `rt` e no silver.
+ Nenhum evento com data no futuro; Pipeline não polui métricas de aprovação.
− Se um projeto mudar e depois **voltar exatamente** ao conteúdo anterior, o UPDATED de volta tem o mesmo
  `event_id` de um evento antigo e é descartado como duplicata. Caso raro e aceito: o próximo UPDATED corrige.
↻ Para eliminar o caso acima, incluir no hash um contador de versão guardado em `ops.producer_state`
  (perde-se o determinismo entre replays).
