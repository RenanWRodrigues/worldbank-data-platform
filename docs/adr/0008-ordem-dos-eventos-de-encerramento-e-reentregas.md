# ADR 0008 — CLOSED nunca antes do último evento e reentregas não reaplicam estado

**Status:** aceita (complementa a 0007, sem revertê-la)

## Contexto
A revisão da Fase 15 achou dois casos em que a ordem `(event_time, produced_at)` da ADR 0007 deixava o estado
atual errado:
* **Live:** um projeto passa do `closing_date` e segue Active na API; qualquer mudança gera UPDATED com
  `event_time` = hora da detecção. Quando o status vira Closed, o CLOSED nascia com `event_time` = data de
  encerramento, **anterior** ao UPDATED, e era ignorado no `rt` e no silver — o projeto ficava Active para sempre.
  Um `closing_date` anterior à aprovação tinha o mesmo efeito (o replay já o prendia na aprovação; o live não).
* **Reentrega com `produced_at` novo:** `make replay` repetido republica o mesmo `event_id` com `produced_at` novo.
  O `realtime-sink` comparava de novo esses eventos já gravados; com APPROVED e CLOSED no mesmo dia, o APPROVED
  reentregue vencia o CLOSED e o projeto voltava a Active no `rt` (e ficava assim se o replay parasse no meio).

## Decisão
* `event_time` do CLOSED no live: hora da detecção se o último evento publicado do projeto foi UPDATED (ou se não
  há `closing_date`); senão `max(closing_date, board_approval_date)`, igual ao replay.
* No `realtime-sink`, só eventos **inseridos agora** em `rt.project_events` (`RETURNING event_id`) geram alerta e
  disputam `rt.projects_latest`; um `event_id` que já existia teve seu efeito aplicado na transação que o gravou.

## Consequências
+ CLOSED sempre vence o evento anterior do mesmo projeto; `rt` e silver (que fica com a primeira chegada de cada
  `event_id`) concordam após replays repetidos.
− O `event_time` de um CLOSED após UPDATED é a hora da detecção, não a data contratual (que segue no payload
  como `closing_date`).
↻ Guardar `last_event_time` em `ops.producer_state` permitiria `max(closing_date, último event_time)` — exige
  migração do schema `ops`.
