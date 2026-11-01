"""Lag dos consumer groups do tópico de projetos → ops.kafka_lag (tarefa `kafka_lag` da DAG de micro-lotes).

`compute_lag` é puro; `measure_lag` consulta o broker sem entrar no grupo (só lê offsets commitados e
watermarks), então não dispara rebalanceamento nos sinks.
"""

from __future__ import annotations

from collections.abc import Mapping

TOPIC = "worldbank.projects.events"
GROUPS = ("lakehouse-sink", "serving-realtime")  # GROUP_ID de ingestion/lake_sink.py e realtime_sink.py

INSERT_SQL = "INSERT INTO ops.kafka_lag (measured_at, consumer_group, topic, lag) VALUES (now(), %s, %s, %s)"


def compute_lag(watermarks: Mapping[int, tuple[int, int]], committed: Mapping[int, int | None]) -> int:
    """Soma, por partição, de `high − commitado`.

    `watermarks` = {partição: (low, high)}; `committed` = {partição: offset} (None ou negativo = grupo sem
    commit, que com `auto.offset.reset=earliest` lerá desde `low`). Commit abaixo de `low` (mensagens já
    expiradas pela retenção) também conta desde `low`.
    """
    total = 0
    for partition, (low, high) in watermarks.items():
        offset = committed.get(partition)
        start = low if offset is None or offset < low else offset
        total += max(high - start, 0)
    return total


def over_limit(lags: Mapping[str, int], max_lag: int) -> dict[str, int]:
    """Grupos com lag acima de `max_lag`."""
    return {group: lag for group, lag in lags.items() if lag > max_lag}


def measure_lag(bootstrap: str, groups: tuple[str, ...] = GROUPS, topic: str = TOPIC) -> dict[str, int]:
    """{grupo: lag} lido do broker."""
    from confluent_kafka import Consumer, TopicPartition

    lags: dict[str, int] = {}
    for group in groups:
        consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
        try:
            metadata = consumer.list_topics(topic, timeout=10).topics[topic]
            if metadata.error is not None:
                raise RuntimeError(f"tópico {topic}: {metadata.error}")
            partitions = [TopicPartition(topic, p) for p in metadata.partitions]
            committed = {tp.partition: tp.offset for tp in consumer.committed(partitions, timeout=10)}
            watermarks = {
                tp.partition: consumer.get_watermark_offsets(tp, timeout=10, cached=False)
                for tp in partitions
            }
            lags[group] = compute_lag(watermarks, committed)
        finally:
            consumer.close()
    return lags


def record_lag(cur, lags: Mapping[str, int], topic: str = TOPIC) -> None:
    """Uma linha por grupo em ops.kafka_lag, todas com o mesmo `measured_at` (now() da transação)."""
    cur.executemany(INSERT_SQL, [(group, topic, lag) for group, lag in lags.items()])
