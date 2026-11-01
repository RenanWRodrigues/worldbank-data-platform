"""Fábricas de producer, consumer e conexão Postgres com import tardio (testes não precisam das libs)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


def producer_config(bootstrap: str) -> dict[str, Any]:
    """Producer idempotente: acks=all e sem duplicatas em retries do próprio producer."""
    return {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "zstd",
    }


def consumer_config(bootstrap: str, group_id: str) -> dict[str, Any]:
    """Commit manual (só depois de persistir) e leitura do início para grupos novos."""
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }


def pg_params(env: Mapping[str, str] = os.environ) -> dict[str, Any]:
    return {
        "host": env.get("POSTGRES_HOST", "postgres"),
        "port": int(env.get("POSTGRES_PORT", "5432")),
        "dbname": env.get("POSTGRES_DB", "worldbank"),
        "user": env.get("POSTGRES_USER", "wb"),
        "password": env.get("POSTGRES_PASSWORD", ""),
    }


def bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def make_producer(bootstrap: str | None = None):
    from confluent_kafka import Producer

    return Producer(producer_config(bootstrap or bootstrap_servers()))


def make_consumer(group_id: str, topics: list[str], bootstrap: str | None = None):
    from confluent_kafka import Consumer

    consumer = Consumer(consumer_config(bootstrap or bootstrap_servers(), group_id))
    consumer.subscribe(topics)
    return consumer


def connect_pg():
    import psycopg2

    return psycopg2.connect(**pg_params())
