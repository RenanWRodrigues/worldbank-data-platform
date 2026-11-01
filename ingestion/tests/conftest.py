"""Fakes de Kafka e Postgres para testar o I/O dos serviços de ingestion sem broker nem banco.

Todos gravam o que acontece num `journal` compartilhado (lista de tuplas), para os testes conferirem a
ordem das operações (ex.: gravar → registrar → commit).
"""

from __future__ import annotations

from collections.abc import Callable

import pytest


class FakeCursor:
    """Cursor psycopg2 falso: grava SQL e parâmetros e devolve linhas prontas em `fetchall`."""

    def __init__(self, journal: list, rows: list | None = None, fail_on: str | None = None):
        self.journal = journal
        self.rows = rows or []
        self.fail_on = fail_on
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(f"falha simulada em {self.fail_on}")
        self.executed.append((sql, params))
        self.journal.append(("execute", sql, params))

    def executemany(self, sql, rows):
        rows = list(rows)
        self.executed.append((sql, rows))
        self.journal.append(("executemany", sql, rows))

    def fetchall(self):
        return list(self.rows)


class FakeConnection:
    """Conexão psycopg2 falsa: `with con` faz commit no sucesso e rollback na exceção, como a real."""

    def __init__(self, journal: list | None = None, rows: list | None = None, fail_on: str | None = None):
        self.journal = journal if journal is not None else []
        self.cur = FakeCursor(self.journal, rows, fail_on)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.commits += 1
            self.journal.append(("pg_commit",))
        else:
            self.rollbacks += 1
            self.journal.append(("pg_rollback",))
        return False

    def close(self):
        self.closed = True
        self.journal.append(("pg_close",))


class FakeMessage:
    """Mensagem do confluent_kafka: `value()`, `partition()`, `offset()`, `error()`."""

    def __init__(self, value: bytes | None, partition: int = 0, offset: int = 0, error: str | None = None):
        self._value, self._partition, self._offset, self._error = value, partition, offset, error

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class FakeConsumer:
    """Consumer falso que entrega `script` (lotes para `consume`, mensagens/None para `poll`).

    Quando o roteiro acaba chama `on_exhausted` (os testes usam para ligar a flag `_stop` do serviço).
    """

    def __init__(self, journal: list, script: list, on_exhausted: Callable[[], None]):
        self.journal = journal
        self.script = list(script)
        self.on_exhausted = on_exhausted
        self.commits = 0
        self.closed = False

    def _next(self, empty):
        if not self.script:
            self.on_exhausted()
            return empty
        return self.script.pop(0)

    def consume(self, num_messages=1, timeout=-1):
        self.journal.append(("consume", num_messages))
        return self._next([])

    def poll(self, timeout=-1):
        return self._next(None)

    def commit(self, asynchronous=True):
        assert asynchronous is False, "commit precisa ser síncrono (at-least-once)"
        self.commits += 1
        self.journal.append(("kafka_commit",))

    def close(self):
        self.closed = True
        self.journal.append(("kafka_close",))


class FakeProducer:
    """Producer do confluent_kafka: grava `produce`, pode lançar BufferError e simular falhas de entrega."""

    def __init__(self, buffer_errors: int = 0, remaining: int = 0, delivery_error: str | None = None):
        self.buffer_errors = buffer_errors
        self.remaining = remaining
        self.delivery_error = delivery_error
        self.produced: list[dict] = []
        self.polls: list[float] = []
        self.flushes: list[float] = []
        self._pending: list[Callable] = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError("fila local cheia")
        self.produced.append({"topic": topic, "key": key, "value": value})
        self._pending.append(on_delivery)

    def poll(self, timeout=0):
        self.polls.append(timeout)
        return 0

    def flush(self, timeout=None):
        self.flushes.append(timeout)
        for callback in self._pending:
            callback(self.delivery_error, None)
        self._pending = []
        return self.remaining


@pytest.fixture
def journal() -> list:
    return []


@pytest.fixture
def fakes():
    """Acesso às classes falsas (conftest não é importável pelos módulos de teste)."""

    class Fakes:
        Cursor = FakeCursor
        Connection = FakeConnection
        Message = FakeMessage
        Consumer = FakeConsumer
        Producer = FakeProducer

    return Fakes
