"""Fábricas de kafka_utils com `confluent_kafka` e `psycopg2` falsos (imports tardios via sys.modules)."""

from __future__ import annotations

import sys
import types

import kafka_utils as ku
import pytest


@pytest.fixture
def fake_kafka(monkeypatch):
    """Módulo `confluent_kafka` falso: Producer/Consumer guardam a configuração recebida."""
    module = types.ModuleType("confluent_kafka")

    class Producer:
        def __init__(self, config):
            self.config = config

    class Consumer:
        def __init__(self, config):
            self.config = config
            self.topics = None

        def subscribe(self, topics):
            self.topics = topics

    module.Producer, module.Consumer = Producer, Consumer
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    return module


def test_bootstrap_padrao_e_o_listener_interno(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    assert ku.bootstrap_servers() == "kafka:9092"


def test_bootstrap_vem_do_ambiente(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    assert ku.bootstrap_servers() == "localhost:9094"


def test_make_producer_usa_configuracao_idempotente_do_ambiente(fake_kafka, monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    producer = ku.make_producer()
    assert producer.config == ku.producer_config("localhost:9094")


def test_make_producer_bootstrap_explicito_vence_o_ambiente(fake_kafka, monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    assert ku.make_producer("broker:1").config["bootstrap.servers"] == "broker:1"


def test_make_consumer_assina_os_topicos_com_commit_manual(fake_kafka):
    consumer = ku.make_consumer("lakehouse-sink", ["worldbank.projects.events"], bootstrap="kafka:9092")
    assert consumer.topics == ["worldbank.projects.events"]
    assert consumer.config == ku.consumer_config("kafka:9092", "lakehouse-sink")
    assert consumer.config["enable.auto.commit"] is False


def test_connect_pg_passa_parametros_do_ambiente(monkeypatch):
    import psycopg2

    captured = {}
    monkeypatch.setattr(psycopg2, "connect", lambda **kw: captured.update(kw) or "conexao")
    for key, value in {"POSTGRES_HOST": "db", "POSTGRES_PORT": "6543", "POSTGRES_USER": "u"}.items():
        monkeypatch.setenv(key, value)
    assert ku.connect_pg() == "conexao"
    assert (captured["host"], captured["port"], captured["user"]) == ("db", 6543, "u")


def test_pg_params_padroes_sem_ambiente():
    params = ku.pg_params({})
    assert params == {"host": "postgres", "port": 5432, "dbname": "worldbank", "user": "wb", "password": ""}


def test_pg_params_porta_invalida_falha():
    with pytest.raises(ValueError):
        ku.pg_params({"POSTGRES_PORT": "cinco"})
