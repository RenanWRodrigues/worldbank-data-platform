"""kafka_utils importa sem confluent_kafka/psycopg2 e monta configurações seguras."""

import kafka_utils as ku


def test_producer_idempotente_com_acks_all():
    cfg = ku.producer_config("kafka:9092")
    assert cfg["bootstrap.servers"] == "kafka:9092"
    assert cfg["acks"] == "all" and cfg["enable.idempotence"] is True


def test_consumer_sem_auto_commit_lendo_do_inicio():
    cfg = ku.consumer_config("kafka:9092", "serving-realtime")
    assert cfg["group.id"] == "serving-realtime"
    assert cfg["enable.auto.commit"] is False  # commit só depois de persistir (at-least-once)
    assert cfg["auto.offset.reset"] == "earliest"


def test_parametros_do_postgres_vem_do_ambiente():
    env = {
        "POSTGRES_HOST": "postgres",
        "POSTGRES_USER": "wb",
        "POSTGRES_PASSWORD": "x",
        "POSTGRES_DB": "worldbank",
    }
    assert ku.pg_params(env) == {
        "host": "postgres",
        "port": 5432,
        "dbname": "worldbank",
        "user": "wb",
        "password": "x",
    }
