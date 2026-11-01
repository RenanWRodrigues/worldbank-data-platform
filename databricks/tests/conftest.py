"""SparkSession local e bronze montado a partir de NDJSON em arquivos (como o Auto Loader lê a landing).

Nada aqui cria DataFrame a partir de listas Python (`createDataFrame`): isso exige o worker Python do
Spark, que não sobe em algumas máquinas Windows. Os testes passam só pela JVM, como as transformações.
"""

from __future__ import annotations

import itertools
import json
import os
import sys

import pytest

_counter = itertools.count()


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("worldbank-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")  # como no serverless: cast/parse inválido lança erro
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def bronze(spark, tmp_path):
    """bronze(*arquivos): cada arquivo é uma lista de dicts ou de linhas cruas (str) → DataFrame bronze."""
    from pyspark.sql import functions as F

    def _make(*files: list) -> object:
        folder = tmp_path / f"bronze-{next(_counter)}"
        folder.mkdir()
        paths = []
        for i, records in enumerate(files or ([],)):
            lines = [r if isinstance(r, str) else json.dumps(r) for r in records]
            path = folder / f"part-{i:03d}.json"
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            paths.append(str(path))
        return (
            # Arquivos, não a pasta: listar pasta no Windows exige a hadoop.dll
            spark.read.text(paths)
            .withColumnRenamed("value", "raw_json")
            .withColumn("source_file", F.input_file_name())
            .withColumn("file_modified_at", F.current_timestamp())
            .withColumn("ingested_at", F.current_timestamp())
        )

    return _make
