# Databricks notebook source
# MAGIC %md
# MAGIC # _common — utilitários dos notebooks (só I/O; a lógica fica em `src/wb_transforms.py`)
# MAGIC Usado com `%run ./_common`. Define `CATALOG`, `SCHEMA`, `VOLUME`, `RUN_ID`, `JOB`, `wt` e as funções.

# COMMAND ----------

import sys
from pathlib import Path

# src/ do bundle: o notebook roda com o diretório de trabalho na própria pasta (arquivos do workspace)
sys.path.append(str(Path("../src").resolve()))

import wb_transforms as wt
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "worldbank")
dbutils.widgets.text("schema", "lakehouse")
dbutils.widgets.text("volume", "landing")
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("job", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
RUN_ID = dbutils.widgets.get("run_id")
JOB = dbutils.widgets.get("job")

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
CHECKPOINTS = f"{LANDING}/_checkpoints"
# Pastas da landing (docs/contratos/registros-normalizados.md) → tabela bronze
LANDING_DIRS = {
    "bronze_indicators": "indicators",
    "bronze_countries": "reference/countries",
    "bronze_project_events": "projects/events",
}

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------


def fq(name: str) -> str:
    """Nome completo da tabela no Unity Catalog."""
    return f"{CATALOG}.{SCHEMA}.{name}"


def autoload_text(bronze_table: str) -> int:
    """Auto Loader (text + availableNow): arquivos novos ou regravados entram no bronze; devolve o total."""
    source = f"{LANDING}/{LANDING_DIRS[bronze_table]}"
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.partitionColumns", "")  # `date=…`/`run=…` no caminho não viram colunas
        # Reexecutar uma DAG com o mesmo run_id regrava o arquivo no mesmo caminho: relê (o silver deduplica e
        # fica com o extracted_at mais novo). Os micro-lotes de eventos têm nome único e nunca são regravados.
        .option("cloudFiles.allowOverwrites", "true")
        .load(source)
        .select(
            F.col("value").alias("raw_json"),
            F.col("_metadata.file_path").alias("source_file"),
            F.col("_metadata.file_modification_time").alias("file_modified_at"),
            F.current_timestamp().alias("ingested_at"),
        )
        .writeStream.option("checkpointLocation", f"{CHECKPOINTS}/{bronze_table}")
        .trigger(availableNow=True)
        .toTable(fq(bronze_table))
    )
    query.awaitTermination()
    return bronze(bronze_table).count()


def save(df, name: str):
    """Reconstrói a tabela inteira (silver/gold são derivados do bronze: reexecutar é idempotente).

    Devolve a tabela relida do Delta: as etapas seguintes e o check_quality não recalculam a linhagem.
    """
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq(name))
    return spark.table(fq(name))


def table_or_none(name: str):
    return spark.table(fq(name)) if spark.catalog.tableExists(fq(name)) else None


def empty_bronze():
    """Bronze vazio com as colunas certas (para silver_countries quando ainda não há metadados de países)."""
    return spark.range(0).select(
        F.lit(None).cast("string").alias("raw_json"),
        F.lit(None).cast("string").alias("source_file"),
        F.lit(None).cast("timestamp").alias("file_modified_at"),
        F.lit(None).cast("timestamp").alias("ingested_at"),
    )


def bronze(name: str):
    """Tabela bronze, ou bronze vazio se a landing ainda não teve arquivo (Auto Loader não cria a tabela)."""
    table = table_or_none(name)
    return table if table is not None else empty_bronze()


def countries_and_indicators():
    """silver_indicators e silver_countries atuais (vazios se o fluxo de indicadores ainda não rodou)."""
    ind = table_or_none("silver_indicators")
    if ind is None:
        ind = wt.silver_indicators(empty_bronze())
    countries = table_or_none("silver_countries")
    if countries is None:
        countries = wt.silver_countries(empty_bronze(), ind)
    return ind, countries
