# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — catálogo, schema, Volume da landing e pastas (idempotente)

# COMMAND ----------

# MAGIC %pip install pycountry==24.6.1

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as exc:  # Free Edition pode não permitir criar catálogo
    if not spark.catalog.databaseExists(f"{CATALOG}.information_schema"):
        raise RuntimeError(
            f"Não consegui criar o catálogo {CATALOG!r} ({exc}). Crie-o pela interface ou use o catálogo "
            "existente (ex.: `workspace`) em DATABRICKS_CATALOG e na variável `catalog` do bundle."
        ) from exc

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# Auto Loader falha se a pasta de origem não existir: cria todas antes da primeira carga
for folder in [*LANDING_DIRS.values(), "_checkpoints"]:
    dbutils.fs.mkdirs(f"{LANDING}/{folder}")

print(f"pronto: {CATALOG}.{SCHEMA}, landing em {LANDING} (run {RUN_ID})")
