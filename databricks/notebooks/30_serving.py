# Databricks notebook source
# MAGIC %md
# MAGIC # 30_serving — `gold_dim_year` e `gold_agg_country_year` de TODOS os fatos + `check_quality`
# MAGIC Sempre a partir de todo o gold/silver (indicadores e projetos), nunca só do job atual: assim o espelho
# MAGIC do sync com DELETE não apaga colunas do outro fluxo. O parâmetro `job` define o que precisa
# MAGIC sair não vazio.

# COMMAND ----------

# MAGIC %pip install pycountry==24.6.1

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

if JOB not in wt.JOB_TABLES:
    raise ValueError(f"parâmetro job inválido: {JOB!r} (use {sorted(wt.JOB_TABLES)})")

fact_indicator = table_or_none("gold_fact_indicator")
fact_project = table_or_none("gold_fact_project")
for name, df in wt.gold_serving_tables(fact_indicator, fact_project).items():
    save(df, f"gold_{name}")

# COMMAND ----------

# Relidas do Delta: a checagem não recalcula a linhagem e vê exatamente o que o sync vai copiar
tables = {name: t for name in wt.PRIMARY_KEYS if (t := table_or_none(f"gold_{name}")) is not None}
expected = wt.expected_tables(JOB, tables)
counts = wt.check_quality(tables, expected)
print(f"check_quality ok (job {JOB}, run {RUN_ID}, exigidas {expected}):", counts)
