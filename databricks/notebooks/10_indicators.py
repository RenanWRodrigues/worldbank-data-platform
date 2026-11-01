# Databricks notebook source
# MAGIC %md
# MAGIC # 10_indicators — landing → bronze → silver → gold de indicadores e países
# MAGIC Produz `gold_dim_country`, `gold_dim_indicator` e `gold_fact_indicator`; `30_serving` faz o resto.

# COMMAND ----------

# MAGIC %pip install pycountry==24.6.1

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

loaded = {name: autoload_text(name) for name in ("bronze_indicators", "bronze_countries")}
print("bronze:", loaded)

# COMMAND ----------

silver_ind = save(wt.silver_indicators(bronze("bronze_indicators")), "silver_indicators")
silver_countries = save(wt.silver_countries(bronze("bronze_countries"), silver_ind), "silver_countries")

# COMMAND ----------

gold = {
    name: save(df, f"gold_{name}")
    for name, df in wt.gold_indicator_tables(silver_ind, silver_countries).items()
}
print({name: df.count() for name, df in gold.items()})
