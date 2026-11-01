# Databricks notebook source
# MAGIC %md
# MAGIC # 20_projects — micro-lotes de eventos → bronze → silver → `gold_fact_project`
# MAGIC Disparado pela DAG de micro-lotes só quando há arquivos novos em `ops.landing_files`.

# COMMAND ----------

# MAGIC %pip install pycountry==24.6.1

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

print("bronze_project_events:", autoload_text("bronze_project_events"))
events = save(wt.silver_project_events(bronze("bronze_project_events")), "silver_project_events")

# COMMAND ----------

# País do projeto: metadados do fluxo de indicadores, se já existirem (senão o país fica nulo, sem falhar)
silver_ind, silver_countries = countries_and_indicators()
projects = save(wt.silver_projects(events, silver_countries), "silver_projects")
dim_country = wt.gold_indicator_tables(silver_ind, silver_countries)["dim_country"]
fact_project = save(wt.gold_project_table(projects, dim_country), "gold_fact_project")
print("gold_fact_project:", fact_project.count())
