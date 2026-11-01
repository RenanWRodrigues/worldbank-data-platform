# Alvos de desenvolvimento. `make help` lista tudo.
SHELL := /bin/bash
# Prefere o .venv (Windows ou Linux/macOS): o `python` do PATH pode ser outra versão sem as dependências
PY ?= $(firstword $(wildcard .venv/Scripts/python.exe .venv/bin/python) python)
# CLI do Kaggle instalado pelo requirements-dev no .venv (pode não estar no PATH)
KAGGLE ?= $(firstword $(wildcard .venv/Scripts/kaggle.exe .venv/bin/kaggle) kaggle)
.DEFAULT_GOAL := help

.PHONY: help setup env lint format test test-spark test-integration test-airflow coverage \
        sample data up up-stream down logs ps replay live stream api-demo psql migrate bundle-validate bundle-deploy

help:  ## lista os alvos
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup:  ## instala dependências de dev e o pre-commit
	$(PY) -m pip install -r requirements-dev.txt
	pre-commit install

env:  ## cria .env a partir do exemplo (não sobrescreve)
	@test -f .env && echo ".env já existe" || (cp .env.example .env && echo ".env criado — preencha DATABRICKS_* e AIRFLOW_JWT_SECRET")

lint:  ## ruff check + format check
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

format:  ## corrige lint e formata
	$(PY) -m ruff check --fix .
	$(PY) -m ruff format .

test:  ## testes unitários (sem Docker, sem Spark)
	$(PY) -m pytest -m "not spark and not integration and not airflow" -q

test-spark:  ## transformações Spark com pyspark local
	@# Sem testes Spark o pytest sairia com código 5 ("nada coletado")
	@if ls databricks/tests/test_*.py >/dev/null 2>&1; then $(PY) -m pytest -m spark -q; else echo "sem testes Spark ainda"; fi

test-integration:  ## testes contra a stack Docker (make up antes)
	$(PY) -m pytest -m integration -q

test-airflow:  ## include/ e integridade das DAGs num container avulso do Airflow (não precisa da stack de pé)
	@# MSYS_NO_PATHCONV: no Git Bash do Windows, /opt/... viraria C:/Program Files/Git/opt/...
	@# PYTHONDONTWRITEBYTECODE: o container não deixa .pyc com caminhos /opt/... nas pastas montadas
	@# DAGBAG_IMPORT_TIMEOUT: com a stack de pé e o Docker em 4 GB, importar as DAGs passou dos 30 s padrão
	MSYS_NO_PATHCONV=1 docker compose run --rm --no-deps -e PYTHONDONTWRITEBYTECODE=1 \
		-e AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120 \
		--entrypoint python airflow-scheduler \
		-m pytest /opt/airflow/tests -q -p no:cacheprovider

coverage:  ## testes unitários + Spark com relatório de cobertura
	$(PY) -m pytest -m "not integration and not airflow" --cov --cov-report=term-missing

sample:  ## dados SINTÉTICOS em data/raw (só para testes)
	$(PY) tests/fixtures/make_sample_data.py data/raw

data:  ## baixa o dataset real do Kaggle para data/raw
	KAGGLE="$(KAGGLE)" ./scripts/download_kaggle.sh data/raw

up:  ## sobe a stack
	docker compose up -d --build

up-stream:  ## sobe só o fluxo de projetos, sem Airflow (máquinas com 8 GB)
	docker compose up -d --build postgres kafka kafka-init projects-producer lake-sink realtime-sink api

down:  ## para a stack (mantém volumes)
	docker compose down

ps:
	docker compose ps

logs:  ## logs do producer e dos dois consumidores
	docker compose logs -f --tail=50 projects-producer lake-sink realtime-sink

replay:  ## (re)inicia o replay do histórico de projetos no Kafka
	PRODUCER_MODE=replay docker compose up -d --build --force-recreate projects-producer

live:  ## troca o producer para a Projects API do World Bank
	PRODUCER_MODE=live docker compose up -d --build --force-recreate projects-producer

stream:  ## acompanha os eventos ao vivo pela API (SSE)
	curl -N http://localhost:8000/v1/live/stream

api-demo:  ## chamadas de exemplo da API de serving
	curl -s localhost:8000/v1/pipeline/status; echo
	curl -s "localhost:8000/v1/live/projects?limit=5"; echo
	curl -s "localhost:8000/v1/rankings/NY.GDP.MKTP.CD?top=5"; echo

psql:  ## console SQL no DW
	docker compose exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

migrate:  ## aplica postgres/migrations/*.sql (idempotentes) com o realtime-sink parado e o recria
	docker compose stop realtime-sink
	@for f in postgres/migrations/*.sql; do \
		echo "== $$f"; \
		docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"' < "$$f" || exit 1; \
	done
	docker compose up -d --build realtime-sink

# Perfil do Databricks CLI (`databricks auth profiles`); vazio = DEFAULT. Ex.: make bundle-deploy DBX_PROFILE=meu-perfil
DBX_PROFILE ?=
DBX_ARGS = -t dev $(if $(DBX_PROFILE),-p $(DBX_PROFILE))

bundle-validate:  ## valida o Asset Bundle (databricks/databricks.yml)
	cd databricks && databricks bundle validate $(DBX_ARGS)

bundle-deploy:  ## publica notebooks e job no Databricks
	cd databricks && databricks bundle deploy $(DBX_ARGS)
