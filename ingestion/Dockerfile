# Imagem do producer e dos consumidores. Contexto de build = raiz do repositório (precisa do contrato do evento).
FROM python:3.12.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/ingestion

WORKDIR /app
COPY ingestion/requirements.txt ingestion/requirements.txt
RUN pip install --no-cache-dir -r ingestion/requirements.txt

# Mesmo layout do repositório: events.py acha o schema em ../docs/contratos/
COPY docs/contratos/evento-kafka.schema.json docs/contratos/evento-kafka.schema.json
COPY ingestion/*.py ingestion/

RUN useradd --uid 10001 --no-create-home app
USER app
WORKDIR /app/ingestion
CMD ["python", "projects_producer.py"]
