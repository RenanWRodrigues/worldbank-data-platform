# Airflow 3.1 com LocalExecutor (Task SDK, metadados no Postgres)
FROM apache/airflow:3.1.8-python3.12

ARG AIRFLOW_VERSION=3.1.8
ARG PYTHON_VERSION=3.12

# Constraints oficiais da versão: providers e libs compatíveis entre si
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
