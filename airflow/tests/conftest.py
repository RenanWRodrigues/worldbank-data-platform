"""No container não há pyproject.toml: registra o marker `airflow` para o pytest não avisar."""


def pytest_configure(config):
    config.addinivalue_line("markers", "airflow: precisa do Airflow instalado (roda dentro do container)")
