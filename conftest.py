"""Fixtures compartilhadas por todos os testes.

Fica na raiz do repositório para valer também em api/, ingestion/, databricks/ e airflow/.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent


@pytest.fixture(scope="session")
def raw_dir(tmp_path_factory) -> Path:
    """Diretório com a amostra sintética em todos os formatos do dataset do Kaggle."""
    from make_sample_data import main as make_sample  # tests/fixtures está no pythonpath

    out = tmp_path_factory.mktemp("raw")
    make_sample(out)
    return out


@pytest.fixture(scope="session")
def kafka_schema() -> dict:
    return json.loads((ROOT / "docs" / "contratos" / "evento-kafka.schema.json").read_text(encoding="utf-8"))


@pytest.fixture
def make_event():
    """Fábrica de eventos válidos segundo o contrato Kafka v2."""

    def _make(event_type: str = "PROJECT_APPROVED", project_id: str = "P123456", **payload) -> dict:
        now = datetime.now(UTC).isoformat()
        return {
            "event_id": "6f0c5a1e-8d0b-4b7a-9a77-2b1d8c3e9f10",
            "event_type": event_type,
            "event_time": now,
            "produced_at": now,
            "source": "kaggle-replay",
            "project_id": project_id,
            "payload": {
                "project_id": project_id,
                "status": "Active",
                "total_commitment_usd": 1e6,
                "board_approval_date": "2020-01-15",
                "source": "kaggle-replay",
                **payload,
            },
        }

    return _make


@pytest.fixture(scope="session")
def api_sample():
    """Carrega respostas de exemplo das APIs do World Bank (tests/fixtures/api/)."""

    def _load(name: str):
        return json.loads((ROOT / "tests" / "fixtures" / "api" / name).read_text(encoding="utf-8"))

    return _load
