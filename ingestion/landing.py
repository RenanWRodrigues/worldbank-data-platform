"""Landing dos micro-lotes: Volume do Databricks (Files API) ou disco local, com o mesmo layout."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

RETRY_STATUS = {429, 500, 502, 503, 504}


class LandingError(RuntimeError):
    """Falha ao gravar na landing."""


def to_ndjson(records: Iterable[dict[str, Any]]) -> bytes:
    """Uma linha JSON por registro (formato lido pelo Auto Loader do bronze)."""
    lines = (json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in records)
    return "".join(f"{line}\n" for line in lines).encode("utf-8")


class LocalLanding:
    """Disco local (desenvolvimento sem Databricks); escrita atômica via arquivo temporário."""

    def __init__(self, root: Path | str):
        self.root = Path(root)

    def write(self, rel_path: str, data: bytes) -> str:
        target = self.root / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f"{target.name}.tmp")
        tmp.write_bytes(data)
        tmp.replace(target)
        return target.as_posix()


class VolumeLanding:
    """Volume do Unity Catalog via Files API (`PUT /api/2.0/fs/files/Volumes/...`), só conexão de saída."""

    def __init__(
        self,
        host: str,
        token: str,
        catalog: str,
        schema: str,
        volume: str,
        session=None,
        retries: int = 5,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.host = host.rstrip("/")
        self.token = token
        self.base = f"/Volumes/{catalog}/{schema}/{volume}"
        self.session = session
        self.retries = retries
        self.sleep = sleep

    def _session(self):
        if self.session is None:
            import requests

            self.session = requests.Session()
        return self.session

    def write(self, rel_path: str, data: bytes) -> str:
        path = f"{self.base}/{rel_path}"
        url = f"{self.host}/api/2.0/fs/files{path}"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/octet-stream"}
        for attempt in range(self.retries):
            response = self._session().put(
                url, data=data, headers=headers, params={"overwrite": "true"}, timeout=120
            )
            if response.status_code < 300:
                return path
            if response.status_code not in RETRY_STATUS or attempt == self.retries - 1:
                raise LandingError(f"Files API HTTP {response.status_code} em {path}: {response.text[:200]}")
            self.sleep(2.0 * 2**attempt)
        raise LandingError(f"Files API sem resposta em {path}")


def make_landing(env: Mapping[str, str] = os.environ, session=None) -> LocalLanding | VolumeLanding:
    """Volume quando há DATABRICKS_HOST e DATABRICKS_TOKEN; senão disco local em LANDING_DIR."""
    host, token = env.get("DATABRICKS_HOST", ""), env.get("DATABRICKS_TOKEN", "")
    if host and token:
        return VolumeLanding(
            host=host,
            token=token,
            catalog=env.get("DATABRICKS_CATALOG", "worldbank"),
            schema=env.get("DATABRICKS_SCHEMA", "lakehouse"),
            volume=env.get("DATABRICKS_VOLUME", "landing"),
            session=session,
        )
    return LocalLanding(env.get("LANDING_DIR", "/data/landing"))
