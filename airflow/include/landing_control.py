"""Controle dos arquivos da landing em ops.landing_files (`has_new_files` e `mark_processed` das DAGs).

O snapshot de pendentes é o maior `file_id` visto antes do job: arquivos que chegarem durante o job ficam
pendentes e disparam a próxima execução (no máximo uma execução a mais, nunca um arquivo perdido).
"""

from __future__ import annotations

PENDING_SQL = """
SELECT max(file_id), count(*) FROM ops.landing_files
WHERE dataset = %s AND processed_at IS NULL
"""

VOLUME_PREFIX = "/Volumes/"

OUTSIDE_VOLUME_SQL = """
SELECT path FROM ops.landing_files
WHERE dataset = %s AND processed_at IS NULL AND path NOT LIKE '/Volumes/%%'
ORDER BY file_id
"""

MARK_SQL = """
UPDATE ops.landing_files SET processed_at = now()
WHERE dataset = %s AND processed_at IS NULL AND file_id <= %s
"""


def pending_files(cur, dataset: str) -> tuple[int | None, int]:
    """(maior file_id pendente ou None, quantidade de pendentes) do `dataset`."""
    cur.execute(PENDING_SQL, (dataset,))
    max_id, count = cur.fetchone()
    return max_id, count


class LandingError(RuntimeError):
    """Arquivo pendente que o job do Databricks nunca vai ler."""


def check_in_volume(paths: list[str]) -> None:
    """Falha se algum pendente está fora do Volume (landing local, gravada sem credenciais do Databricks).

    Sem isso o `mark_processed` o marcaria como processado e os eventos nunca chegariam ao dw.
    """
    if paths:
        exemplos = ", ".join(paths[:3])
        raise LandingError(
            f"{len(paths)} arquivo(s) pendente(s) fora de {VOLUME_PREFIX} (ex.: {exemplos}). O lake-sink "
            "gravou em disco local: envie-os ao Volume e atualize o path em ops.landing_files (ver runbook)"
        )


def pending_outside_volume(cur, dataset: str) -> list[str]:
    """Paths dos pendentes do `dataset` que não estão no Volume."""
    cur.execute(OUTSIDE_VOLUME_SQL, (dataset,))
    return [path for (path,) in cur.fetchall()]


MARK_FILES_SQL = """
UPDATE ops.landing_files SET processed_at = now()
WHERE file_id = ANY(%s) AND processed_at IS NULL
"""


def mark_files(cur, file_ids: list[int]) -> int:
    """Marca exatamente os arquivos gravados pela execução (extrações da DAG semanal e do bootstrap)."""
    cur.execute(MARK_FILES_SQL, (list(file_ids),))
    return cur.rowcount


def mark_processed(cur, dataset: str, up_to: int) -> int:
    """Marca como processados os pendentes do `dataset` com `file_id <= up_to`; devolve quantos."""
    cur.execute(MARK_SQL, (dataset, up_to))
    return cur.rowcount
