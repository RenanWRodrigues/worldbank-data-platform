"""Checagens de qualidade do dw depois do sync (tarefa `data_quality`).

Cada checagem é um SELECT que conta violações; qualquer contagem > 0 falha a tarefa (e a DAG fica vermelha).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from kafka_health import TOPIC


class QualityError(RuntimeError):
    """dw reprovado."""


@dataclass(frozen=True)
class Check:
    name: str
    sql: str  # devolve uma linha com a contagem de violações
    params: tuple[Any, ...] = ()


def micro_batch_checks(max_delay_minutes: int, max_lag: int) -> list[Check]:
    """Checagens do micro-lote de projetos.

    rt × dw usa `rt.projects_latest.updated_at` (chegada ao rt), nunca `event_time` (histórico no replay).
    """
    return [
        Check(
            "fact_project.approval_year sem dim_year",
            """SELECT count(*) FROM dw.fact_project f
               WHERE f.approval_year IS NOT NULL
                 AND NOT EXISTS (SELECT 1 FROM dw.dim_year y WHERE y.year = f.approval_year)""",
        ),
        Check(
            "fact_project com valor negativo",
            """SELECT count(*) FROM dw.fact_project
               WHERE total_amount_usd < 0 OR ibrd_amount_usd < 0
                  OR ida_amount_usd < 0 OR grant_amount_usd < 0""",
        ),
        Check(
            "agg_country_year com projetos negativos",
            """SELECT count(*) FROM dw.agg_country_year
               WHERE projects_approved < 0 OR projects_total_amount_usd < 0""",
        ),
        Check(
            f"projeto no rt há mais de {max_delay_minutes} min e ausente do dw",
            """SELECT count(*) FROM rt.projects_latest r
               WHERE r.updated_at < now() - make_interval(mins => %s)
                 AND NOT EXISTS (SELECT 1 FROM dw.fact_project f WHERE f.project_id = r.project_id)""",
            (max_delay_minutes,),
        ),
        Check(
            f"consumer group com lag acima de {max_lag}",
            """SELECT count(*) FROM (
                 SELECT DISTINCT ON (consumer_group) lag FROM ops.kafka_lag
                 WHERE topic = %s ORDER BY consumer_group, measured_at DESC
               ) ultimo WHERE lag > %s""",
            (TOPIC, max_lag),
        ),
    ]


def weekly_checks(current_year: int, min_countries: int = 180, min_indicators: int = 4) -> list[Check]:
    """Checagens da carga semanal de indicadores; mínimos viram "quanto falta" (0 = ok)."""
    return [
        Check(
            f"menos de {min_countries} países não agregados com indicador",
            """SELECT greatest(%s - count(DISTINCT f.country_code), 0)
               FROM dw.fact_indicator f JOIN dw.dim_country c USING (country_code)
               WHERE NOT c.is_aggregate""",
            (min_countries,),
        ),
        Check(
            f"menos de {min_indicators} indicadores com dados",
            "SELECT greatest(%s - count(DISTINCT indicator_code), 0) FROM dw.fact_indicator",
            (min_indicators,),
        ),
        Check("fact_indicator com valor negativo", "SELECT count(*) FROM dw.fact_indicator WHERE value < 0"),
        Check(
            "percentual fora de 0–100",
            """SELECT count(*) FROM dw.fact_indicator f JOIN dw.dim_indicator i USING (indicator_code)
               WHERE i.unit = '%%' AND (f.value < 0 OR f.value > 100)""",
        ),
        Check(
            f"população sem ano ≥ {current_year - 2}",
            """SELECT CASE WHEN max(year) >= %s THEN 0 ELSE 1 END
               FROM dw.fact_indicator WHERE indicator_code = 'SP.POP.TOTL'""",
            (current_year - 2,),
        ),
    ]


def run_checks(cur, checks: Iterable[Check]) -> dict[str, int]:
    """Executa todas as checagens e só então falha, listando todas as violações de uma vez."""
    results = {}
    for check in checks:
        cur.execute(check.sql, check.params)
        results[check.name] = cur.fetchone()[0]
    failed = {name: n for name, n in results.items() if n}
    if failed:
        raise QualityError("; ".join(f"{name}: {n}" for name, n in failed.items()))
    return results
