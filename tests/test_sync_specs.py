"""SPECS do sync_gold batem com o DDL do dw, com o gold do Spark e com o contrato (sem Docker)."""

import re
from pathlib import Path

import pytest
import sync_gold as sg

ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "postgres" / "init" / "01_init.sql").read_text(encoding="utf-8")
DOC = (ROOT / "docs" / "modelo-de-dados.md").read_text(encoding="utf-8")


def dw_tables() -> dict[str, str]:
    return dict(re.findall(r"CREATE TABLE dw\.(\w+) \((.*?)\n\);", SQL, flags=re.S))


def columns(body: str) -> list[str]:
    cols = [m.group(1) for line in body.splitlines() if (m := re.match(r"\s+([a-z_][a-z0-9_]*)\s", line))]
    return [c for c in cols if c not in {"primary", "unique", "check", "foreign"}]


def test_specs_cobrem_o_dw_com_colunas_na_ordem_do_ddl():
    tables = dw_tables()
    assert set(sg.SPECS) == set(tables) - {"load_audit"}
    for name, spec in sg.SPECS.items():
        assert list(spec.columns) == columns(tables[name]), name  # COPY usa a ordem do gold = ordem do DDL


def test_chaves_iguais_as_primary_keys_do_ddl():
    for name, body in dw_tables().items():
        if name == "load_audit":
            continue
        composta = re.search(r"PRIMARY KEY \(([^)]*)\)", body)
        keys = (
            [k.strip() for k in composta.group(1).split(",")]
            if composta
            else re.findall(r"^\s+(\w+)\s+[^\n]*PRIMARY KEY", body, flags=re.M)
        )
        assert list(sg.SPECS[name].keys) == keys, name


def test_toda_fk_do_ddl_e_tratada_no_sync():
    """FK nullable → fk_nullify; FK NOT NULL → fk_filter. Assim o upsert nunca viola uma FK."""
    for name, body in dw_tables().items():
        for line in body.splitlines():
            ref = re.match(r"\s+(\w+)\s.*REFERENCES dw\.(\w+) \((\w+)\)", line)
            if not ref:
                continue
            fk = sg.ForeignKey(ref.group(1), ref.group(2), ref.group(3))
            regras = sg.SPECS[name].fk_filter if "NOT NULL" in line else sg.SPECS[name].fk_nullify
            assert fk in regras, (name, fk)


def test_job_tables_iguais_ao_contrato():
    for job, tables in sg.JOB_TABLES.items():
        linha = next(line for line in DOC.splitlines() if line.startswith("|") and f"`{job}`" in line)
        assert linha.rsplit("|", 2)[-2].strip() == ", ".join(tables), job


@pytest.mark.spark
def test_job_tables_iguais_ao_lakehouse():
    pytest.importorskip("pyspark")
    import wb_transforms as wt

    assert wt.JOB_TABLES == sg.JOB_TABLES
