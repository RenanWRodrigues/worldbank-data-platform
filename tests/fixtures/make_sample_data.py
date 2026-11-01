"""
Gera uma amostra PEQUENA e SINTÉTICA nos mesmos formatos do dataset do Kaggle,
para testar o pipeline antes de baixar os dados reais.

  python tests/fixtures/make_sample_data.py data/sample   (ou: make sample -> data/raw)

Não use a amostra no dashboard final — os valores são inventados.
"""

from __future__ import annotations

import random
import sqlite3
import sys
from pathlib import Path

import pandas as pd

COUNTRIES = [
    (
        "Brazil",
        "BRA",
        "BR",
        "Federative Republic of Brazil;Federative Republic of Brazil",
        "Latin America and Caribbean",
    ),
    ("Kenya", "KEN", "KE", "Republic of Kenya;Republic of Kenya", "Africa"),
    ("India", "IND", "IN", "Republic of India;Republic of India", "South Asia"),
    (
        "Egypt, Arab Rep.",
        "EGY",
        "EG",
        "Arab Republic of Egypt;Arab Republic of Egypt",
        "Middle East and North Africa",
    ),
    ("Korea, Rep.", "KOR", "KR", "Republic of Korea;Republic of Korea", "East Asia and Pacific"),
    ("World", "WLD", None, "World;World", "Other"),
]
YEARS = [str(y) for y in range(2010, 2018)]
INDICATORS = {
    "population_data.csv": ("Population, total", "SP.POP.TOTL", lambda: random.randint(10**6, 10**9)),
    "gdp_data.csv": ("GDP (current US$)", "NY.GDP.MKTP.CD", lambda: random.uniform(1e10, 3e12)),
    "rural_population_percent.csv": (
        "Rural population (% of total population)",
        "SP.RUR.TOTL.ZS",
        lambda: random.uniform(10, 80),
    ),
    "electricity_access_percent.csv": (
        "Access to electricity (% of population)",
        "EG.ELC.ACCS.ZS",
        lambda: random.uniform(30, 100),
    ),
}
HEADER = '"Data Source","World Development Indicators",\n\n"Last Updated Date","2018-06-28",\n\n'


def wide(ind_name, ind_code, gen):
    rows = []
    for name, code, *_ in COUNTRIES:
        r = {
            "Country Name": name,
            "Country Code": code,
            "Indicator Name": ind_name,
            "Indicator Code": ind_code,
        }
        r.update({y: (None if random.random() < 0.1 else gen()) for y in YEARS})
        rows.append(r)
    return pd.DataFrame(rows)


def main(out: Path):
    random.seed(42)
    out.mkdir(parents=True, exist_ok=True)
    for fname, (iname, icode, gen) in INDICATORS.items():
        df = wide(iname, icode, gen)
        (out / fname).write_text(HEADER + df.to_csv(index=False), encoding="utf-8")
        if fname == "population_data.csv":
            df.to_json(out / "population_data.json", orient="records")
            with sqlite3.connect(out / "population_data.db") as con:
                df.to_sql("population_data", con, if_exists="replace")
            recs = []
            for _, r in df.iterrows():
                for y in YEARS:
                    v = "" if pd.isna(r[y]) else str(r[y])
                    recs.append(
                        f'<record><field name="Country or Area" key="{r["Country Code"]}">{r["Country Name"]}</field>'
                        f'<field name="Item" key="{icode}">{iname}</field>'
                        f'<field name="Year">{y}</field><field name="Value">{v}</field></record>'
                    )
            (out / "population_data.xml").write_text(
                '<?xml version="1.0" encoding="utf-8"?><Root><data>' + "".join(recs) + "</data></Root>",
                encoding="utf-8",
            )

    projects = []
    for i in range(60):
        _name, _code, _iso2, wb_name, region = random.choice(COUNTRIES)
        y = random.choice(YEARS)
        projects.append(
            {
                "id": f"P{160000 + i}",
                "regionname": region,
                "countryname": wb_name,
                "prodline": "PE",
                "lendinginstr": random.choice(["Investment Project Financing", "Development Policy Lending"]),
                "status": random.choice(["Active", "Closed", "Pipeline"]),
                "project_name": f"Projeto exemplo {i}",
                "boardapprovaldate": f"{y}-0{random.randint(1, 9)}-15T00:00:00Z",
                "closingdate": f"{int(y) + 5}-12-31T00:00:00Z",
                "totalamt": f"{random.randint(1, 500) * 1_000_000:,}",
                "ibrdcommamt": "0",
                "idacommamt": f"{random.randint(0, 100) * 1_000_000:,}",
                "grantamt": "0",
                "lendprojectcost": f"{random.randint(1, 600) * 1_000_000:,}",
                # Como no projects_data.csv real: sem `countrycode`, `mjsector1` vazio e o setor em `mjsector`
                "sector1": "(Historic)Highways!$!100!$!TH",
                "mjsector1": "",
                "mjsector": random.choice(
                    [
                        "Energy and Extractives;Energy and Extractives",
                        "Agriculture; Fishing and Forestry;Agriculture; Fishing and Forestry",
                    ]
                ),
            }
        )
    pd.DataFrame(projects).to_csv(out / "projects_data.csv", index=False)
    print(f"Amostra sintética gerada em {out.resolve()}")


if __name__ == "__main__":
    main(Path(sys.argv[1] if len(sys.argv) > 1 else "data/sample"))
