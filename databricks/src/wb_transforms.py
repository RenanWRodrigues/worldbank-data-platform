"""Transformações do lakehouse (DataFrame → DataFrame). Notebooks só leem e gravam.

Só API de DataFrame (compatível com serverless): sem RDD, `sparkContext` ou UDF Python linha a linha.
Tabelas gold = DDL do dw em postgres/init/01_init.sql; registros de entrada em docs/contratos/.
"""

from __future__ import annotations

from collections.abc import Iterable

import pycountry
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Fonte única de indicadores do lakehouse: código → (coluna em agg_country_year, unidade de dim_indicator).
# A unidade não vem da API. DEFAULT_INDICATORS de ingestion/worldbank_client.py tem os mesmos códigos.
INDICATORS: dict[str, tuple[str, str]] = {
    "SP.POP.TOTL": ("population", "count"),
    "NY.GDP.MKTP.CD": ("gdp_usd", "US$"),
    "SP.RUR.TOTL.ZS": ("rural_population_pct", "%"),
    "EG.ELC.ACCS.ZS": ("electricity_access_pct", "%"),
}

SOURCE_PRIORITY = {"worldbank-api": 0, "kaggle": 1}  # menor vence; outra fonte é descartada (CHECK do dw)
EVENT_TYPES = ("PROJECT_APPROVED", "PROJECT_UPDATED", "PROJECT_CLOSED")  # contrato do evento Kafka
# Autoridade da fonte no estado do projeto (ADR 0009): maior vence antes de event_time; igual a
# SOURCE_AUTHORITY de ingestion/events.py (teste compara). Fonte desconhecida fica abaixo de todas.
EVENT_SOURCE_AUTHORITY = {"kaggle-replay": 0, "worldbank-projects-api": 1}
YEAR_RANGE = (1900, 2100)  # fora disso é dado corrompido (e o cast para smallint estouraria)

# ---------------------------------------------------------------- schemas (from_json sempre explícito)

INDICATOR_SCHEMA = T.StructType(
    [
        T.StructField("country_iso3", T.StringType()),
        T.StructField("country_name", T.StringType()),
        T.StructField("indicator_code", T.StringType()),
        T.StructField("indicator_name", T.StringType()),
        T.StructField("year", T.IntegerType()),
        T.StructField("value", T.DoubleType()),
        T.StructField("unit", T.StringType()),
        T.StructField("obs_status", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("extracted_at", T.StringType()),
    ]
)

COUNTRY_SCHEMA = T.StructType(
    [
        T.StructField("country_iso3", T.StringType()),
        T.StructField("country_iso2", T.StringType()),
        T.StructField("country_name", T.StringType()),
        T.StructField("region", T.StringType()),
        T.StructField("income_level", T.StringType()),
        T.StructField("is_aggregate", T.BooleanType()),
        T.StructField("capital_city", T.StringType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("extracted_at", T.StringType()),
    ]
)

PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("project_id", T.StringType()),
        T.StructField("project_name", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("country_name", T.StringType()),
        T.StructField("country_iso2", T.StringType()),
        T.StructField("region_name", T.StringType()),
        T.StructField("board_approval_date", T.StringType()),
        T.StructField("closing_date", T.StringType()),
        T.StructField("total_commitment_usd", T.DoubleType()),
        T.StructField("ibrd_commitment_usd", T.DoubleType()),
        T.StructField("ida_commitment_usd", T.DoubleType()),
        T.StructField("grant_amount_usd", T.DoubleType()),
        T.StructField("lending_instrument", T.StringType()),
        T.StructField("major_sector", T.StringType()),
        T.StructField("borrower", T.StringType()),
        T.StructField("source", T.StringType()),
    ]
)

# Linha da landing de eventos = evento do contrato Kafka + posição no Kafka (gravada pelo lake-sink)
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("produced_at", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("project_id", T.StringType()),
        T.StructField("payload", PAYLOAD_SCHEMA),
        T.StructField("kafka_partition", T.IntegerType()),
        T.StructField("kafka_offset", T.LongType()),
    ]
)

# ---------------------------------------------------------------- países por nome

# Agregados do World Bank que só aparecem no Kaggle (fora do /country da API): sem a lista, viram país.
# Os demais códigos não ISO do CSV estão no /country; XKX (Kosovo) e CHI (Ilhas do Canal) são países.
KAGGLE_ONLY_AGGREGATES = frozenset({"FCS"})

# Nome (casefold) → ISO3/código WDI, ou None para o que não é país. Vem antes do pycountry.
COUNTRY_OVERRIDES: dict[str, str | None] = {
    name.casefold(): code
    for name, code in {
        "Republic of Korea": "KOR",
        "Korea, Rep.": "KOR",
        "Korea, Republic of": "KOR",
        "Kosovo": "XKX",  # código do World Bank (não há ISO)
        "Republic of Kosovo": "XKX",
        "West Bank and Gaza": "PSE",
        "Cote d'Ivoire": "CIV",
        "Republic of Cote d'Ivoire": "CIV",
        "Vietnam": "VNM",
        "Socialist Republic of Vietnam": "VNM",
        "Turkiye": "TUR",
        "Republic of Turkiye": "TUR",
        "Turkey": "TUR",
        "Congo, Democratic Republic of": "COD",
        "Democratic Republic of the Congo": "COD",
        "Congo, Dem. Rep.": "COD",
        "Congo, Republic of": "COG",
        "Republic of Congo": "COG",
        "Congo, Rep.": "COG",
        "Egypt, Arab Republic of": "EGY",
        "Egypt, Arab Rep.": "EGY",
        "Yemen, Republic of": "YEM",
        "Republic of Yemen": "YEM",
        "Kyrgyz Republic": "KGZ",
        "Slovak Republic": "SVK",
        "Gambia, The": "GMB",
        "The Gambia": "GMB",
        "Bahamas, The": "BHS",
        "Micronesia, Federated States of": "FSM",
        "Russia": "RUS",
        "Lao PDR": "LAO",
        "Iran, Islamic Rep.": "IRN",
        "Venezuela, Republica Bolivariana de": "VEN",
        "Venezuela, RB": "VEN",
        "St. Lucia": "LCA",
        "St. Vincent and the Grenadines": "VCT",
        "Sao Tome and Principe": "STP",
        "Cabo Verde": "CPV",
        "Eswatini": "SWZ",
        "North Macedonia": "MKD",
        # projects_data.csv do Kaggle: nomes oficiais antigos e nomes truncados em 40 caracteres na origem
        "Republic of Rwanda": "RWA",
        "Republic of Turkey": "TUR",
        "Democratic Socialist Republic of Sri Lan": "LKA",
        "Democratic Socialist Republic of Sri Lanka": "LKA",
        "Republic of Niger": "NER",
        "Macedonia": "MKD",
        "Republic of Togo": "TGO",
        "Oriental Republic of Uruguay": "URY",
        "The Independent State of Papua New Guine": "PNG",
        "The Independent State of Papua New Guinea": "PNG",
        "Somali Democratic Republic": "SOM",
        "Democratic Republic of Sao Tome and Prin": "STP",
        "Democratic Republic of Sao Tome and Principe": "STP",
        "Republic of the Union of Myanmar": "MMR",
        "Co-operative Republic of Guyana": "GUY",
        "State of Eritrea": "ERI",
        "Kingdom of Swaziland": "SWZ",
        "Republica Bolivariana de Venezuela": "VEN",
        "St. Kitts and Nevis": "KNA",
        "Socialist People's Libyan Arab Jamahiriy": "LBY",
        # Regiões e agregados: nunca viram país (busca aproximada transformaria "Africa" em South Africa)
        "Africa": None,
        "World": None,
        "Western and Central Africa": None,
        "Eastern and Southern Africa": None,
        "Middle East and North Africa": None,
        "South Asia": None,
        "East Asia and Pacific": None,
        "Europe and Central Asia": None,
        "Latin America and Caribbean": None,
        "Latin America": None,
    }.items()
}


def resolve_country_name(name: str | None, known_codes: set[str]) -> str | None:
    """Nome → código de país em silver_countries: override, depois `pycountry.countries.lookup` exato.

    Nunca busca aproximada. Códigos ("KE", "KEN") não são aceitos como nome, e o resultado só vale se o país
    existir em `known_codes` (países não agregados de silver_countries) — senão o FK do gold quebraria.
    """
    key = " ".join((name or "").split())  # o CSV do Kaggle traz espaços duplos ("Islamic  Republic of …")
    if len(key) <= 3:
        return None
    if key.casefold() in COUNTRY_OVERRIDES:
        code = COUNTRY_OVERRIDES[key.casefold()]
    else:
        try:
            code = pycountry.countries.lookup(key).alpha_3
        except LookupError:
            code = None
    return code if code in known_codes else None


# ---------------------------------------------------------------- utilitários


def parse_bronze(bronze: DataFrame, schema: T.StructType) -> DataFrame:
    """Bronze (raw_json, source_file, ingested_at…) → colunas do schema; JSON inválido vira linha nula."""
    return bronze.select(F.from_json("raw_json", schema).alias("r"), "source_file", "ingested_at").select(
        "r.*", "source_file", "ingested_at"
    )


def _first_by(df: DataFrame, keys: Iterable[str], *order: Column) -> DataFrame:
    """Uma linha por chave natural (row_number na ordem dada)."""
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _clean(col: str) -> Column:
    """Texto sem espaços nas pontas; vazio → nulo."""
    trimmed = F.trim(F.col(col))
    return F.when(trimmed == "", None).otherwise(trimmed)


def _timestamp(col: str) -> Column:
    """ISO 8601 → timestamp; inválido vira nulo (com ANSI, como no serverless, `to_timestamp` lançaria)."""
    return F.try_to_timestamp(F.col(col))


def _date(col: str) -> Column:
    """`YYYY-MM-DD` (ou ISO com hora) → date pelos 10 primeiros caracteres: não depende do fuso da sessão."""
    return F.try_to_timestamp(F.substring(F.col(col), 1, 10), F.lit("yyyy-MM-dd")).cast("date")


def _finite(col: str) -> Column:
    """Número presente e finito (NaN passa em IS NOT NULL e em `>= 0` no Spark)."""
    return F.col(col).isNotNull() & ~F.isnan(F.col(col)) & (F.abs(F.col(col)) != float("inf"))


def _literal_table(
    df_for_session: DataFrame, rows: list[tuple[str, str]], names: tuple[str, str]
) -> DataFrame:
    """DataFrame pequeno a partir de literais, sem createDataFrame (que exige o worker Python)."""
    spark = df_for_session.sparkSession
    if not rows:
        return spark.range(0).select(
            F.lit(None).cast("string").alias(names[0]), F.lit(None).cast("string").alias(names[1])
        )
    structs = [F.struct(F.lit(a).alias(names[0]), F.lit(b).alias(names[1])) for a, b in rows]
    return spark.range(1).select(F.inline(F.array(*structs)))


# ---------------------------------------------------------------- silver


def silver_indicators(bronze_indicators: DataFrame) -> DataFrame:
    """País × indicador × ano. API vence Kaggle; entre extrações da mesma fonte, a mais recente.

    Valores nulos são descartados antes da prioridade: um `null` da API não apaga o valor do Kaggle.
    """
    priority = F.coalesce(
        *[F.when(F.col("source") == s, F.lit(p)) for s, p in SOURCE_PRIORITY.items()],
        F.lit(len(SOURCE_PRIORITY)),
    )
    df = (
        parse_bronze(bronze_indicators, INDICATOR_SCHEMA)
        # Chave normalizada ANTES do dedup: " bra" e "BRA" são o mesmo país
        .withColumn("country_code", F.upper(_clean("country_iso3")))
        .withColumn("indicator_code", _clean("indicator_code"))
        .filter(F.col("country_code").isNotNull() & F.col("indicator_code").isNotNull())
        .filter(F.col("year").between(*YEAR_RANGE) & _finite("value"))
        .filter(F.col("source").isin(list(SOURCE_PRIORITY)))
        .withColumn("extracted_at", _timestamp("extracted_at"))
        .withColumn("_priority", priority)
    )
    best = _first_by(
        df,
        ["country_code", "indicator_code", "year"],
        F.col("_priority"),
        F.col("extracted_at").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
        F.col("source_file"),
        F.col("value"),  # ordem total: reprocessar dá sempre o mesmo resultado
    )
    return best.select(
        "country_code",
        "country_name",
        "indicator_code",
        "indicator_name",
        F.col("year").cast("smallint").alias("year"),
        F.col("value").cast("double").alias("value"),
        _clean("unit").alias("unit"),
        _clean("obs_status").alias("obs_status"),
        "source",
        "extracted_at",
    )


def silver_countries(bronze_countries: DataFrame, silver_ind: DataFrame) -> DataFrame:
    """Países e agregados dos metadados da API (última extração) + países que só aparecem nos indicadores.

    Os do segundo grupo (ex.: só no Kaggle) entram com região, renda e ISO2 nulos e `is_aggregate = false`,
    salvo os de `KAGGLE_ONLY_AGGREGATES`.
    """
    api = _first_by(
        parse_bronze(bronze_countries, COUNTRY_SCHEMA)
        .withColumn("country_code", F.upper(_clean("country_iso3")))
        .filter("country_code IS NOT NULL")
        .withColumn("extracted_at", _timestamp("extracted_at")),
        ["country_code"],
        F.col("extracted_at").desc_nulls_last(),
        F.col("ingested_at").desc_nulls_last(),
        F.col("source_file"),
    ).select(
        "country_code",
        "country_name",
        F.upper(_clean("country_iso2")).alias("country_iso2"),
        _clean("region").alias("region"),
        _clean("income_level").alias("income_level"),
        F.coalesce("is_aggregate", F.lit(False)).alias("is_aggregate"),
    )
    only_indicators = (
        silver_ind.groupBy("country_code")
        .agg(F.max("country_name").alias("country_name"))
        .join(api.select("country_code"), "country_code", "left_anti")
        .select(
            "country_code",
            "country_name",
            F.lit(None).cast("string").alias("country_iso2"),
            F.lit(None).cast("string").alias("region"),
            F.lit(None).cast("string").alias("income_level"),
            F.col("country_code").isin(list(KAGGLE_ONLY_AGGREGATES)).alias("is_aggregate"),
        )
    )
    return api.unionByName(only_indicators)


def silver_project_events(bronze_events: DataFrame) -> DataFrame:
    """Eventos deduplicados por `event_id` (at-least-once: reentregas e replays chegam repetidos)."""
    df = (
        parse_bronze(bronze_events, EVENT_SCHEMA)
        .filter("event_id IS NOT NULL AND project_id IS NOT NULL")
        .filter(F.col("event_type").isin(list(EVENT_TYPES)))
        .withColumn("event_time", _timestamp("event_time"))
        .withColumn("produced_at", _timestamp("produced_at"))
        .filter("event_time IS NOT NULL AND produced_at IS NOT NULL")
    )
    # Primeira chegada vence, como o ON CONFLICT DO NOTHING do rt (o offset muda se o tópico for recriado)
    first = _first_by(
        df,
        ["event_id"],
        F.col("ingested_at").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("source_file"),
    )
    return first.select(
        "event_id",
        "event_type",
        "event_time",
        "produced_at",
        "source",
        "project_id",
        "payload",
        "kafka_partition",
        "kafka_offset",
    )


def _canonical_sector(col: str) -> Column:
    """Nome oficial do grande setor (o da Projects API): o Kaggle troca a vírgula por "; ".

    Igual a `canonical_sector` de ingestion/worldbank_client.py (estado atual do rt). Só no estado do projeto:
    o payload em silver_project_events fica como veio da fonte.
    """
    return F.regexp_replace(_clean(col), "; ", ", ")


def _resolve_by_name(projects: DataFrame, countries: DataFrame) -> DataFrame:
    """Mapa nome → país dos nomes ainda sem país (poucos: coletados no driver, sem UDF)."""
    known = {r["country_code"] for r in countries.filter("NOT is_aggregate").select("country_code").collect()}
    names = [
        r["country_name_raw"]
        for r in projects.filter("_iso2_code IS NULL AND country_name_raw IS NOT NULL")
        .select("country_name_raw")
        .distinct()
        .collect()
    ]
    pairs = [(n, code) for n in names if (code := resolve_country_name(n, known)) is not None]
    return _literal_table(projects, pairs, ("_name_key", "_name_code"))


def silver_projects(silver_events: DataFrame, silver_countries_df: DataFrame) -> DataFrame:
    """Estado atual de cada projeto = último evento + país resolvido.

    Ordem do último evento: autoridade da fonte (Projects API vence o replay, ADR 0009), event_time,
    produced_at, offset — a mesma do realtime-sink.
    País: ISO2 só se existir em silver_countries e não for agregado (`3A`, `1W` → nulo); depois o nome.
    """
    authority = F.coalesce(
        *[F.when(F.col("source") == s, F.lit(a)) for s, a in EVENT_SOURCE_AUTHORITY.items()],
        F.lit(-1),
    )
    latest = _first_by(
        silver_events,
        ["project_id"],
        authority.desc(),
        F.col("event_time").desc(),
        F.col("produced_at").desc(),
        # Empate total: fica o primeiro aplicado, como no rt (lá a comparação é estritamente "maior")
        F.col("kafka_offset").asc_nulls_last(),
        F.col("event_id"),
    )
    p = "payload."
    projects = latest.select(
        "project_id",
        F.col(p + "project_name").alias("project_name"),
        F.col(p + "status").alias("status"),
        F.trim(F.col(p + "country_name")).alias("country_name_raw"),
        F.upper(F.trim(F.col(p + "country_iso2"))).alias("country_iso2"),
        F.col(p + "region_name").alias("region_name"),
        _date(p + "board_approval_date").alias("board_approval_date"),
        _date(p + "closing_date").alias("closing_date"),
        F.col(p + "total_commitment_usd").alias("total_commitment_usd"),
        F.col(p + "ibrd_commitment_usd").alias("ibrd_commitment_usd"),
        F.col(p + "ida_commitment_usd").alias("ida_commitment_usd"),
        F.col(p + "grant_amount_usd").alias("grant_amount_usd"),
        F.col(p + "lending_instrument").alias("lending_instrument"),
        _canonical_sector(p + "major_sector").alias("major_sector"),
        F.col(p + "borrower").alias("borrower"),
        F.col("event_id").alias("last_event_id"),
        F.col("event_type").alias("last_event_type"),
        F.col("event_time").alias("last_event_time"),
        F.col("produced_at").alias("last_produced_at"),
    )
    iso2_map = (
        silver_countries_df.filter("NOT is_aggregate AND country_iso2 IS NOT NULL")
        .groupBy(F.col("country_iso2").alias("_iso2_key"))
        .agg(F.min("country_code").alias("_iso2_code"))
    )
    by_iso2 = projects.join(iso2_map, projects["country_iso2"].eqNullSafe(iso2_map["_iso2_key"]), "left")
    by_name = _resolve_by_name(by_iso2, silver_countries_df)
    resolved = by_iso2.join(by_name, by_iso2["country_name_raw"].eqNullSafe(by_name["_name_key"]), "left")
    return resolved.select(
        *projects.columns,
        F.coalesce("_iso2_code", "_name_code").alias("country_code"),
        F.year("board_approval_date").cast("smallint").alias("approval_year"),
    )


# ---------------------------------------------------------------- gold (= dw.* de postgres/init/01_init.sql)

PRIMARY_KEYS: dict[str, list[str]] = {
    "dim_country": ["country_code"],
    "dim_indicator": ["indicator_code"],
    "dim_year": ["year"],
    "fact_indicator": ["country_code", "indicator_code", "year"],
    "fact_project": ["project_id"],
    "agg_country_year": ["country_code", "year"],
}

# O que cada job produz (e a DAG correspondente sincroniza), na ordem do sync (= JOB_TABLES de sync_gold.py)
JOB_TABLES: dict[str, list[str]] = {
    "worldbank_indicators": [
        "dim_country",
        "dim_indicator",
        "dim_year",
        "fact_indicator",
        "agg_country_year",
    ],
    "worldbank_projects": ["dim_year", "fact_project", "agg_country_year"],
}

# Coluna do gold → campo do payload (dólares; a API em milhões já foi convertida na ingestão)
_AMOUNTS = {
    "total_amount_usd": "total_commitment_usd",
    "ibrd_amount_usd": "ibrd_commitment_usd",
    "ida_amount_usd": "ida_commitment_usd",
    "grant_amount_usd": "grant_amount_usd",
}


def _non_negative(col: str) -> Column:
    return F.when(_finite(col) & (F.col(col) >= 0), F.col(col).cast("double"))


def gold_indicator_tables(silver_ind: DataFrame, silver_countries_df: DataFrame) -> dict[str, DataFrame]:
    """dim_country, dim_indicator e fact_indicator (só indicadores de INDICATORS e países de dim_country)."""
    dim_country = silver_countries_df.filter(F.length("country_code") == 3).select(
        "country_code",
        "country_iso2",
        F.coalesce("country_name", "country_code").alias("country_name"),
        "region",
        "income_level",
        F.col("is_aggregate").cast("boolean").alias("is_aggregate"),
    )
    codes = list(INDICATORS)
    units = F.create_map(*[F.lit(x) for code, (_, unit) in INDICATORS.items() for x in (code, unit)])
    in_scope = silver_ind.filter(F.col("indicator_code").isin(codes))
    names = in_scope.groupBy("indicator_code").agg(F.max("indicator_name").alias("indicator_name"))
    # Todos os códigos de INDICATORS entram, mesmo sem linha ainda (o nome cai para o código)
    all_codes = silver_ind.sparkSession.range(1).select(
        F.explode(F.array(*[F.lit(c) for c in codes])).alias("indicator_code")
    )
    dim_indicator = all_codes.join(names, "indicator_code", "left").select(
        "indicator_code",
        F.coalesce("indicator_name", "indicator_code").alias("indicator_name"),
        units[F.col("indicator_code")].alias("unit"),
    )
    fact_indicator = in_scope.join(dim_country.select("country_code"), "country_code", "inner").select(
        "country_code",
        "indicator_code",
        F.col("year").cast("smallint").alias("year"),
        F.col("value").cast("double").alias("value"),
        "source",
    )
    return {"dim_country": dim_country, "dim_indicator": dim_indicator, "fact_indicator": fact_indicator}


def gold_project_table(silver_projects_df: DataFrame, dim_country: DataFrame) -> DataFrame:
    """fact_project: FK de país válida (país não agregado de dim_country) ou nula; valores ≥ 0 em dólares."""
    valid = dim_country.filter("NOT is_aggregate").select(F.col("country_code").alias("_valid_code"))
    joined = silver_projects_df.join(
        valid, silver_projects_df["country_code"] == valid["_valid_code"], "left"
    )
    return joined.select(
        "project_id",
        F.col("_valid_code").alias("country_code"),
        "country_name_raw",
        "region_name",
        "project_name",
        "status",
        "lending_instrument",
        "major_sector",
        "board_approval_date",
        F.col("approval_year").cast("smallint").alias("approval_year"),
        "closing_date",
        *[_non_negative(src).alias(dst) for dst, src in _AMOUNTS.items()],
        "last_event_type",
        "last_event_time",
    )


def gold_serving_tables(
    fact_indicator: DataFrame | None, fact_project: DataFrame | None
) -> dict[str, DataFrame]:
    """dim_year e agg_country_year de TODOS os fatos que existirem (só indicadores, só projetos ou ambos).

    O `30_serving` passa os fatos derivados de todo o silver, nunca só do que o job atual processou: assim o
    espelho do sync (com DELETE) não apaga as colunas do outro fluxo.
    """
    if fact_indicator is None and fact_project is None:
        raise ValueError("gold_serving_tables precisa de pelo menos um fato (indicadores ou projetos)")
    keys = ["country_code", "year"]
    parts = []
    if fact_indicator is not None:
        parts.append(
            fact_indicator.groupBy(*keys).agg(
                *[
                    F.max(F.when(F.col("indicator_code") == code, F.col("value"))).alias(col)
                    for code, (col, _) in INDICATORS.items()
                ]
            )
        )
    if fact_project is not None:
        parts.append(
            fact_project.filter("country_code IS NOT NULL AND approval_year IS NOT NULL")
            .groupBy("country_code", F.col("approval_year").alias("year"))
            .agg(
                F.count("*").cast("int").alias("projects_approved"),
                F.sum("total_amount_usd").cast("double").alias("projects_total_amount_usd"),
            )
        )
    agg = parts[0] if len(parts) == 1 else parts[0].join(parts[1], keys, "full_outer")
    for col, _ in INDICATORS.values():
        if col not in agg.columns:
            agg = agg.withColumn(col, F.lit(None).cast("double"))
    if fact_project is None:
        agg = agg.withColumn("projects_approved", F.lit(None).cast("int")).withColumn(
            "projects_total_amount_usd", F.lit(None).cast("double")
        )
    else:  # há dados de projetos: país × ano sem projeto é 0, não "desconhecido"
        agg = agg.withColumn("projects_approved", F.coalesce("projects_approved", F.lit(0))).withColumn(
            "projects_total_amount_usd", F.coalesce("projects_total_amount_usd", F.lit(0.0))
        )
    agg_country_year = agg.select(
        "country_code",
        F.col("year").cast("smallint").alias("year"),
        "population",
        "gdp_usd",
        F.when(F.col("population") > 0, F.col("gdp_usd") / F.col("population")).alias("gdp_per_capita_usd"),
        "rural_population_pct",
        "electricity_access_pct",
        "projects_approved",
        "projects_total_amount_usd",
    )
    years = []
    if fact_indicator is not None:
        years.append(fact_indicator.select("year"))
    if fact_project is not None:
        years.append(
            fact_project.filter("approval_year IS NOT NULL").select(F.col("approval_year").alias("year"))
        )
    all_years = years[0] if len(years) == 1 else years[0].unionByName(years[1])
    dim_year = all_years.distinct().select(
        F.col("year").cast("smallint").alias("year"),
        F.concat((F.floor(F.col("year") / 10) * 10).cast("int").cast("string"), F.lit("s")).alias("decade"),
        F.make_date(F.col("year").cast("int"), F.lit(1), F.lit(1)).alias("year_date"),
    )
    return {"dim_year": dim_year, "agg_country_year": agg_country_year}


# ---------------------------------------------------------------- qualidade


# Espelho dos NOT NULL / CHECK do DDL do dw (postgres/init/01_init.sql): falhar aqui, não no sync
DDL_RULES: dict[str, list[str]] = {
    "dim_country": ["country_name IS NOT NULL", "length(country_code) = 3", "is_aggregate IS NOT NULL"],
    "dim_indicator": ["indicator_name IS NOT NULL", "unit IN ('%', 'US$', 'count')"],
    "dim_year": ["decade IS NOT NULL", "year_date IS NOT NULL"],
    "fact_indicator": ["value IS NOT NULL", "source IN ('worldbank-api', 'kaggle')"],
}

# (filha, coluna, pai, coluna do pai). Nulo na filha não é órfão (fact_project.country_code é opcional).
FOREIGN_KEYS = [
    ("fact_indicator", "country_code", "dim_country", "country_code"),
    ("fact_indicator", "indicator_code", "dim_indicator", "indicator_code"),
    ("fact_indicator", "year", "dim_year", "year"),
    ("fact_project", "country_code", "dim_country", "country_code"),
    ("fact_project", "approval_year", "dim_year", "year"),
    ("agg_country_year", "country_code", "dim_country", "country_code"),
    ("agg_country_year", "year", "dim_year", "year"),
]


def expected_tables(job: str, tables: dict[str, DataFrame]) -> list[str]:
    """Tabelas que precisam sair não vazias no `job`.

    Exceção: no job de projetos, se nenhum projeto tem país resolvido (ambiente novo, sem metadados de
    países), `agg_country_year` vazio é legítimo e não pode reprovar o job.
    """
    expected = list(JOB_TABLES[job])
    if job == "worldbank_projects" and not tables["fact_project"].filter("country_code IS NOT NULL").take(1):
        expected.remove("agg_country_year")
    return expected


class QualityError(RuntimeError):
    """Gold reprovado: o job falha e nada é sincronizado com o DW."""


def check_quality(tables: dict[str, DataFrame], expected: Iterable[str]) -> dict[str, int]:
    """PK não nula e única em todas as tabelas; "não vazia" só nas `expected` (as que o job atual produz).

    Assim o job de projetos roda com `fact_indicator` vazio (caso "só projetos") sem falhar.
    Devolve a contagem de linhas por tabela; levanta QualityError com todos os problemas encontrados.
    """
    expected = set(expected)
    problems, counts = [], {}
    for name, df in tables.items():
        keys = PRIMARY_KEYS.get(name)
        if keys is None:
            problems.append(f"{name}: sem chave primária conhecida em PRIMARY_KEYS")
            continue
        null_key = F.lit(False)
        for k in keys:
            null_key = null_key | F.col(k).isNull()
        rules = DDL_RULES.get(name, [])
        stats = df.agg(
            F.count("*").alias("n"),
            F.coalesce(F.sum(null_key.cast("int")), F.lit(0)).alias("nulls"),
            *[F.coalesce(F.sum((~F.coalesce(F.expr(r), F.lit(False))).cast("int")), F.lit(0)) for r in rules],
        ).collect()[0]
        dups = df.groupBy(*keys).count().filter("count > 1").count()
        counts[name] = stats["n"]
        if stats["nulls"]:
            problems.append(f"{name}: {stats['nulls']} linha(s) com chave nula")
        if dups:
            problems.append(f"{name}: {dups} chave(s) duplicada(s)")
        if name in expected and stats["n"] == 0:
            problems.append(f"{name}: vazia (o job deveria produzi-la)")
        for rule, broken in zip(rules, stats[2:], strict=True):
            if broken:
                problems.append(f"{name}: {broken} linha(s) violam `{rule}` (o dw recusaria)")
    for child, col, parent, parent_col in FOREIGN_KEYS:
        if child in tables and parent in tables:
            parent_keys = tables[parent].select(F.col(parent_col).alias(col))
            orphans = tables[child].filter(F.col(col).isNotNull()).join(parent_keys, col, "left_anti").count()
            if orphans:
                problems.append(f"{child}.{col}: {orphans} linha(s) sem {parent}.{parent_col}")
    problems += [f"{name}: esperada e ausente" for name in sorted(expected - set(tables))]
    if problems:
        raise QualityError("check_quality reprovou o gold:\n- " + "\n- ".join(problems))
    return counts
