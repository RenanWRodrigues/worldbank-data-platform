# Registros normalizados (saída de `ingestion/worldbank_client.py` e `kaggle_loader.py`)

Formato único gravado na landing (NDJSON), independente da fonte. Na landing de eventos
(`projects/events/…`), cada linha é o evento Kafka mais `kafka_partition` e `kafka_offset` (gravados pelo lake-sink).

Extração da Indicators API: `/country/all/indicator/{código}?format=json&per_page=1000`, seguindo `page/pages`.
A unidade do indicador para o DW **não** vem daqui — ver `INDICATORS` em `databricks/src/wb_transforms.py` (dim_indicator).

## Indicador — `indicators/source=<api|kaggle>/indicator=<código>/run=<id>/part-00000.json`
| campo | tipo | origem API | origem Kaggle |
|---|---|---|---|
| country_iso3 | str | `countryiso3code` (linhas sem ISO3 descartadas) | `Country Code` |
| country_name | str | `country.value` | `Country Name` |
| indicator_code / indicator_name | str | `indicator.id` / `indicator.value` | `Indicator Code` / `Indicator Name` |
| year | int | `date` (só anos de 4 dígitos) | colunas `1960…2017` (formato largo → longo) |
| value | float \| null | `value` | célula (nulos não viram linha) |
| unit, obs_status | str \| null | `unit`, `obs_status` (string vazia `""` → null) | null |
| source | str | `worldbank-api` | `kaggle` |
| extracted_at | ISO 8601 | hora da extração | hora do bootstrap |

## País — `reference/countries/run=<id>/part-00000.json`
country_iso3 (`id`), country_iso2 (`iso2Code`), country_name, region (`region.value`),
income_level (`incomeLevel.value`), is_aggregate (`region.value == "Aggregates"`),
capital_city, longitude, latitude, extracted_at.

## Projeto — `payload` dos eventos Kafka
| campo | API (Projects v2) | Kaggle (`projects_data.csv`) |
|---|---|---|
| project_id | `id` | `id` |
| country_name | `countryshortname`/`countryname` (1º valor, antes de `;`) | `countryname` "X;X" → "X" |
| country_iso2 | `countrycode` (lista ou texto) | — (o CSV real não tem a coluna: null; o país sai do nome no silver) |
| board_approval_date / closing_date | ISO `2018-06-28T00:00:00Z` ou `6/30/2023 12:00:00 AM` → `YYYY-MM-DD` | idem |
| project_name, status | `project_name`, `status` | `project_name`, `status` |
| region_name | `regionname` | `regionname` |
| total_commitment_usd | `totalcommamt`/`totalamt` em dólares; senão `curr_total_commitment` × 1e6 | `totalamt` "6,060,000" |
| ibrd_commitment_usd | `ibrdcommamt` em dólares; senão `curr_ibrd_commitment` × 1e6 | `ibrdcommamt` |
| ida_commitment_usd | `idacommamt` em dólares; senão `curr_ida_commitment` × 1e6 | `idacommamt` |
| grant_amount_usd | `grantamt` em dólares | `grantamt` |
| lending_instrument | `lendinginstr` | `lendinginstr` |
| major_sector | grande setor do subsetor principal: `mjsector[i].Name` com `sector[i].Name == sector1.Name` (senão `mjsector[0]`); `mjsector1Name` se vier. **Nunca** `sector1.Name` (é subsetor). No payload o nome fica como a fonte escreve (o Kaggle troca a vírgula por "; "); `rt.projects_latest` e `silver_projects` usam o nome oficial, com vírgula (`canonical_sector`) | `mjsector` "Setor;Setor" → 1º item (separador `;` sem espaço: "Agriculture; Fishing and Forestry" é um nome só); `mjsector1` (antes de `!$!`) se vier preenchido |
| borrower | `borrower` | `borrower` (se existir) |

Regras comuns dos valores: vírgulas de milhar removidas; ausente ou vazio → null; **negativo → null** (o contrato
exige ≥ 0 e o DW também). `country_iso2` é copiado como vier (inclusive códigos regionais como `3A`): a decisão de
que não é país fica no silver.
