"""Cliente das APIs do World Bank (Indicators v2 e Projects v2).

HTTP com retries fica em `get_json` e nas funções `fetch_*`; parsing e normalização são funções puras que
devolvem os formatos de docs/contratos/registros-normalizados.md. Nada a jusante depende do JSON cru.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

INDICATORS_API = "https://api.worldbank.org/v2"
PROJECTS_API = "https://search.worldbank.org/api/v2/projects"
# O padrão da Indicators API é 50 por página (~340 páginas por indicador)
PER_PAGE = 1000
PROJECTS_ROWS = 1000
# Campos que normalize_project lê (`fl` corta o download pela metade: sem resumo, documentos etc.)
PROJECT_FIELDS = (
    "id", "project_name", "status", "countryshortname", "countryname", "countrycode", "regionname",
    "boardapprovaldate", "closingdate", "totalcommamt", "totalamt", "curr_total_commitment", "ibrdcommamt",
    "curr_ibrd_commitment", "idacommamt", "curr_ida_commitment", "grantamt", "lendinginstr", "mjsector1Name",
    "sector1", "sector", "mjsector", "mjsector1", "borrower",
)  # fmt: skip
TIMEOUT_SECONDS = 60
RETRY_STATUS = {429, 500, 502, 503, 504}

# Mesmos códigos de INDICATORS em databricks/src/wb_transforms.py (um teste compara)
DEFAULT_INDICATORS = ("SP.POP.TOTL", "NY.GDP.MKTP.CD", "SP.RUR.TOTL.ZS", "EG.ELC.ACCS.ZS")

SOURCE_API = "worldbank-api"
SOURCE_PROJECTS_API = "worldbank-projects-api"


class WorldBankAPIError(RuntimeError):
    """Resposta de erro da API ou tentativas esgotadas."""


# ---------------------------------------------------------------- HTTP


def make_session():
    """Sessão `requests` com User-Agent do projeto (import tardio: os testes usam sessão falsa)."""
    import requests

    session = requests.Session()
    session.headers["User-Agent"] = "worldbank-data-platform/2 (portfolio)"
    return session


def get_json(
    session,
    url: str,
    params: dict[str, Any],
    retries: int = 5,
    backoff: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    """GET com retries em 429/5xx e erros de conexão (backoff exponencial ou `Retry-After`)."""
    last_error = ""
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=TIMEOUT_SECONDS)
        except OSError as exc:  # requests.ConnectionError/Timeout herdam de OSError
            last_error = f"{type(exc).__name__}: {exc}"
            wait = backoff * 2**attempt
        else:
            if response.status_code < 400:
                return response.json()
            last_error = f"HTTP {response.status_code} em {url}"
            if response.status_code not in RETRY_STATUS:
                raise WorldBankAPIError(last_error)
            retry_after = response.headers.get("Retry-After", "")
            wait = float(retry_after) if retry_after.isdigit() else backoff * 2**attempt
        if attempt < retries - 1:
            sleep(wait)
    raise WorldBankAPIError(f"{last_error} (após {retries} tentativas)")


def _session(session):
    return session if session is not None else make_session()


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


# ---------------------------------------------------------------- Valores


def clean_str(value: Any) -> str | None:
    """Texto sem espaços nas pontas; vazio ou ausente → None."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_float(value: Any) -> float | None:
    """Número (aceita vírgula de milhar); vazio ou inválido → None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def parse_amount(value: Any) -> float | None:
    """Valor monetário ≥ 0: negativo → None (o contrato do evento e o DW exigem ≥ 0)."""
    number = parse_float(value)
    return number if number is not None and number >= 0 else None


_DATE_FORMATS = ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y")


def parse_date(value: Any) -> str | None:
    """ISO (`2018-06-28T00:00:00Z`) ou americano (`6/30/2023 12:00:00 AM`) → `YYYY-MM-DD`."""
    text = clean_str(value)
    if text is None:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}", text):
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date().isoformat()
        except ValueError:
            return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def content_hash(payload: dict[str, Any]) -> str:
    """SHA-256 do JSON canônico (chaves ordenadas): base do diff do modo live e do event_id."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def indicator_codes(value: str | None) -> list[str]:
    """Lista de `WB_INDICATORS` separada por vírgula; vazia → DEFAULT_INDICATORS."""
    codes = [c.strip() for c in (value or "").split(",") if c.strip()]
    return codes or list(DEFAULT_INDICATORS)


# ---------------------------------------------------------------- Indicators API


def parse_indicator_page(body: Any) -> tuple[list[dict], int, int]:
    """Resposta `[metadados, linhas]` → (linhas, página, páginas). Erro vem como `[{"message": …}]`."""
    if not isinstance(body, list) or not body:
        raise WorldBankAPIError(f"resposta inesperada da Indicators API: {str(body)[:200]}")
    meta = body[0]
    if "message" in meta:
        detail = "; ".join(f"{m.get('key')}: {m.get('value')}" for m in meta["message"])
        raise WorldBankAPIError(detail)
    rows = body[1] if len(body) > 1 and body[1] else []
    return rows, int(meta.get("page", 1)), int(meta.get("pages", 0))


def normalize_indicator_row(row: dict, extracted_at: str) -> dict | None:
    """Linha da API → registro de indicador; sem ISO3 ou com ano que não é de 4 dígitos → None."""
    iso3 = clean_str(row.get("countryiso3code"))
    year = clean_str(row.get("date"))
    if not iso3 or not year or not re.fullmatch(r"\d{4}", year):
        return None
    indicator = row.get("indicator") or {}
    country = row.get("country") or {}
    return {
        "country_iso3": iso3,
        "country_name": clean_str(country.get("value")),
        "indicator_code": clean_str(indicator.get("id")),
        "indicator_name": clean_str(indicator.get("value")),
        "year": int(year),
        "value": parse_float(row.get("value")),
        "unit": clean_str(row.get("unit")),
        "obs_status": clean_str(row.get("obs_status")),
        "source": SOURCE_API,
        "extracted_at": extracted_at,
    }


def _paged_rows(session, url: str, params: dict[str, Any]) -> Iterable[dict]:
    page, pages = 1, 1
    while page <= pages:
        body = get_json(session, url, {**params, "format": "json", "per_page": PER_PAGE, "page": page})
        rows, _, pages = parse_indicator_page(body)
        yield from rows
        page += 1


def fetch_indicator(code: str, session=None, extracted_at: str | None = None) -> list[dict]:
    """Todas as linhas de um indicador para todos os países (`/country/all/indicator/{código}`)."""
    extracted_at = extracted_at or now_iso()
    url = f"{INDICATORS_API}/country/all/indicator/{code}"
    records = (normalize_indicator_row(r, extracted_at) for r in _paged_rows(_session(session), url, {}))
    return [r for r in records if r is not None]


# ---------------------------------------------------------------- Países


def normalize_country(row: dict, extracted_at: str) -> dict:
    """Metadados de país/agregado (`/country`); agregado = `region.value == "Aggregates"`."""
    region = clean_str((row.get("region") or {}).get("value"))
    return {
        "country_iso3": clean_str(row.get("id")),
        "country_iso2": clean_str(row.get("iso2Code")),
        "country_name": clean_str(row.get("name")),
        "region": region,
        "income_level": clean_str((row.get("incomeLevel") or {}).get("value")),
        "is_aggregate": region == "Aggregates",
        "capital_city": clean_str(row.get("capitalCity")),
        "longitude": parse_float(row.get("longitude")),
        "latitude": parse_float(row.get("latitude")),
        "extracted_at": extracted_at,
    }


def fetch_countries(session=None, extracted_at: str | None = None) -> list[dict]:
    extracted_at = extracted_at or now_iso()
    rows = _paged_rows(_session(session), f"{INDICATORS_API}/country", {})
    return [normalize_country(r, extracted_at) for r in rows]


def aggregate_codes(countries: Iterable[dict]) -> set[str]:
    """ISO3 dos agregados (World, regiões, faixas de renda) segundo os metadados da API."""
    return {c["country_iso3"] for c in countries if c["is_aggregate"]}


def country_coverage(records: Iterable[dict], aggregates: set[str]) -> int:
    """Países distintos, não agregados, com pelo menos um valor não nulo (base do coverage_check)."""
    return len(
        {r["country_iso3"] for r in records if r["value"] is not None and r["country_iso3"] not in aggregates}
    )


# ---------------------------------------------------------------- Projects API (e projetos do Kaggle)


def parse_projects_page(body: dict) -> list[dict]:
    """`projects` pode vir como objeto `{id: projeto}` ou como lista."""
    projects = body.get("projects") or []
    return list(projects.values()) if isinstance(projects, dict) else list(projects)


def _first(value: Any) -> Any:
    """Primeiro item de uma lista (a API devolve alguns campos como lista); texto fica como está."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _amount(raw: dict, field: str, millions_field: str | None = None) -> float | None:
    """Valor em dólares; se ausente, o campo `curr_*` em milhões × 1e6."""
    value = parse_amount(_first(raw.get(field)))
    if value is None and millions_field:
        millions = parse_amount(_first(raw.get(millions_field)))
        value = millions * 1e6 if millions is not None else None
    return value


# Separador de listas do Kaggle: ";" sem espaço depois ("Agriculture; Fishing and Forestry" é um nome só)
_KAGGLE_LIST_SEP = re.compile(r";(?! )")


def _name(item: Any) -> str | None:
    """Item `{"Name": ...}` das listas da Projects API (ou texto) → nome limpo."""
    return clean_str(item.get("Name") if isinstance(item, dict) else item)


def _api_major_sector(raw: dict) -> str | None:
    """Projects API: grande setor do subsetor principal (`sector1`).

    `sector` (subsetores) e `mjsector` (grandes setores) são listas paralelas; `sector1` nem sempre é o
    1º item.
    Sem correspondência, fica o 1º grande setor. `sector1` sozinho é subsetor e nunca vira grande setor.
    """
    majors = raw.get("mjsector")
    if not isinstance(majors, list) or not majors:
        return None
    main = _name(raw.get("sector1"))
    subsectors = [_name(s) for s in raw.get("sector") or []]
    if main and main in subsectors and subsectors.index(main) < len(majors):
        return _name(majors[subsectors.index(main)])
    return _name(majors[0])


def canonical_sector(name: Any) -> str | None:
    """Nome oficial do grande setor (o da Projects API): o Kaggle troca a vírgula do nome por "; ".

    Só para o estado atual (rt.projects_latest e silver): o payload do evento fica como veio da fonte, porque
    mudá-lo mudaria o hash e o `event_id` dos eventos já publicados. Igual a `_canonical_sector` do lakehouse.
    """
    text = clean_str(name)
    return text.replace("; ", ", ") if text else None


def _major_sector(raw: dict) -> str | None:
    """Mesmo significado nas duas fontes: o grande setor do subsetor principal (docs/contratos)."""
    if isinstance(raw.get("mjsector"), list):
        return clean_str(_first(raw.get("mjsector1Name"))) or _api_major_sector(raw)
    candidates = [
        raw.get("mjsector1Name"),
        raw.get("mjsector1"),
        raw.get("mjsector"),  # projects_data.csv real: "Setor;Setor" (mjsector1 vem vazio)
    ]
    for value in candidates:
        text = clean_str(_first(value))
        if text:
            # Kaggle: "Setor!$!percentual!$!código" ou lista "Setor A;Setor B"
            return clean_str(_KAGGLE_LIST_SEP.split(text.split("!$!")[0])[0])
    return None


def normalize_project(raw: dict, source: str = SOURCE_PROJECTS_API) -> dict:
    """Projeto da Projects API ou do `projects_data.csv` → payload do evento Kafka."""
    country = clean_str(_first(raw.get("countryshortname"))) or clean_str(_first(raw.get("countryname")))
    iso2 = clean_str(_first(raw.get("countrycode")))
    total = _amount(raw, "totalcommamt", "curr_total_commitment")
    if total is None:
        total = _amount(raw, "totalamt")
    return {
        "project_id": clean_str(raw.get("id")),
        "project_name": clean_str(_first(raw.get("project_name"))),
        "status": clean_str(raw.get("status")),
        "country_name": clean_str(country.split(";")[0]) if country else None,
        # Copiado como veio (inclusive regionais como 3A); só mais de 2 letras quebraria o contrato
        "country_iso2": iso2 if iso2 and len(iso2) <= 2 else None,
        "region_name": clean_str(_first(raw.get("regionname"))),
        "board_approval_date": parse_date(raw.get("boardapprovaldate")),
        "closing_date": parse_date(raw.get("closingdate")),
        "total_commitment_usd": total,
        "ibrd_commitment_usd": _amount(raw, "ibrdcommamt", "curr_ibrd_commitment"),
        "ida_commitment_usd": _amount(raw, "idacommamt", "curr_ida_commitment"),
        "grant_amount_usd": _amount(raw, "grantamt"),
        "lending_instrument": clean_str(_first(raw.get("lendinginstr"))),
        "major_sector": _major_sector(raw),
        "borrower": clean_str(_first(raw.get("borrower"))),
        "source": source,
    }


def fetch_projects(
    session=None,
    start_date: str | None = None,
    end_date: str | None = None,
    status: str | None = None,
    ids: list[str] | None = None,
    rows: int = PROJECTS_ROWS,
) -> list[dict]:
    """Projetos normalizados, paginando por `os/rows` até a primeira página vazia ou o `total` da resposta.

    O `total` limita a paginação: se a API ignorasse `os` e repetisse a página, o laço nunca terminaria.
    Filtros: aprovação entre `start_date` e `end_date`, `status` (ex.: Active) e lista de `ids`.
    """
    session = _session(session)
    params: dict[str, Any] = {"format": "json", "rows": rows, "fl": ",".join(PROJECT_FIELDS)}
    if start_date:
        params["strdate"] = start_date
    if end_date:
        params["enddate"] = end_date
    if status:
        params["status"] = status
    if ids:
        params["id"] = ",".join(ids)
    projects: list[dict] = []
    offset = 0
    while True:
        body = get_json(session, PROJECTS_API, {**params, "os": offset})
        page = parse_projects_page(body)
        if not page:
            return projects
        projects.extend(normalize_project(p) for p in page if clean_str(p.get("id")))
        offset += rows
        total = parse_float(body.get("total"))
        if total is not None and offset >= total:
            return projects
