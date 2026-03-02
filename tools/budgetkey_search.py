import json
from typing import Any

import requests
import streamlit as st

API_BASE_URL = "https://next.obudget.org/search"
REQUEST_TIMEOUT_SECONDS = 30


def digits_only(value: Any) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.lstrip("0")


def first_scalar(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def safe_text(value: Any) -> str:
    scalar = first_scalar(value)
    if scalar is None:
        return ""
    return str(scalar).strip()


def safe_number(value: Any) -> float | None:
    scalar = first_scalar(value)
    if scalar is None:
        return None
    if isinstance(scalar, (int, float)):
        return float(scalar)
    if isinstance(scalar, str):
        cleaned = scalar.replace(",", "").replace("₪", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _value_text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    if value is None:
        return []
    return [str(value)]


def _contains_text(value: Any, needle: str) -> bool:
    if not needle:
        return True
    needle_cf = needle.casefold()
    for item in _value_text_list(value):
        if needle_cf in item.casefold():
            return True
    return False


def _record_matches_hp(doc_type: str, record: dict[str, Any], hp_norm: str) -> bool:
    if not hp_norm:
        return False

    if doc_type == "contract-spending":
        candidates = _value_text_list(record.get("supplier_code"))
        return any(digits_only(candidate) == hp_norm for candidate in candidates)

    if doc_type == "supports":
        candidates = _value_text_list(record.get("entity_id"))
        return any(digits_only(candidate) == hp_norm for candidate in candidates)

    entity_candidates: list[str] = []
    for field_name in ("entity_id", "id", "company_id", "name"):
        entity_candidates.extend(_value_text_list(record.get(field_name)))
    return any(digits_only(candidate) == hp_norm for candidate in entity_candidates)


def _record_year(record: dict[str, Any], doc_type: str) -> int | None:
    if doc_type == "contract-spending":
        raw_date = safe_text(record.get("order_date"))
        if len(raw_date) >= 4 and raw_date[:4].isdigit():
            return int(raw_date[:4])
        return None

    if doc_type == "supports":
        raw_year = safe_text(record.get("year_requested"))
        if raw_year.isdigit():
            return int(raw_year)
        return None

    return None


def _record_matches_years(
    record: dict[str, Any], doc_type: str, years: set[int]
) -> bool:
    if not years or doc_type not in {"contract-spending", "supports"}:
        return True
    record_year = _record_year(record, doc_type)
    return record_year in years if record_year is not None else False


def _record_matches_publisher(record: dict[str, Any], publisher_filter: str) -> bool:
    if not publisher_filter:
        return True
    return _contains_text(record.get("publisher"), publisher_filter)


def _record_matches_keyword(
    record: dict[str, Any], doc_type: str, keyword: str
) -> bool:
    if not keyword:
        return True

    if doc_type == "contract-spending":
        fields = (
            "purpose",
            "description",
            "page_title",
            "budget_title",
            "supplier_name",
        )
    elif doc_type == "supports":
        fields = (
            "recipient",
            "entity_name",
            "page_title",
            "support_title",
            "program_title",
            "budget_title",
        )
    else:
        return True

    return any(_contains_text(record.get(field), keyword) for field in fields)


def _build_filters(doc_type: str, hp_norm: str, publisher_filter: str) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = []
    if doc_type == "contract-spending":
        filters.append({"path": "supplier_code", "terms": [hp_norm]})
        if publisher_filter:
            filters.append({"path": "publisher", "terms": [publisher_filter]})
    elif doc_type == "supports":
        filters.append({"path": "entity_id", "terms": [hp_norm]})
    elif doc_type == "entities":
        filters.append({"path": "id", "terms": [hp_norm]})
    return filters


def _build_query(doc_type: str, hp_norm: str, keyword: str, publisher_filter: str) -> str:
    if doc_type == "entities":
        return hp_norm

    parts: list[str] = [hp_norm]
    if keyword:
        parts.append(keyword.strip())
    if publisher_filter and doc_type == "contract-spending":
        # Fallback signal when server-side filters are ineffective.
        parts.append(publisher_filter.strip())
    return " ".join(part for part in parts if part)


@st.cache_data(ttl=600, show_spinner=False)
def search_page(
    doc_type: str, q: str, size: int, from_offset: int, filters_json: str = ""
) -> dict[str, Any]:
    url = f"{API_BASE_URL}/{doc_type}"
    params: dict[str, Any] = {"q": q, "size": size, "from": from_offset}
    if filters_json:
        params["filters"] = filters_json

    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        hits = payload.get("search_results", [])
        records = [hit.get("source", {}) for hit in hits if isinstance(hit, dict)]
        total = (
            payload.get("search_counts", {})
            .get("_current", {})
            .get("total_overall", len(records))
        )
        return {
            "ok": True,
            "error": "",
            "records": records,
            "total": int(total) if isinstance(total, int) else len(records),
            "url": response.url,
        }
    except requests.RequestException as exc:
        return {
            "ok": False,
            "error": f"API request failed: {exc}",
            "records": [],
            "total": 0,
            "url": url,
        }
    except ValueError as exc:
        return {
            "ok": False,
            "error": f"Invalid JSON from API: {exc}",
            "records": [],
            "total": 0,
            "url": url,
        }


def _deduplicate_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    output: list[dict[str, Any]] = []

    for record in records:
        hp = safe_text(record.get("hp"))
        doc_type = safe_text(record.get("doc_type"))
        doc_id = safe_text(record.get("doc_id") or record.get("order_id") or record.get("id"))
        key = (hp, doc_type, doc_id)
        if key in seen:
            continue
        seen.add(key)
        output.append(record)
    return output


def fetch_doc_type_for_hp(
    doc_type: str,
    hp: str,
    company_name: str,
    years: list[int],
    publisher_filter: str,
    keyword: str,
    page_size: int,
    row_cap: int,
) -> dict[str, Any]:
    hp_norm = digits_only(hp)
    if not hp_norm:
        return {
            "status": "error",
            "records": [],
            "error": "Invalid HP value after normalization.",
            "capped": False,
            "fetched_raw_rows": 0,
            "total_available": 0,
            "request_url": "",
        }

    years_set = {int(year) for year in years}
    filters = _build_filters(doc_type, hp_norm, publisher_filter)
    filters_json = json.dumps(filters, ensure_ascii=False) if filters else ""
    q = _build_query(doc_type, hp_norm, keyword, publisher_filter)

    query_params_used = {
        "doc_type": doc_type,
        "hp": hp_norm,
        "q": q,
        "size": page_size,
        "from": "paged",
        "filters": filters,
        "years": sorted(years_set),
        "publisher_filter": publisher_filter,
        "keyword": keyword,
        "row_cap": row_cap,
        "post_filtering": ["hp", "years", "publisher", "keyword"],
    }

    offset = 0
    fetched_raw_rows = 0
    total_available = 0
    capped = False
    request_url = ""
    collected: list[dict[str, Any]] = []
    page_count = 0

    while True:
        if fetched_raw_rows >= row_cap:
            capped = True
            break

        request_size = min(page_size, max(1, row_cap - fetched_raw_rows))
        page_result = search_page(
            doc_type=doc_type,
            q=q,
            size=request_size,
            from_offset=offset,
            filters_json=filters_json,
        )
        request_url = page_result.get("url", request_url)

        if not page_result.get("ok", False):
            if page_count == 0:
                return {
                    "status": "error",
                    "records": [],
                    "error": page_result.get("error", "Unknown API error"),
                    "capped": False,
                    "fetched_raw_rows": 0,
                    "total_available": 0,
                    "request_url": request_url,
                }
            break

        page_records = page_result.get("records", [])
        total_available = int(page_result.get("total", total_available))
        if not page_records:
            break

        fetched_raw_rows += len(page_records)
        page_count += 1

        for record in page_records:
            if not _record_matches_hp(doc_type, record, hp_norm):
                continue
            if not _record_matches_years(record, doc_type, years_set):
                continue
            if doc_type == "contract-spending" and not _record_matches_publisher(
                record, publisher_filter
            ):
                continue
            if doc_type in {"contract-spending", "supports"} and not _record_matches_keyword(
                record, doc_type, keyword
            ):
                continue

            record_with_meta = dict(record)
            record_with_meta["hp"] = hp_norm
            record_with_meta["company_name"] = company_name or ""
            record_with_meta["doc_type"] = doc_type
            record_with_meta["query_params_used"] = json.dumps(
                query_params_used, ensure_ascii=False
            )
            collected.append(record_with_meta)

        offset += request_size
        if total_available and offset >= total_available:
            break
        if len(page_records) < request_size:
            break

    deduped = _deduplicate_records(collected)
    status = "ok" if deduped else "empty"
    return {
        "status": status,
        "records": deduped,
        "error": "",
        "capped": capped,
        "fetched_raw_rows": fetched_raw_rows,
        "total_available": total_available,
        "request_url": request_url,
    }
