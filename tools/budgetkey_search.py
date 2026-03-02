import json
from typing import Any

import requests
import streamlit as st

API_BASE_URL = "https://next.obudget.org/search"
REQUEST_TIMEOUT_SECONDS = 30


def digits_only(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if value != value:
            return ""
        if value.is_integer():
            text = str(int(value))
        else:
            text = str(value)
    elif isinstance(value, int):
        text = str(value)
    else:
        text = str(value).strip()

    if text.lower() in {"nan", "none"}:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return "".join(ch for ch in text if ch.isdigit())


def _normalized_for_compare(value: Any) -> str:
    digits = digits_only(value)
    if not digits:
        return ""
    return digits.lstrip("0") or "0"


def _digits_equal(value_a: Any, value_b: Any) -> bool:
    return _normalized_for_compare(value_a) == _normalized_for_compare(value_b)


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
    # HP mapping is driven by q=<hp>; avoid hard field assumptions at this stage.
    _ = (doc_type, record, hp_norm)
    return True


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
    return filters


def _build_query(doc_type: str, hp_norm: str, keyword: str) -> str:
    parts: list[str] = [hp_norm]
    if keyword and doc_type in {"contract-spending", "supports"}:
        parts.append(keyword.strip())
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


def _run_paged_pull(
    *,
    doc_type: str,
    hp_norm: str,
    company_name: str,
    q: str,
    years_set: set[int],
    publisher_filter: str,
    keyword: str,
    page_size: int,
    row_cap: int,
    filters_json: str,
    server_strategy: str,
    query_params_base: dict[str, Any],
) -> dict[str, Any]:
    offset = 0
    fetched_raw_rows = 0
    total_available = 0
    capped = False
    request_url = ""
    collected: list[dict[str, Any]] = []
    page_count = 0
    first_page_hits: int | None = None
    last_error = ""
    debug_pages: list[dict[str, Any]] = []

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
        page_records = page_result.get("records", [])
        page_hits = len(page_records)
        total_available = int(page_result.get("total", total_available))

        debug_pages.append(
            {
                "doc_type": doc_type,
                "server_strategy": server_strategy,
                "q": q,
                "filters": filters_json or "",
                "size": request_size,
                "from": offset,
                "hits_returned": page_hits,
                "total_available": total_available,
                "url": request_url,
                "ok": bool(page_result.get("ok", False)),
                "error": page_result.get("error", ""),
            }
        )

        if not page_result.get("ok", False):
            last_error = page_result.get("error", "Unknown API error")
            if page_count == 0:
                return {
                    "status": "error",
                    "records": [],
                    "error": last_error,
                    "capped": False,
                    "fetched_raw_rows": 0,
                    "total_available": 0,
                    "request_url": request_url,
                    "debug_pages": debug_pages,
                    "first_page_hits": 0,
                }
            break

        if first_page_hits is None:
            first_page_hits = page_hits

        if not page_records:
            break

        fetched_raw_rows += len(page_records)
        page_count += 1

        query_params_used = dict(query_params_base)
        query_params_used["server_strategy"] = server_strategy
        query_params_used["filters"] = filters_json

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
    if status == "empty" and last_error:
        status = "error"
    return {
        "status": status,
        "records": deduped,
        "error": last_error,
        "capped": capped,
        "fetched_raw_rows": fetched_raw_rows,
        "total_available": total_available,
        "request_url": request_url,
        "debug_pages": debug_pages,
        "first_page_hits": first_page_hits or 0,
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
    q = _build_query(doc_type, hp_norm, keyword)

    query_params_base = {
        "doc_type": doc_type,
        "hp": hp_norm,
        "q": q,
        "size": page_size,
        "from": "paged",
        "years": sorted(years_set),
        "publisher_filter": publisher_filter,
        "keyword": keyword,
        "row_cap": row_cap,
        "post_filtering": ["years", "publisher", "keyword"],
    }

    if doc_type == "contract-spending":
        supplier_filters = _build_filters(doc_type, hp_norm, publisher_filter)
        supplier_filters_json = (
            json.dumps(supplier_filters, ensure_ascii=False) if supplier_filters else ""
        )

        filtered_result = _run_paged_pull(
            doc_type=doc_type,
            hp_norm=hp_norm,
            company_name=company_name,
            q=q,
            years_set=years_set,
            publisher_filter=publisher_filter,
            keyword=keyword,
            page_size=page_size,
            row_cap=row_cap,
            filters_json=supplier_filters_json,
            server_strategy="q_plus_supplier_code_filter",
            query_params_base=query_params_base,
        )

        debug_pages = list(filtered_result.get("debug_pages", []))
        need_fallback = (
            filtered_result.get("status") == "error"
            or filtered_result.get("first_page_hits", 0) == 0
        )
        if not need_fallback:
            filtered_result["debug_pages"] = debug_pages
            return filtered_result

        fallback_result = _run_paged_pull(
            doc_type=doc_type,
            hp_norm=hp_norm,
            company_name=company_name,
            q=q,
            years_set=years_set,
            publisher_filter=publisher_filter,
            keyword=keyword,
            page_size=page_size,
            row_cap=row_cap,
            filters_json="",
            server_strategy="q_only_fallback",
            query_params_base=query_params_base,
        )
        fallback_result["debug_pages"] = debug_pages + fallback_result.get(
            "debug_pages", []
        )
        return fallback_result

    return _run_paged_pull(
        doc_type=doc_type,
        hp_norm=hp_norm,
        company_name=company_name,
        q=q,
        years_set=years_set,
        publisher_filter=publisher_filter,
        keyword=keyword,
        page_size=page_size,
        row_cap=row_cap,
        filters_json="",
        server_strategy="q_primary",
        query_params_base=query_params_base,
    )
