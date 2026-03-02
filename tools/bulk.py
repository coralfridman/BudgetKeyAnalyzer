from typing import Any

import pandas as pd

from tools.budgetkey_search import digits_only, fetch_doc_type_for_hp

HP_COLUMN_CANDIDATES = ["ח.פ", "חפ", "hp", "registration", "company_id"]
NAME_COLUMN_CANDIDATES = ["חברה", "company_name", "name"]


def _normalize_column_name(column_name: str) -> str:
    return "".join(ch.lower() for ch in str(column_name).strip() if ch.isalnum())


def detect_hp_column(columns: list[str]) -> str | None:
    normalized_to_original = {
        _normalize_column_name(column): column for column in columns
    }
    for candidate in HP_COLUMN_CANDIDATES:
        normalized_candidate = _normalize_column_name(candidate)
        if normalized_candidate in normalized_to_original:
            return normalized_to_original[normalized_candidate]
    return None


def detect_name_column(columns: list[str]) -> str | None:
    normalized_to_original = {
        _normalize_column_name(column): column for column in columns
    }
    for candidate in NAME_COLUMN_CANDIDATES:
        normalized_candidate = _normalize_column_name(candidate)
        if normalized_candidate in normalized_to_original:
            return normalized_to_original[normalized_candidate]
    return None


def normalize_uploaded_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, str, str | None]:
    if df.empty:
        raise ValueError("Uploaded Excel is empty.")

    hp_column = detect_hp_column([str(col) for col in df.columns])
    if not hp_column:
        raise ValueError(
            "Could not detect HP column. Expected one of: "
            "['ח.פ', 'חפ', 'HP', 'registration', 'company_id']."
        )

    name_column = detect_name_column([str(col) for col in df.columns])
    normalized = pd.DataFrame()
    normalized["hp"] = df[hp_column].map(digits_only)
    normalized["company_name"] = (
        df[name_column].astype(str).where(df[name_column].notna(), "")
        if name_column
        else ""
    )
    normalized["company_name"] = normalized["company_name"].astype(str).str.strip()
    normalized = normalized.loc[normalized["hp"].astype(str).str.strip().ne("")].copy()
    normalized = normalized.drop_duplicates(subset=["hp"]).reset_index(drop=True)
    return normalized, hp_column, name_column


def process_single_hp(
    hp: str,
    company_name: str,
    selected_doc_types: list[str],
    years: list[int],
    publisher_filter: str,
    keyword: str,
    page_size: int,
    row_cap: int,
) -> dict[str, Any]:
    doc_results: dict[str, dict[str, Any]] = {}
    has_error = False
    has_data = False

    for doc_type in selected_doc_types:
        try:
            result = fetch_doc_type_for_hp(
                doc_type=doc_type,
                hp=hp,
                company_name=company_name,
                years=years,
                publisher_filter=publisher_filter,
                keyword=keyword,
                page_size=page_size,
                row_cap=row_cap,
            )
            doc_results[doc_type] = result
            if result.get("status") == "error":
                has_error = True
            if result.get("records"):
                has_data = True
        except Exception as exc:  # Defensive catch for per-HP resilience.
            doc_results[doc_type] = {
                "status": "error",
                "records": [],
                "error": f"Unexpected error: {exc}",
                "capped": False,
                "fetched_raw_rows": 0,
                "total_available": 0,
                "request_url": "",
            }
            has_error = True

    if has_error:
        overall_status = "error"
    elif has_data:
        overall_status = "ok"
    else:
        overall_status = "empty"

    return {
        "hp": hp,
        "company_name": company_name,
        "overall_status": overall_status,
        "doc_results": doc_results,
    }
