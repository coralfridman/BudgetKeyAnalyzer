import json
from typing import Any

import numpy as np
import pandas as pd


def stringify_cell(value: Any) -> Any:
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item is not None)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value


def records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.json_normalize(records, sep=".")
    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].map(stringify_cell)
    return df


def ensure_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in ("hp", "company_name", "doc_type", "query_params_used"):
        if column not in result.columns:
            result[column] = ""
    return result


def _as_text(value: Any) -> str:
    if isinstance(value, list):
        for item in value:
            text = _as_text(item)
            if text:
                return text
        return ""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def _coalesce_text_columns(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    result = pd.Series([""] * len(df), index=df.index, dtype=object)
    for column in candidates:
        if column not in df.columns:
            continue
        values = df[column].map(_as_text)
        fill_mask = result.eq("") & values.ne("")
        result.loc[fill_mask] = values.loc[fill_mask]
    return result


def build_procurement_df(contract_df: pd.DataFrame) -> pd.DataFrame:
    required_columns = [
        "שנה",
        "תקנה תקציבית",
        "משרד",
        "סך כולל",
        "תיאור",
        "מול מי ההתקשרות",
        "סוג ההוצאה",
    ]
    detail_columns = ["hp", "company_name", "תג אזהרה", "_purpose_base", "_missing_amount"]

    if contract_df.empty:
        return pd.DataFrame(columns=required_columns + detail_columns)

    work = contract_df.copy()

    executed = pd.to_numeric(work.get("executed", pd.Series(index=work.index)), errors="coerce")
    volume = pd.to_numeric(work.get("volume", pd.Series(index=work.index)), errors="coerce")
    amount = executed.where(executed.notna(), volume)
    missing_amount_mask = amount.isna()
    amount = amount.fillna(0)

    order_date_series = pd.to_datetime(
        work.get("order_date", pd.Series(index=work.index, dtype=object)),
        errors="coerce",
    )
    year_series = order_date_series.dt.year.astype("Int64")

    budget_code = _coalesce_text_columns(work, ["budget_code"])
    publisher = _coalesce_text_columns(work, ["publisher"])
    purpose_base = _coalesce_text_columns(
        work, ["purpose", "description", "title", "page_title", "budget_title"]
    )
    supplier_name = _coalesce_text_columns(work, ["supplier_name", "supplier", "entity_name"])
    expense_type = _coalesce_text_columns(work, ["expense_type", "spending_type", "type", "kind"])
    expense_type = expense_type.mask(expense_type.eq(""), "רכש")
    expense_type = expense_type.where(~missing_amount_mask, expense_type + " ⚠")

    hp_values = (
        work["hp"].map(_as_text)
        if "hp" in work.columns
        else pd.Series([""] * len(work), index=work.index, dtype=object)
    )
    company_values = (
        work["company_name"].map(_as_text)
        if "company_name" in work.columns
        else pd.Series([""] * len(work), index=work.index, dtype=object)
    )
    warning_tag = missing_amount_mask.map(lambda is_missing: "⚠ חסר executed/volume" if is_missing else "")

    output = pd.DataFrame(
        {
            "שנה": year_series,
            "תקנה תקציבית": budget_code,
            "משרד": publisher,
            "סך כולל": amount.astype(float),
            "תיאור": purpose_base,
            "מול מי ההתקשרות": supplier_name,
            "סוג ההוצאה": expense_type,
            "hp": hp_values,
            "company_name": company_values,
            "תג אזהרה": warning_tag,
            "_purpose_base": purpose_base,
            "_missing_amount": missing_amount_mask,
        }
    )

    output["_sort_year"] = output["שנה"].fillna(-1).astype(int)
    output = output.sort_values(
        by=["_sort_year", "סך כולל"], ascending=[False, False]
    ).drop(columns=["_sort_year"])
    return output.reset_index(drop=True)


def build_master_joined_table(
    contract_df: pd.DataFrame, supports_df: pd.DataFrame, entities_df: pd.DataFrame
) -> pd.DataFrame:
    frames = []
    for frame in (contract_df, supports_df, entities_df):
        if not frame.empty:
            frames.append(ensure_output_columns(frame))
    if not frames:
        return pd.DataFrame(columns=["hp", "company_name", "doc_type", "query_params_used"])
    return pd.concat(frames, ignore_index=True)


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def contract_amount_series(contract_df: pd.DataFrame) -> pd.Series:
    if contract_df.empty:
        return pd.Series(dtype=float)
    executed = _to_numeric(contract_df.get("executed", pd.Series(dtype=float)))
    volume = _to_numeric(contract_df.get("volume", pd.Series(dtype=float)))
    return executed.where(executed.notna(), volume)


def supports_amount_series(supports_df: pd.DataFrame) -> pd.Series:
    if supports_df.empty:
        return pd.Series(dtype=float)
    return _to_numeric(supports_df.get("amount_total", pd.Series(dtype=float)))


def publisher_suggestions_from_results(contract_df: pd.DataFrame, top_n: int = 20) -> list[str]:
    if contract_df.empty or "publisher" not in contract_df.columns:
        return []
    suggestions = (
        contract_df["publisher"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
        .value_counts()
        .head(top_n)
        .index.tolist()
    )
    return suggestions


def _fmt_amount(value: float | int | None) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    return f"{float(value):,.0f}"


def _top_series(df: pd.DataFrame, group_col: str, amount_col: pd.Series, top_n: int = 10) -> pd.Series:
    if df.empty or group_col not in df.columns or amount_col.empty:
        return pd.Series(dtype=float)
    work = pd.DataFrame({group_col: df[group_col], "amount": amount_col})
    work[group_col] = work[group_col].astype(str).str.strip()
    work = work.loc[work[group_col].ne("") & work["amount"].notna()]
    if work.empty:
        return pd.Series(dtype=float)
    return work.groupby(group_col)["amount"].sum().sort_values(ascending=False).head(top_n)


def _top_purpose_counts(contract_df: pd.DataFrame, top_n: int = 10) -> pd.Series:
    if contract_df.empty:
        return pd.Series(dtype=float)
    purpose_col = None
    for candidate in ("purpose", "description", "page_title"):
        if candidate in contract_df.columns:
            purpose_col = candidate
            break
    if not purpose_col:
        return pd.Series(dtype=float)
    values = (
        contract_df[purpose_col]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
    )
    if values.empty:
        return pd.Series(dtype=float)
    return values.value_counts().head(top_n)


def _contract_yearly_totals(contract_df: pd.DataFrame, amounts: pd.Series) -> pd.Series:
    if contract_df.empty or "order_date" not in contract_df.columns or amounts.empty:
        return pd.Series(dtype=float)
    dates = pd.to_datetime(contract_df["order_date"], errors="coerce")
    work = pd.DataFrame({"year": dates.dt.year, "amount": amounts})
    work = work.loc[work["year"].notna() & work["amount"].notna()]
    if work.empty:
        return pd.Series(dtype=float)
    work["year"] = work["year"].astype(int)
    return work.groupby("year")["amount"].sum().sort_index()


def _supports_yearly_totals(supports_df: pd.DataFrame, amounts: pd.Series) -> pd.Series:
    if supports_df.empty or amounts.empty or "year_requested" not in supports_df.columns:
        return pd.Series(dtype=float)
    years = pd.to_numeric(supports_df["year_requested"], errors="coerce")
    work = pd.DataFrame({"year": years, "amount": amounts})
    work = work.loc[work["year"].notna() & work["amount"].notna()]
    if work.empty:
        return pd.Series(dtype=float)
    work["year"] = work["year"].astype(int)
    return work.groupby("year")["amount"].sum().sort_index()


def _unusually_high_hps(contract_df: pd.DataFrame, supports_df: pd.DataFrame) -> pd.Series:
    hp_totals = pd.Series(dtype=float)
    if not contract_df.empty and "hp" in contract_df.columns:
        contract_amounts = contract_amount_series(contract_df)
        contract_work = pd.DataFrame({"hp": contract_df["hp"], "amount": contract_amounts})
        contract_work = contract_work.loc[contract_work["amount"].notna()]
        hp_totals = hp_totals.add(
            contract_work.groupby("hp")["amount"].sum(), fill_value=0.0
        )
    if not supports_df.empty and "hp" in supports_df.columns:
        supports_amounts = supports_amount_series(supports_df)
        supports_work = pd.DataFrame({"hp": supports_df["hp"], "amount": supports_amounts})
        supports_work = supports_work.loc[supports_work["amount"].notna()]
        hp_totals = hp_totals.add(
            supports_work.groupby("hp")["amount"].sum(), fill_value=0.0
        )

    hp_totals = hp_totals.sort_values(ascending=False)
    if hp_totals.empty:
        return hp_totals

    mean_value = hp_totals.mean()
    std_value = hp_totals.std(ddof=0)
    threshold = mean_value + (2 * std_value if std_value > 0 else mean_value)
    return hp_totals.loc[hp_totals > threshold]


def compute_management_summary(
    contract_df: pd.DataFrame,
    supports_df: pd.DataFrame,
    entities_df: pd.DataFrame,
    status_df: pd.DataFrame,
    selected_doc_types: list[str],
) -> dict[str, Any]:
    contract_amounts = contract_amount_series(contract_df)
    supports_amounts = supports_amount_series(supports_df)

    total_contract_records = len(contract_df)
    total_support_records = len(supports_df)
    total_entities_records = len(entities_df)

    contract_total_amount = float(contract_amounts.fillna(0).sum()) if not contract_amounts.empty else 0.0
    supports_total_amount = float(supports_amounts.fillna(0).sum()) if not supports_amounts.empty else 0.0

    unique_suppliers = (
        contract_df.get("supplier_name", pd.Series(dtype=str))
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
        .nunique()
        if not contract_df.empty
        else 0
    )
    unique_recipients = (
        supports_df.get("recipient", pd.Series(dtype=str))
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
        .nunique()
        if not supports_df.empty
        else 0
    )

    top_publishers = _top_series(contract_df, "publisher", contract_amounts, top_n=10)
    top_suppliers = _top_series(contract_df, "supplier_name", contract_amounts, top_n=10)
    top_purposes = _top_purpose_counts(contract_df, top_n=10)

    no_data_hps = []
    if not status_df.empty:
        doc_status_cols = [f"{doc_type}_status" for doc_type in selected_doc_types]
        subset = status_df.copy()
        for col in doc_status_cols:
            if col not in subset.columns:
                subset[col] = "not_selected"
        empty_mask = subset[doc_status_cols].isin(["empty", "not_selected"]).all(axis=1)
        no_data_hps = subset.loc[empty_mask, "hp"].astype(str).tolist()

    unusual_hps = _unusually_high_hps(contract_df, supports_df)
    contract_yearly = _contract_yearly_totals(contract_df, contract_amounts)
    supports_yearly = _supports_yearly_totals(supports_df, supports_amounts)

    insights: list[str] = []
    insights.append(
        f"Fetched {total_contract_records:,} contract-spending rows and {total_support_records:,} supports rows."
    )
    insights.append(
        f"Total contract amount (executed fallback volume): {_fmt_amount(contract_total_amount)}."
    )
    insights.append(
        f"Total supports amount_total: {_fmt_amount(supports_total_amount)}."
    )
    insights.append(
        f"Unique suppliers: {unique_suppliers:,}; unique recipients: {unique_recipients:,}."
    )

    if not top_publishers.empty:
        insights.append(
            f"Top publisher: {top_publishers.index[0]} with {_fmt_amount(top_publishers.iloc[0])}."
        )
    if len(top_publishers) >= 3:
        top3_sum = float(top_publishers.head(3).sum())
        insights.append(
            f"Top 3 publishers account for {_fmt_amount(top3_sum)} total amount."
        )

    if not top_suppliers.empty:
        insights.append(
            f"Top supplier: {top_suppliers.index[0]} with {_fmt_amount(top_suppliers.iloc[0])}."
        )

    if not top_purposes.empty:
        insights.append(
            f"Most frequent purpose: {top_purposes.index[0]} ({int(top_purposes.iloc[0]):,} rows)."
        )

    if len(contract_yearly) >= 2:
        first_year = int(contract_yearly.index.min())
        last_year = int(contract_yearly.index.max())
        delta = float(contract_yearly.iloc[-1] - contract_yearly.iloc[0])
        direction = "increased" if delta >= 0 else "decreased"
        insights.append(
            f"Contract totals {direction} from {_fmt_amount(contract_yearly.iloc[0])} "
            f"in {first_year} to {_fmt_amount(contract_yearly.iloc[-1])} in {last_year}."
        )
    elif len(contract_yearly) == 1:
        year = int(contract_yearly.index[0])
        insights.append(
            f"Contract totals are available for a single year ({year}): {_fmt_amount(contract_yearly.iloc[0])}."
        )

    if len(supports_yearly) >= 2:
        first_year = int(supports_yearly.index.min())
        last_year = int(supports_yearly.index.max())
        delta = float(supports_yearly.iloc[-1] - supports_yearly.iloc[0])
        direction = "increased" if delta >= 0 else "decreased"
        insights.append(
            f"Supports totals {direction} from {_fmt_amount(supports_yearly.iloc[0])} "
            f"in {first_year} to {_fmt_amount(supports_yearly.iloc[-1])} in {last_year}."
        )

    insights.append(
        f"HPs with no data in selected doc-types: {len(no_data_hps):,}."
        + (f" Examples: {', '.join(no_data_hps[:5])}." if no_data_hps else "")
    )

    if not unusual_hps.empty:
        top_unusual = unusual_hps.head(5)
        top_unusual_text = ", ".join(
            f"{hp} ({_fmt_amount(amount)})" for hp, amount in top_unusual.items()
        )
        insights.append(
            f"Unusually high total HPs (mean + 2*std threshold): {top_unusual_text}."
        )
    else:
        insights.append("No unusually high HP totals detected under mean + 2*std threshold.")

    if len(insights) < 8:
        insights.append("Publisher concentration remains measurable from current filtered result set.")
    if len(insights) < 8:
        insights.append("Review top purposes and suppliers together to validate spending context.")
    insights = insights[:15]

    summary_rows = [
        ("contract_spending_records", f"{total_contract_records:,}"),
        ("supports_records", f"{total_support_records:,}"),
        ("entities_records", f"{total_entities_records:,}"),
        ("contract_total_amount", _fmt_amount(contract_total_amount)),
        ("supports_total_amount_total", _fmt_amount(supports_total_amount)),
        ("unique_suppliers", f"{unique_suppliers:,}"),
        ("unique_recipients", f"{unique_recipients:,}"),
        ("no_data_hps_count", f"{len(no_data_hps):,}"),
        ("no_data_hps_list", ", ".join(no_data_hps[:30])),
    ]

    if not top_publishers.empty:
        for index, (publisher, amount) in enumerate(top_publishers.items(), start=1):
            summary_rows.append((f"top_publisher_{index}", f"{publisher} | {_fmt_amount(amount)}"))

    executive_summary_df = pd.DataFrame(summary_rows, columns=["metric", "value"])

    return {
        "kpis": {
            "contract_spending_records": total_contract_records,
            "supports_records": total_support_records,
            "entities_records": total_entities_records,
            "contract_total_amount": contract_total_amount,
            "supports_total_amount_total": supports_total_amount,
            "unique_suppliers": unique_suppliers,
            "unique_recipients": unique_recipients,
            "no_data_hps": no_data_hps,
        },
        "top_publishers": top_publishers,
        "top_suppliers": top_suppliers,
        "top_purposes": top_purposes,
        "insights": insights,
        "executive_summary_df": executive_summary_df,
        "no_data_hps": no_data_hps,
    }
