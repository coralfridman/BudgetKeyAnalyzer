import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from tools.analysis import (
    build_master_joined_table,
    build_procurement_df,
    compute_management_summary,
    ensure_output_columns,
    publisher_suggestions_from_results,
    records_to_dataframe as records_to_dataframe_bulk,
)
from tools.budgetkey_search import digits_only, safe_number, safe_text, search_page
from tools.bulk import normalize_uploaded_dataframe, process_single_hp
from tools.export import (
    build_combined_report_xlsx,
    build_report_bundle_zip,
    dataframe_to_csv_bytes,
    dataframe_to_excel_bytes,
)
from tools.plots import create_management_charts, figure_to_png_bytes


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def human_number(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return "N/A"


def stringify_for_table(value: Any) -> Any:
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item is not None)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value


def search_api(doc_type: str, q: str, size: int, from_offset: int, filters_json: str = "") -> dict[str, Any]:
    response = search_page(
        doc_type=doc_type,
        q=q,
        size=int(size),
        from_offset=int(from_offset),
        filters_json=filters_json,
    )
    return {
        "ok": bool(response.get("ok")),
        "records": response.get("records", []),
        "total": safe_int(response.get("total"), 0),
        "error": response.get("error", ""),
        "url": response.get("url", ""),
    }


def _value_matches_filter(value: Any, expected: str, field_name: str) -> bool:
    if isinstance(value, list):
        return any(_value_matches_filter(item, expected, field_name) for item in value)
    if value is None:
        return False
    if field_name in {"supplier_code", "entity_id"}:
        left = digits_only(value)
        right = digits_only(expected)
        left_norm = left.lstrip("0") or "0" if left else ""
        right_norm = right.lstrip("0") or "0" if right else ""
        return left_norm == right_norm
    return expected.casefold() in str(value).casefold()


def _filter_seems_applied(
    records: list[dict[str, Any]], total: int, field_name: str, expected: str
) -> bool:
    if total == 0:
        return True
    if not records:
        return False
    return all(
        _value_matches_filter(record.get(field_name), expected, field_name)
        for record in records
    )


def search_with_filter_fallback(
    doc_type: str,
    size: int,
    from_offset: int,
    filter_field: str,
    filter_value: str,
    fallback_q: str,
    base_q: str = "",
) -> dict[str, Any]:
    filter_candidates = [
        [{"field": filter_field, "value": filter_value}],
        [{"path": filter_field, "value": filter_value}],
        [{"path": filter_field, "terms": [filter_value]}],
        [{"field": filter_field, "terms": [filter_value]}],
    ]

    for candidate in filter_candidates:
        filters_json = json.dumps(candidate, ensure_ascii=False)
        result = search_api(
            doc_type=doc_type,
            q=base_q,
            size=size,
            from_offset=from_offset,
            filters_json=filters_json,
        )
        if not result["ok"]:
            continue
        if _filter_seems_applied(
            records=result["records"],
            total=result["total"],
            field_name=filter_field,
            expected=filter_value,
        ):
            result["strategy"] = f"filters ({filter_field})"
            return result

    fallback_result = search_api(
        doc_type=doc_type,
        q=fallback_q,
        size=size,
        from_offset=from_offset,
        filters_json="",
    )
    fallback_result["strategy"] = "fallback to q"
    return fallback_result


def records_to_display_df(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.json_normalize(records, sep=".")
    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].map(stringify_for_table)
    return df


def records_to_analysis_df(records: list[dict[str, Any]]) -> tuple[pd.DataFrame, str | None]:
    rows: list[dict[str, Any]] = []
    for record in records:
        publisher = safe_text(record.get("publisher"))
        supplier = safe_text(
            record.get("supplier_name")
            or record.get("recipient")
            or record.get("entity_name")
            or record.get("name")
        )
        rows.append(
            {
                "publisher": publisher,
                "supplier": supplier,
                "executed": safe_number(record.get("executed")),
                "volume": safe_number(record.get("volume")),
                "order_date": pd.to_datetime(record.get("order_date"), errors="coerce"),
            }
        )

    analysis_df = pd.DataFrame(rows)
    if analysis_df.empty:
        return analysis_df, None

    if analysis_df["executed"].notna().any():
        metric_col = "executed"
    elif analysis_df["volume"].notna().any():
        metric_col = "volume"
    else:
        metric_col = None
    return analysis_df, metric_col


def generate_insights(
    analysis_df: pd.DataFrame,
    shown_rows: int,
    total_rows: int,
    metric_col: str | None,
) -> list[str]:
    insights: list[str] = []
    insights.append(
        f"Showing {shown_rows} rows on this page out of {total_rows} total matching records."
    )

    if analysis_df.empty:
        insights.append("No records returned for this query and pagination window.")
        insights.append("Try lowering the offset or broadening the search terms.")
        insights.append("If you expected results, retry with a fallback free-text query.")
        return insights[:6]

    if metric_col:
        total_metric = analysis_df[metric_col].fillna(0).sum()
        insights.append(
            f"Total {metric_col} in current page: {human_number(total_metric)}."
        )

    publishers = analysis_df.loc[
        analysis_df["publisher"].astype(str).str.strip().ne(""), "publisher"
    ]
    if not publishers.empty:
        insights.append(f"Unique publishers in current page: {publishers.nunique()}.")

    suppliers = analysis_df.loc[
        analysis_df["supplier"].astype(str).str.strip().ne(""), "supplier"
    ]
    if not suppliers.empty:
        insights.append(f"Unique suppliers/recipients in current page: {suppliers.nunique()}.")

    if metric_col and not publishers.empty:
        by_pub = (
            analysis_df.loc[analysis_df["publisher"].astype(str).str.strip().ne("")]
            .groupby("publisher")[metric_col]
            .sum()
            .sort_values(ascending=False)
        )
        if not by_pub.empty:
            top_publisher = by_pub.index[0]
            top_value = by_pub.iloc[0]
            insights.append(
                f"Top publisher by {metric_col}: {top_publisher} ({human_number(top_value)})."
            )

    valid_dates = analysis_df["order_date"].dropna()
    if not valid_dates.empty:
        insights.append(
            f"Order dates span from {valid_dates.min().date()} to {valid_dates.max().date()}."
        )

    if len(insights) < 3:
        insights.append("Some expected fields are missing (e.g., publisher/supplier/order_date).")
    if len(insights) < 3:
        insights.append("Consider switching search mode for richer fields.")
    return insights[:6]


def placeholder_chart(title: str, message: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.axis("off")
    ax.set_title(title)
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=11)
    fig.tight_layout()
    return fig


def create_charts(analysis_df: pd.DataFrame, metric_col: str | None) -> dict[str, plt.Figure]:
    charts: dict[str, plt.Figure] = {}

    publisher_title = "Top 10 Publishers by Executed (fallback: Volume)"
    if not analysis_df.empty:
        publisher_df = analysis_df.loc[
            analysis_df["publisher"].astype(str).str.strip().ne("")
        ].copy()
    else:
        publisher_df = pd.DataFrame()
    if not publisher_df.empty:
        if metric_col:
            top_publishers = (
                publisher_df.groupby("publisher")[metric_col]
                .sum()
                .sort_values(ascending=False)
                .head(10)
                .sort_values(ascending=True)
            )
            ylabel = metric_col
        else:
            top_publishers = (
                publisher_df["publisher"]
                .value_counts()
                .head(10)
                .sort_values(ascending=True)
            )
            ylabel = "count"
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.barh(top_publishers.index, top_publishers.values)
        ax.set_title(publisher_title)
        ax.set_xlabel(ylabel)
        ax.set_ylabel("publisher")
        fig.tight_layout()
        charts["publishers"] = fig
    else:
        charts["publishers"] = placeholder_chart(
            publisher_title, "No publisher data available for this result set."
        )

    supplier_title = "Top 10 Suppliers by Executed/Volume"
    if not analysis_df.empty:
        supplier_df = analysis_df.loc[
            analysis_df["supplier"].astype(str).str.strip().ne("")
        ].copy()
    else:
        supplier_df = pd.DataFrame()
    if not supplier_df.empty:
        if metric_col:
            top_suppliers = (
                supplier_df.groupby("supplier")[metric_col]
                .sum()
                .sort_values(ascending=False)
                .head(10)
                .sort_values(ascending=True)
            )
            ylabel = metric_col
        else:
            top_suppliers = (
                supplier_df["supplier"]
                .value_counts()
                .head(10)
                .sort_values(ascending=True)
            )
            ylabel = "count"
        fig, ax = plt.subplots(figsize=(9, 5))
        ax.barh(top_suppliers.index, top_suppliers.values)
        ax.set_title(supplier_title)
        ax.set_xlabel(ylabel)
        ax.set_ylabel("supplier")
        fig.tight_layout()
        charts["suppliers"] = fig
    else:
        charts["suppliers"] = placeholder_chart(
            supplier_title, "No supplier/recipient data available for this result set."
        )

    monthly_title = "Monthly Time Series"
    if not analysis_df.empty:
        time_df = analysis_df.dropna(subset=["order_date"]).copy()
    else:
        time_df = pd.DataFrame()
    if not time_df.empty:
        time_df["month"] = time_df["order_date"].dt.to_period("M").dt.to_timestamp()
        if metric_col:
            monthly = time_df.groupby("month")[metric_col].sum().sort_index()
            ylabel = metric_col
        else:
            monthly = time_df.groupby("month").size().sort_index()
            ylabel = "count"
        fig, ax = plt.subplots(figsize=(9, 4.5))
        ax.plot(monthly.index, monthly.values, marker="o")
        ax.set_title(monthly_title)
        ax.set_xlabel("month")
        ax.set_ylabel(ylabel)
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()
        charts["monthly"] = fig
    else:
        charts["monthly"] = placeholder_chart(
            monthly_title, "No valid order_date values available for this result set."
        )

    return charts


def publisher_suggestions_single(analysis_df: pd.DataFrame) -> pd.DataFrame:
    if analysis_df.empty:
        return pd.DataFrame(columns=["publisher", "rows"])
    counts = (
        analysis_df.loc[analysis_df["publisher"].astype(str).str.strip().ne(""), "publisher"]
        .value_counts()
        .head(15)
        .reset_index()
    )
    counts.columns = ["publisher", "rows"]
    return counts


def render_result_panel(panel_title: str, result: dict[str, Any], download_key_prefix: str) -> None:
    st.subheader(panel_title)
    if not result.get("ok", False):
        st.error(result.get("error", "Unknown API error"))
        return

    strategy = result.get("strategy")
    if strategy:
        st.caption(f"Search strategy: {strategy}")
    st.caption(f"API URL: {result.get('url', '')}")

    records = result.get("records", [])
    total = safe_int(result.get("total"), 0)
    display_df = records_to_display_df(records)
    analysis_df, metric_col = records_to_analysis_df(records)
    charts = create_charts(analysis_df, metric_col)
    insights = generate_insights(
        analysis_df=analysis_df,
        shown_rows=len(display_df),
        total_rows=total,
        metric_col=metric_col,
    )

    st.markdown("**Insights**")
    for item in insights:
        st.markdown(f"- {item}")

    st.markdown("**Results table**")
    if display_df.empty:
        st.info("No rows returned for this page.")
    st.dataframe(display_df, use_container_width=True)

    csv_bytes = dataframe_to_csv_bytes(display_df)
    excel_bytes = dataframe_to_excel_bytes(display_df, sheet_name="results")

    chart_bytes = {}
    for name, fig in charts.items():
        chart_bytes[name] = figure_to_png_bytes(fig)
        plt.close(fig)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name=f"{download_key_prefix}.csv",
            mime="text/csv",
            key=f"{download_key_prefix}_csv",
        )
    with col2:
        st.download_button(
            "Download Excel (XLSX)",
            data=excel_bytes,
            file_name=f"{download_key_prefix}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{download_key_prefix}_xlsx",
        )
    with col3:
        charts_zip_payload = build_report_bundle_zip(
            report_xlsx_bytes=excel_bytes,
            raw_csv_bytes={f"{download_key_prefix}.csv": csv_bytes},
            raw_xlsx_bytes={},
            chart_png_bytes={
                f"{download_key_prefix}_{name}.png": payload
                for name, payload in chart_bytes.items()
            },
            report_md="Charts bundle for single-query mode.",
        )
        st.download_button(
            "Download PNG charts (ZIP)",
            data=charts_zip_payload,
            file_name=f"{download_key_prefix}_charts.zip",
            mime="application/zip",
            key=f"{download_key_prefix}_png",
        )

    st.markdown("**Charts**")
    st.image(chart_bytes["publishers"], caption="Top publishers chart", use_container_width=True)
    st.image(chart_bytes["suppliers"], caption="Top suppliers chart", use_container_width=True)
    st.image(chart_bytes["monthly"], caption="Monthly time series chart", use_container_width=True)


def render_procurement_view(
    contract_df: pd.DataFrame, key_prefix: str, is_bulk: bool
) -> None:
    st.markdown("### Procurement view (רכש)")
    st.write("רשימת התקשרויות רכש: על מה הייתה ההוצאה ומול מי ההתקשרות")

    procurement_df = build_procurement_df(contract_df, keep_extra_columns=True)
    if procurement_df.empty:
        st.info("No contract-spending records available for procurement view.")
        return

    year_options = (
        procurement_df["שנה"]
        .dropna()
        .astype(int)
        .drop_duplicates()
        .sort_values(ascending=False)
        .tolist()
    )
    selected_years = st.multiselect(
        "סינון לפי שנה",
        options=year_options,
        default=year_options,
        key=f"{key_prefix}_proc_years",
    )
    ministry_filter = st.text_input(
        "סינון משרד (publisher)",
        key=f"{key_prefix}_proc_ministry_filter",
    ).strip()
    keyword_filter = st.text_input(
        "מילת מפתח בתיאור",
        key=f"{key_prefix}_proc_keyword_filter",
    ).strip()

    filtered = procurement_df.copy()
    if year_options and selected_years:
        filtered = filtered.loc[
            filtered["שנה"].fillna(-1).astype(int).isin(selected_years)
        ]
    if ministry_filter:
        filtered = filtered.loc[
            filtered["משרד"].astype(str).str.contains(ministry_filter, case=False, na=False)
        ]
    if keyword_filter:
        filtered = filtered.loc[
            filtered["תיאור"].astype(str).str.contains(keyword_filter, case=False, na=False)
        ]

    total_rows = len(filtered)
    total_amount = pd.to_numeric(filtered["סך כולל"], errors="coerce").fillna(0).sum()

    supplier_top = (
        filtered["מול מי ההתקשרות"]
        .fillna("")
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
    )
    if not supplier_top.empty:
        supplier_top = (
            filtered.loc[supplier_top.index]
            .assign(_supplier=supplier_top)
            .groupby("_supplier")["סך כולל"]
            .sum()
            .sort_values(ascending=False)
            .head(3)
        )
    else:
        supplier_top = pd.Series(dtype=float)

    purpose_source = (
        filtered["_purpose_base"]
        if "_purpose_base" in filtered.columns
        else filtered["תיאור"]
    )
    purpose_top = (
        purpose_source.fillna("")
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
        .value_counts()
        .head(3)
    )

    s_col1, s_col2, s_col3, s_col4 = st.columns(4)
    s_col1.metric("# רשומות רכש", f"{total_rows:,}")
    s_col2.metric("סך הוצאה (executed/volume)", human_number(total_amount))
    with s_col3:
        st.markdown("**Top 3 suppliers by amount**")
        if supplier_top.empty:
            st.caption("No supplier data")
        else:
            for supplier, amount in supplier_top.items():
                st.markdown(f"- {supplier}: {human_number(amount)}")
    with s_col4:
        st.markdown("**Top 3 purposes by count**")
        if purpose_top.empty:
            st.caption("No purpose data")
        else:
            for purpose, count in purpose_top.items():
                st.markdown(f"- {purpose}: {int(count):,}")

    warning_count = int(filtered.get("_missing_amount", pd.Series(dtype=bool)).sum())
    if warning_count > 0:
        st.warning(
            f"{warning_count:,} rows are missing both executed and volume; marked with ⚠ in 'סוג ההוצאה'."
        )

    required_cols = [
        "שנה",
        "תקנה תקציבית",
        "משרד",
        "סך כולל",
        "תיאור",
        "מול מי ההתקשרות",
        "סוג ההוצאה",
    ]
    show_details = False
    if is_bulk:
        show_details = st.checkbox("show details", key=f"{key_prefix}_proc_show_details")

    display_cols = required_cols.copy()
    if show_details:
        display_cols.extend(["hp", "company_name", "תג אזהרה"])

    table_df = filtered[display_cols].copy()
    st.dataframe(table_df, use_container_width=True)

    export_cols = required_cols + ["hp", "company_name", "תג אזהרה"]
    export_cols = [col for col in export_cols if col in filtered.columns]
    export_df = filtered[export_cols].copy()
    csv_bytes = dataframe_to_csv_bytes(export_df)
    excel_bytes = dataframe_to_excel_bytes(export_df, sheet_name="procurement_view")
    e_col1, e_col2 = st.columns(2)
    with e_col1:
        st.download_button(
            "Download procurement table (CSV)",
            data=csv_bytes,
            file_name=f"{key_prefix}_procurement.csv",
            mime="text/csv",
            key=f"{key_prefix}_proc_csv",
        )
    with e_col2:
        st.download_button(
            "Download procurement table (XLSX)",
            data=excel_bytes,
            file_name=f"{key_prefix}_procurement.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_proc_xlsx",
        )


def build_report_markdown(
    query_params: dict[str, Any],
    insights: list[str],
    no_data_hps: list[str],
    cap_events: list[dict[str, Any]],
) -> str:
    lines = [
        "# BudgetKey Bulk Management Report",
        "",
        "## Query Parameters Used",
    ]
    for key, value in query_params.items():
        lines.append(f"- **{key}**: {value}")

    lines.append("")
    lines.append("## Main Insights")
    for index, insight in enumerate(insights, start=1):
        lines.append(f"{index}. {insight}")

    lines.append("")
    lines.append("## HPs With No Data")
    if no_data_hps:
        for hp in no_data_hps:
            lines.append(f"- {hp}")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Cap Warnings")
    if cap_events:
        for event in cap_events:
            lines.append(
                f"- HP {event['hp']} / {event['doc_type']} hit cap {event['row_cap']} "
                f"(fetched_raw_rows={event['fetched_raw_rows']})."
            )
    else:
        lines.append("- No cap warnings.")

    return "\n".join(lines)


def run_bulk_pull(
    normalized_hp_df: pd.DataFrame,
    selected_doc_types: list[str],
    years: list[int],
    publisher_filter: str,
    keyword: str,
    page_size: int,
    row_cap: int,
    max_workers: int,
    debug_requests: bool,
) -> dict[str, Any]:
    progress = st.progress(0.0, text="Starting bulk pull...")
    status_placeholder = st.empty()
    first_hp_value = (
        str(normalized_hp_df.iloc[0]["hp"]) if not normalized_hp_df.empty else ""
    )
    first_hp_debug_pages: list[dict[str, Any]] = []

    status_rows: list[dict[str, Any]] = []
    cap_events: list[dict[str, Any]] = []
    records_map: dict[str, list[dict[str, Any]]] = {
        "contract-spending": [],
        "supports": [],
        "entities": [],
    }

    future_map: dict[Any, tuple[str, str]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for _, row in normalized_hp_df.iterrows():
            hp = str(row["hp"])
            company_name = str(row.get("company_name", ""))
            future = executor.submit(
                process_single_hp,
                hp=hp,
                company_name=company_name,
                selected_doc_types=selected_doc_types,
                years=years,
                publisher_filter=publisher_filter,
                keyword=keyword,
                page_size=page_size,
                row_cap=row_cap,
                debug_enabled=debug_requests,
            )
            future_map[future] = (hp, company_name)

        total_hps = len(future_map)
        completed = 0
        for future in as_completed(future_map):
            hp, company_name = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "hp": hp,
                    "company_name": company_name,
                    "overall_status": "error",
                    "doc_results": {
                        doc_type: {
                            "status": "error",
                            "records": [],
                            "error": f"Unexpected worker error: {exc}",
                            "capped": False,
                            "fetched_raw_rows": 0,
                            "total_available": 0,
                            "debug_pages": [],
                            "warning": "",
                            "fetched_candidates_rows": 0,
                            "local_matched_rows": 0,
                            "unverified_rows": 0,
                        }
                        for doc_type in selected_doc_types
                    },
                }

            row_status = {
                "hp": result.get("hp", hp),
                "company_name": result.get("company_name", company_name),
                "overall_status": result.get("overall_status", "error"),
            }

            for doc_type in selected_doc_types:
                doc_result = result.get("doc_results", {}).get(doc_type, {})
                doc_records = doc_result.get("records", [])
                records_map[doc_type].extend(doc_records)

                row_status[f"{doc_type}_status"] = doc_result.get("status", "error")
                row_status[f"{doc_type}_rows"] = len(doc_records)
                row_status[f"{doc_type}_error"] = doc_result.get("error", "")
                row_status[f"{doc_type}_warning"] = doc_result.get("warning", "")
                row_status[f"{doc_type}_candidates"] = doc_result.get(
                    "fetched_candidates_rows", 0
                )
                row_status[f"{doc_type}_matched"] = doc_result.get(
                    "local_matched_rows", 0
                )
                row_status[f"{doc_type}_capped"] = bool(doc_result.get("capped", False))
                if debug_requests and result.get("hp", hp) == first_hp_value:
                    first_hp_debug_pages.extend(doc_result.get("debug_pages", []))

                if doc_result.get("capped", False):
                    cap_events.append(
                        {
                            "hp": result.get("hp", hp),
                            "doc_type": doc_type,
                            "row_cap": row_cap,
                            "fetched_raw_rows": doc_result.get("fetched_raw_rows", 0),
                        }
                    )

            status_rows.append(row_status)
            completed += 1
            progress.progress(
                completed / total_hps,
                text=f"Processed {completed}/{total_hps} HP values",
            )

            status_df_partial = pd.DataFrame(status_rows).sort_values("hp")
            status_placeholder.dataframe(status_df_partial, use_container_width=True)

    status_df = pd.DataFrame(status_rows).sort_values("hp") if status_rows else pd.DataFrame()

    contract_df = ensure_output_columns(
        records_to_dataframe_bulk(records_map.get("contract-spending", []))
    )
    supports_df = ensure_output_columns(
        records_to_dataframe_bulk(records_map.get("supports", []))
    )
    entities_df = ensure_output_columns(records_to_dataframe_bulk(records_map.get("entities", [])))
    master_df = build_master_joined_table(contract_df, supports_df, entities_df)

    summary = compute_management_summary(
        contract_df=contract_df,
        supports_df=supports_df,
        entities_df=entities_df,
        status_df=status_df,
        selected_doc_types=selected_doc_types,
    )

    chart_figures = create_management_charts(contract_df, supports_df)
    chart_png_bytes = {
        f"{name}.png": figure_to_png_bytes(fig) for name, fig in chart_figures.items()
    }
    for fig in chart_figures.values():
        plt.close(fig)

    query_params = {
        "selected_doc_types": ", ".join(selected_doc_types),
        "years": ", ".join(str(year) for year in years) if years else "all",
        "publisher_filter": publisher_filter or "None",
        "keyword": keyword or "None",
        "page_size": str(page_size),
        "row_cap_per_doc_type_per_hp": str(row_cap),
        "max_workers": str(max_workers),
        "input_hp_count": str(len(normalized_hp_df)),
        "run_date": str(date.today()),
    }

    report_md = build_report_markdown(
        query_params=query_params,
        insights=summary.get("insights", []),
        no_data_hps=summary.get("no_data_hps", []),
        cap_events=cap_events,
    )

    raw_csv_map = {
        "contract_spending_raw.csv": dataframe_to_csv_bytes(contract_df),
        "supports_raw.csv": dataframe_to_csv_bytes(supports_df),
    }
    raw_xlsx_map = {
        "contract_spending_raw.xlsx": dataframe_to_excel_bytes(
            contract_df, sheet_name="contract_spending_raw"
        ),
        "supports_raw.xlsx": dataframe_to_excel_bytes(
            supports_df, sheet_name="supports_raw"
        ),
    }
    if "entities" in selected_doc_types:
        raw_csv_map["entities_lookup.csv"] = dataframe_to_csv_bytes(entities_df)
        raw_xlsx_map["entities_lookup.xlsx"] = dataframe_to_excel_bytes(
            entities_df, sheet_name="entities_lookup"
        )

    combined_xlsx_bytes = build_combined_report_xlsx(
        executive_summary_df=summary["executive_summary_df"],
        contract_df=contract_df,
        supports_df=supports_df,
        entities_df=entities_df,
        insights=summary["insights"],
        query_params={k: str(v) for k, v in query_params.items()},
        include_entities_sheet=("entities" in selected_doc_types),
        chart_file_names=sorted(chart_png_bytes.keys()),
    )

    report_bundle_bytes = build_report_bundle_zip(
        report_xlsx_bytes=combined_xlsx_bytes,
        raw_csv_bytes=raw_csv_map,
        raw_xlsx_bytes=raw_xlsx_map,
        chart_png_bytes=chart_png_bytes,
        report_md=report_md,
    )

    publisher_suggestions = publisher_suggestions_from_results(contract_df, top_n=20)

    return {
        "status_df": status_df,
        "contract_df": contract_df,
        "supports_df": supports_df,
        "entities_df": entities_df,
        "master_df": master_df,
        "summary": summary,
        "query_params": query_params,
        "cap_events": cap_events,
        "chart_png_bytes": chart_png_bytes,
        "raw_csv_map": raw_csv_map,
        "raw_xlsx_map": raw_xlsx_map,
        "combined_xlsx_bytes": combined_xlsx_bytes,
        "report_bundle_bytes": report_bundle_bytes,
        "report_md": report_md,
        "publisher_suggestions": publisher_suggestions,
        "first_hp_debug": {
            "hp": first_hp_value,
            "pages": first_hp_debug_pages,
        },
        "debug_enabled": debug_requests,
    }


def render_bulk_results(result: dict[str, Any]) -> None:
    summary = result["summary"]
    kpis = summary["kpis"]

    st.subheader("Management Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Contract records", f"{kpis['contract_spending_records']:,}")
    col2.metric("Supports records", f"{kpis['supports_records']:,}")
    col3.metric("Entities records", f"{kpis['entities_records']:,}")

    col4, col5, col6 = st.columns(3)
    col4.metric("Contract total amount", human_number(kpis["contract_total_amount"]))
    col5.metric(
        "Supports total amount_total", human_number(kpis["supports_total_amount_total"])
    )
    col6.metric(
        "Unique suppliers / recipients",
        f"{kpis['unique_suppliers']:,} / {kpis['unique_recipients']:,}",
    )

    st.markdown("**Insights (Executive)**")
    for insight in summary["insights"]:
        st.markdown(f"- {insight}")

    no_data_hps = summary.get("no_data_hps", [])
    st.markdown("**HPs with no data**")
    if no_data_hps:
        st.write(", ".join(no_data_hps))
    else:
        st.write("None")

    if result["cap_events"]:
        st.warning(
            "Some HP/doc-type pulls hit the configured row cap. See details below."
        )
        st.dataframe(pd.DataFrame(result["cap_events"]), use_container_width=True)

    st.markdown("### Charts")
    st.image(
        result["chart_png_bytes"]["top_publishers_by_amount.png"],
        caption="Top 10 publishers by amount",
        use_container_width=True,
    )
    st.image(
        result["chart_png_bytes"]["top_suppliers_by_amount.png"],
        caption="Top 10 suppliers by amount",
        use_container_width=True,
    )
    st.image(
        result["chart_png_bytes"]["top_purposes_by_count.png"],
        caption="Top 10 purposes by count",
        use_container_width=True,
    )
    st.image(
        result["chart_png_bytes"]["amount_time_series.png"],
        caption="Amount over time",
        use_container_width=True,
    )

    st.markdown("### Per-HP Status")
    st.dataframe(result["status_df"], use_container_width=True)

    status_df = result.get("status_df", pd.DataFrame())
    if not status_df.empty and "contract-spending_warning" in status_df.columns:
        contract_warn_df = status_df.loc[
            status_df["contract-spending_warning"].astype(str).str.strip().ne(""),
            [
                col
                for col in [
                    "hp",
                    "company_name",
                    "contract-spending_warning",
                    "contract-spending_candidates",
                    "contract-spending_matched",
                ]
                if col in status_df.columns
            ],
        ]
        if not contract_warn_df.empty:
            st.warning("Some HP values had no contract-spending supplier_code matches.")
            st.dataframe(contract_warn_df, use_container_width=True)

    if result.get("debug_enabled", False):
        first_hp_debug = result.get("first_hp_debug", {})
        with st.expander("Debug panel (first HP requests)", expanded=False):
            st.markdown(
                "Shows exact request URLs, per-page hits, and local matched rows for first uploaded HP."
            )
            st.write(f"First HP: {first_hp_debug.get('hp', '')}")
            debug_pages = first_hp_debug.get("pages", [])
            if debug_pages:
                debug_df = pd.DataFrame(debug_pages)
                debug_cols = [
                    "doc_type",
                    "server_strategy",
                    "q",
                    "filters",
                    "size",
                    "from",
                    "hits_returned",
                    "local_matched_rows",
                    "kept_after_filters",
                    "total_available",
                    "url",
                    "ok",
                    "error",
                ]
                debug_cols = [col for col in debug_cols if col in debug_df.columns]
                st.dataframe(debug_df[debug_cols], use_container_width=True)
            else:
                st.caption("No debug pages captured for the first HP.")

    render_procurement_view(
        contract_df=result["contract_df"],
        key_prefix="bulk_contract_spending",
        is_bulk=True,
    )

    st.markdown("### Combined Results by Doc-Type")
    st.markdown("**contract-spending**")
    st.dataframe(result["contract_df"], use_container_width=True)
    st.markdown("**supports**")
    st.dataframe(result["supports_df"], use_container_width=True)
    st.markdown("**entities lookup**")
    st.dataframe(result["entities_df"], use_container_width=True)

    st.markdown("### Master Joined Table")
    st.dataframe(result["master_df"], use_container_width=True)

    st.markdown("### Downloads")
    raw_col1, raw_col2 = st.columns(2)
    with raw_col1:
        st.download_button(
            "Download contract-spending CSV",
            data=result["raw_csv_map"]["contract_spending_raw.csv"],
            file_name="contract_spending_raw.csv",
            mime="text/csv",
            key="bulk_contract_csv",
        )
        st.download_button(
            "Download supports CSV",
            data=result["raw_csv_map"]["supports_raw.csv"],
            file_name="supports_raw.csv",
            mime="text/csv",
            key="bulk_supports_csv",
        )
        if "entities_lookup.csv" in result["raw_csv_map"]:
            st.download_button(
                "Download entities CSV",
                data=result["raw_csv_map"]["entities_lookup.csv"],
                file_name="entities_lookup.csv",
                mime="text/csv",
                key="bulk_entities_csv",
            )
    with raw_col2:
        st.download_button(
            "Download contract-spending XLSX",
            data=result["raw_xlsx_map"]["contract_spending_raw.xlsx"],
            file_name="contract_spending_raw.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="bulk_contract_xlsx",
        )
        st.download_button(
            "Download supports XLSX",
            data=result["raw_xlsx_map"]["supports_raw.xlsx"],
            file_name="supports_raw.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="bulk_supports_xlsx",
        )
        if "entities_lookup.xlsx" in result["raw_xlsx_map"]:
            st.download_button(
                "Download entities XLSX",
                data=result["raw_xlsx_map"]["entities_lookup.xlsx"],
                file_name="entities_lookup.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="bulk_entities_xlsx",
            )

    st.download_button(
        "Download Combined XLSX Report",
        data=result["combined_xlsx_bytes"],
        file_name="budgetkey_bulk_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="bulk_combined_xlsx",
    )

    st.download_button(
        "Download Report Bundle (ZIP)",
        data=result["report_bundle_bytes"],
        file_name="budgetkey_report_bundle.zip",
        mime="application/zip",
        key="bulk_bundle_zip",
    )


st.set_page_config(page_title="BudgetKey Team App", layout="wide")
st.title("BudgetKey Team App (Streamlit)")
st.write(
    "Search BudgetKey public data by registration number (ח.פ.), use of money, "
    "ministry/publisher, or run bulk Excel extraction for team reporting."
)

with st.sidebar:
    st.header("Mode")
    search_mode = st.radio(
        "Choose mode",
        (
            "1) Search by registration number (ח.פ.)",
            "2) Search by use of money",
            "3) Search by ministry/publisher",
            "4) Bulk Excel (ח.פ.)",
        ),
    )
    st.divider()
    st.header("Single-search pagination")
    size = st.number_input("size", min_value=1, max_value=500, value=50, step=10)
    from_offset = st.number_input("from", min_value=0, max_value=1_000_000, value=0, step=50)
    if st.button("Clear cached API responses"):
        st.cache_data.clear()
        st.success("Cache cleared.")

if search_mode.startswith("1"):
    hp_value = st.text_input("Registration number (ח.פ.)", key="hp_input")
    if st.button("Run ח.פ. search", type="primary"):
        hp = hp_value.strip()
        if not hp:
            st.warning("Please enter a registration number (ח.פ.).")
        else:
            with st.spinner("Running entities + contract-spending + supports searches..."):
                st.session_state["hp_results"] = {
                    "hp": hp,
                    "entities": search_api(
                        doc_type="entities",
                        q=hp,
                        size=int(size),
                        from_offset=int(from_offset),
                    ),
                    "contracts": search_with_filter_fallback(
                        doc_type="contract-spending",
                        size=int(size),
                        from_offset=int(from_offset),
                        filter_field="supplier_code",
                        filter_value=hp,
                        fallback_q=hp,
                    ),
                    "supports": search_with_filter_fallback(
                        doc_type="supports",
                        size=int(size),
                        from_offset=int(from_offset),
                        filter_field="entity_id",
                        filter_value=hp,
                        fallback_q=hp,
                    ),
                }

    hp_results = st.session_state.get("hp_results")
    if hp_results:
        st.info(f"Showing latest ח.פ. search for: {hp_results.get('hp', '')}")
        tab_entities, tab_contracts, tab_supports = st.tabs(
            ["Entities", "Contract Spending", "Supports"]
        )
        with tab_entities:
            render_result_panel("Entities", hp_results["entities"], "hp_entities")
        with tab_contracts:
            render_result_panel(
                "Contract Spending", hp_results["contracts"], "hp_contract_spending"
            )
            contract_records = hp_results["contracts"].get("records", [])
            contract_proc_df = records_to_dataframe_bulk(contract_records)
            render_procurement_view(
                contract_df=contract_proc_df,
                key_prefix="hp_contract_spending",
                is_bulk=False,
            )
        with tab_supports:
            render_result_panel("Supports", hp_results["supports"], "hp_supports")

elif search_mode.startswith("2"):
    purpose_query = st.text_input(
        "Use-of-money query (matches purpose/description)", key="purpose_query_input"
    )
    publisher_filter = st.text_input(
        "Optional publisher (ministry) filter", key="purpose_publisher_filter_input"
    )
    if st.button("Run use-of-money search", type="primary"):
        q = purpose_query.strip()
        publisher = publisher_filter.strip()
        if not q:
            st.warning("Please enter a use-of-money query.")
        else:
            with st.spinner("Running contract-spending search..."):
                if publisher:
                    fallback_q = f"{q} {publisher}".strip()
                    result = search_with_filter_fallback(
                        doc_type="contract-spending",
                        size=int(size),
                        from_offset=int(from_offset),
                        filter_field="publisher",
                        filter_value=publisher,
                        fallback_q=fallback_q,
                        base_q=q,
                    )
                else:
                    result = search_api(
                        doc_type="contract-spending",
                        q=q,
                        size=int(size),
                        from_offset=int(from_offset),
                    )
                    result["strategy"] = "q only"

                st.session_state["purpose_results"] = {
                    "q": q,
                    "publisher": publisher,
                    "result": result,
                }

    purpose_results = st.session_state.get("purpose_results")
    if purpose_results:
        st.info(
            "Showing latest use-of-money search: "
            f"q='{purpose_results.get('q', '')}', "
            f"publisher='{purpose_results.get('publisher', '') or 'None'}'"
        )
        render_result_panel(
            "Contract Spending Results",
            purpose_results["result"],
            "purpose_contract_spending",
        )
        suggestions_df = publisher_suggestions_single(
            records_to_analysis_df(purpose_results["result"].get("records", []))[0]
        )
        st.markdown("**Publisher suggestions from current results**")
        if suggestions_df.empty:
            st.caption("No publisher suggestions available for this page.")
        else:
            st.dataframe(suggestions_df, use_container_width=True)

elif search_mode.startswith("3"):
    ministry_query = st.text_input("Ministry / publisher", key="ministry_query_input")
    if st.button("Run ministry/publisher search", type="primary"):
        publisher = ministry_query.strip()
        if not publisher:
            st.warning("Please enter a ministry/publisher.")
        else:
            with st.spinner("Running contract-spending publisher search..."):
                result = search_with_filter_fallback(
                    doc_type="contract-spending",
                    size=int(size),
                    from_offset=int(from_offset),
                    filter_field="publisher",
                    filter_value=publisher,
                    fallback_q=publisher,
                )
                st.session_state["ministry_results"] = {
                    "publisher": publisher,
                    "result": result,
                }

    ministry_results = st.session_state.get("ministry_results")
    if ministry_results:
        st.info(
            "Showing latest ministry/publisher search for: "
            f"{ministry_results.get('publisher', '')}"
        )
        render_result_panel(
            "Contract Spending Results",
            ministry_results["result"],
            "ministry_contract_spending",
        )
        suggestions_df = publisher_suggestions_single(
            records_to_analysis_df(ministry_results["result"].get("records", []))[0]
        )
        st.markdown("**Publisher suggestions from current results**")
        if suggestions_df.empty:
            st.caption("No publisher suggestions available for this page.")
        else:
            st.dataframe(suggestions_df, use_container_width=True)

else:
    st.subheader("Bulk Excel (ח.פ.)")
    st.write(
        "Upload an Excel file with HP column and pull contract/support/entity data in bulk "
        "with pagination, concurrency, and full report exports."
    )
    uploaded_file = st.file_uploader(
        "Upload Excel (.xlsx)",
        type=["xlsx"],
        help=(
            "Required HP column auto-detected from: ח.פ / חפ / HP / registration / company_id. "
            "Optional company name column: חברה / company_name / name."
        ),
    )

    selected_doc_types: list[str] = []
    st.markdown("**Doc-types to fetch**")
    contract_checked = st.checkbox("contract-spending", value=True)
    supports_checked = st.checkbox("supports", value=True)
    entities_checked = st.checkbox("entities (lookup)", value=False)
    if contract_checked:
        selected_doc_types.append("contract-spending")
    if supports_checked:
        selected_doc_types.append("supports")
    if entities_checked:
        selected_doc_types.append("entities")

    year_options = list(range(2018, 2027))
    years_selected = st.multiselect(
        "Years filter",
        options=year_options,
        default=[2021, 2022, 2023],
        help=(
            "contract-spending filtered by order_date year; "
            "supports filtered by year_requested."
        ),
    )

    publisher_suggestions = st.session_state.get("bulk_publisher_suggestions", [])
    suggested_publisher = st.selectbox(
        "Publisher suggestion (optional)",
        options=[""] + publisher_suggestions,
        index=0,
    )
    publisher_text = st.text_input(
        "Ministry/Publisher filter (optional)",
        help="Best-effort server filter via filters JSON; fallback behavior still applied.",
    )
    publisher_filter = publisher_text.strip() or suggested_publisher.strip()

    keyword = st.text_input(
        "Use-of-money keyword (optional)",
        help=(
            "contract-spending keyword against purpose/description; "
            "supports keyword against recipient/program text."
        ),
    ).strip()

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        page_size = st.number_input(
            "Page size per request",
            min_value=10,
            max_value=500,
            value=100,
            step=10,
        )
    with col_b:
        row_cap = st.number_input(
            "Row cap per doc-type per HP",
            min_value=100,
            max_value=10000,
            value=5000,
            step=100,
        )
    with col_c:
        max_workers = st.number_input(
            "Thread workers",
            min_value=1,
            max_value=20,
            value=5,
            step=1,
        )
    debug_requests = st.checkbox(
        "Debug requests (first HP)",
        value=False,
        help=(
            "When enabled, shows first HP request params, per-page hits, and local matched counts."
        ),
    )

    normalized_hp_df = pd.DataFrame()
    if uploaded_file is not None:
        try:
            uploaded_df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False)
            normalized_hp_df, hp_col_name, name_col_name = normalize_uploaded_dataframe(uploaded_df)
            st.success(
                "Detected HP column: "
                f"'{hp_col_name}'"
                + (f", company name column: '{name_col_name}'." if name_col_name else ".")
            )
            st.caption(f"Normalized HP rows: {len(normalized_hp_df):,}")
            st.dataframe(normalized_hp_df.head(50), use_container_width=True)
        except Exception as exc:
            st.error(f"Failed to parse uploaded Excel: {exc}")

    if st.button("Run Bulk Pull", type="primary"):
        if uploaded_file is None:
            st.warning("Please upload an Excel file first.")
        elif normalized_hp_df.empty:
            st.warning("No valid HP rows found after normalization.")
        elif not selected_doc_types:
            st.warning("Please select at least one doc-type.")
        else:
            with st.spinner("Running bulk extraction... this may take a while for many HPs."):
                bulk_result = run_bulk_pull(
                    normalized_hp_df=normalized_hp_df,
                    selected_doc_types=selected_doc_types,
                    years=sorted(int(year) for year in years_selected),
                    publisher_filter=publisher_filter,
                    keyword=keyword,
                    page_size=int(page_size),
                    row_cap=int(row_cap),
                    max_workers=int(max_workers),
                    debug_requests=debug_requests,
                )
            st.session_state["bulk_result"] = bulk_result
            st.session_state["bulk_publisher_suggestions"] = bulk_result["publisher_suggestions"]
            st.success("Bulk pull completed.")

    bulk_result = st.session_state.get("bulk_result")
    if bulk_result:
        render_bulk_results(bulk_result)
