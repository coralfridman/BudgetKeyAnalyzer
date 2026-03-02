import io
import json
import zipfile
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st

API_BASE_URL = "https://next.obudget.org/search"
REQUEST_TIMEOUT_SECONDS = 30


def _first_scalar(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _to_text(value: Any) -> str:
    value = _first_scalar(value)
    if value is None:
        return ""
    return str(value).strip()


def _to_number(value: Any) -> float | None:
    value = _first_scalar(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("₪", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _digits_only(value: Any) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.lstrip("0")


def _value_matches_filter(value: Any, expected: str, field_name: str) -> bool:
    if isinstance(value, list):
        return any(_value_matches_filter(item, expected, field_name) for item in value)
    if value is None:
        return False
    if field_name in {"supplier_code", "entity_id"}:
        return _digits_only(value) == _digits_only(expected)
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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _human_number(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return "N/A"


def _stringify_for_table(value: Any) -> Any:
    if isinstance(value, list):
        scalar_items = [str(item) for item in value if item is not None]
        return " | ".join(scalar_items)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value


@st.cache_data(ttl=600, show_spinner=False)
def search_api(
    doc_type: str,
    q: str,
    size: int,
    from_offset: int,
    filters_json: str = "",
) -> dict[str, Any]:
    url = f"{API_BASE_URL}/{doc_type}"
    params: dict[str, Any] = {"size": size, "from": from_offset}
    if q:
        params["q"] = q
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
            "records": records,
            "total": _safe_int(total, len(records)),
            "error": "",
            "url": response.url,
        }
    except requests.RequestException as exc:
        return {
            "ok": False,
            "records": [],
            "total": 0,
            "error": f"API request failed: {exc}",
            "url": url,
        }
    except ValueError as exc:
        return {
            "ok": False,
            "records": [],
            "total": 0,
            "error": f"Invalid JSON response from API: {exc}",
            "url": url,
        }


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
            df[column] = df[column].map(_stringify_for_table)
    return df


def records_to_analysis_df(records: list[dict[str, Any]]) -> tuple[pd.DataFrame, str | None]:
    rows: list[dict[str, Any]] = []
    for record in records:
        publisher = _to_text(record.get("publisher"))
        supplier = _to_text(
            record.get("supplier_name")
            or record.get("recipient")
            or record.get("entity_name")
            or record.get("name")
        )
        rows.append(
            {
                "publisher": publisher,
                "supplier": supplier,
                "executed": _to_number(record.get("executed")),
                "volume": _to_number(record.get("volume")),
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
            f"Total {metric_col} in current page: {_human_number(total_metric)}."
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
                f"Top publisher by {metric_col}: {top_publisher} ({_human_number(top_value)})."
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


def _placeholder_chart(title: str, message: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.axis("off")
    ax.set_title(title)
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=11)
    fig.tight_layout()
    return fig


def create_charts(analysis_df: pd.DataFrame, metric_col: str | None) -> dict[str, plt.Figure]:
    charts: dict[str, plt.Figure] = {}

    # Chart A: Top 10 publishers by executed (fallback volume)
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
        charts["publishers"] = _placeholder_chart(
            publisher_title, "No publisher data available for this result set."
        )

    # Chart B: Top 10 suppliers by executed/volume
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
        charts["suppliers"] = _placeholder_chart(
            supplier_title, "No supplier/recipient data available for this result set."
        )

    # Chart C: Monthly time series if order_date exists
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
        charts["monthly"] = _placeholder_chart(
            monthly_title, "No valid order_date values available for this result set."
        )

    return charts


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="results")
    output.seek(0)
    return output.getvalue()


def figure_to_png_bytes(fig: plt.Figure) -> bytes:
    output = io.BytesIO()
    fig.savefig(output, format="png", dpi=150, bbox_inches="tight")
    output.seek(0)
    return output.getvalue()


def charts_to_zip_bytes(charts: dict[str, plt.Figure], prefix: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, fig in charts.items():
            archive.writestr(f"{prefix}_{name}.png", figure_to_png_bytes(fig))
    output.seek(0)
    return output.getvalue()


def publisher_suggestions(analysis_df: pd.DataFrame) -> pd.DataFrame:
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
    total = _safe_int(result.get("total"), 0)
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
    excel_bytes = dataframe_to_excel_bytes(display_df)
    charts_zip_bytes = charts_to_zip_bytes(charts, prefix=download_key_prefix)

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
            mime=(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
            key=f"{download_key_prefix}_xlsx",
        )
    with col3:
        st.download_button(
            "Download PNG charts (ZIP)",
            data=charts_zip_bytes,
            file_name=f"{download_key_prefix}_charts.zip",
            mime="application/zip",
            key=f"{download_key_prefix}_png",
        )

    st.markdown("**Charts**")
    st.pyplot(charts["publishers"])
    st.pyplot(charts["suppliers"])
    st.pyplot(charts["monthly"])

    for fig in charts.values():
        plt.close(fig)


st.set_page_config(page_title="BudgetKey Team App", layout="wide")
st.title("BudgetKey Team App (Streamlit)")
st.write(
    "Search BudgetKey public data by registration number (ח.פ.), use of money, "
    "or ministry/publisher."
)

with st.sidebar:
    st.header("Pagination")
    size = st.number_input("size", min_value=1, max_value=500, value=50, step=10)
    from_offset = st.number_input("from", min_value=0, max_value=1_000_000, value=0, step=50)
    if st.button("Clear cached API responses"):
        st.cache_data.clear()
        st.success("Cache cleared.")

search_mode = st.radio(
    "Choose search mode",
    (
        "1) Search by registration number (ח.פ.)",
        "2) Search by use of money",
        "3) Search by ministry/publisher",
    ),
)

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
        suggestions_df = publisher_suggestions(
            records_to_analysis_df(purpose_results["result"].get("records", []))[0]
        )
        st.markdown("**Publisher suggestions from current results**")
        if suggestions_df.empty:
            st.caption("No publisher suggestions available for this page.")
        else:
            st.dataframe(suggestions_df, use_container_width=True)

else:
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
        suggestions_df = publisher_suggestions(
            records_to_analysis_df(ministry_results["result"].get("records", []))[0]
        )
        st.markdown("**Publisher suggestions from current results**")
        if suggestions_df.empty:
            st.caption("No publisher suggestions available for this page.")
        else:
            st.dataframe(suggestions_df, use_container_width=True)
