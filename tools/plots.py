import io

import matplotlib.pyplot as plt
import pandas as pd

from tools.analysis import contract_amount_series, supports_amount_series


def _placeholder_chart(title: str, message: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4.8))
    ax.axis("off")
    ax.set_title(title)
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=11)
    fig.tight_layout()
    return fig


def _top_publishers_chart(contract_df: pd.DataFrame) -> plt.Figure:
    title = "Top 10 Publishers by Amount"
    if contract_df.empty or "publisher" not in contract_df.columns:
        return _placeholder_chart(title, "No publisher data available.")

    amounts = contract_amount_series(contract_df)
    work = pd.DataFrame({"publisher": contract_df["publisher"], "amount": amounts})
    work["publisher"] = work["publisher"].astype(str).str.strip()
    work = work.loc[work["publisher"].ne("") & work["amount"].notna()]
    if work.empty:
        return _placeholder_chart(title, "No publisher amount data available.")

    top = (
        work.groupby("publisher")["amount"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .sort_values(ascending=True)
    )
    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.barh(top.index, top.values)
    ax.set_title(title)
    ax.set_xlabel("amount")
    ax.set_ylabel("publisher")
    fig.tight_layout()
    return fig


def _top_suppliers_chart(contract_df: pd.DataFrame) -> plt.Figure:
    title = "Top 10 Suppliers by Amount"
    if contract_df.empty or "supplier_name" not in contract_df.columns:
        return _placeholder_chart(title, "No supplier data available.")

    amounts = contract_amount_series(contract_df)
    work = pd.DataFrame({"supplier_name": contract_df["supplier_name"], "amount": amounts})
    work["supplier_name"] = work["supplier_name"].astype(str).str.strip()
    work = work.loc[work["supplier_name"].ne("") & work["amount"].notna()]
    if work.empty:
        return _placeholder_chart(title, "No supplier amount data available.")

    top = (
        work.groupby("supplier_name")["amount"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .sort_values(ascending=True)
    )
    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.barh(top.index, top.values)
    ax.set_title(title)
    ax.set_xlabel("amount")
    ax.set_ylabel("supplier")
    fig.tight_layout()
    return fig


def _top_purposes_chart(contract_df: pd.DataFrame) -> plt.Figure:
    title = "Top 10 Purposes by Count"
    if contract_df.empty:
        return _placeholder_chart(title, "No contract rows available.")

    purpose_col = None
    for candidate in ("purpose", "description", "page_title"):
        if candidate in contract_df.columns:
            purpose_col = candidate
            break
    if not purpose_col:
        return _placeholder_chart(title, "No purpose/description fields available.")

    values = (
        contract_df[purpose_col]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
    )
    if values.empty:
        return _placeholder_chart(title, "Purpose values are empty after filtering.")

    top = values.value_counts().head(10).sort_values(ascending=True)
    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.barh(top.index, top.values)
    ax.set_title(title)
    ax.set_xlabel("count")
    ax.set_ylabel("purpose")
    fig.tight_layout()
    return fig


def _time_series_chart(contract_df: pd.DataFrame, supports_df: pd.DataFrame) -> plt.Figure:
    title = "Amount Over Time (Month/Year)"
    has_contract_dates = not contract_df.empty and "order_date" in contract_df.columns
    has_support_year = not supports_df.empty and "year_requested" in supports_df.columns

    if not has_contract_dates and not has_support_year:
        return _placeholder_chart(title, "No date fields available for time series.")

    fig, ax = plt.subplots(figsize=(10, 4.8))
    plotted = False

    if has_contract_dates:
        contract_amounts = contract_amount_series(contract_df)
        contract_dates = pd.to_datetime(contract_df["order_date"], errors="coerce")
        contract_work = pd.DataFrame({"date": contract_dates, "amount": contract_amounts})
        contract_work = contract_work.loc[
            contract_work["date"].notna() & contract_work["amount"].notna()
        ]
        if not contract_work.empty:
            contract_work["month"] = (
                contract_work["date"].dt.to_period("M").dt.to_timestamp()
            )
            monthly = contract_work.groupby("month")["amount"].sum().sort_index()
            ax.plot(
                monthly.index,
                monthly.values,
                marker="o",
                linewidth=1.8,
                label="contract-spending",
            )
            plotted = True

    if has_support_year:
        support_amounts = supports_amount_series(supports_df)
        support_years = pd.to_numeric(supports_df["year_requested"], errors="coerce")
        support_work = pd.DataFrame({"year": support_years, "amount": support_amounts})
        support_work = support_work.loc[
            support_work["year"].notna() & support_work["amount"].notna()
        ]
        if not support_work.empty:
            support_work["date"] = pd.to_datetime(
                support_work["year"].astype(int).astype(str) + "-01-01"
            )
            yearly = support_work.groupby("date")["amount"].sum().sort_index()
            ax.plot(
                yearly.index,
                yearly.values,
                marker="s",
                linewidth=1.8,
                label="supports",
            )
            plotted = True

    if not plotted:
        plt.close(fig)
        return _placeholder_chart(title, "No valid date + amount combinations found.")

    ax.set_title(title)
    ax.set_xlabel("date")
    ax.set_ylabel("amount")
    ax.tick_params(axis="x", rotation=45)
    ax.legend()
    fig.tight_layout()
    return fig


def create_management_charts(
    contract_df: pd.DataFrame, supports_df: pd.DataFrame
) -> dict[str, plt.Figure]:
    return {
        "top_publishers_by_amount": _top_publishers_chart(contract_df),
        "top_suppliers_by_amount": _top_suppliers_chart(contract_df),
        "top_purposes_by_count": _top_purposes_chart(contract_df),
        "amount_time_series": _time_series_chart(contract_df, supports_df),
    }


def figure_to_png_bytes(fig: plt.Figure) -> bytes:
    output = io.BytesIO()
    fig.savefig(output, format="png", dpi=150, bbox_inches="tight")
    output.seek(0)
    return output.getvalue()
