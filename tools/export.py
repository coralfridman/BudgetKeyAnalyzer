import io
import zipfile

import pandas as pd


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "results") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    output.seek(0)
    return output.getvalue()


def build_combined_report_xlsx(
    executive_summary_df: pd.DataFrame,
    contract_df: pd.DataFrame,
    supports_df: pd.DataFrame,
    entities_df: pd.DataFrame,
    insights: list[str],
    query_params: dict[str, str],
    include_entities_sheet: bool,
    chart_file_names: list[str],
) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        executive_summary_df.to_excel(
            writer, index=False, sheet_name="Executive_Summary", startrow=0
        )
        startrow = len(executive_summary_df) + 2

        insights_df = pd.DataFrame(
            {"insight_no": list(range(1, len(insights) + 1)), "insight": insights}
        )
        insights_df.to_excel(
            writer,
            index=False,
            sheet_name="Executive_Summary",
            startrow=startrow,
        )

        params_df = pd.DataFrame(
            [{"query_parameter": key, "value": value} for key, value in query_params.items()]
        )
        params_df.to_excel(
            writer,
            index=False,
            sheet_name="Executive_Summary",
            startrow=startrow + len(insights_df) + 3,
        )

        charts_df = pd.DataFrame({"chart_file": chart_file_names})
        charts_df.to_excel(
            writer,
            index=False,
            sheet_name="Executive_Summary",
            startrow=startrow + len(insights_df) + len(params_df) + 6,
        )

        contract_df.to_excel(writer, index=False, sheet_name="contract_spending_raw")
        supports_df.to_excel(writer, index=False, sheet_name="supports_raw")
        if include_entities_sheet:
            entities_df.to_excel(writer, index=False, sheet_name="entities_lookup")

    output.seek(0)
    return output.getvalue()


def build_report_bundle_zip(
    report_xlsx_bytes: bytes,
    raw_csv_bytes: dict[str, bytes],
    raw_xlsx_bytes: dict[str, bytes],
    chart_png_bytes: dict[str, bytes],
    report_md: str,
) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("management_report.xlsx", report_xlsx_bytes)
        for file_name, csv_bytes in raw_csv_bytes.items():
            archive.writestr(file_name, csv_bytes)
        for file_name, xlsx_bytes in raw_xlsx_bytes.items():
            archive.writestr(file_name, xlsx_bytes)
        for file_name, png_bytes in chart_png_bytes.items():
            archive.writestr(file_name, png_bytes)
        archive.writestr("report.md", report_md.encode("utf-8"))
    output.seek(0)
    return output.getvalue()
