# BudgetKey Team Streamlit App

Team Streamlit app for exploring and extracting public BudgetKey data.

Main modes:

1. Search by Israeli registration number (ח.פ.)
2. Search by "use of money" (purpose/description)
3. Search by ministry/publisher
4. **Bulk Excel (ח.פ.)** for multi-company extraction + management report bundle

API used:

- BudgetKey Search API: `https://next.obudget.org/search/<doc-type>`
- Query parameters used by the app: `q`, `size`, `from`, `filters` (JSON)
- API docs: `documentation/UsingTheAPI.md` in the BudgetKey repo

---

## Repository structure

- `app.py` - Streamlit UI and orchestration
- `tools/budgetkey_search.py` - cached Search API paging + per-HP doc fetch
- `tools/bulk.py` - Excel normalization and per-HP worker function
- `tools/analysis.py` - KPI/insight computation and summary tables
- `tools/plots.py` - management chart generation
- `tools/export.py` - CSV/XLSX/ZIP/report exports
- `sample_input/sample_bulk_hp.xlsx` - sample bulk input file (3 fake HPs)
- `requirements.txt`
- `README.md`

---

## Run locally

1. Clone and enter repo:

   ```bash
   git clone <your-repo-url>
   cd <your-repo-folder>
   ```

2. (Recommended) Create virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run app:

   ```bash
   streamlit run app.py
   ```

5. Open URL shown in terminal (usually `http://localhost:8501`).

---

## Deploy to Streamlit Community Cloud (shareable link)

1. Push repository to GitHub.
2. Open [https://share.streamlit.io](https://share.streamlit.io) and sign in.
3. Click **New app**.
4. Select repository + branch.
5. Set main file path to `app.py`.
6. Click **Deploy**.
7. Use the generated public URL as your team shareable link.

No secrets are required (public API only).

---

## Bulk Excel mode

Open sidebar mode: **`4) Bulk Excel (ח.פ.)`**

### Expected Excel format

Upload `.xlsx` with:

- Required HP column (auto-detected from one of):
  - `ח.פ`
  - `חפ`
  - `HP`
  - `registration`
  - `company_id`
- Optional company-name column (auto-detected from one of):
  - `חברה`
  - `company_name`
  - `name`

The app normalizes HP to digits-only and drops empty HP rows.

### Example filters

- Doc-types:
  - `contract-spending`
  - `supports`
  - `entities` (optional lookup)
- Years:
  - e.g. `2021, 2022, 2023`
  - `contract-spending` filtered by `order_date` year
  - `supports` filtered by `year_requested`
- Ministry/publisher filter:
  - free text
  - optional dropdown suggestions from prior run results
- Use-of-money keyword:
  - `contract-spending`: purpose/description text
  - `supports`: recipient/program text

### Outputs generated

After **Run Bulk Pull**:

- Progress bar + per-HP status table (`ok` / `empty` / `error`)
- Combined raw tables per doc-type
- Master joined table (`hp`, `company_name`, `doc_type`, `query_params_used`)
- Management summary:
  - KPI totals
  - top publishers/suppliers/purposes
  - no-data HP list
  - anomaly highlights
- Charts:
  - top publishers by amount
  - top suppliers by amount
  - top purposes by count
  - amount over time

Downloads:

- Raw CSV + XLSX (contract-spending / supports; entities when selected)
- Combined XLSX report with sheets:
  - `Executive_Summary`
  - `contract_spending_raw`
  - `supports_raw`
  - `entities_lookup` (if selected)
- **Download Report Bundle (ZIP)** containing:
  - combined report XLSX
  - raw CSVs
  - raw XLSX files
  - chart PNGs
  - `report.md` with query params + insights

### Troubleshooting

- **No data for some HPs**: expected in many cases; see `no_data_hps` in summary.
- **Cap warning shown**: row cap reached (`default 2000 rows/doc-type/HP`); increase cap if needed.
- **Some HP statuses are error**: temporary API/network issue; rerun often resolves partial failures.
- **Publisher filter seems broad**: API filter support is best-effort; app also applies local post-filtering.

---

## Single-search examples

- **ח.פ. search**
  - `513819245`
  - `590000931`
- **Use-of-money**
  - Query: `מחשוב`
  - Optional publisher: `משרד הבריאות`
- **Ministry/publisher**
  - `משרד החינוך`
  - `משרד התחבורה`
