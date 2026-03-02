# BudgetKey Team Streamlit App

Streamlit web app for exploring public BudgetKey data:

- Search by Israeli registration number (ח.פ.)
- Search by "use of money" (purpose/description)
- Search by ministry/publisher

The app uses the public BudgetKey Search API (`https://next.obudget.org/search/...`), includes pagination (`size` / `from`), table exports (CSV/XLSX), chart export (PNG in ZIP), and cached API calls (`st.cache_data`).

---

## Features

### 1) Search by registration number (ח.פ.)

- Queries:
  - `entities` (with `q=<hp>`)
  - `contract-spending` with `supplier_code=<hp>` filter attempt
  - `supports` with `entity_id=<hp>` filter attempt
- If filters do not apply, automatically falls back to `q=<hp>`.
- Results shown in 3 tabs:
  - **Entities**
  - **Contract Spending**
  - **Supports**

### 2) Search by use of money

- Queries `contract-spending` with `q` against purpose/description.
- Optional publisher/ministry filter attempt.
- If filter does not apply, falls back to combined free text query.

### 3) Search by ministry/publisher

- Queries `contract-spending` with publisher filter attempt.
- If filter does not apply, falls back to `q=<publisher>`.
- Shows publisher suggestions from current results.

### After each search

- Data table (`pandas.DataFrame`)
- 3-6 insights bullets
- Matplotlib charts:
  1. Top 10 publishers by executed (fallback: volume)
  2. Top 10 suppliers by executed/volume
  3. Monthly time series when `order_date` exists
- Export buttons:
  - Download CSV
  - Download Excel (XLSX)
  - Download PNG charts (ZIP)

---

## Files

- `app.py` - Streamlit app
- `requirements.txt` - Python dependencies
- `README.md` - Setup/deploy instructions

---

## Run locally

1. Clone the repo and enter it:

   ```bash
   git clone <your-repo-url>
   cd <your-repo-folder>
   ```

2. (Recommended) Create and activate a virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run Streamlit:

   ```bash
   streamlit run app.py
   ```

5. Open the local URL shown in terminal (usually `http://localhost:8501`).

---

## Deploy to Streamlit Community Cloud (shareable link)

1. Push this repo to GitHub.
2. Go to [https://share.streamlit.io](https://share.streamlit.io) and sign in.
3. Click **New app**.
4. Select:
   - Your repository
   - Branch
   - Main file path: `app.py`
5. Click **Deploy**.
6. After deployment completes, Streamlit provides a public, shareable app URL.

No secrets are required for this app (public API).

---

## Example searches

Use these examples after app launch:

- **ח.פ. search**
  - `513819245`
  - `590000931`

- **Use-of-money search**
  - Query: `מחשוב`
  - Optional publisher: `משרד הבריאות`

- **Ministry/publisher search**
  - `משרד החינוך`
  - `משרד התחבורה`

---

## Notes

- API responses are cached for 10 minutes via `st.cache_data`.
- Pagination controls are in the sidebar:
  - `size` = number of rows to fetch
  - `from` = starting offset
- The app handles:
  - empty results
  - missing fields
  - API/network errors
