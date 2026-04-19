# La Crosse County, WI — Motivated Seller Lead Scraper

Automated daily scraper that pulls distressed-property leads from La Crosse County, Wisconsin public records, scores them for motivated-seller likelihood, and outputs both a JSON feed for a dashboard and a GoHighLevel-ready CSV for direct contact import.

Built for **Sell My House Fast LLC**.

---

## What it collects

Documents recorded in the last **7 days** (configurable) in these categories:

| Code | Lead Type |
|------|-----------|
| `LP` | Lis Pendens |
| `NOFC` | Notice of Foreclosure |
| `TAXDEED` | Tax Deed |
| `JUD` / `CCJ` / `DRJUD` | Judgment / Certified Judgment / Domestic Judgment |
| `LNIRS` / `LNFED` / `LNCORPTX` | IRS / Federal / Corporate Tax Lien |
| `LN` / `LNMECH` / `LNHOA` | General / Mechanic / HOA Lien |
| `MEDLN` | Medicaid Lien |
| `PRO` | Probate / Estate |
| `NOC` | Notice of Commencement |
| `RELLP` | Release of Lis Pendens |

For each record: **doc number, doc type, filed date, grantor (owner), grantee, legal description, amount, direct URL, property address, mailing address.**

---

## Data sources

| Source | Purpose | Method |
|--------|---------|--------|
| [Register of Deeds Tapestry](https://lacrossecounty.org/registerofdeeds/tapestry) | Document search (LP, NOFC, liens, probate, etc.) | Playwright (async) |
| [LandNav portal](https://apps.lacrossecounty.org/landrecordsportallandnav/landnav.html) | Bulk parcel DBF for owner → address lookup | requests + `__doPostBack` |
| [WCCA](https://wcca.wicourts.gov/) | Circuit court case data | requests (stub — see caveats) |
| [Treasurer](https://www.lacrossecounty.org/treasurer) | Delinquent tax lists | requests + BeautifulSoup |
| [WI DOR sales](https://propertyinfo.revenue.wi.gov/) | Recent sales comps | requests (stub) |

---

## Seller Score (0–100)

```
Base                    30
Each flag              +10
LP + foreclosure combo +20
Amount > $100k         +15
Amount > $50k          +10
New this week           +5
Has address             +5
```

Flags: `Lis pendens`, `Pre-foreclosure`, `Judgment lien`, `Tax lien`, `Mechanic lien`, `Probate / estate`, `LLC / corp owner`, `New this week`.

---

## File structure

```
.
├── .github/workflows/scrape.yml   # Daily cron + Pages deploy
├── dashboard/
│   ├── records.json               # Latest feed (served via Pages)
│   └── ghl_export.csv             # GHL import CSV
├── data/
│   ├── records.json               # Same feed, repo-committed
│   └── ghl_export.csv             # Same CSV
└── scraper/
    ├── fetch.py                   # Main pipeline
    └── requirements.txt
```

---

## Setup

### 1. Create the repo

```bash
git init lacrosse-scraper && cd lacrosse-scraper
# drop these files in, then:
git add .
git commit -m "initial scraper"
git branch -M main
git remote add origin git@github.com:<you>/lacrosse-scraper.git
git push -u origin main
```

### 2. Enable GitHub Pages

In repo **Settings → Pages**, set **Source** to **GitHub Actions**. The workflow publishes `dashboard/` automatically on every run.

### 3. Enable Actions write permissions

**Settings → Actions → General → Workflow permissions** → select **Read and write permissions** (the workflow commits `records.json` back to `main`).

### 4. First run

Go to **Actions → La Crosse County scraper → Run workflow**. After it completes:
- `dashboard/records.json` and `data/records.json` will be updated and committed
- Dashboard is live at `https://<you>.github.io/lacrosse-scraper/`
- CSV is at `https://<you>.github.io/lacrosse-scraper/ghl_export.csv`

Subsequent runs fire daily at **07:00 UTC** (≈ 2am CDT / 1am CST).

---

## Running locally

```bash
cd lacrosse-scraper
python -m venv .venv && source .venv/bin/activate
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium

LOOKBACK_DAYS=7 python scraper/fetch.py
```

For debugging Tapestry selectors, edit `scrape_tapestry()` in `fetch.py` and change `headless=True` to `headless=False` so you can watch the browser.

---

## GHL import

1. Grab `dashboard/ghl_export.csv` (or `data/ghl_export.csv`).
2. In GHL: **Contacts → Import → Upload CSV**.
3. Map the columns — they already use GHL's standard field names:
   - `First Name`, `Last Name`
   - `Mailing Address`, `Mailing City`, `Mailing State`, `Mailing Zip`
   - `Property Address`, `Property City`, `Property State`, `Property Zip`
   - Custom fields: `Lead Type`, `Document Type`, `Date Filed`, `Document Number`, `Amount/Debt Owed`, `Seller Score`, `Motivated Seller Flags`, `Source`, `Public Records URL`
4. Assign to the pipeline stage of your choice — typically **New** in your distressed-property pipeline, since every row here is a freshly-recorded lead.

Pre-create those custom fields in GHL before your first import or GHL will ignore them.

---

## Tuning

| What | Where |
|------|-------|
| Lookback window | `LOOKBACK_DAYS` env var (default 7) |
| Doc-type mapping | `DOC_CATEGORY_MAP` list near the top of `fetch.py` |
| Scoring weights | `score_record()` function |
| Retry count | `MAX_RETRIES` constant (default 3) |
| Cron schedule | `.github/workflows/scrape.yml` — `schedule.cron` |

To shift the cron to 7am CST (which is 13:00 UTC), change `"0 7 * * *"` → `"0 13 * * *"`.

---

## Caveats

**Tapestry and LandNav are third-party vendor UIs** with tenant-customized DOMs. The scraper probes for common input names (`DateFrom`/`FromDate`/etc.) and result-grid header labels (`doc type`, `grantor`, `recorded`), but if either vendor changes their layout you'll need to tighten the selectors. Run with `headless=False` locally to see what's actually in the DOM.

**WCCA and WI DOR** block non-interactive scraping in practice (token-gated forms, CAPTCHAs). The current stubs discover entry-point URLs but don't pull cases. A full WCCA pull needs a dedicated Playwright flow with session handling — build it as a follow-up if it becomes a bottleneck.

**The parcel DBF download requires the LandNav portal to expose a public bulk link.** If the portal requires auth, the scraper logs the issue and continues without address enrichment — records will still be collected, they'll just be missing property/mailing addresses.

**Bad records never crash the run.** Every network call is retry-wrapped (3 attempts with backoff), every parse is wrapped in `safe()`, and if the whole pipeline explodes the outer handler still writes an empty `records.json` so the dashboard never goes blank.

---

## Troubleshooting

**"Playwright: browserType.launch: Executable doesn't exist"**
→ Run `python -m playwright install --with-deps chromium`.

**Workflow fails with "permission denied to push"**
→ Settings → Actions → General → Workflow permissions → **Read and write**.

**Dashboard shows 0 records but logs look OK**
→ Tapestry layout probably changed. Run locally with `headless=False`, watch what happens in the search form, and update the selectors in `_parse_tapestry_results()` + the date-input probe loop in `scrape_tapestry()`.

**DBF load shows 0 owners**
→ The column names in the DBF don't match any of the probed keys (`OWNER`, `OWN1`, etc.). Open the DBF with `python -c "from dbfread import DBF; print(list(DBF('path.dbf').fields))"` and add the missing names to the key tuples in `ParcelIndex.load_from_dbf`.

**GHL import shows blank contact names**
→ Owner field is likely an entity (`ACME LLC`). The scraper puts entities in `Last Name` with an empty `First Name`, which is correct — just make sure your GHL import mapping doesn't mark `First Name` as required.
