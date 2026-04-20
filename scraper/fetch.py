#!/usr/bin/env python3
"""La Crosse County WI - Motivated Seller Lead Scraper (WCCA-based).

Data source: Wisconsin Circuit Court Access (WCCA) public search.
URL: https://wcca.wicourts.gov/advancedCaseSearchResults.html
La Crosse county number: 32. Case types: CV, PR, SC.
"""

from __future__ import annotations
import csv, json, logging, os, re, sys
from datetime import date, timedelta
from pathlib import Path
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lacrosse")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
TODAY = date.today()
CUTOFF = TODAY - timedelta(days=LOOKBACK_DAYS)
COUNTY_NO = 32
CASE_TYPES = ["CV", "PR", "SC"]
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"; DATA = ROOT / "data"
DASH.mkdir(exist_ok=True); DATA.mkdir(exist_ok=True)

def fetch_wcca(case_type: str) -> list[dict]:
    url = "https://wcca.wicourts.gov/advancedCaseSearchResults.html"
    params = {"countyNo": COUNTY_NO, "caseType": case_type,
              "filingDateStart": CUTOFF.strftime("%m-%d-%Y"),
              "filingDateEnd": TODAY.strftime("%m-%d-%Y")}
    log.info("WCCA GET %s %s", case_type, params)
    r = requests.get(url, params=params, headers={"User-Agent": UA, "Accept": "text/html"}, timeout=30)
    log.info("WCCA %s -> HTTP %s, %d bytes", case_type, r.status_code, len(r.content))
    if r.status_code != 200:
        log.error("WCCA returned %s body=%s", r.status_code, r.text[:400])
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    rows = []
    table = soup.find("table")
    if table is None:
        log.warning("WCCA %s no table found", case_type)
        return []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 7: continue
        case_no = cells[0].get_text(strip=True)
        if not re.match(r"^\d{4}[A-Z]{2}\d+", case_no): continue
        link = cells[0].find("a")
        href = link["href"] if link and link.has_attr("href") else ""
        if href and href.startswith("/"): href = "https://wcca.wicourts.gov" + href
        rows.append({
            "case_no": case_no,
            "filing_date": cells[1].get_text(strip=True),
            "county": cells[2].get_text(strip=True),
            "status": cells[3].get_text(strip=True),
            "name": cells[4].get_text(strip=True),
            "dob": cells[5].get_text(strip=True),
            "caption": cells[6].get_text(strip=True),
            "url": href,
            "case_type": case_type,
        })
    log.info("WCCA %s -> %d cases parsed", case_type, len(rows))
    return rows

def classify(caption: str) -> tuple[str, list[str]]:
    c = caption.lower()
    flags = []
    lead_type = "Civil"
    if "estate of" in c or "in re:" in c and "estate" in c:
        flags.append("Probate/estate"); lead_type = "Probate"
    if any(w in c for w in ["bank", "mortgage", "lender", "loan", "credit union", "financial"]):
        flags.append("Bank plaintiff (likely foreclosure)")
        if lead_type == "Civil": lead_type = "Foreclosure"
    if any(w in c for w in ["llc", "l.l.c.", "corp", "inc."]):
        flags.append("LLC/corp involved")
    if "deceased" in c: flags.append("Deceased party")
    return lead_type, flags

def score(rec: dict) -> int:
    s = 30
    s += 10 * len(rec.get("flags", []))
    if rec.get("case_type") == "PR": s += 15
    if "Bank plaintiff (likely foreclosure)" in rec.get("flags", []): s += 20
    return min(s, 100)

def main():
    log.info("=" * 60)
    log.info("La Crosse Scraper | %s -> %s (%d days)", CUTOFF, TODAY, LOOKBACK_DAYS)
    log.info("=" * 60)
    all_rows = []
    for ct in CASE_TYPES:
        try:
            all_rows.extend(fetch_wcca(ct))
        except Exception as e:
            log.exception("WCCA %s failed: %s", ct, e)
    seen = set(); unique = []
    for r in all_rows:
        if r["case_no"] in seen: continue
        seen.add(r["case_no"])
        lead_type, flags = classify(r["caption"])
        r["lead_type"] = lead_type; r["flags"] = flags
        r["seller_score"] = score(r)
        r["source"] = "Wisconsin Circuit Courts (WCCA)"
        unique.append(r)
    unique.sort(key=lambda x: x["seller_score"], reverse=True)
    payload = {"generated": TODAY.isoformat(), "lookback_days": LOOKBACK_DAYS,
               "total": len(unique), "records": unique}
    (DASH / "records.json").write_text(json.dumps(payload, indent=2))
    (DATA / "records.json").write_text(json.dumps(payload, indent=2))
    with (DATA / "ghl_export.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Case Number", "Filing Date", "Name", "Caption", "Lead Type",
                    "Flags", "Seller Score", "Source", "URL"])
        for r in unique:
            w.writerow([r["case_no"], r["filing_date"], r["name"], r["caption"],
                        r["lead_type"], "; ".join(r["flags"]), r["seller_score"],
                        r["source"], r["url"]])
    (DASH / "ghl_export.csv").write_bytes((DATA / "ghl_export.csv").read_bytes())
    log.info("=" * 60)
    log.info("DONE: %d unique leads saved", len(unique))
    log.info("=" * 60)

if __name__ == "__main__":
    main()
