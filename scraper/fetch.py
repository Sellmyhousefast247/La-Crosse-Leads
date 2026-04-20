#!/usr/bin/env python3
"""La Crosse County WI - Motivated Seller Lead Scraper.

Uses WCCA internal JSON API at /jsonPost/advancedCaseSearch.
La Crosse countyNo=32. Case types: CV (civil/foreclosure), PR (probate), SC (small claims).
"""

from __future__ import annotations
import csv, json, logging, os, sys
from datetime import date, timedelta
from pathlib import Path
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lacrosse")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
TODAY = date.today()
CUTOFF = TODAY - timedelta(days=LOOKBACK_DAYS)
COUNTY_NO = 32
CASE_TYPES = [("CV", "Civil / Foreclosure"), ("PR", "Probate"), ("SC", "Small Claims")]
WCCA_URL = "https://wcca.wicourts.gov/jsonPost/advancedCaseSearch"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
DATA = ROOT / "data"
DASH.mkdir(exist_ok=True)
DATA.mkdir(exist_ok=True)

def fetch_wcca(case_type: str) -> list[dict]:
    payload = {
        "includeMissingDob": True,
        "includeMissingMiddleName": True,
        "countyNo": COUNTY_NO,
        "attyType": "partyAtty",
        "caseType": case_type,
        "filingDate": {
            "start": CUTOFF.strftime("%m-%d-%Y"),
            "end": TODAY.strftime("%m-%d-%Y"),
        },
    }
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://wcca.wicourts.gov",
        "Referer": "https://wcca.wicourts.gov/advanced.html",
    }
    log.info("WCCA POST %s %s -> %s", case_type, payload["filingDate"]["start"], payload["filingDate"]["end"])
    try:
        r = requests.post(WCCA_URL, json=payload, headers=headers, timeout=30)
    except Exception as e:
        log.exception("WCCA %s network error: %s", case_type, e)
        return []
    log.info("WCCA %s -> HTTP %s, %d bytes", case_type, r.status_code, len(r.content))
    if r.status_code != 200:
        log.error("WCCA %s body: %s", case_type, r.text[:400])
        return []
    try:
        data = r.json()
    except Exception:
        log.error("WCCA %s returned non-JSON: %s", case_type, r.text[:400])
        return []
    cases = (data.get("result") or {}).get("cases") or []
    log.info("WCCA %s -> %d cases", case_type, len(cases))
    rows = []
    for c in cases:
        case_no = c.get("caseNo") or ""
        rows.append({
            "case_no": case_no,
            "filing_date": c.get("filingDate") or "",
            "county": c.get("countyName") or "",
            "status": c.get("status") or "",
            "name": c.get("partyName") or "",
            "dob": c.get("dob") or "",
            "caption": c.get("caption") or "",
            "url": f"https://wcca.wicourts.gov/caseDetail.html?caseNo={case_no}&countyNo={COUNTY_NO}",
            "case_type": case_type,
        })
    return rows

def classify(caption: str, case_type: str) -> tuple[str, list[str]]:
    c = (caption or "").lower()
    flags: list[str] = []
    lead_type = "Civil"
    if case_type == "PR":
        lead_type = "Probate"
        flags.append("Probate / estate")
    elif case_type == "SC":
        lead_type = "Small Claims"
    if "estate of" in c or "deceased" in c:
        if "Probate / estate" not in flags:
            flags.append("Probate / estate")
        if lead_type == "Civil":
            lead_type = "Probate"
    if any(w in c for w in ["bank", "mortgage", "lender", "loan", "credit union", "financial", "fidelity", "ally bank"]):
        flags.append("Bank plaintiff (likely foreclosure)")
        if lead_type == "Civil":
            lead_type = "Foreclosure"
    if any(w in c for w in ["llc", "l.l.c.", " corp", " inc"]):
        flags.append("LLC / corporate party")
    if "eviction" in c or "writ of restitution" in c:
        flags.append("Eviction")
    if "foreclosure" in c:
        flags.append("Foreclosure")
        if lead_type == "Civil":
            lead_type = "Foreclosure"
    return lead_type, flags

def score(rec: dict) -> int:
    s = 30
    s += 10 * len(rec.get("flags", []))
    if rec.get("case_type") == "PR":
        s += 15
    if "Bank plaintiff (likely foreclosure)" in rec.get("flags", []):
        s += 20
    if "Foreclosure" in rec.get("flags", []):
        s += 10
    return min(s, 100)

def main() -> int:
    log.info("=" * 60)
    log.info("La Crosse Scraper | %s -> %s (%d days)", CUTOFF, TODAY, LOOKBACK_DAYS)
    log.info("=" * 60)
    all_rows: list[dict] = []
    for ct, label in CASE_TYPES:
        try:
            all_rows.extend(fetch_wcca(ct))
        except Exception as e:
            log.exception("%s (%s) failed: %s", ct, label, e)
    seen: set[str] = set()
    unique: list[dict] = []
    for r in all_rows:
        if r["case_no"] in seen:
            continue
        seen.add(r["case_no"])
        lead_type, flags = classify(r["caption"], r["case_type"])
        r["lead_type"] = lead_type
        r["flags"] = flags
        r["seller_score"] = score(r)
        r["source"] = "Wisconsin Circuit Courts (WCCA)"
        unique.append(r)
    unique.sort(key=lambda x: (x["seller_score"], x["filing_date"]), reverse=True)
    payload = {
        "generated": TODAY.isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "date_from": CUTOFF.isoformat(),
        "date_to": TODAY.isoformat(),
        "total": len(unique),
        "records": unique,
    }
    (DASH / "records.json").write_text(json.dumps(payload, indent=2))
    (DATA / "records.json").write_text(json.dumps(payload, indent=2))
    csv_path = DATA / "ghl_export.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Case Number", "Filing Date", "Party Name", "Caption", "Lead Type",
                    "Flags", "Seller Score", "Status", "Source", "Public Records URL"])
        for r in unique:
            w.writerow([r["case_no"], r["filing_date"], r["name"], r["caption"],
                        r["lead_type"], "; ".join(r["flags"]), r["seller_score"],
                        r["status"], r["source"], r["url"]])
    (DASH / "ghl_export.csv").write_bytes(csv_path.read_bytes())
    log.info("=" * 60)
    log.info("DONE: %d unique leads saved", len(unique))
    log.info("=" * 60)
    return 0

if __name__ == "__main__":
    sys.exit(main())
