#!/usr/bin/env python3
"""La Crosse County WI - Motivated Seller Lead Scraper.

Pipeline:
  1. Query WCCA JSON API (/jsonPost/advancedCaseSearch) for CV/PR/SC cases
     filed in the lookback window (default 7 days), La Crosse countyNo=32.
  2. Classify lead type, flags, seller score.
  3. Enrich each lead with property address via the La Crosse County Public
     Portal (Catalis LandNav at pp-lacrosse-co-wi-fb.app.landnav.com).
     Guest login -> Real Estate Search by owner last/first name.
  4. Write dashboard/records.json + data/records.json + ghl_export.csv.
"""
from __future__ import annotations
import csv, json, logging, os, re, sys, time
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

PORTAL_BASE = "https://pp-lacrosse-co-wi-fb.app.landnav.com"
PORTAL_LOGIN_PAGE = PORTAL_BASE + "/login/index/"
PORTAL_GUEST_LOGIN = PORTAL_BASE + "/login/GuestLogin"
PORTAL_SEARCH = PORTAL_BASE + "/Search/RealEstate/Search/Search"

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
DATA = ROOT / "data"
DASH.mkdir(exist_ok=True)
DATA.mkdir(exist_ok=True)


# ---------- WCCA ----------

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


# ---------- Classification ----------

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
    if rec.get("property_address"):
        s += 5
    return min(s, 100)


# ---------- Name parsing ----------

ENTITY_PAT = re.compile(
    r"\b(LLC|L\.L\.C\.|INC|INCORPORATED|CORP|CORPORATION|LTD|LP|LLP|"
    r"BANK|MORTGAGE|LENDERS?|CREDIT UNION|FINANCIAL|FUND|TRUST|ESTATE|"
    r"ASSOCIATION|COMPANY|CO\.?|ENTERPRISES|HOLDINGS|PARTNERS|GROUP|"
    r"N\.?A\.?|PLLC|PC)\b",
    re.IGNORECASE,
)

def is_entity(name: str) -> bool:
    return bool(ENTITY_PAT.search(name or ""))


def split_name(party_name: str) -> tuple[str, str]:
    """Return (last_name, first_name) for a WCCA partyName string.
    WCCA formats: 'LAST, FIRST M' or 'In the Estate of FIRST M LAST' or 'Entity Name'.
    """
    n = (party_name or "").strip()
    if not n:
        return "", ""
    # 'In the Estate of First M Last'
    m = re.match(r"(?i)^in the estate of\s+(.+)$", n)
    if m:
        parts = m.group(1).strip().split()
        if len(parts) >= 2:
            return parts[-1].upper(), parts[0].upper()
        return parts[0].upper() if parts else "", ""
    # 'LAST, FIRST M'
    if "," in n:
        last, rest = n.split(",", 1)
        first = rest.strip().split()[0] if rest.strip() else ""
        return last.strip().upper(), first.upper()
    # Entity or plain 'First Last'
    if is_entity(n):
        return "", ""
    parts = n.split()
    if len(parts) >= 2:
        return parts[-1].upper(), parts[0].upper()
    return parts[0].upper() if parts else "", ""


# ---------- Portal enrichment ----------

class PortalClient:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": UA,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._logged_in = False

    def login(self) -> bool:
        try:
            log.info("Portal: GET login page")
            r = self.s.get(PORTAL_LOGIN_PAGE, timeout=30)
            if r.status_code != 200:
                log.warning("Portal login page HTTP %s", r.status_code)
                return False
            m = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
            if not m:
                log.warning("Portal: no anti-forgery token found")
                return False
            token = m.group(1)
            log.info("Portal: submitting guest login")
            r2 = self.s.post(
                PORTAL_GUEST_LOGIN,
                data={"returnUrl": "", "__RequestVerificationToken": token},
                headers={"Referer": PORTAL_LOGIN_PAGE},
                timeout=30,
                allow_redirects=True,
            )
            log.info("Portal: login HTTP %s", r2.status_code)
            self._logged_in = r2.status_code in (200, 302)
            return self._logged_in
        except Exception as e:
            log.exception("Portal login error: %s", e)
            return False

    def search(self, last: str, first: str = "") -> list[dict]:
        if not self._logged_in:
            return []
        if not last:
            return []
        body = {
            "AddressSearchType": "0",
            "LastName": last.upper(),
            "FirstName": first.upper(),
            "OwnerStatus": "3",  # All Except Former
            "TaxYearSearchType": "0",
            "MinTaxYear": str(TODAY.year - 1),
            "OrderByField": "propertyNumber",
            "PageNumber": "1",
            "PageSize": "200",
        }
        try:
            r = self.s.post(
                PORTAL_SEARCH,
                data=body,
                headers={
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": PORTAL_BASE + "/Search/RealEstate/Search",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                },
                timeout=30,
            )
        except Exception as e:
            log.warning("Portal search %s error: %s", last, e)
            return []
        if r.status_code != 200:
            return []
        try:
            j = r.json()
        except Exception:
            return []
        data = j.get("data")
        if not data:
            return []
        return list(data.values()) if isinstance(data, dict) else list(data)

    def best_match(self, last: str, first: str) -> dict | None:
        """Try first+last; if 0, try last-only and pick best."""
        # Try exact first+last
        if first:
            results = self.search(last, first)
            if len(results) == 1:
                return results[0]
            if len(results) > 1:
                # multiple exact matches - prefer La Crosse City / Onalaska City
                return pick_preferred(results)
        # Fallback: last name only
        results = self.search(last, "")
        if not results:
            return None
        if len(results) == 1:
            return results[0]
        # Filter by first name initial
        if first:
            init = first[0].upper() if first else ""
            filtered = [r for r in results if (r.get("FirstName") or "").upper().startswith(first[:3].upper())]
            if len(filtered) == 1:
                return filtered[0]
            if filtered:
                return pick_preferred(filtered)
        # Too ambiguous - only accept if <= 3 results total (rare surname)
        if len(results) <= 3:
            return pick_preferred(results)
        return None


PREFERRED_MUNI = ("CITY OF LA CROSSE", "CITY OF ONALASKA", "VILLAGE OF WEST SALEM", "VILLAGE OF HOLMEN")

def pick_preferred(results: list[dict]) -> dict:
    # Prefer current owner + preferred municipality
    def rank(r):
        muni = (r.get("MunicipalityDescription") or "").upper()
        status = (r.get("OwnerStatus") or "").upper()
        m = 0
        for i, p in enumerate(PREFERRED_MUNI):
            if p in muni:
                m = 10 - i
                break
        s = 5 if "CURRENT" in status else 0
        return m + s
    return sorted(results, key=rank, reverse=True)[0]


def format_address(p: dict) -> dict:
    """Clean up address strings from portal response."""
    pa = (p.get("ConcatenatedPropertyAddress") or "").strip()
    ta = (p.get("ConcatenatedTaxAddress") or "").strip()
    muni = (p.get("MunicipalityDescription") or "").strip()
    # Derive city from municipality
    city = muni
    for pfx in ("CITY OF ", "VILLAGE OF ", "TOWN OF "):
        if muni.upper().startswith(pfx):
            city = muni[len(pfx):].title()
            break
    return {
        "property_address": pa,
        "mailing_address": ta,
        "property_city": city,
        "property_state": "WI",
        "property_zip": "",
        "municipality": muni.title(),
        "parcel_id": p.get("UserDefinedId") or "",
        "owner_of_record": p.get("ConcatenatedName") or "",
        "address_source": "La Crosse County Public Portal (Catalis)",
    }


def enrich_with_addresses(leads: list[dict]) -> None:
    """Mutates leads in place, adding address fields where possible."""
    client = PortalClient()
    if not client.login():
        log.warning("Portal login failed - skipping address enrichment")
        return
    hits = 0
    for i, r in enumerate(leads):
        last, first = split_name(r.get("name", ""))
        if not last:
            continue
        try:
            match = client.best_match(last, first)
        except Exception as e:
            log.warning("enrich %s error: %s", r.get("case_no"), e)
            match = None
        if match:
            r.update(format_address(match))
            hits += 1
        else:
            r.setdefault("property_address", "")
            r.setdefault("address_source", "")
        # polite delay
        time.sleep(0.25)
        if (i + 1) % 10 == 0:
            log.info("enrich progress: %d/%d, %d hits", i + 1, len(leads), hits)
    log.info("Address enrichment: %d/%d leads enriched (%.0f%%)",
             hits, len(leads), 100 * hits / max(len(leads), 1))


# ---------- Main ----------

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
        r["source"] = "Wisconsin Circuit Courts (WCCA)"
        unique.append(r)

    # Address enrichment
    try:
        enrich_with_addresses(unique)
    except Exception as e:
        log.exception("Enrichment pipeline error: %s", e)

    # Re-score now that address presence is known
    for r in unique:
        r["seller_score"] = score(r)

    unique.sort(key=lambda x: (x["seller_score"], x["filing_date"]), reverse=True)

    payload = {
        "generated": TODAY.isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "date_from": CUTOFF.isoformat(),
        "date_to": TODAY.isoformat(),
        "total": len(unique),
        "enriched": sum(1 for r in unique if r.get("property_address")),
        "records": unique,
    }
    (DASH / "records.json").write_text(json.dumps(payload, indent=2))
    (DATA / "records.json").write_text(json.dumps(payload, indent=2))

    # GHL CSV - GoHighLevel standard field names
    csv_path = DATA / "ghl_export.csv"
    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "First Name", "Last Name", "Property Address", "Property City",
            "Property State", "Property Zip", "Mailing Address",
            "Lead Type", "Document Type", "Date Filed", "Document Number",
            "Seller Score", "Motivated Seller Flags", "Source",
            "Public Records URL", "Parcel Number", "Owner of Record", "Address Source",
        ])
        for r in unique:
            nm = r.get("name", "")
            first, last = "", nm
            if "," in nm:
                last, rest = nm.split(",", 1)
                first = rest.strip().split()[0] if rest.strip() else ""
                last = last.strip()
            elif not is_entity(nm) and " " in nm:
                parts = nm.split()
                first = parts[0]
                last = " ".join(parts[1:])
            w.writerow([
                first, last,
                r.get("property_address", ""),
                r.get("property_city", ""),
                r.get("property_state", ""),
                r.get("property_zip", ""),
                r.get("mailing_address", ""),
                r["lead_type"],
                r["case_type"],
                r["filing_date"],
                r["case_no"],
                r["seller_score"],
                "; ".join(r["flags"]),
                r["source"],
                r["url"],
                r.get("parcel_id", ""),
                r.get("owner_of_record", ""),
                r.get("address_source", ""),
            ])
    (DASH / "ghl_export.csv").write_bytes(csv_path.read_bytes())
    log.info("=" * 60)
    log.info("DONE: %d unique leads (%d with addresses)", len(unique), payload["enriched"])
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
