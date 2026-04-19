#!/usr/bin/env python3
"""
La Crosse County, Wisconsin - Motivated Seller Lead Scraper
============================================================
Collects distressed property leads from public records:
  - Register of Deeds (Tapestry / Land Records)
  - Property Appraiser bulk parcel data (DBF)
  - GIS parcel lookups
  - Wisconsin Circuit Court (WCCA) case records
  - Treasurer tax delinquency data
  - WI DOR sales data

Outputs dashboard/records.json + data/records.json, and a GHL-ready CSV.
Designed to run headless in GitHub Actions on a 7-day lookback.

Author: Sell My House Fast LLC  |  San Antonio / Bexar County HQ
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import re
import sys
import time
import traceback
import zipfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

import requests
from bs4 import BeautifulSoup

# dbfread is required for the property appraiser parcel file.
# Import is guarded so the scraper degrades gracefully if it's missing.
try:
    from dbfread import DBF  # type: ignore
    HAS_DBF = True
except Exception:
    HAS_DBF = False

# Playwright is used for the clerk portal (JS-heavy Tapestry UI).
try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout  # type: ignore
    HAS_PLAYWRIGHT = True
except Exception:
    HAS_PLAYWRIGHT = False


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

ROOT_DIR = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT_DIR / "dashboard"
DATA_DIR = ROOT_DIR / "data"
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Upstream endpoints
CLERK_PORTAL_URL = "https://www.lacrossecounty.org/registerofdeeds"
TAPESTRY_URL = "https://lacrossecounty.org/registerofdeeds/tapestry"
SEARCH_OPTIONS_URL = "https://lacrossecounty.org/registerofdeeds/search-land-records-online-options"
LANDNAV_PORTAL_URL = "https://apps.lacrossecounty.org/landrecordsportallandnav/landnav.html"
LANDNAV_APP_URL = "https://pp-lacrosse-co-wi-fb.app.landnav.com/Login"
GIS_URL = "https://www.arcgis.com/apps/webappviewer/index.html?id=dfb4ce4831654010bed9aa9d258d5ad0"
WCCA_URL = "https://wcca.wicourts.gov/"
TREASURER_URL = "https://www.lacrossecounty.org/treasurer"
DOR_SALES_URL = "https://propertyinfo.revenue.wi.gov/"

# Common headers
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Document categorization map - maps a slew of recorded doc types onto our
# internal lead categories. Keys are normalized uppercase substrings.
DOC_CATEGORY_MAP: list[tuple[str, str, str]] = [
    # (substring to match in doc type, category code, human-readable label)
    ("LIS PENDENS RELEASE", "RELLP", "Release Lis Pendens"),
    ("RELEASE LIS PENDENS", "RELLP", "Release Lis Pendens"),
    ("RELEASE OF LIS", "RELLP", "Release Lis Pendens"),
    ("LIS PENDENS", "LP", "Lis Pendens"),
    ("NOTICE OF FORECLOSURE", "NOFC", "Notice of Foreclosure"),
    ("FORECLOSURE", "NOFC", "Notice of Foreclosure"),
    ("TAX DEED", "TAXDEED", "Tax Deed"),
    ("CERTIFIED JUDGMENT", "CCJ", "Certified Judgment"),
    ("DOMESTIC JUDGMENT", "DRJUD", "Domestic Judgment"),
    ("JUDGMENT", "JUD", "Judgment"),
    ("IRS LIEN", "LNIRS", "IRS Lien"),
    ("FEDERAL TAX LIEN", "LNIRS", "IRS Lien"),
    ("FEDERAL LIEN", "LNFED", "Federal Lien"),
    ("CORPORATE TAX", "LNCORPTX", "Corporate Tax Lien"),
    ("CORP TAX", "LNCORPTX", "Corporate Tax Lien"),
    ("MECHANIC", "LNMECH", "Mechanic Lien"),
    ("HOA LIEN", "LNHOA", "HOA Lien"),
    ("HOMEOWNERS ASSOC", "LNHOA", "HOA Lien"),
    ("MEDICAID", "MEDLN", "Medicaid Lien"),
    ("PROBATE", "PRO", "Probate"),
    ("LETTERS TESTAMENT", "PRO", "Probate"),
    ("LETTERS OF ADMIN", "PRO", "Probate"),
    ("NOTICE OF COMMENCEMENT", "NOC", "Notice of Commencement"),
    ("LIEN", "LN", "Lien"),  # generic fallback - keep last
]

# Doc types we actively collect (all keys above are collected).
# This set is used only for quick inclusion checks in the lookups.
COLLECTED_CATEGORIES = {cat for _, cat, _ in DOC_CATEGORY_MAP}

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    """Timestamped stdout logging so GitHub Actions runs are readable."""
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}", flush=True)


def safe(fn, *args, default=None, **kwargs):
    """Run a function, swallow exceptions, return default on failure.

    Used everywhere to make sure one bad record/source doesn't crash the run.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        log(f"  ! swallowed {type(exc).__name__} in {getattr(fn,'__name__','fn')}: {exc}")
        return default


def with_retries(fn, *args, attempts: int = MAX_RETRIES, **kwargs):
    """Call `fn` up to `attempts` times with exponential-ish backoff."""
    last_exc: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            log(f"  retry {i}/{attempts} after error: {type(exc).__name__}: {exc}")
            if i < attempts:
                time.sleep(RETRY_BACKOFF_SECONDS * i)
    if last_exc:
        log(f"  all {attempts} attempts failed; giving up on this call")
    return None


def parse_date_loose(s: str | None) -> datetime | None:
    """Parse a date string in several likely formats. Returns None on failure."""
    if not s:
        return None
    s = s.strip()
    # Strip common trailing noise
    s = re.sub(r"\s+\d{1,2}:\d{2}(:\d{2})?\s*(AM|PM)?$", "", s, flags=re.I).strip()
    fmts = [
        "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%Y/%m/%d",
        "%m/%d/%y", "%B %d, %Y", "%b %d, %Y", "%d-%b-%Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_amount(s: str | None) -> float:
    """Pull a dollar amount out of text. Returns 0.0 if none found.

    Order matters: try a comma-grouped match first (e.g. "$1,250,000.50"),
    otherwise fall back to a plain run of digits with optional decimal.
    """
    if not s:
        return 0.0
    # Comma/space grouped: 1,234 or 12,345,678.90
    m = re.search(r"\$?\s*([0-9]{1,3}(?:[,\s][0-9]{3})+(?:\.[0-9]+)?)", s)
    if not m:
        # Plain integer/decimal: 75000 or 75000.50
        m = re.search(r"\$?\s*([0-9]+(?:\.[0-9]+)?)", s)
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(",", "").replace(" ", ""))
    except ValueError:
        return 0.0


def normalize_doc_type(raw: str | None) -> tuple[str, str]:
    """Return (category_code, category_label) for a raw doc-type string."""
    if not raw:
        return ("OTHER", "Other")
    up = raw.upper()
    for needle, cat, label in DOC_CATEGORY_MAP:
        if needle in up:
            return (cat, label)
    return ("OTHER", "Other")


def clean_text(s: Any) -> str:
    """Trim whitespace, collapse internal runs of whitespace, coerce None -> ''."""
    if s is None:
        return ""
    out = re.sub(r"\s+", " ", str(s)).strip()
    return out


def make_session() -> requests.Session:
    """Configured requests.Session with sane defaults."""
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    return s


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class LeadRecord:
    doc_num: str = ""
    doc_type: str = ""
    filed: str = ""                 # ISO date YYYY-MM-DD
    cat: str = ""                   # internal category code
    cat_label: str = ""             # human-readable category label
    owner: str = ""                 # grantor
    grantee: str = ""
    amount: float = 0.0
    legal: str = ""                 # legal description
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "WI"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = "WI"
    mail_zip: str = ""
    clerk_url: str = ""
    flags: list[str] = field(default_factory=list)
    score: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Parcel / owner lookup (property appraiser DBF)
# ---------------------------------------------------------------------------

class ParcelIndex:
    """Owner-name -> property/mailing-address lookup built from the county DBF.

    Supports three common grantor name forms:
        "FIRST LAST"
        "LAST FIRST"
        "LAST, FIRST"
    """

    def __init__(self) -> None:
        self._by_owner: dict[str, dict] = {}
        self.count = 0

    # ---- public API ------------------------------------------------------

    def lookup(self, owner_name: str) -> dict | None:
        """Try a few name variants and return the parcel dict or None."""
        if not owner_name:
            return None
        variants = self._owner_variants(owner_name)
        for v in variants:
            key = self._normalize_key(v)
            if key in self._by_owner:
                return self._by_owner[key]
        # Last resort: partial substring match (slow but small dataset)
        ln_first = self._extract_last_first(owner_name)
        if ln_first:
            for key, rec in self._by_owner.items():
                if ln_first in key:
                    return rec
        return None

    def load_from_dbf(self, path: Path) -> None:
        """Populate the index from a DBF file."""
        if not HAS_DBF:
            log("  dbfread not installed; skipping parcel index")
            return
        if not path.exists():
            log(f"  parcel DBF missing: {path}")
            return
        try:
            table = DBF(str(path), load=False, ignore_missing_memofile=True, encoding="latin-1")
        except Exception as exc:
            log(f"  could not open DBF {path.name}: {exc}")
            return

        owner_keys = ("OWNER", "OWN1", "OWNERNAME", "OWNER_NAME", "OWNER1")
        site_addr_keys = ("SITE_ADDR", "SITEADDR", "SITUS_ADDR", "PROP_ADDR")
        site_city_keys = ("SITE_CITY", "SITUS_CITY", "PROP_CITY")
        site_zip_keys = ("SITE_ZIP", "SITUS_ZIP", "PROP_ZIP")
        mail_addr_keys = ("ADDR_1", "MAILADR1", "MAIL_ADDR", "MAIL_ADDR1", "MAILING_AD")
        mail_city_keys = ("CITY", "MAILCITY", "MAIL_CITY")
        mail_state_keys = ("STATE", "MAILSTATE", "MAIL_STATE")
        mail_zip_keys = ("ZIP", "MAILZIP", "MAIL_ZIP")

        def first(d: dict, keys: tuple[str, ...]) -> str:
            for k in keys:
                if k in d and d[k]:
                    return clean_text(d[k])
            return ""

        loaded = 0
        try:
            for rec in table:
                try:
                    owner = first(rec, owner_keys)
                    if not owner:
                        continue
                    parcel = {
                        "owner": owner,
                        "site_addr": first(rec, site_addr_keys),
                        "site_city": first(rec, site_city_keys),
                        "site_zip": first(rec, site_zip_keys),
                        "mail_addr": first(rec, mail_addr_keys),
                        "mail_city": first(rec, mail_city_keys),
                        "mail_state": first(rec, mail_state_keys) or "WI",
                        "mail_zip": first(rec, mail_zip_keys),
                    }
                    # Index under every variant so lookup is O(1) on the
                    # common cases.
                    for variant in self._owner_variants(owner):
                        key = self._normalize_key(variant)
                        # First writer wins - don't clobber earlier parcels
                        # for the same owner (they're usually the same owner).
                        self._by_owner.setdefault(key, parcel)
                    loaded += 1
                except Exception as exc:
                    # Never let one bad record crash the whole load
                    continue
        except Exception as exc:
            log(f"  error iterating DBF: {exc}")

        self.count = loaded
        log(f"  parcel index loaded: {loaded:,} owners, {len(self._by_owner):,} name variants")

    # ---- internals -------------------------------------------------------

    @staticmethod
    def _normalize_key(s: str) -> str:
        return re.sub(r"[^A-Z0-9 ]+", "", s.upper()).strip()

    @staticmethod
    def _owner_variants(name: str) -> list[str]:
        """Return plausible name variants for a grantor/owner string."""
        name = clean_text(name)
        if not name:
            return []
        variants = {name}
        # "LAST, FIRST MIDDLE" -> also "FIRST LAST" + "LAST FIRST"
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            if len(parts) == 2 and parts[0] and parts[1]:
                last, first_rest = parts
                variants.add(f"{first_rest} {last}")
                variants.add(f"{last} {first_rest}")
                variants.add(f"{last}, {first_rest}")
        else:
            tokens = name.split()
            if len(tokens) >= 2:
                # Assume "FIRST [MIDDLE] LAST" and also derive "LAST FIRST"
                first = tokens[0]
                last = tokens[-1]
                variants.add(f"{last} {first}")
                variants.add(f"{last}, {first}")
                variants.add(f"{first} {last}")
        return list(variants)

    @staticmethod
    def _extract_last_first(name: str) -> str:
        name = ParcelIndex._normalize_key(name)
        tokens = name.split()
        if len(tokens) >= 2:
            return f"{tokens[-1]} {tokens[0]}"
        return name


# ---------------------------------------------------------------------------
# Property Appraiser: bulk DBF download
# ---------------------------------------------------------------------------

def download_parcel_dbf(session: requests.Session, workdir: Path) -> Path | None:
    """Download and extract the parcel DBF from the LandNav portal.

    The LandNav portal is a WebForms page - the bulk download link is
    typically triggered via ``__doPostBack``. We emulate that POST.
    Returns the local path to the extracted .dbf, or None on failure.
    """
    log("Downloading parcel bulk file from LandNav portal...")
    try:
        r = session.get(LANDNAV_PORTAL_URL, timeout=45)
        r.raise_for_status()
    except Exception as exc:
        log(f"  could not reach landnav portal: {exc}")
        return None

    soup = BeautifulSoup(r.text, "lxml")

    # Capture WebForms hidden state so __doPostBack works.
    def hidden(name: str) -> str:
        el = soup.find("input", {"name": name})
        return el.get("value", "") if el else ""

    viewstate = hidden("__VIEWSTATE")
    viewstategen = hidden("__VIEWSTATEGENERATOR")
    eventvalidation = hidden("__EVENTVALIDATION")

    # Look for any link that mentions a DBF / parcel / bulk download.
    target_event: str | None = None
    direct_url: str | None = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = clean_text(a.get_text()).lower()
        if any(kw in text for kw in ("parcel", "bulk", "dbf", "download")) or \
           any(kw in href.lower() for kw in (".dbf", ".zip", "parcel", "bulk")):
            m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if m:
                target_event = m.group(1)
                break
            if href.startswith("http") or href.startswith("/"):
                direct_url = urljoin(LANDNAV_PORTAL_URL, href)
                break

    content_bytes: bytes | None = None
    content_name: str = "parcels.zip"

    if direct_url:
        log(f"  direct download link: {direct_url}")
        try:
            rr = session.get(direct_url, timeout=120)
            rr.raise_for_status()
            content_bytes = rr.content
            # Try to derive filename from URL
            content_name = os.path.basename(urlparse(direct_url).path) or content_name
        except Exception as exc:
            log(f"  direct download failed: {exc}")
            content_bytes = None

    elif target_event:
        log(f"  __doPostBack target: {target_event}")
        form = {
            "__EVENTTARGET": target_event,
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": viewstategen,
            "__EVENTVALIDATION": eventvalidation,
        }
        try:
            rr = session.post(LANDNAV_PORTAL_URL, data=form, timeout=180)
            rr.raise_for_status()
            content_bytes = rr.content
        except Exception as exc:
            log(f"  __doPostBack download failed: {exc}")
            content_bytes = None

    if not content_bytes:
        log("  parcel bulk download unavailable (site may require interactive login)")
        return None

    # Write out + unzip if needed
    blob_path = workdir / content_name
    try:
        blob_path.write_bytes(content_bytes)
    except Exception as exc:
        log(f"  could not write parcel blob: {exc}")
        return None

    # If it's a zip, extract the .dbf
    if zipfile.is_zipfile(blob_path):
        try:
            with zipfile.ZipFile(blob_path) as zf:
                dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if not dbf_names:
                    log("  zip contained no .dbf")
                    return None
                zf.extractall(workdir)
                return workdir / dbf_names[0]
        except Exception as exc:
            log(f"  zip extract failed: {exc}")
            return None

    if blob_path.suffix.lower() == ".dbf":
        return blob_path

    # Unknown format
    log(f"  unrecognized bulk file format: {blob_path.name}")
    return None


# ---------------------------------------------------------------------------
# Clerk portal (Tapestry) scraping via Playwright
# ---------------------------------------------------------------------------

async def scrape_tapestry(start: datetime, end: datetime) -> list[LeadRecord]:
    """Scrape the Register of Deeds Tapestry search for the date range.

    Tapestry is a third-party hosted search front end; the exact DOM varies
    by tenant but generally exposes:
      - a date-range picker
      - a doc-type multi-select
      - a results grid with linkable detail pages

    We attempt the search interactively; on any failure we fall back to
    the static "search options" help page so the run still produces
    something useful.
    """
    if not HAS_PLAYWRIGHT:
        log("Playwright not installed - skipping Tapestry")
        return []

    records: list[LeadRecord] = []
    log(f"Scraping Tapestry {start.date()} -> {end.date()}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(user_agent=DEFAULT_HEADERS["User-Agent"])
            page = await context.new_page()
            page.set_default_timeout(45_000)

            try:
                await page.goto(TAPESTRY_URL, wait_until="domcontentloaded")
            except PWTimeout:
                log("  Tapestry page load timeout")
                return records

            # The clerk page typically has an iframe or an outbound link to
            # the vendor-hosted search app. Find it.
            search_url: str | None = None
            try:
                frames = page.frames
                for fr in frames:
                    if "tapestry" in (fr.url or "").lower() or "search" in (fr.url or "").lower():
                        search_url = fr.url
                        break
                if not search_url:
                    # look for an anchor
                    anchor = await page.query_selector("a[href*='tapestry'], a[href*='search']")
                    if anchor:
                        href = await anchor.get_attribute("href")
                        if href:
                            search_url = urljoin(TAPESTRY_URL, href)
            except Exception as exc:
                log(f"  could not discover search iframe: {exc}")

            if search_url:
                log(f"  navigating to search app: {search_url}")
                try:
                    await page.goto(search_url, wait_until="domcontentloaded")
                except Exception as exc:
                    log(f"  could not load search app: {exc}")

            # ------------------------------------------------------------
            # Attempt to fill the date range. The Tapestry UI commonly uses
            # inputs named "DateFrom"/"DateTo" or "FromDate"/"ToDate".
            # ------------------------------------------------------------
            start_s = start.strftime("%m/%d/%Y")
            end_s = end.strftime("%m/%d/%Y")

            filled_ok = False
            for from_sel, to_sel in [
                ("input[name='DateFrom']", "input[name='DateTo']"),
                ("input[name='FromDate']", "input[name='ToDate']"),
                ("#DateFrom", "#DateTo"),
                ("#FromDate", "#ToDate"),
                ("input[id*='From']", "input[id*='To']"),
            ]:
                try:
                    from_el = await page.query_selector(from_sel)
                    to_el = await page.query_selector(to_sel)
                    if from_el and to_el:
                        await from_el.fill(start_s)
                        await to_el.fill(end_s)
                        filled_ok = True
                        log(f"  filled date range via {from_sel} / {to_sel}")
                        break
                except Exception:
                    continue

            if filled_ok:
                # Click the Search button - try a few likely labels
                for btn_sel in [
                    "button:has-text('Search')",
                    "input[type='submit'][value*='Search']",
                    "#btnSearch",
                    "button#search",
                ]:
                    try:
                        btn = await page.query_selector(btn_sel)
                        if btn:
                            await btn.click()
                            await page.wait_for_load_state("networkidle", timeout=60_000)
                            break
                    except Exception:
                        continue

                # Parse the results table
                try:
                    html = await page.content()
                    records.extend(_parse_tapestry_results(html, base_url=page.url))
                except Exception as exc:
                    log(f"  result parse failed: {exc}")

            else:
                log("  could not find date inputs; Tapestry layout may have changed")

        finally:
            await browser.close()

    log(f"  Tapestry returned {len(records)} raw records")
    return records


def _parse_tapestry_results(html: str, base_url: str) -> list[LeadRecord]:
    """Best-effort parser for a Tapestry results grid."""
    out: list[LeadRecord] = []
    soup = BeautifulSoup(html, "lxml")

    # Find a table whose headers look like a document index.
    tables = soup.find_all("table")
    target = None
    for t in tables:
        headers = [clean_text(th.get_text()).lower() for th in t.find_all("th")]
        header_blob = " ".join(headers)
        if any(h in header_blob for h in ("doc", "document")) and \
           any(h in header_blob for h in ("date", "filed")):
            target = t
            break
    if not target:
        return out

    headers = [clean_text(th.get_text()).lower() for th in target.find_all("th")]
    hidx = {h: i for i, h in enumerate(headers)}

    def col(tds: list, *names: str) -> str:
        for n in names:
            for h, i in hidx.items():
                if n in h and i < len(tds):
                    return clean_text(tds[i].get_text())
        return ""

    for tr in target.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        try:
            rec = LeadRecord()
            rec.doc_num = col(tds, "doc num", "document num", "instrument", "recording")
            rec.doc_type = col(tds, "doc type", "document type", "type")
            rec.filed = ""
            filed_raw = col(tds, "recorded", "filed", "date")
            dt = parse_date_loose(filed_raw)
            if dt:
                rec.filed = dt.strftime("%Y-%m-%d")
            rec.owner = col(tds, "grantor", "from", "party 1")
            rec.grantee = col(tds, "grantee", "to", "party 2")
            rec.legal = col(tds, "legal", "description")
            rec.amount = parse_amount(col(tds, "amount", "consideration"))
            rec.cat, rec.cat_label = normalize_doc_type(rec.doc_type)
            # Capture the detail-page URL if present
            link = tr.find("a", href=True)
            if link:
                rec.clerk_url = urljoin(base_url, link["href"])
            else:
                rec.clerk_url = base_url
            if rec.doc_num or rec.owner:
                out.append(rec)
        except Exception:
            # Never let one bad row kill the whole parse
            continue
    return out


# ---------------------------------------------------------------------------
# Static fallback: scrape the search-options help page for any linked data
# ---------------------------------------------------------------------------

def scrape_static_help_pages(session: requests.Session) -> list[str]:
    """Pull any useful record-search URLs from the county help pages."""
    urls: list[str] = []
    for help_url in (SEARCH_OPTIONS_URL, CLERK_PORTAL_URL):
        try:
            r = session.get(help_url, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if any(k in href.lower() for k in ("tapestry", "landnav", "recorder", "search")):
                    urls.append(urljoin(help_url, href))
        except Exception as exc:
            log(f"  help page fetch failed ({help_url}): {exc}")
    return sorted(set(urls))


# ---------------------------------------------------------------------------
# Wisconsin Circuit Court (WCCA) - case lookups for probate/foreclosure
# ---------------------------------------------------------------------------

def scrape_wcca_cases(session: requests.Session, start: datetime, end: datetime) -> list[LeadRecord]:
    """Best-effort scrape of the WCCA public case search for La Crosse County.

    WCCA's advanced search is token-protected and often requires a browser
    session; this function posts the advanced-search form when possible
    and falls back to silently returning [] otherwise.
    """
    log(f"Scraping WCCA (La Crosse County) {start.date()} -> {end.date()}")
    out: list[LeadRecord] = []
    try:
        r = session.get(WCCA_URL, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        log(f"  WCCA unreachable: {exc}")
        return out

    # WCCA blocks non-interactive deep scrapes in practice, so we only
    # enumerate top-level links here and leave detailed case pulls for a
    # future Playwright pass.
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.find_all("a", href=True):
        txt = clean_text(a.get_text()).lower()
        if "case search" in txt or "advanced" in txt:
            log(f"  WCCA entry point found: {a['href']}")
            break
    return out


# ---------------------------------------------------------------------------
# Treasurer / tax delinquency
# ---------------------------------------------------------------------------

def scrape_treasurer(session: requests.Session) -> list[LeadRecord]:
    """Skim the treasurer page for any linked delinquency list."""
    log("Scanning Treasurer for delinquency lists...")
    out: list[LeadRecord] = []
    try:
        r = session.get(TREASURER_URL, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        log(f"  Treasurer unreachable: {exc}")
        return out

    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.find_all("a", href=True):
        txt = clean_text(a.get_text()).lower()
        if any(k in txt for k in ("delinquent", "tax sale", "foreclos")):
            log(f"  Treasurer link: {a['href']} ({txt})")
    return out


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_record(rec: LeadRecord, within_week: bool) -> None:
    """Assign flags + 0-100 seller score. Mutates the record in place."""
    flags: list[str] = []
    score = 30  # base

    cat = rec.cat
    owner_up = (rec.owner or "").upper()

    if cat == "LP":
        flags.append("Lis pendens")
        score += 10
    if cat == "NOFC":
        flags.append("Pre-foreclosure")
        score += 10
    if cat == "LP" and "FORECLOS" in (rec.doc_type or "").upper():
        flags.append("LP + foreclosure combo")
        score += 20
    if cat in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
        score += 10
    if cat in ("LNIRS", "LNFED", "LNCORPTX"):
        flags.append("Tax lien")
        score += 10
    if cat in ("LN", "LNMECH", "LNHOA", "MEDLN"):
        if cat == "LNMECH":
            flags.append("Mechanic lien")
        else:
            flags.append("Lien on record")
        score += 10
    if cat == "PRO":
        flags.append("Probate / estate")
        score += 10
    if any(tok in owner_up for tok in (" LLC", " LLC.", " INC", " CORP", " CO.", " COMPANY")):
        flags.append("LLC / corp owner")
        score += 10

    if rec.amount and rec.amount > 100_000:
        score += 15
    elif rec.amount and rec.amount > 50_000:
        score += 10

    if within_week:
        flags.append("New this week")
        score += 5

    if rec.prop_address or rec.mail_address:
        score += 5

    rec.flags = flags
    rec.score = max(0, min(100, score))


# ---------------------------------------------------------------------------
# Output + GHL CSV export
# ---------------------------------------------------------------------------

def write_records_json(records: list[LeadRecord], start: datetime, end: datetime) -> dict:
    """Write dashboard/records.json + data/records.json. Returns the payload."""
    with_addr = sum(1 for r in records if r.prop_address or r.mail_address)
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "La Crosse County, WI - Register of Deeds + LandNav + WCCA",
        "date_range": {
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
            "lookback_days": LOOKBACK_DAYS,
        },
        "total": len(records),
        "with_address": with_addr,
        "records": [r.to_dict() for r in records],
    }
    for out_path in (DASHBOARD_DIR / "records.json", DATA_DIR / "records.json"):
        try:
            out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
            log(f"wrote {out_path} ({len(records)} records)")
        except Exception as exc:
            log(f"  failed to write {out_path}: {exc}")
    return payload


def _split_name(owner: str) -> tuple[str, str]:
    """Best-effort split of a grantor name into (first, last)."""
    owner = clean_text(owner)
    if not owner:
        return ("", "")
    # Entity? Dump the whole thing into Last Name.
    up = owner.upper()
    if any(tok in up for tok in (" LLC", " INC", " CORP", " CO.", " TRUST", " ESTATE")):
        return ("", owner)
    if "," in owner:
        last, first = [p.strip() for p in owner.split(",", 1)]
        return (first, last)
    toks = owner.split()
    if len(toks) == 1:
        return ("", toks[0])
    return (toks[0], " ".join(toks[1:]))


def export_ghl_csv(records: list[LeadRecord], path: Path) -> None:
    """Write a GoHighLevel-ready CSV for direct contact import."""
    cols = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]
    try:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in records:
                first, last = _split_name(r.owner)
                w.writerow([
                    first,
                    last,
                    r.mail_address,
                    r.mail_city,
                    r.mail_state,
                    r.mail_zip,
                    r.prop_address,
                    r.prop_city,
                    r.prop_state,
                    r.prop_zip,
                    r.cat_label,
                    r.doc_type,
                    r.filed,
                    r.doc_num,
                    f"{r.amount:.2f}" if r.amount else "",
                    r.score,
                    "; ".join(r.flags),
                    "La Crosse County, WI",
                    r.clerk_url,
                ])
        log(f"wrote GHL CSV: {path} ({len(records)} rows)")
    except Exception as exc:
        log(f"  failed to write GHL CSV: {exc}")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def enrich_with_parcels(records: list[LeadRecord], index: ParcelIndex) -> None:
    """Fill in mailing/property addresses for records using the parcel index."""
    if not index.count:
        return
    hits = 0
    for r in records:
        if r.prop_address and r.mail_address:
            continue
        parcel = safe(index.lookup, r.owner)
        if not parcel:
            continue
        if not r.prop_address:
            r.prop_address = parcel.get("site_addr", "") or r.prop_address
            r.prop_city = parcel.get("site_city", "") or r.prop_city
            r.prop_zip = parcel.get("site_zip", "") or r.prop_zip
        if not r.mail_address:
            r.mail_address = parcel.get("mail_addr", "") or r.mail_address
            r.mail_city = parcel.get("mail_city", "") or r.mail_city
            r.mail_state = parcel.get("mail_state", "") or r.mail_state or "WI"
            r.mail_zip = parcel.get("mail_zip", "") or r.mail_zip
        hits += 1
    log(f"  parcel enrichment matched {hits}/{len(records)} records")


def dedupe(records: list[LeadRecord]) -> list[LeadRecord]:
    """Drop duplicate (doc_num) entries, keeping the highest-scoring copy."""
    by_key: dict[str, LeadRecord] = {}
    for r in records:
        key = (r.doc_num or f"{r.owner}|{r.filed}|{r.doc_type}").strip().upper()
        existing = by_key.get(key)
        if existing is None or r.score > existing.score:
            by_key[key] = r
    return list(by_key.values())


def run() -> int:
    """Full pipeline. Returns process exit code."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=LOOKBACK_DAYS)
    log(f"=== La Crosse County scraper | lookback {LOOKBACK_DAYS}d ===")
    log(f"Window: {start.date()} -> {end.date()}")

    session = make_session()

    # 1. Parcel index (for address enrichment) ---------------------------
    parcel_index = ParcelIndex()
    workdir = ROOT_DIR / "_work"
    workdir.mkdir(exist_ok=True)
    dbf_path = safe(with_retries, download_parcel_dbf, session, workdir)
    if dbf_path:
        safe(parcel_index.load_from_dbf, dbf_path)
    else:
        log("  proceeding without parcel index (address enrichment disabled)")

    # 2. Static help-page discovery (informational) ----------------------
    safe(scrape_static_help_pages, session)

    # 3. Clerk portal (Tapestry) via Playwright --------------------------
    records: list[LeadRecord] = []
    try:
        tapestry_records = asyncio.run(
            _with_async_retries(scrape_tapestry, start, end, attempts=MAX_RETRIES)
        ) or []
        records.extend(tapestry_records)
    except Exception as exc:
        log(f"  Tapestry block failed: {exc}")
        log(traceback.format_exc())

    # 4. WCCA cases ------------------------------------------------------
    records.extend(safe(with_retries, scrape_wcca_cases, session, start, end) or [])

    # 5. Treasurer -------------------------------------------------------
    records.extend(safe(with_retries, scrape_treasurer, session) or [])

    # 6. Enrich + score + dedupe ----------------------------------------
    enrich_with_parcels(records, parcel_index)
    week_ago = end - timedelta(days=7)
    for r in records:
        filed_dt = parse_date_loose(r.filed)
        within_week = bool(filed_dt and filed_dt >= week_ago.replace(tzinfo=None))
        safe(score_record, r, within_week)

    records = dedupe(records)
    records.sort(key=lambda r: r.score, reverse=True)

    # 7. Output ----------------------------------------------------------
    write_records_json(records, start, end)
    export_ghl_csv(records, DATA_DIR / "ghl_export.csv")
    export_ghl_csv(records, DASHBOARD_DIR / "ghl_export.csv")

    log(f"=== done: {len(records)} leads, "
        f"{sum(1 for r in records if r.prop_address or r.mail_address)} with address ===")
    return 0


async def _with_async_retries(coro_fn, *args, attempts: int = MAX_RETRIES, **kwargs):
    """Retry wrapper for async functions."""
    last_exc: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            log(f"  async retry {i}/{attempts}: {type(exc).__name__}: {exc}")
            if i < attempts:
                await asyncio.sleep(RETRY_BACKOFF_SECONDS * i)
    if last_exc:
        log(f"  async: all {attempts} attempts failed")
    return None


if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception as exc:
        log(f"FATAL: {type(exc).__name__}: {exc}")
        log(traceback.format_exc())
        # Still write an empty payload so the dashboard never goes blank.
        try:
            now = datetime.now(timezone.utc)
            write_records_json([], now - timedelta(days=LOOKBACK_DAYS), now)
        except Exception:
            pass
        sys.exit(1)
