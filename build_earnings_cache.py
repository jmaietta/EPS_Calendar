#!/usr/bin/env python3
"""
build_earnings_cache.py

Robust builder for earnings_cache.json used by the T2D Earnings Calendar.

Behavior:
- Reads tickers from eps_calendar_universe.csv
- Calls AlphaVantage EARNINGS_CALENDAR ONCE (datatype=csv)
- Validates the response hard:
    * Must have expected columns
    * Must have enough raw rows
    * Must have enough rows AFTER filtering to your universe
- ONLY if checks pass:
    * Archives previous earnings_cache.json to earnings_history/
    * Writes new earnings_cache.json
- If checks FAIL:
    * Prints clear error
    * DOES NOT touch existing earnings_cache.json

Result: front-end always sees last known-good JSON snapshot.
"""

import csv
import io
import json
import os
import sys
from datetime import datetime
from typing import List, Dict

import requests

# ---------- CONFIG ----------

UNIVERSE_CSV = "eps_calendar_universe.csv"
CACHE_JSON = "earnings_cache.json"
ARCHIVE_DIR = "earnings_history"

HORIZON = "3month"  # 3month | 6month | 12month

# AlphaVantage API key from environment
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")

ALPHA_URL_TEMPLATE = (
    "https://www.alphavantage.co/query"
    "?function=EARNINGS_CALENDAR"
    "&horizon={horizon}"
    "&apikey={api_key}"
    "&datatype=csv"
)

# Sanity thresholds (tune if you want)
MIN_RAW_ROWS = 100        # if we get less than this from AV, assume it's junk
MIN_FILTERED_ROWS = 10    # if < this in your universe, refuse to overwrite cache


# ---------- HELPERS ----------

def require_api_key() -> str:
    if not ALPHAVANTAGE_API_KEY:
        print(
            "ERROR: ALPHAVANTAGE_API_KEY is not set.\n"
            "Set it in your environment or GitHub Actions secret, e.g.:\n"
            "  export ALPHAVANTAGE_API_KEY='YOUR_KEY_HERE'\n"
        )
        sys.exit(1)
    return ALPHAVANTAGE_API_KEY


def load_universe(path: str) -> List[str]:
    """
    Load ticker universe from eps_calendar_universe.csv.
    Tries to detect 'ticker' column; otherwise uses first column.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Universe file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        raise RuntimeError("Universe CSV is empty")

    header = [c.strip().lower() for c in rows[0]]
    if "ticker" in header:
        ticker_idx = header.index("ticker")
        start = 1
    else:
        ticker_idx = 0
        start = 0

    tickers = set()
    for row in rows[start:]:
        if not row:
            continue
        t = (row[ticker_idx] or "").strip().upper()
        if not t or t in ("TICKER", "..."):
            continue
        tickers.add(t)

    if not tickers:
        raise RuntimeError("No tickers found in universe CSV")

    return sorted(tickers)


def fetch_earnings_calendar_from_api(api_key: str) -> List[Dict[str, str]]:
    """
    Call AlphaVantage EARNINGS_CALENDAR once and return rows as dicts.

    If the response looks like rate-limit / error / wrong shape,
    raise an error instead of returning junk.
    """
    url = ALPHA_URL_TEMPLATE.format(api_key=api_key, horizon=HORIZON)
    print(f"Requesting EARNINGS_CALENDAR (horizon={HORIZON}) from AlphaVantage…")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    text = resp.text.strip()
    if not text:
        raise RuntimeError("Empty response from provider.")

    # If they send JSON, it's a Note / Error, not real CSV.
    if text.startswith("{"):
        try:
            obj = json.loads(text)
        except Exception:
            raise RuntimeError("Provider returned JSON that could not be parsed.")
        msg = obj.get("Note") or obj.get("Error Message") or obj.get("Information") or text
        raise RuntimeError(f"Provider error: {msg}")

    # Parse CSV
    buffer = io.StringIO(text)
    reader = csv.DictReader(buffer)
    rows = list(reader)

    headers = [h.strip().lower() for h in (reader.fieldnames or [])]
    if "symbol" not in headers or "reportdate" not in headers:
        raise RuntimeError(
            "Provider CSV missing expected 'symbol'/'reportDate' columns. "
            "Response may be an error or format change."
        )

    if len(rows) < MIN_RAW_ROWS:
        raise RuntimeError(
            f"Provider returned too few rows ({len(rows)} < {MIN_RAW_ROWS}). "
            "Likely rate limit / partial data. Refusing to update cache."
        )

    print(f"Loaded {len(rows)} raw rows from provider.")
    return rows


def build_filtered_rows(universe: List[str], raw_rows: List[Dict[str, str]]) -> List[Dict]:
    """
    Filter provider rows down to your universe and normalize fields.
    """
    universe_set = set(universe)

    print(f"Filtering into T2D universe ({len(universe)} tickers)…")
    filtered: List[Dict] = []

    for r in raw_rows:
        symbol = (r.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        if symbol not in universe_set:
            continue

        report_date = (r.get("reportDate") or "").strip()
        if not report_date:
            continue

        name = (r.get("name") or "").strip()
        fiscal = (r.get("fiscalDateEnding") or "").strip()
        estimate = (r.get("estimate") or "").strip()
        currency = (r.get("currency") or "").strip()

        filtered.append(
            {
                "symbol": symbol,
                "name": name,
                "reportDate": report_date,
                "fiscalDateEnding": fiscal,
                "estimate": estimate if estimate != "" else None,
                "currency": currency,
            }
        )

    print(f"Filtered down to {len(filtered)} rows in your universe.")
    return filtered


def verify_sanity(raw_rows: List[Dict], filtered_rows: List[Dict]):
    """
    Decide if the data looks sane enough to overwrite the cache.
    Raise RuntimeError if not.
    """
    if len(raw_rows) < MIN_RAW_ROWS:
        raise RuntimeError(
            f"Sanity check failed: raw rows {len(raw_rows)} < {MIN_RAW_ROWS}."
        )
    if len(filtered_rows) < MIN_FILTERED_ROWS:
        raise RuntimeError(
            f"Sanity check failed: filtered rows {len(filtered_rows)} < {MIN_FILTERED_ROWS}. "
            "Refusing to overwrite cache with almost-empty universe."
        )


def archive_previous_cache(current_path: str):
    """
    If an existing earnings_cache.json is present, copy it into
    earnings_history/earnings_cache_<timestamp>.json before overwriting.
    """
    if not os.path.exists(current_path):
        return

    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    try:
        with open(current_path, "r", encoding="utf-8") as f:
            old_data = json.load(f)
    except Exception:
        with open(current_path, "r", encoding="utf-8") as f:
            old_text = f.read()
        old_data = old_text

    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_name = f"earnings_cache_{stamp}.json"
    archive_path = os.path.join(ARCHIVE_DIR, archive_name)

    with open(archive_path, "w", encoding="utf-8") as af:
        if isinstance(old_data, (dict, list)):
            json.dump(old_data, af, ensure_ascii=False)
        else:
            af.write(str(old_data))

    print(f"Archived previous cache to {archive_path}")


def write_cache_json(rows: List[Dict], path: str):
    """
    Archive previous cache (if any), then write new earnings_cache.json.
    """
    archive_previous_cache(path)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)
    print(f"Wrote {len(rows)} rows to {path}")


# ---------- MAIN ----------

def main():
    print("=== T2D earnings_cache.json builder ===")
    print(f"Universe file : {UNIVERSE_CSV}")
    print(f"Output file  : {CACHE_JSON}")
    print(f"Archive dir  : {ARCHIVE_DIR}")
    print()

    api_key = require_api_key()

    try:
        # 1) Load universe
        print("Loading universe…")
        universe = load_universe(UNIVERSE_CSV)
        print(f"Loaded universe of {len(universe)} tickers.")

        # 2) Fetch calendar once
        print("Fetching earnings calendar…")
        raw_rows = fetch_earnings_calendar_from_api(api_key)

        # 3) Filter + normalize
        filtered_rows = build_filtered_rows(universe, raw_rows)

        # 4) Sanity check before touching cache
        verify_sanity(raw_rows, filtered_rows)

        # 5) Archive previous + write JSON cache
        write_cache_json(filtered_rows, CACHE_JSON)

        print("\nDone. New cache written successfully.")

    except Exception as e:
        print("\nERROR during earnings cache build:")
        print(f"  {e}")
        if os.path.exists(CACHE_JSON):
            print("Keeping existing earnings_cache.json untouched.")
        else:
            print("No existing cache found; you'll have no data until a successful run.")
        sys.exit(1)


if __name__ == "__main__":
    main()
