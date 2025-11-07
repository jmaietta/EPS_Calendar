#!/usr/bin/env python3
"""
build_earnings_cache.py

Generates earnings_cache.json for the T2D Earnings Calendar frontend.

- Reads tickers from eps_calendar_universe.csv
- Calls AlphaVantage EARNINGS_CALENDAR ONCE
- Filters rows to your universe
- Writes earnings_cache.json with:
  [
    {
      "symbol": "AAPL",
      "name": "...",
      "reportDate": "2025-11-06",
      "fiscalDateEnding": "2025-09-30",
      "estimate": "1.23",
      "currency": "USD"
    },
    ...
  ]
"""

import csv
import io
import json
import os
import sys
from typing import List, Dict

import requests

# ---------- CONFIG ----------

UNIVERSE_CSV = "eps_calendar_universe.csv"
CACHE_JSON = "earnings_cache.json"

# AlphaVantage API key:
# Prefer environment variable; if you want, you can hard-code it here.
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")
HORIZON = "3month"  # 3month | 6month | 12month

ALPHA_URL = (
    "https://www.alphavantage.co/query"
    f"?function=EARNINGS_CALENDAR&horizon={HORIZON}"
    f"&apikey={{api_key}}&datatype=csv"
)


# ---------- HELPERS ----------

def require_api_key() -> str:
    """
    Ensure we have an API key, otherwise bail out cleanly.
    """
    if not ALPHAVANTAGE_API_KEY:
        print(
            "ERROR: ALPHAVANTAGE_API_KEY is not set.\n"
            "Set it in your environment, e.g.:\n"
            "  export ALPHAVANTAGE_API_KEY='YOUR_KEY_HERE'\n"
        )
        sys.exit(1)
    return ALPHAVANTAGE_API_KEY


def load_universe(path: str) -> List[str]:
    """
    Load ticker universe from eps_calendar_universe.csv.
    Tries to detect a 'ticker' column; otherwise uses first column.
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


def fetch_earnings_calendar(api_key: str) -> List[Dict[str, str]]:
    """
    Call AlphaVantage EARNINGS_CALENDAR once and return rows as dicts.
    """
    url = ALPHA_URL.format(api_key=api_key)
    print(f"Requesting EARNINGS_CALENDAR (horizon={HORIZON})…")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    text = resp.text.strip()
    if not text:
        print("Empty response from provider.")
        return []

    # If they send a JSON error / rate limit note, surface it
    if text.startswith("{"):
        try:
            obj = json.loads(text)
        except Exception:
            raise RuntimeError("Provider returned JSON, but it could not be parsed.")
        msg = obj.get("Note") or obj.get("Error Message") or obj.get("Information") or text
        raise RuntimeError(f"Provider error: {msg}")

    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def build_filtered_rows(universe: List[str], raw_rows: List[Dict[str, str]]) -> List[Dict]:
    """
    Filter AlphaVantage rows down to your universe and normalize fields.
    """
    universe_set = set(universe)

    print(f"Received {len(raw_rows)} rows from provider.")
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


def write_cache_json(rows: List[Dict], path: str):
    """
    Write rows to earnings_cache.json for the frontend.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)
    print(f"Wrote {len(rows)} rows to {path}")


# ---------- MAIN ----------

def main():
    print("=== T2D earnings_cache.json builder ===")
    print(f"Universe file: {UNIVERSE_CSV}")
    print(f"Output file : {CACHE_JSON}")
    print()

    api_key = require_api_key()

    # 1) Load universe
    print("Loading universe…")
    universe = load_universe(UNIVERSE_CSV)
    print(f"Loaded universe of {len(universe)} tickers.")

    # 2) Fetch calendar once
    print("Fetching earnings calendar…")
    raw_rows = fetch_earnings_calendar(api_key)

    # 3) Filter + normalize
    rows = build_filtered_rows(universe, raw_rows)

    # 4) Write JSON cache
    write_cache_json(rows, CACHE_JSON)

    print("\nDone. Place earnings_cache.json next to index.html on your host.")


if __name__ == "__main__":
    main()
