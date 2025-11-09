#!/usr/bin/env python3
"""
backfill_earnings_history.py

One-time (or occasional) backfill for T2D Earnings Calendar, using ONE
AlphaVantage EARNINGS_CALENDAR call.

Behavior:
- Reads tickers from eps_calendar_universe.csv (your universe).
- Calls EARNINGS_CALENDAR once with horizon=3month.
- Filters rows to only symbols in your universe.
- Groups by reportDate.
- For reportDate within the last 30 calendar days (inclusive), writes:
      earnings_history/earnings_YYYY-MM-DD.json
  if that file does NOT already exist.

Does NOT touch earnings_cache.json or earnings_latest.json.
"""

import csv
import io
import json
import os
import sys
from datetime import datetime, date, timedelta
from typing import List, Dict

import requests

UNIVERSE_CSV = "eps_calendar_universe.csv"
HISTORY_DIR = "earnings_history"

# You asked for a 30-day window:
DAYS_TO_BACKFILL = 30

# Still just one API call; 3 months of data is plenty for 30 days:
HORIZON = "3month"  # valid: 3month | 6month | 12month

ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")

ALPHA_URL_TEMPLATE = (
    "https://www.alphavantage.co/query"
    "?function=EARNINGS_CALENDAR"
    "&horizon={horizon}"
    "&apikey={api_key}"
    "&datatype=csv"
)


def require_api_key() -> str:
    if not ALPHAVANTAGE_API_KEY:
        print(
            "ERROR: ALPHAVANTAGE_API_KEY is not set.\n"
            "Set it in your environment, e.g.:\n"
            "  export ALPHAVANTAGE_API_KEY='YOUR_KEY_HERE'\n"
        )
        sys.exit(1)
    return ALPHAVANTAGE_API_KEY


def load_universe(path: str) -> List[str]:
    """Load tickers from eps_calendar_universe.csv."""
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


def fetch_calendar_once(api_key: str) -> List[Dict[str, str]]:
    """One EARNINGS_CALENDAR call → list of row dicts."""
    url = ALPHA_URL_TEMPLATE.format(api_key=api_key, horizon=HORIZON)
    print(f"Requesting EARNINGS_CALENDAR (horizon={HORIZON}) from AlphaVantage…")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    text = resp.text.strip()
    if not text:
        raise RuntimeError("Empty response from provider.")

    # If it's JSON, it's an error/note, not the CSV we want.
    if text.startswith("{"):
        obj = json.loads(text)
        msg = obj.get("Note") or obj.get("Error Message") or obj.get("Information") or text
        raise RuntimeError(f"Provider error: {msg}")

    buffer = io.StringIO(text)
    reader = csv.DictReader(buffer)
    rows = list(reader)

    headers = [h.strip().lower() for h in (reader.fieldnames or [])]
    if "symbol" not in headers or "reportdate" not in headers:
        raise RuntimeError("CSV missing 'symbol'/'reportDate' columns.")

    print(f"Loaded {len(rows)} raw rows from provider.")
    return rows


def filter_to_universe(universe: List[str], raw_rows: List[Dict[str, str]]) -> List[Dict]:
    """Keep only rows whose symbol is in your universe."""
    universe_set = set(universe)
    print(f"Filtering into T2D universe ({len(universe)} tickers)…")

    out: List[Dict] = []
    for r in raw_rows:
        symbol = (r.get("symbol") or "").strip().upper()
        if not symbol or symbol not in universe_set:
            continue

        report_date = (r.get("reportDate") or "").strip()
        if not report_date:
            continue

        rec = {
            "symbol": symbol,
            "name": (r.get("name") or "").strip(),
            "reportDate": report_date,
            "fiscalDateEnding": (r.get("fiscalDateEnding") or "").strip(),
            "estimate": (r.get("estimate") or "").strip() or None,
            "currency": (r.get("currency") or "").strip(),
        }
        out.append(rec)

    print(f"Filtered down to {len(out)} rows in your universe.")
    return out


def group_by_date(rows: List[Dict]) -> Dict[str, List[Dict]]:
    grouped: Dict[str, List[Dict]] = {}
    for r in rows:
        d = r["reportDate"]
        grouped.setdefault(d, []).append(r)
    return grouped


def backfill_history(grouped: Dict[str, List[Dict]]):
    """
    For each reportDate within [today - 30 days, today], write:
      earnings_history/earnings_YYYY-MM-DD.json
    if it does NOT already exist.
    """
    os.makedirs(HISTORY_DIR, exist_ok=True)

    today = date.today()
    cutoff = today - timedelta(days=DAYS_TO_BACKFILL)

    created = 0
    skipped_existing = 0
    skipped_out_of_range = 0

    for date_str, rows in grouped.items():
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            print(f"Skipping invalid reportDate: {date_str!r}")
            continue

        if d > today or d < cutoff:
            skipped_out_of_range += 1
            continue

        out_path = os.path.join(HISTORY_DIR, f"earnings_{date_str}.json")

        if os.path.exists(out_path):
            print(f"Already exists, skipping: {out_path}")
            skipped_existing += 1
            continue

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)

        print(f"Wrote {len(rows)} rows -> {out_path}")
        created += 1

    print("\nBackfill summary:")
    print(f"  New history files created : {created}")
    print(f"  Existing files skipped    : {skipped_existing}")
    print(f"  Outside 30-day window     : {skipped_out_of_range}")


def main():
    print("=== T2D earnings_history backfill (30-day window) ===")
    print(f"Universe file     : {UNIVERSE_CSV}")
    print(f"History directory : {HISTORY_DIR}")
    print(f"Days to backfill  : {DAYS_TO_BACKFILL}")
    print(f"Horizon (API)     : {HORIZON}")
    print()

    api_key = require_api_key()

    # 1) Universe
    print("Loading universe…")
    universe = load_universe(UNIVERSE_CSV)
    print(f"Loaded {len(universe)} tickers.")

    # 2) ONE API call to AlphaVantage
    print("Fetching calendar from AlphaVantage (single call)…")
    raw_rows = fetch_calendar_once(api_key)

    # 3) Filter down to your tickers
    filtered = filter_to_universe(universe, raw_rows)

    # 4) Group by reportDate & write history files
    grouped = group_by_date(filtered)
    backfill_history(grouped)

    print("\nDone. You can now commit the new JSON files in earnings_history/.")


if __name__ == "__main__":
    main()
