"""
universe_builder.py — Build tradable universe filtered by market cap.

Pipeline (per user spec):
  1. Read tickers from listing CSVs (NASDAQ / NYSE / other).
  2. Drop obvious non-common tickers (warrants, units, rights, preferreds, when-issued).
  3. Fetch market cap via yfinance.
  4. Mark rows where market_cap >= --min-market-cap (default 500,000,000 USD).
  5. Save to Parquet + CSV with columns:
       ticker, company, exchange, market_cap, pass_market_cap_filter, universe_timestamp

NOTE: This is the ONLY hard universe filter. Price (>$5) and average dollar volume
($10M) are NOT applied here — scanner downstream may surface those as informational
metrics, not as gating filters.

Usage:
  python universe_builder.py
  python universe_builder.py --max-tickers 50         # smoke test
  python universe_builder.py --min-market-cap 1e9
  python universe_builder.py --output data/tradable_universe.parquet
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import time
import warnings
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

warnings.filterwarnings("ignore")


# ── Listing CSV readers ──────────────────────────────────────────────────────

# Tokens that mark non-common-stock instruments (warrants, units, rights,
# preferreds, when-issued, etc.). Matched against the security/company name
# in a case-insensitive way.
NON_COMMON_NAME_TOKENS = (
    " warrant",
    " warrants",
    " right",
    " rights",
    " unit",
    " units",
    " when issued",
    " when-issued",
    " preferred",
    " depositary",  # ADR/ADS often kept; we only drop "preferred depositary"
    " notes",
    " bond",
    " trust preferred",
)

# Suffix letters commonly used by NASDAQ/NYSE for warrants, rights, units,
# preferreds. We drop tickers ending with these letters when length > 4.
NON_COMMON_SUFFIXES = ("W", "R", "U", "P")


def _looks_non_common(ticker: str, name: str) -> bool:
    """Heuristic filter for warrants / units / preferred / rights."""
    t = (ticker or "").upper().strip()
    n = (name or "").lower()

    # Name-based
    for tok in NON_COMMON_NAME_TOKENS:
        if tok in n:
            # "American Depositary Shares" should NOT be filtered — only drop
            # entries that explicitly say preferred / warrant / etc.
            if tok == " depositary" and "preferred" not in n:
                continue
            return True

    # Common-stock tickers are usually 1–4 letters. NASDAQ uses 5-letter
    # extensions: trailing W=warrant, R=right, U=unit, P=preferred.
    if len(t) >= 5 and t[-1] in NON_COMMON_SUFFIXES:
        return True
    # Dot-class suffixes like ".W", ".U" are also non-common (after we
    # canonicalise dashes to dots they may show up here).
    if "." in t:
        suf = t.split(".")[-1]
        if suf in {"W", "WS", "U", "R", "P", "PR"}:
            return True
    return False


def _read_nasdaq(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "Symbol" not in df.columns:
        return pd.DataFrame()
    out = pd.DataFrame({
        "ticker":   df["Symbol"].astype(str).str.strip().str.upper(),
        "company":  df.get("Company Name", df.get("Security Name", "")).astype(str).str.strip(),
        "exchange": "NASDAQ",
    })
    # Drop test issues if column exists
    if "Test Issue" in df.columns:
        out = out[df["Test Issue"].astype(str).str.upper() != "Y"]
    # Drop obvious ETFs to keep universe focused on equities
    if "ETF" in df.columns:
        out = out[df["ETF"].astype(str).str.upper() != "Y"]
    # Combine name with security name for non-common detection
    sec = df.get("Security Name", df.get("Company Name", "")).astype(str)
    out["_secname"] = sec.values[: len(out)] if len(sec) >= len(out) else sec
    return out


def _read_nyse(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    sym_col = "ACT Symbol" if "ACT Symbol" in df.columns else df.columns[0]
    name_col = "Company Name" if "Company Name" in df.columns else df.columns[1]
    out = pd.DataFrame({
        "ticker":   df[sym_col].astype(str).str.strip().str.upper(),
        "company":  df[name_col].astype(str).str.strip(),
        "exchange": "NYSE",
    })
    out["_secname"] = out["company"]
    return out


def _read_other(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    sym_col = "ACT Symbol" if "ACT Symbol" in df.columns else df.columns[0]
    name_col = "Company Name" if "Company Name" in df.columns else df.columns[1]
    sec_col = "Security Name" if "Security Name" in df.columns else name_col
    out = pd.DataFrame({
        "ticker":   df[sym_col].astype(str).str.strip().str.upper(),
        "company":  df[name_col].astype(str).str.strip(),
        # Exchange code: N=NYSE, A=NYSE American, P=NYSE Arca, Z=BATS, V=IEX
        "exchange": df.get("Exchange", "OTHER").astype(str),
    })
    if "ETF" in df.columns:
        out = out[df["ETF"].astype(str).str.upper() != "Y"]
    if "Test Issue" in df.columns:
        out = out[df["Test Issue"].astype(str).str.upper() != "Y"]
    out["_secname"] = df[sec_col].astype(str).values[: len(out)]
    return out


READERS = {
    "nasdaq-listed.csv": _read_nasdaq,
    "nyse-listed.csv":   _read_nyse,
    "other-listed.csv":  _read_other,
}


def load_listings(input_files: List[str]) -> pd.DataFrame:
    """Load and clean listing CSVs into a unified frame."""
    frames: List[pd.DataFrame] = []
    for fname in input_files:
        p = Path(fname)
        if not p.exists():
            print(f"  [WARN] Listing file not found, skipping: {p}", flush=True)
            continue
        reader = None
        for key, fn in READERS.items():
            if p.name.lower() == key:
                reader = fn
                break
        if reader is None:
            print(f"  [WARN] No reader for {p.name} — using generic first-col reader.", flush=True)
            df = pd.read_csv(p)
            sym_col = df.columns[0]
            name_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
            df = pd.DataFrame({
                "ticker":   df[sym_col].astype(str).str.strip().str.upper(),
                "company":  df[name_col].astype(str).str.strip(),
                "exchange": "UNKNOWN",
            })
            df["_secname"] = df["company"]
            frames.append(df)
            continue
        try:
            df = reader(p)
            print(f"  Loaded {len(df):,} rows from {p.name}", flush=True)
            frames.append(df)
        except Exception as e:
            print(f"  [ERROR] Failed reading {p}: {e}", flush=True)

    if not frames:
        return pd.DataFrame(columns=["ticker", "company", "exchange", "_secname"])

    df = pd.concat(frames, ignore_index=True)

    # Drop blanks / placeholders
    df = df[df["ticker"].str.len() > 0]
    df = df[~df["ticker"].isin({"SYMBOL", "TICKER", "NAN", "FILE CREATION TIME"})]

    # Drop obvious non-common
    mask = df.apply(lambda r: _looks_non_common(r["ticker"], r["_secname"]), axis=1)
    n_dropped = int(mask.sum())
    df = df[~mask]

    # Some tickers contain "$" or "/" — drop those too
    df = df[~df["ticker"].str.contains(r"[\$/]", regex=True, na=False)]

    # yfinance uses "-" for share classes (e.g., BRK.B → BRK-B)
    df["ticker"] = df["ticker"].str.replace(".", "-", regex=False)

    # Dedup, prefer first occurrence (NASDAQ before NYSE before other due to
    # input order). Keep highest-priority exchange.
    df = df.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
    df = df.drop(columns=["_secname"], errors="ignore")

    print(f"  After cleaning: {len(df):,} tickers ({n_dropped:,} non-common dropped)", flush=True)
    return df


# ── Market cap fetch ─────────────────────────────────────────────────────────

def fetch_market_caps(tickers: List[str], chunk_size: int = 50) -> pd.Series:
    """Fetch market cap for each ticker via yfinance. Returns Series indexed by ticker."""
    import yfinance as yf

    caps: dict = {}
    n = len(tickers)
    print(f"  Fetching market cap for {n:,} tickers (chunk={chunk_size})...", flush=True)

    for start in range(0, n, chunk_size):
        chunk = tickers[start:start + chunk_size]
        try:
            tk = yf.Tickers(" ".join(chunk))
            for t in chunk:
                mc: Optional[float] = None
                try:
                    info = getattr(tk.tickers.get(t), "fast_info", None)
                    if info is not None:
                        mc = info.get("market_cap") if hasattr(info, "get") else getattr(info, "market_cap", None)
                except Exception:
                    mc = None
                # Fallback to slower .info
                if not mc:
                    try:
                        info_slow = tk.tickers[t].info or {}
                        mc = info_slow.get("marketCap")
                    except Exception:
                        mc = None
                caps[t] = float(mc) if mc else None
        except Exception as e:
            print(f"    [WARN] chunk {start//chunk_size + 1} failed: {e}", flush=True)
            for t in chunk:
                caps.setdefault(t, None)

        done = min(start + chunk_size, n)
        if done % (chunk_size * 4) == 0 or done == n:
            ok = sum(1 for v in caps.values() if v)
            print(f"    Progress {done}/{n} — caps captured: {ok}", flush=True)
        time.sleep(0.2)

    return pd.Series(caps, name="market_cap")


# ── Main pipeline ────────────────────────────────────────────────────────────

def build_universe(
    min_market_cap: float,
    output_path: str,
    input_files: List[str],
    max_tickers: Optional[int] = None,
) -> pd.DataFrame:
    print(f"\n{'='*60}", flush=True)
    print(f"  Universe Builder — Tradable Universe", flush=True)
    print(f"  時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"  Min market cap: ${min_market_cap:,.0f}", flush=True)
    print(f"  Output: {output_path}", flush=True)
    print(f"{'='*60}\n", flush=True)

    print("Step 1/3: 讀取上市清單...", flush=True)
    listings = load_listings(input_files)
    if listings.empty:
        print("  [ERROR] 沒有任何上市清單可用。", flush=True)
        return pd.DataFrame()

    if max_tickers and max_tickers > 0:
        listings = listings.head(max_tickers).reset_index(drop=True)
        print(f"  [SMOKE] limiting to first {max_tickers} tickers", flush=True)

    print(f"\nStep 2/3: 取得市值 (yfinance)...", flush=True)
    caps = fetch_market_caps(listings["ticker"].tolist())

    df = listings.copy()
    df["market_cap"] = df["ticker"].map(caps).astype("float64")
    df["pass_market_cap_filter"] = df["market_cap"].fillna(0) >= float(min_market_cap)
    df["universe_timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = df[["ticker", "company", "exchange", "market_cap",
             "pass_market_cap_filter", "universe_timestamp"]]

    n_pass = int(df["pass_market_cap_filter"].sum())
    n_total = len(df)
    n_known_cap = int(df["market_cap"].notna().sum())
    print(f"\nStep 3/3: 儲存結果...", flush=True)
    print(f"  Total rows:           {n_total:,}", flush=True)
    print(f"  Market cap captured:  {n_known_cap:,}", flush=True)
    print(f"  Pass >= ${min_market_cap:,.0f}: {n_pass:,}", flush=True)

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        df.to_parquet(str(out_path), index=False)
        print(f"  ✅ Parquet saved: {out_path}", flush=True)
    except Exception as e:
        print(f"  [WARN] Parquet save failed ({e})", flush=True)

    csv_path = out_path.with_suffix(".csv")
    try:
        df.to_csv(str(csv_path), index=False)
        print(f"  ✅ CSV saved:     {csv_path}", flush=True)
    except Exception as e:
        print(f"  [WARN] CSV save failed: {e}", flush=True)

    print(f"\n{'='*60}\n", flush=True)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build tradable universe filtered by market cap (>= 500M default).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--min-market-cap",
        type=float,
        default=500_000_000,
        help="Minimum market cap in USD (default: 500_000_000).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/tradable_universe.parquet",
        help="Output path (default: data/tradable_universe.parquet).",
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=0,
        help="Smoke-test cap on input listings before yfinance fetch (0 = no cap).",
    )
    parser.add_argument(
        "--input-files",
        type=str,
        nargs="*",
        default=None,
        help="Listing CSVs to read. Default: nasdaq-listed.csv nyse-listed.csv other-listed.csv (if present).",
    )

    args = parser.parse_args()

    if args.input_files:
        input_files = args.input_files
    else:
        candidates = ["nasdaq-listed.csv", "nyse-listed.csv", "other-listed.csv"]
        input_files = [c for c in candidates if Path(c).exists()]
        if not input_files:
            print("[ERROR] No default listing CSVs found in cwd.", flush=True)
            sys.exit(1)

    build_universe(
        min_market_cap=args.min_market_cap,
        output_path=args.output,
        input_files=input_files,
        max_tickers=args.max_tickers if args.max_tickers > 0 else None,
    )


if __name__ == "__main__":
    main()
