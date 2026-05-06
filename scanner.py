"""
scanner.py — 每日盤後預掃描器 (Fast Mode Pre-Scanner)
======================================================
獨立運行，將掃描結果存入 Parquet / CSV，讓 Streamlit 只需讀取快取。

Universe loading priority (when no --tickers / --universe-file given):
  1. data/tradable_universe.parquet (or .csv) — built by universe_builder.py.
     This is the canonical universe: market cap >= 500M USD only.
     Price (>$5) and avg dollar volume ($10M) are NOT hard filters here.
  2. Fallback DEFAULT_TICKERS curated list.

There is no shortlist cap of 100 — every qualifying ticker is scanned. UI
display row limits are display-only and must be labelled as such.

Usage examples:
  python scanner.py
  python scanner.py --tickers "AAPL,MSFT,NVDA,SPY"
  python scanner.py --universe-file tickers.csv --benchmark SPY --max-tickers 500
  python scanner.py --tickers "AAPL,MSFT,SPY" --max-tickers 3 --output data/test_scan.parquet

CLI Arguments:
  --tickers         Comma-separated ticker list
  --universe-file   CSV file with ticker column (first col or 'ticker'/'symbol')
  --output          Output path (default: data/latest_scan.parquet)
  --benchmark       Benchmark ticker (default: SPY)
  --max-tickers     Optional safety limit; 0 = no limit (default 0)
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
import time
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

warnings.filterwarnings("ignore")

# ── Default curated universe ──────────────────────────────────────────────────
DEFAULT_TICKERS: List[str] = [
    # Mega-cap / market movers
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO", "AMD",
    "JPM", "V", "MA", "UNH", "HD", "COST", "NFLX", "LLY", "ABBV",
    # AI / Cloud
    "PLTR", "AI", "SOUN", "BBAI", "SNOW", "DDOG", "NET", "CRM", "NOW",
    "MSFT", "GOOGL", "AMZN", "ORCL",
    # Semiconductors
    "AMAT", "ARM", "QCOM", "INTC", "MU", "KLAC", "LRCX", "ASML",
    # Cybersecurity
    "CRWD", "PANW", "ZS", "OKTA", "FTNT", "CYBR", "S",
    # Energy / Clean
    "CEG", "VST", "CCJ", "OKLO", "NNE", "ENPH", "NEE",
    # Space / Defense
    "RKLB", "ASTS", "KTOS", "LMT", "NOC", "RTX", "BA",
    # Biotech
    "MRNA", "GILD", "REGN", "VRTX", "CRSP", "RXRX", "ILMN",
    # Finance
    "GS", "BAC", "BRK-B", "C", "MS", "SPGI", "CME", "MCO",
    # Consumer / Retail
    "AMZN", "WMT", "TGT", "TSLA", "NKE", "DIS", "SBUX",
    # ETFs for reference
    "SPY", "QQQ", "IWM", "DIA", "SOXX", "CIBR", "AIQ", "XBI", "NLR", "ITA",
]
# Deduplicate while preserving order
_seen: set = set()
DEFAULT_TICKERS = [t for t in DEFAULT_TICKERS if not (_seen.add(t) or t in _seen - {t})]

# RS status thresholds
RS_LABELS = {
    "OUTPERFORM": "跑贏指數",
    "NEAR_BREAK": "接近突破",
    "TURN_UP":    "剛轉強",
    "LAGGING":    "跑輸指數",
}

VOLUME_SIGNAL_LABELS = {
    "BREAKOUT":  "放量突破",
    "PULLBACK":  "縮量回踩",
    "DIVERGE":   "量價背馳",
    "NORMAL":    "正常量能",
}

SETUP_TYPE_LABELS = {
    "HOT_MOMENTUM":  "強勢延續",
    "NEAR_BREAKOUT": "接近爆發",
    "LOW_RISK":      "低風險回踩",
    "EARLY_TURN":    "早期轉強",
    "OVEREXTENDED":  "過度延伸",
    "LAGGING":       "跑輸觀望",
}


# ── Utility ──────────────────────────────────────────────────────────────────

def _safe_pct(a: float, b: float) -> Optional[float]:
    """Return (a-b)/b * 100, or None if invalid."""
    try:
        if b == 0 or pd.isna(b) or pd.isna(a):
            return None
        return round((a - b) / abs(b) * 100, 2)
    except Exception:
        return None


def _ytd_return(series: pd.Series) -> Optional[float]:
    """Return YTD % change from the first trading day of the current year."""
    try:
        year_start = datetime.date(datetime.date.today().year, 1, 1)
        series = series.dropna()
        idx = series.index
        if hasattr(idx, "date"):
            mask = idx.date >= year_start
        else:
            mask = pd.to_datetime(idx).date >= year_start
        ytd_s = series[mask]
        if len(ytd_s) < 2:
            return None
        return round((float(ytd_s.iloc[-1]) - float(ytd_s.iloc[0])) / abs(float(ytd_s.iloc[0])) * 100, 2)
    except Exception:
        return None


def _classify_rs(rel_5d: Optional[float], rel_1m: Optional[float]) -> str:
    """Classify RS status from 5D and 1M relative returns vs benchmark."""
    r5 = rel_5d if rel_5d is not None else 0.0
    r1m = rel_1m if rel_1m is not None else 0.0
    if r1m >= 2 and r5 >= 0:
        return RS_LABELS["OUTPERFORM"]
    elif r1m >= -1 or r5 >= 1:
        return RS_LABELS["NEAR_BREAK"]
    elif r5 >= 0.5:
        return RS_LABELS["TURN_UP"]
    else:
        return RS_LABELS["LAGGING"]


def _classify_volume(price: float, prev_price: float, vol: float, avg_vol: float) -> str:
    price_up = price > prev_price
    vol_ratio = vol / avg_vol if avg_vol > 0 else 1.0
    if vol_ratio >= 1.5 and price_up:
        return VOLUME_SIGNAL_LABELS["BREAKOUT"]
    elif vol_ratio <= 0.7 and not price_up:
        return VOLUME_SIGNAL_LABELS["PULLBACK"]
    elif vol_ratio <= 0.7 and price_up:
        return VOLUME_SIGNAL_LABELS["DIVERGE"]
    else:
        return VOLUME_SIGNAL_LABELS["NORMAL"]


def _classify_setup(
    rs_status: str,
    dist_ma20: Optional[float],
    dist_ma50: Optional[float],
    dist_20d_high: Optional[float],
    vol_signal: str,
    rel_5d: Optional[float],
) -> str:
    d20 = dist_ma20 if dist_ma20 is not None else 0.0
    d20h = dist_20d_high if dist_20d_high is not None else 0.0
    r5 = rel_5d if rel_5d is not None else 0.0

    # Over-extended
    if d20 > 12:
        return SETUP_TYPE_LABELS["OVEREXTENDED"]

    # Hot momentum
    if rs_status == RS_LABELS["OUTPERFORM"] and -5 <= d20 <= 12:
        return SETUP_TYPE_LABELS["HOT_MOMENTUM"]

    # Near breakout
    if rs_status in (RS_LABELS["OUTPERFORM"], RS_LABELS["NEAR_BREAK"]) and r5 >= -1:
        return SETUP_TYPE_LABELS["NEAR_BREAKOUT"]

    # Low-risk pullback
    if rs_status in (RS_LABELS["NEAR_BREAK"], RS_LABELS["TURN_UP"]) and -8 <= d20 <= 4 and d20h < -3:
        return SETUP_TYPE_LABELS["LOW_RISK"]

    # Early turn
    if rs_status == RS_LABELS["TURN_UP"]:
        return SETUP_TYPE_LABELS["EARLY_TURN"]

    return SETUP_TYPE_LABELS["LAGGING"]


# ── Core scanning functions ───────────────────────────────────────────────────

def fetch_benchmark_returns(benchmark: str = "SPY") -> Dict[str, Optional[float]]:
    """Fetch benchmark 5D/1M/3M/YTD returns. Returns dict with keys ret_5d, ret_1m, ret_3m, ret_ytd."""
    import yfinance as yf
    result: Dict[str, Optional[float]] = {
        "ret_5d": None, "ret_1m": None, "ret_3m": None, "ret_ytd": None
    }
    try:
        df = yf.download(benchmark, period="1y", progress=False, auto_adjust=True)
        if df.empty:
            return result
        closes = df["Close"].squeeze().dropna()
        if len(closes) < 2:
            return result
        p = float(closes.iloc[-1])
        n = len(closes)

        def _n_ago(days: int) -> Optional[float]:
            idx = max(0, n - days - 1)
            b = float(closes.iloc[idx])
            return round((p - b) / abs(b) * 100, 2) if b != 0 else None

        result["ret_5d"]  = _n_ago(5)
        result["ret_1m"]  = _n_ago(21)
        result["ret_3m"]  = _n_ago(63)
        result["ret_ytd"] = _ytd_return(closes)
    except Exception as e:
        print(f"  [WARN] Benchmark {benchmark} fetch error: {e}")
    return result


def scan_chunk(
    tickers: List[str],
    benchmark_returns: Dict[str, Optional[float]],
    benchmark: str,
    chunk_idx: int,
    total_chunks: int,
) -> List[Dict]:
    """Download price+volume data for one chunk, compute all signals. Returns list of row dicts."""
    import yfinance as yf

    rows: List[Dict] = []
    if not tickers:
        return rows

    print(f"  Chunk {chunk_idx}/{total_chunks}: downloading {len(tickers)} tickers...", flush=True)

    try:
        raw = yf.download(
            tickers if len(tickers) > 1 else tickers[0],
            period="1y",
            progress=False,
            auto_adjust=True,
            group_by="column",
        )
    except Exception as e:
        print(f"  [ERROR] Chunk {chunk_idx} download failed: {e}", flush=True)
        return rows

    if raw is None or raw.empty:
        print(f"  [WARN] Chunk {chunk_idx}: empty data returned", flush=True)
        return rows

    # Normalise multi-level columns vs single ticker
    has_multi = isinstance(raw.columns, pd.MultiIndex)

    def _get_col(col_name: str, ticker: str) -> pd.Series:
        try:
            if has_multi:
                return raw[col_name][ticker].dropna()
            else:
                # single ticker: columns are just OHLCV
                return raw[col_name].dropna()
        except Exception:
            return pd.Series(dtype=float)

    scan_ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for ticker in tickers:
        try:
            closes = _get_col("Close", ticker)
            volumes = _get_col("Volume", ticker)

            if len(closes) < 22:
                continue  # Not enough history

            # ── Prices ────────────────────────────────────────────────
            price = float(closes.iloc[-1])
            n = len(closes)

            def _pct_ago(days: int) -> Optional[float]:
                idx = max(0, n - days - 1)
                b = float(closes.iloc[idx])
                return _safe_pct(price, b)

            ret_5d  = _pct_ago(5)
            ret_1m  = _pct_ago(21)
            ret_3m  = _pct_ago(63)
            ret_ytd = _ytd_return(closes)

            # ── Relative returns (vs benchmark) ───────────────────────
            b5  = benchmark_returns.get("ret_5d")
            b1m = benchmark_returns.get("ret_1m")
            b3m = benchmark_returns.get("ret_3m")

            rel_5d  = round(ret_5d  - b5,  2) if ret_5d  is not None and b5  is not None else None
            rel_1m  = round(ret_1m  - b1m, 2) if ret_1m  is not None and b1m is not None else None
            rel_3m  = round(ret_3m  - b3m, 2) if ret_3m  is not None and b3m is not None else None

            rs_status = _classify_rs(rel_5d, rel_1m)

            # ── Moving averages ───────────────────────────────────────
            ma20 = float(closes.rolling(20).mean().iloc[-1]) if n >= 20 else None
            ma50 = float(closes.rolling(50).mean().iloc[-1]) if n >= 50 else None

            dist_ma20 = _safe_pct(price, ma20) if ma20 else None
            dist_ma50 = _safe_pct(price, ma50) if ma50 else None

            # Distance from 20-day high
            high_20 = float(closes.iloc[-20:].max()) if n >= 20 else price
            dist_20d_high = _safe_pct(price, high_20)

            # ── Volume ────────────────────────────────────────────────
            latest_volume = int(volumes.iloc[-1]) if len(volumes) >= 1 else 0
            avg20_volume  = float(volumes.iloc[-20:].mean()) if len(volumes) >= 20 else latest_volume
            volume_ratio  = round(latest_volume / avg20_volume, 2) if avg20_volume > 0 else 1.0

            prev_price = float(closes.iloc[-2]) if n >= 2 else price
            volume_signal = _classify_volume(price, prev_price, latest_volume, avg20_volume)

            # ── Volatility ────────────────────────────────────────────
            daily_rets = closes.pct_change().dropna()
            vol_20d = round(float(daily_rets.iloc[-20:].std()) * (252 ** 0.5) * 100, 2) if len(daily_rets) >= 20 else None

            # ── Setup classification ──────────────────────────────────
            setup_type = _classify_setup(rs_status, dist_ma20, dist_ma50, dist_20d_high, volume_signal, rel_5d)

            rows.append({
                "ticker":                ticker,
                "latest_price":          round(price, 4),
                "ret_5d":                ret_5d,
                "ret_1m":                ret_1m,
                "ret_3m":                ret_3m,
                "ret_ytd":               ret_ytd,
                "bench_ret_5d":          benchmark_returns.get("ret_5d"),
                "bench_ret_1m":          benchmark_returns.get("ret_1m"),
                "bench_ret_3m":          benchmark_returns.get("ret_3m"),
                "bench_ret_ytd":         benchmark_returns.get("ret_ytd"),
                "rel_5d":                rel_5d,
                "rel_1m":                rel_1m,
                "rel_3m":                rel_3m,
                "rs_status":             rs_status,
                "ma20":                  round(ma20, 4) if ma20 else None,
                "ma50":                  round(ma50, 4) if ma50 else None,
                "distance_to_ma20_pct":  dist_ma20,
                "distance_to_ma50_pct":  dist_ma50,
                "distance_from_20d_high_pct": dist_20d_high,
                "latest_volume":         latest_volume,
                "avg20_volume":          round(avg20_volume, 0),
                "volume_ratio":          volume_ratio,
                "volume_signal":         volume_signal,
                "volatility_20d":        vol_20d,
                "setup_type":            setup_type,
                "scan_timestamp":        scan_ts,
                "benchmark":             benchmark,
            })

        except Exception as e:
            print(f"    [SKIP] {ticker}: {e}", flush=True)
            continue

    print(f"  Chunk {chunk_idx}/{total_chunks}: {len(rows)}/{len(tickers)} tickers processed.", flush=True)
    return rows


def run_scan(
    tickers: List[str],
    benchmark: str = "SPY",
    output_path: str = "data/latest_scan.parquet",
    max_tickers: int = 0,
    chunk_size: int = 200,
) -> pd.DataFrame:
    """Main scan runner. Downloads, computes signals, saves output. Returns DataFrame."""

    # Apply safety limit
    if max_tickers and max_tickers > 0:
        tickers = tickers[:max_tickers]

    print(f"\n{'='*60}", flush=True)
    print(f"  美股預掃描器 — Fast Mode Pre-Scanner", flush=True)
    print(f"  時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"  股票數量: {len(tickers)}, 基準: {benchmark}", flush=True)
    print(f"  輸出: {output_path}", flush=True)
    print(f"{'='*60}\n", flush=True)

    # 1. Benchmark returns
    print("Step 1/3: 取得基準指數表現...", flush=True)
    bench_returns = fetch_benchmark_returns(benchmark)
    print(f"  {benchmark}: 5D={bench_returns['ret_5d']}, 1M={bench_returns['ret_1m']}, "
          f"3M={bench_returns['ret_3m']}, YTD={bench_returns['ret_ytd']}", flush=True)

    # 2. Chunk scanning
    print(f"\nStep 2/3: 分批掃描 {len(tickers)} 隻股票 (chunk={chunk_size})...", flush=True)
    all_rows: List[Dict] = []
    chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]

    for idx, chunk in enumerate(chunks, 1):
        chunk_rows = scan_chunk(chunk, bench_returns, benchmark, idx, len(chunks))
        all_rows.extend(chunk_rows)
        if len(chunks) > 1:
            time.sleep(0.5)  # polite pause between chunks

    # 3. Build and save DataFrame
    print(f"\nStep 3/3: 整理並儲存結果...", flush=True)
    if not all_rows:
        print("  [WARN] 無任何結果，退出。", flush=True)
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Sort: hot momentum first
    setup_order = {v: i for i, v in enumerate(SETUP_TYPE_LABELS.values())}
    df["_sort"] = df["setup_type"].map(setup_order).fillna(99)
    df = df.sort_values(["_sort", "rel_1m"], ascending=[True, False]).drop(columns=["_sort"])
    df = df.reset_index(drop=True)

    # Ensure output directory exists
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Save Parquet (preferred) + CSV fallback
    saved_parquet = False
    saved_csv = False

    try:
        df.to_parquet(str(out_path), index=False)
        saved_parquet = True
        print(f"  ✅ Parquet 已儲存: {out_path}", flush=True)
    except Exception as e:
        print(f"  [WARN] Parquet 儲存失敗 ({e}), 嘗試 CSV...", flush=True)

    # Always save CSV alongside
    csv_path = out_path.with_suffix(".csv")
    try:
        df.to_csv(str(csv_path), index=False)
        saved_csv = True
        print(f"  ✅ CSV 已儲存: {csv_path}", flush=True)
    except Exception as e:
        print(f"  [WARN] CSV 儲存失敗: {e}", flush=True)

    if not saved_parquet and not saved_csv:
        print("  [ERROR] 無法儲存任何格式，請檢查目錄權限。", flush=True)

    print(f"\n{'='*60}", flush=True)
    print(f"  掃描完成! 共 {len(df)} 筆記錄", flush=True)
    print(f"  Setup 分佈:", flush=True)
    for label in SETUP_TYPE_LABELS.values():
        cnt = (df["setup_type"] == label).sum()
        if cnt:
            print(f"    {label}: {cnt}", flush=True)
    print(f"{'='*60}\n", flush=True)

    return df


# ── Cache loading helpers (importable by Streamlit) ──────────────────────────

def load_latest_scan_cache(
    path: str = "data/latest_scan.parquet",
) -> Tuple[pd.DataFrame, Optional[str], str]:
    """
    Load the latest scan cache from Parquet or CSV.

    Returns:
        (df, scan_timestamp_str, status_message)
        df is empty DataFrame if no cache found.
    """
    base = Path(path)
    parquet_path = base.with_suffix(".parquet") if base.suffix != ".parquet" else base
    csv_path = base.with_suffix(".csv")

    # Try Parquet first
    for p in [parquet_path, csv_path]:
        if p.exists():
            try:
                if p.suffix == ".parquet":
                    df = pd.read_parquet(str(p))
                else:
                    df = pd.read_csv(str(p))

                if df.empty:
                    continue

                ts = None
                if "scan_timestamp" in df.columns:
                    ts = str(df["scan_timestamp"].iloc[0])

                age_str = ""
                try:
                    mtime = datetime.datetime.fromtimestamp(p.stat().st_mtime)
                    age = datetime.datetime.now() - mtime
                    hours = int(age.total_seconds() // 3600)
                    mins  = int((age.total_seconds() % 3600) // 60)
                    age_str = f"{hours}h {mins}m 前" if hours else f"{mins}m 前"
                except Exception:
                    pass

                status = f"✅ 快取載入成功 ({p.name}) — {len(df)} 筆 — 更新於 {age_str}"
                return df, ts, status

            except Exception as e:
                continue

    return pd.DataFrame(), None, "⚠️ 尚未有掃描快取。請運行 `python scanner.py` 生成。"


def get_scanner_status(path: str = "data/latest_scan.parquet") -> Dict:
    """Return a dict with cache status info for display cards."""
    df, ts, status_msg = load_latest_scan_cache(path)
    base = Path(path)
    parquet_path = base.with_suffix(".parquet") if base.suffix != ".parquet" else base
    csv_path     = base.with_suffix(".csv")

    found_path = None
    for p in [parquet_path, csv_path]:
        if p.exists():
            found_path = p
            break

    age_hours = None
    if found_path:
        try:
            mtime = datetime.datetime.fromtimestamp(found_path.stat().st_mtime)
            age_hours = (datetime.datetime.now() - mtime).total_seconds() / 3600
        except Exception:
            pass

    return {
        "has_cache":  not df.empty,
        "row_count":  len(df),
        "timestamp":  ts,
        "status_msg": status_msg,
        "age_hours":  age_hours,
        "path":       str(found_path) if found_path else None,
        "df":         df,
    }


def read_ticker_list_from_file(filepath: str) -> List[str]:
    """Read tickers from CSV. Looks for 'ticker' or 'symbol' column, else uses first column."""
    try:
        df = pd.read_csv(filepath)
        lower_cols = {c.lower(): c for c in df.columns}
        col = lower_cols.get("ticker") or lower_cols.get("symbol") or df.columns[0]
        tickers = [str(t).strip().upper() for t in df[col].dropna().tolist() if str(t).strip()]
        return tickers
    except Exception as e:
        print(f"[ERROR] 讀取 universe file 失敗: {e}", flush=True)
        return []


def load_tradable_universe(
    base_path: str = "data/tradable_universe",
) -> Tuple[List[str], Optional[str]]:
    """
    Load tickers from data/tradable_universe.parquet (or .csv) where
    pass_market_cap_filter is True. Returns (tickers, source_path) or
    ([], None) if no universe file is present.

    The tradable universe is the only hard filter: market cap >= 500M USD
    (configured by universe_builder.py). Price and dollar-volume thresholds
    are NOT applied here.
    """
    parquet_path = Path(base_path + ".parquet")
    csv_path     = Path(base_path + ".csv")
    for p in [parquet_path, csv_path]:
        if not p.exists():
            continue
        try:
            df = pd.read_parquet(str(p)) if p.suffix == ".parquet" else pd.read_csv(str(p))
            if df.empty or "ticker" not in df.columns:
                continue
            if "pass_market_cap_filter" in df.columns:
                # Coerce string "True"/"False" from CSV to bool
                flag = df["pass_market_cap_filter"]
                if flag.dtype == object:
                    flag = flag.astype(str).str.lower().isin({"true", "1", "yes"})
                df = df[flag]
            tickers = [
                str(t).strip().upper()
                for t in df["ticker"].dropna().tolist()
                if str(t).strip()
            ]
            return tickers, str(p)
        except Exception as e:
            print(f"  [WARN] 無法讀取 {p}: {e}", flush=True)
            continue
    return [], None


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="美股盤後預掃描器 — Fast Mode Pre-Scanner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scanner.py
  python scanner.py --tickers "AAPL,MSFT,NVDA"
  python scanner.py --universe-file tickers.csv --benchmark SPY
  python scanner.py --tickers "AAPL,MSFT,SPY" --max-tickers 3 --output data/test_scan.parquet
        """,
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help='Comma-separated ticker list, e.g. "AAPL,MSFT,NVDA"',
    )
    parser.add_argument(
        "--universe-file",
        type=str,
        default=None,
        help="CSV file with ticker column",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/latest_scan.parquet",
        help="Output path (default: data/latest_scan.parquet)",
    )
    parser.add_argument(
        "--benchmark",
        type=str,
        default="SPY",
        help="Benchmark ticker (default: SPY)",
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=0,
        help="Safety limit on number of tickers; 0 = no limit",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=200,
        help="Tickers per yfinance batch (default: 200)",
    )

    args = parser.parse_args()

    # Build ticker list
    tickers: List[str] = []

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    if args.universe_file:
        file_tickers = read_ticker_list_from_file(args.universe_file)
        for t in file_tickers:
            if t not in tickers:
                tickers.append(t)

    universe_source = "explicit args"

    if not tickers:
        # Prefer the tradable universe built by universe_builder.py
        # (market cap >= 500M USD). No 100-ticker cap is applied — every
        # qualifying ticker is scanned unless --max-tickers is set explicitly.
        univ_tickers, univ_path = load_tradable_universe()
        if univ_tickers:
            tickers = univ_tickers
            universe_source = f"tradable_universe ({univ_path}, {len(tickers)} tickers)"
        else:
            tickers = DEFAULT_TICKERS.copy()
            universe_source = f"DEFAULT_TICKERS ({len(tickers)} tickers)"

    print(f"  Universe source: {universe_source}", flush=True)

    # Ensure benchmark is in list
    if args.benchmark not in tickers:
        tickers = [args.benchmark] + tickers

    run_scan(
        tickers=tickers,
        benchmark=args.benchmark,
        output_path=args.output,
        max_tickers=args.max_tickers,
        chunk_size=args.chunk_size,
    )


# Guard: do NOT run on import (safe for Streamlit to import)
if __name__ == "__main__":
    main()
