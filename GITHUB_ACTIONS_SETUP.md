# GitHub Actions Setup — Daily Market Scanner

This repo includes two GitHub Actions workflows:

1. **Build Tradable Universe** (`build_universe.yml`) — runs weekly. Reads the listing CSVs (`nasdaq-listed.csv`, `nyse-listed.csv`, `other-listed.csv`), fetches market caps via yfinance, and writes `data/tradable_universe.parquet` / `.csv`.
2. **Daily Market Scanner** (`daily_scan.yml`) — runs every weekday after the US close. If `data/tradable_universe.parquet` is present, `scanner.py` loads it automatically (otherwise falls back to a small curated `DEFAULT_TICKERS` list).

## Universe rules (per product owner)

- **Hard filter (only one):** market cap >= **500M USD**. Configured via `--min-market-cap` on `universe_builder.py` (default `500000000`).
- **Not** filtered by price (>$5) or average dollar volume ($10M). Those metrics are surfaced downstream as informational, not as gating filters.
- **No shortlist cap of 100.** Every ticker that passes the market-cap filter is scanned. UI display row limits exist for readability and are labelled as display-only.


---

## 1. Push your files to GitHub

```bash
git init                        # skip if repo already exists
git remote add origin https://github.com/<you>/<repo>.git
git add scanner.py test-2.py .github/
git commit -m "feat: add GitHub Actions daily scanner"
git push -u origin main
```

Files that must be in the repo root:
- `scanner.py`
- `universe_builder.py`
- `nasdaq-listed.csv`, `nyse-listed.csv`, `other-listed.csv` (used by `universe_builder.py`)
- `.github/workflows/daily_scan.yml`
- `.github/workflows/build_universe.yml`
- (optional) a `data/` directory — the scanner / universe builder create it automatically

---

## 2. Schedule — when does it run?

The workflow fires at **22:30 UTC, Monday–Friday**.

| Season | US market close (ET) | UTC equivalent | HKT (UTC+8) |
|--------|----------------------|----------------|-------------|
| EDT (Mar–Nov) | 4:00 pm | 20:00 | 06:30 +1 day ✓ |
| EST (Nov–Mar) | 4:00 pm | 21:00 | 06:30 +1 day ✓ |

Results are always ready by 06:30 HKT the next morning.

> GitHub's free tier may delay scheduled jobs by up to ~15 minutes during peak hours.

---

## 3. Run manually (Actions tab)

1. Go to your repo on GitHub → **Actions** tab.
2. Click **Daily Market Scanner** in the left sidebar.
3. Click **Run workflow** (top-right).
4. Fill in optional inputs:

| Input | Description | Default |
|-------|-------------|---------|
| `tickers` | Comma-separated, e.g. `AAPL,MSFT,NVDA` | curated pool |
| `universe_file` | Path to CSV file in repo with a ticker column | — |
| `benchmark` | Benchmark ticker | `SPY` |
| `max_tickers` | Safety cap; `0` = no limit | `0` |
| `commit_results` | Write data files back to repo | `false` |

5. Click **Run workflow** → green button.

---

## 4. Download scan artifacts

After a run completes:

1. Actions tab → click the finished run.
2. Scroll to **Artifacts** section at the bottom.
3. Download **latest-scan-\<run_number\>** — contains both `.parquet` and `.csv`.

Artifacts are kept for **30 days** per run.

---

## 5. Enable automatic commit of results

This lets the workflow push updated `data/` files back into your repo so your Streamlit app can read fresh data without downloading artifacts.

### Option A — per-run (manual dispatch)
Set the `commit_results` input to `true` when you click **Run workflow**.

### Option B — always commit (scheduled + manual)
1. Repo → **Settings** → **Variables** → **Actions** → **New repository variable**.
2. Name: `COMMIT_RESULTS`, Value: `true`.
3. The workflow reads this variable and commits after every run.

### Required permission
Go to **Settings → Actions → General → Workflow permissions** and select  
**"Read and write permissions"**. Without this the commit step will fail with a 403 error.

> **Loop safety:** The commit message includes `[skip ci]`, which prevents GitHub from re-triggering the workflow on the pushed commit.

---

## 6. Streamlit app

`test-2.py` (your Streamlit app) reads `data/latest_scan.parquet` or `data/latest_scan.csv` directly. After the Actions workflow runs and you either download the artifact or enable auto-commit, point Streamlit at the same `data/` directory and it will show the fresh scan results.

---

## 7. Build Tradable Universe workflow

The `build_universe.yml` workflow runs `python universe_builder.py` and uploads / commits `data/tradable_universe.parquet` and `data/tradable_universe.csv`.

| Input | Description | Default |
|-------|-------------|---------|
| `min_market_cap` | Hard filter, USD | `500000000` |
| `max_tickers` | Smoke-test cap on input listings (0 = no cap) | `0` |
| `commit_results` | Commit the universe back to repo | `true` |

Schedule: Sunday 03:00 UTC weekly. Market caps move slowly; weekly is sufficient.

After this workflow runs and commits, `daily_scan.yml` automatically picks up the new universe — no extra config required.

To smoke-test locally:

```bash
python universe_builder.py --max-tickers 50 --output data/test_universe.parquet
```

---

## 8. Limitations & tips

| Topic | Notes |
|-------|-------|
| **Free tier minutes** | GitHub gives 2,000 minutes/month on free plans. A full curated-pool scan takes roughly 2–5 minutes. |
| **yfinance rate limits** | yfinance uses Yahoo Finance's unofficial API. Heavy loads (500+ tickers) may hit soft rate limits; the scanner batches in chunks of 200 with a 0.5 s pause between chunks. |
| **Parquet on Streamlit Cloud** | `pyarrow` is installed by the workflow; make sure it is also in your `requirements.txt` for the Streamlit deployment. |
| **Secrets** | No API keys are required — yfinance is free and unauthenticated. |
| **Cron delay** | GitHub Actions scheduled jobs can be delayed by up to ~30 minutes during busy periods. |
