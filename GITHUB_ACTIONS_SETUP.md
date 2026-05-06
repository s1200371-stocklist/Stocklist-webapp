# GitHub Actions Setup — Daily Market Scanner

This repo includes a GitHub Actions workflow that runs `scanner.py` automatically after the US market closes every weekday, producing `data/latest_scan.parquet` and `data/latest_scan.csv`.

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
- `.github/workflows/daily_scan.yml`
- (optional) a `data/` directory — the scanner creates it automatically

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

## 7. Limitations & tips

| Topic | Notes |
|-------|-------|
| **Free tier minutes** | GitHub gives 2,000 minutes/month on free plans. A full curated-pool scan takes roughly 2–5 minutes. |
| **yfinance rate limits** | yfinance uses Yahoo Finance's unofficial API. Heavy loads (500+ tickers) may hit soft rate limits; the scanner batches in chunks of 200 with a 0.5 s pause between chunks. |
| **Parquet on Streamlit Cloud** | `pyarrow` is installed by the workflow; make sure it is also in your `requirements.txt` for the Streamlit deployment. |
| **Secrets** | No API keys are required — yfinance is free and unauthenticated. |
| **Cron delay** | GitHub Actions scheduled jobs can be delayed by up to ~30 minutes during busy periods. |
