import os
import re
import json
import time
import random
import datetime
import requests
import pandas as pd
import streamlit as st
import yfinance as yf
import concurrent.futures

from datetime import timedelta
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance

# ==========================================
# 1. 頁面設定
# ==========================================
st.set_page_config(
    page_title='🚀 美股全方位量化與 AI 平台',
    page_icon='📈',
    layout='wide'
)

# ==========================================
# 2. 基本工具函數
# ==========================================
def get_headers():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-':
            return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val:
            return float(val.replace('B', '')) * 1000
        if 'M' in val:
            return float(val.replace('M', ''))
        return float(val)
    except Exception:
        return 0.0

def safe_to_string(df, rows=8):
    try:
        if df is None or df.empty:
            return "無數據"
        return df.head(rows).to_string(index=False)
    except Exception:
        return "無數據"

def strip_markdown_code_fence(text):
    if not isinstance(text, str):
        return str(text)
    text = re.sub(r"^```(?:json|text|markdown)?\s*", "", text.strip(), flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text.strip())
    return text.strip()

def clean_ai_response(text):
    if not isinstance(text, str):
        return str(text)

    raw = text.strip()
    raw = strip_markdown_code_fence(raw)

    # 去 <think> 標籤
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL | re.IGNORECASE)

    # 嘗試直接 parse 成 JSON
    try:
        parsed = json.loads(raw)

        if isinstance(parsed, dict):
            if "choices" in parsed and parsed["choices"]:
                msg = parsed["choices"][0].get("message", {})
                if isinstance(msg, dict):
                    content = msg.get("content")
                    if isinstance(content, str) and content.strip():
                        return strip_markdown_code_fence(content.strip())

            if "content" in parsed and isinstance(parsed["content"], str):
                return strip_markdown_code_fence(parsed["content"].strip())

            if parsed.get("role") == "assistant":
                if isinstance(parsed.get("content"), str) and parsed["content"].strip():
                    return strip_markdown_code_fence(parsed["content"].strip())
                if isinstance(parsed.get("final"), str) and parsed["final"].strip():
                    return strip_markdown_code_fence(parsed["final"].strip())
    except Exception:
        pass

    # 如果成段文字裡面包住 JSON
    json_match = re.search(r'(\{.*\})', raw, flags=re.DOTALL)
    if json_match:
        candidate = json_match.group(1)
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                if parsed.get("role") == "assistant":
                    if isinstance(parsed.get("content"), str) and parsed["content"].strip():
                        return strip_markdown_code_fence(parsed["content"].strip())
                    if isinstance(parsed.get("final"), str) and parsed["final"].strip():
                        return strip_markdown_code_fence(parsed["final"].strip())
                if "content" in parsed and isinstance(parsed["content"], str):
                    return strip_markdown_code_fence(parsed["content"].strip())
        except Exception:
            pass

    # 強制清掉 reasoning_content / role / content 字段殘渣
    raw = re.sub(r'"reasoning_content"\s*:\s*".*?"\s*,?', '', raw, flags=re.DOTALL)
    raw = re.sub(r'"role"\s*:\s*"assistant"\s*,?', '', raw, flags=re.DOTALL)
    raw = re.sub(r'"content"\s*:\s*', '', raw, flags=re.DOTALL)

    raw = raw.replace('\\"', '"').replace("\\n", "\n").strip()

    # 去首尾大括號
    raw = re.sub(r'^\{+', '', raw).strip()
    raw = re.sub(r'\}+$', '', raw).strip()

    # 去常見英語 thinking 垃圾行
    bad_line_patterns = [
        r'^\s*we must\b.*$',
        r'^\s*let[\'’]s\b.*$',
        r'^\s*probably\b.*$',
        r'^\s*need to\b.*$',
        r'^\s*add insights\b.*$',
        r'^\s*output exactly\b.*$',
        r'^\s*json\b.*$',
        r'^\s*role\b.*$',
        r'^\s*assistant\b.*$',
        r'^\s*reasoning_content\b.*$',
    ]

    cleaned_lines = []
    for line in raw.splitlines():
        line_strip = line.strip()
        if not line_strip:
            cleaned_lines.append("")
            continue

        skip = False
        for p in bad_line_patterns:
            if re.match(p, line_strip, flags=re.IGNORECASE):
                skip = True
                break
        if not skip:
            cleaned_lines.append(line)

    raw = "\n".join(cleaned_lines)
    raw = re.sub(r'\n{3,}', '\n\n', raw).strip()

    return raw

def call_pollinations(messages, model='openai-fast', timeout=60):
    try:
        response = requests.post(
            'https://text.pollinations.ai/',
            json={
                'messages': messages,
                'model': model
            },
            timeout=timeout
        )
        return clean_ai_response(response.text)
    except Exception as e:
        return f"⚠️ AI 發生錯誤: {e}"

def extract_cantonese_report(text):
    cleaned = clean_ai_response(text)

    anchor = "【🕵️ 另類數據 AI 偵測深度報告】"
    idx = cleaned.find(anchor)
    if idx != -1:
        return cleaned[idx:].strip()

    headings = [
        "【🔥 社交熱度雙引擎：Reddit、StockTwits、X 正喺度推高邊啲股票？】",
        "【🏛️ 聰明錢與政客追蹤：終極內幕買緊乜？】",
        "【🎯 終極五維共振：最強爆發潛力股與高危陷阱】"
    ]

    found_sections = []
    for h in headings:
        pos = cleaned.find(h)
        if pos != -1:
            found_sections.append((pos, h))

    if found_sections:
        found_sections.sort(key=lambda x: x[0])
        rebuilt = ["【🕵️ 另類數據 AI 偵測深度報告】"]
        for i, (pos, heading) in enumerate(found_sections):
            start = pos
            end = found_sections[i + 1][0] if i + 1 < len(found_sections) else len(cleaned)
            chunk = cleaned[start:end].strip()
            rebuilt.append(chunk)
        return "\n\n".join(rebuilt).strip()

    fallback = cleaned
    fallback = re.sub(r'^\s*\{.*?\}\s*$', '', fallback, flags=re.DOTALL)
    fallback = re.sub(r'\n{3,}', '\n\n', fallback).strip()

    if fallback:
        return f"【🕵️ 另類數據 AI 偵測深度報告】\n\n{fallback}"

    return "【🕵️ 另類數據 AI 偵測深度報告】\n\n⚠️ AI 回傳格式異常，建議重新生成一次。"

def extract_stock_sentiment_output(text):
    allowed_labels = [
        "【🔥 極度看好】",
        "【📈 偏向樂觀】",
        "【⚖️ 中性觀望】",
        "【📉 偏向悲觀】",
        "【🧊 極度看淡】"
    ]

    fallback_label = "【⚖️ 中性觀望】"
    fallback_body = "市場消息面暫時未有一面倒優勢，利好與風險並存，現階段較適合保持審慎，等待更多業績、指引或催化消息再判斷後續方向。"

    cleaned = clean_ai_response(text)
    cleaned = re.sub(r'```.*?```', '', cleaned, flags=re.DOTALL)
    lines = [line.strip() for line in cleaned.split('\n') if line.strip()]

    label = fallback_label
    body_lines = []

    for line in lines:
        if line in allowed_labels:
            label = line
            continue

        low = line.lower()
        if "reasoning_content" in low:
            continue
        if '"role"' in low or '"content"' in low:
            continue
        if line.startswith("{") and line.endswith("}"):
            continue

        body_lines.append(line)

    body = "\n\n".join(body_lines).strip()
    body = re.sub(r'\n{3,}', '\n\n', body).strip()
    body = re.sub(r'^[：:、，。；\-\s]+', '', body)

    if not body:
        body = fallback_body

    return label, body

# ==========================================
# 3. 模組 C：另類數據雷達資料源
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_sentiment():
    try:
        url = 'https://apewisdom.io/api/v1.0/filter/all-stocks/page/1'
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                df = pd.DataFrame([
                    {
                        'Ticker': str(item.get('ticker', '')).upper(),
                        'Sentiment': 'Bullish' if item.get('mentions', 0) > 30 else 'Neutral',
                        'Mentions': item.get('mentions', 0) * 5
                    }
                    for item in results[:10]
                ])
                return df, '🟢 ApeWisdom (過去24h數據)'
    except Exception:
        pass

    mock = [
        {'Ticker': 'SPY', 'Sentiment': 'Bullish', 'Mentions': 2420},
        {'Ticker': 'CAR', 'Sentiment': 'Bullish', 'Mentions': 1535},
        {'Ticker': 'ASTS', 'Sentiment': 'Bullish', 'Mentions': 815},
        {'Ticker': 'UNH', 'Sentiment': 'Bullish', 'Mentions': 765},
        {'Ticker': 'MSFT', 'Sentiment': 'Bullish', 'Mentions': 635},
        {'Ticker': 'AMZN', 'Sentiment': 'Bullish', 'Mentions': 485},
        {'Ticker': 'TSLA', 'Sentiment': 'Bearish', 'Mentions': 405},
        {'Ticker': 'NVDA', 'Sentiment': 'Bullish', 'Mentions': 375},
    ]
    return pd.DataFrame(mock), '🔴 離線備援 (WSB)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_stocktwits_trending():
    try:
        url = 'https://api.stocktwits.com/api/2/trending/symbols.json'
        res = requests.get(url, headers=get_headers(), timeout=8)
        if res.status_code == 200:
            symbols = res.json().get('symbols', [])
            if symbols:
                df = pd.DataFrame([
                    {'Ticker': s.get('symbol', ''), 'Name': s.get('title', '')}
                    for s in symbols[:10]
                ])
                return df, '🟢 StockTwits 正常 (即時數據)'
    except Exception:
        pass

    mock = [
        {'Ticker': 'CAR', 'Name': 'Avis Budget Group'},
        {'Ticker': 'UNH', 'Name': 'UnitedHealth Group'},
        {'Ticker': 'NVDS', 'Name': 'AXS 1.25X NVDA Bear ETF'},
        {'Ticker': 'ASTS', 'Name': 'AST SpaceMobile'},
    ]
    return pd.DataFrame(mock), '🔴 離線備援 (StockTwits)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_x_sentiment():
    api_key = None
    try:
        api_key = st.secrets.get("X_SENTIMENT_API_KEY", None)
    except Exception:
        api_key = os.getenv("X_SENTIMENT_API_KEY")

    if api_key:
        try:
            # 之後如有正式供應商，可直接改 endpoint
            url = "https://api.adanos.org/x-stocks/sentiment"
            params = {"limit": 10, "period_days": 7}
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0"
            }
            res = requests.get(url, headers=headers, params=params, timeout=12)
            if res.status_code == 200:
                data = res.json()
                stocks = data.get("stocks", [])
                if stocks:
                    rows = []
                    for item in stocks[:10]:
                        sentiment_score = item.get("sentiment_score", 0)
                        bullish_pct = item.get("bullish_pct", 50)

                        if sentiment_score >= 0.25:
                            label = "Bullish"
                        elif sentiment_score <= -0.25:
                            label = "Bearish"
                        else:
                            label = "Neutral"

                        rows.append({
                            "Ticker": str(item.get("ticker", "")).upper(),
                            "Sentiment": label,
                            "Mentions": item.get("mentions", 0),
                            "Bullish %": item.get("bullish_pct", bullish_pct),
                            "Trend": item.get("trend", "N/A")
                        })

                    return pd.DataFrame(rows), "🟢 X / FinTwit API 正常 (過去7日數據)"
        except Exception:
            pass

    mock = [
        {"Ticker": "TSLA", "Sentiment": "Bullish", "Mentions": 4820, "Bullish %": 68, "Trend": "Rising"},
        {"Ticker": "NVDA", "Sentiment": "Bullish", "Mentions": 3910, "Bullish %": 72, "Trend": "Rising"},
        {"Ticker": "PLTR", "Sentiment": "Bullish", "Mentions": 2440, "Bullish %": 66, "Trend": "Stable"},
        {"Ticker": "AMD",  "Sentiment": "Neutral", "Mentions": 1890, "Bullish %": 54, "Trend": "Stable"},
        {"Ticker": "AAPL", "Sentiment": "Neutral", "Mentions": 1710, "Bullish %": 52, "Trend": "Flat"},
        {"Ticker": "SMCI", "Sentiment": "Bearish", "Mentions": 1580, "Bullish %": 39, "Trend": "Falling"},
        {"Ticker": "META", "Sentiment": "Bullish", "Mentions": 1490, "Bullish %": 63, "Trend": "Rising"},
        {"Ticker": "COIN", "Sentiment": "Bullish", "Mentions": 1380, "Bullish %": 64, "Trend": "Rising"},
    ]
    return pd.DataFrame(mock), "🔴 離線備援 (X / FinTwit)"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    target_tickers = ['NVDA', 'AAPL', 'MSFT', 'AMZN', 'META', 'GOOGL', 'TSLA', 'AMD', 'PLTR', 'CRWD', 'ASTS', 'COIN', 'MARA']
    random.shuffle(target_tickers)
    results = []
    cutoff_date = pd.Timestamp.now(tz=None) - timedelta(days=30)

    def fetch_yf_insider(ticker):
        try:
            tkr = yf.Ticker(ticker)
            trades = tkr.insider_transactions
            if trades is None or trades.empty:
                return

            df = trades.reset_index()
            date_col = next((c for c in df.columns if 'date' in str(c).lower()), None)
            if date_col:
                dt = pd.to_datetime(df[date_col], errors='coerce')
                if getattr(dt.dt, 'tz', None) is not None:
                    dt = dt.dt.tz_localize(None)
                df[date_col] = dt
                df = df[df[date_col] >= cutoff_date]

            text_col = next((c for c in df.columns if 'text' in str(c).lower() or 'trans' in str(c).lower()), None)
            if text_col and not df.empty:
                buys = df[df[text_col].astype(str).str.contains('Buy|Purchase', case=False, na=False)].copy()
                for _, row in buys.head(2).iterrows():
                    shares = row.get('Shares', 0)
                    value = row.get('Value', 0)
                    if pd.notna(value) and float(value) > 0:
                        results.append({
                            'Ticker': ticker,
                            'Owner': str(row.get('Insider', row.get('Name', 'N/A'))).title(),
                            'Relationship': str(row.get('Position', row.get('Title', 'Executive'))).title(),
                            'Cost': f"${float(value)/float(shares):.2f}" if pd.notna(shares) and float(shares) > 0 else 'N/A',
                            'Value': f"${float(value):,.0f}"
                        })
        except Exception:
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_yf_insider, t) for t in target_tickers[:8]]
        concurrent.futures.wait(futures)

    if results:
        df_final = pd.DataFrame(results)
        df_final['SortValue'] = df_final['Value'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)
        df_final = df_final.sort_values('SortValue', ascending=False).drop(columns=['SortValue']).head(10).reset_index(drop=True)
        return df_final

    return pd.DataFrame([
        {'Ticker': 'ASTS', 'Owner': 'Abel Avellan', 'Relationship': 'CEO', 'Cost': '$24.50', 'Value': '$2,500,000'},
        {'Ticker': 'PLTR', 'Owner': 'Alexander Karp', 'Relationship': 'CEO', 'Cost': '$22.50', 'Value': '$1,500,000'},
        {'Ticker': 'CRWD', 'Owner': 'George Kurtz', 'Relationship': 'CEO', 'Cost': '$280.00', 'Value': '$3,200,000'},
    ])

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_congress_trades():
    try:
        url = 'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json'
        res = requests.get(url, headers=get_headers(), timeout=8)
        if res.status_code == 200:
            df = pd.DataFrame(res.json())
            if not df.empty:
                df = df[df['type'].astype(str).str.lower() == 'purchase'].copy()
                dt = pd.to_datetime(df['transaction_date'], errors='coerce')
                if getattr(dt.dt, 'tz', None) is not None:
                    dt = dt.dt.tz_localize(None)
                df['transaction_date'] = dt
                df = df[df['transaction_date'] >= pd.Timestamp.now(tz=None) - timedelta(days=45)]
                df = df.dropna(subset=['transaction_date']).sort_values('transaction_date', ascending=False)
                if not df.empty:
                    df = df[['transaction_date', 'representative', 'ticker', 'amount']].head(10).copy()
                    df.columns = ['Date', 'Politician', 'Ticker', 'Amount']
                    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
                    return df.reset_index(drop=True), '🟢 國會交易 (過去45日數據)'
    except Exception:
        pass

    return pd.DataFrame([
        {'Date': '2026-04-15', 'Politician': 'Nancy Pelosi', 'Ticker': 'PANW', 'Amount': '$1M - $5M'},
        {'Date': '2026-04-12', 'Politician': 'Ro Khanna', 'Ticker': 'CRWD', 'Amount': '$15K - $50K'},
        {'Date': '2026-04-10', 'Politician': 'Michael McCaul', 'Ticker': 'NVDA', 'Amount': '$100K - $250K'},
    ]), '🔴 離線備援 (Congress)'

# ==========================================
# 4. 模組 A：量化與財報引擎
# ==========================================
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, close_condition, batch_size=200, _progress_bar=None, _status_text=None):
    results = {}
    bench_data, used_bench = pd.DataFrame(), ''

    for b in ['QQQ', '^NDX', 'QQQM']:
        try:
            tmp = yf.download(b, period='2y', progress=False, group_by='column', auto_adjust=False)
            if not tmp.empty and 'Close' in tmp.columns:
                bench_data = tmp['Close'].to_frame(name=b) if isinstance(tmp['Close'], pd.Series) else tmp['Close']
                used_bench = b
                break
        except Exception:
            continue

    if bench_data.empty:
        return results

    if getattr(bench_data.index, 'tz', None) is not None:
        bench_data.index = bench_data.index.tz_localize(None)

    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]

    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i + batch_size]

        if _status_text:
            _status_text.markdown(f'**階段 2/3**: 計緊技術指標... (`{min(i + batch_size, len(tickers))}` / `{len(tickers)}`)')
        if _progress_bar:
            _progress_bar.progress(min(1.0, (i + batch_size) / max(len(tickers), 1)))

        try:
            data = yf.download(batch_tickers, period='2y', progress=False, group_by='column', auto_adjust=False)
            if data.empty or 'Close' not in data.columns:
                raise ValueError()

            cp = data['Close']
            if isinstance(cp, pd.Series):
                cp = cp.to_frame(name=batch_tickers[0])

            cp = cp.ffill().dropna(how='all')
            if getattr(cp.index, 'tz', None) is not None:
                cp.index = cp.index.tz_localize(None)

            for ticker in batch_tickers:
                rs, macd_s, sma_t = '無', '無', False

                if ticker in cp.columns and not cp[ticker].dropna().empty:
                    sp = cp[ticker].dropna()

                    if len(sp) > max(sma_short, sma_long) + 1:
                        sn = sp / sp.iloc[0]
                        rl = sn / bench_norm.reindex(sn.index).ffill() * 100
                        rma = rl.rolling(25).mean()

                        if float(rl.iloc[-1]) > float(rma.iloc[-1]):
                            rs = '🚀 啱啱突破' if float(rl.iloc[-2]) <= float(rma.iloc[-2]) else '🔥 已經突破'
                        elif float(rl.iloc[-1]) >= float(rma.iloc[-1]) * 0.95:
                            rs = '🎯 就快突破 (<5%)'

                        e12 = sp.ewm(span=12, adjust=False).mean()
                        e26 = sp.ewm(span=26, adjust=False).mean()
                        ml = e12 - e26
                        sl = ml.ewm(span=9, adjust=False).mean()

                        if float(ml.iloc[-1]) > float(sl.iloc[-1]):
                            macd_s = '🚀 啱啱突破' if float(ml.iloc[-2]) <= float(sl.iloc[-2]) else '🔥 已經突破'
                        elif abs(float(ml.iloc[-1]) - float(sl.iloc[-1])) <= max(abs(float(sl.iloc[-1])) * 0.05, 1e-9):
                            macd_s = '🎯 就快突破 (<5%)'

                        ss = sp.rolling(sma_short).mean()
                        ls = sp.rolling(sma_long).mean()
                        lc = float(sp.iloc[-1])
                        lss = float(ss.iloc[-1])
                        lls = float(ls.iloc[-1])

                        tok = lss > lls
                        if close_condition == 'Close > 短期 SMA':
                            tok = tok and lc > lss
                        elif close_condition == 'Close > 長期 SMA':
                            tok = tok and lc > lls
                        elif close_condition == 'Close > 短期及長期 SMA':
                            tok = tok and lc > lss and lc > lls

                        sma_t = tok

                results[ticker] = {'RS': rs, 'MACD': macd_s, 'SMA_Trend': sma_t}

        except Exception:
            for t in batch_tickers:
                results[t] = {'RS': '無', 'MACD': '無', 'SMA_Trend': False}

        time.sleep(0.5)

    return results

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    def fetch_single(t):
        for _ in range(2):
            try:
                tkr = yf.Ticker(t)
                q = tkr.quarterly_financials
                if q is None or q.empty:
                    q = tkr.quarterly_income_stmt
                if q is None or q.empty:
                    continue

                cols = list(q.columns)[:4]
                try:
                    cols = sorted(cols)
                except Exception:
                    cols = cols[::-1]

                er, sr = None, None

                for r in ['Diluted EPS', 'Basic EPS', 'Normalized EPS']:
                    if r in q.index:
                        er = q.loc[r]
                        break

                for r in ['Total Revenue', 'Operating Revenue']:
                    if r in q.index:
                        sr = q.loc[r]
                        break

                ev = [float(er[c]) if er is not None and pd.notna(er[c]) else None for c in cols]
                sv = [float(sr[c]) if sr is not None and pd.notna(sr[c]) else None for c in cols]

                def fv(vs, s=False):
                    out = []
                    for v in vs:
                        if v is None:
                            out.append('-')
                        elif s and v >= 1e9:
                            out.append(f'{v/1e9:.2f}B')
                        elif s and v >= 1e6:
                            out.append(f'{v/1e6:.2f}M')
                        else:
                            out.append(f'{v:.2f}')
                    return ' | '.join(out)

                def fg(vs):
                    out = ['-']
                    for i in range(1, len(vs)):
                        if vs[i] is not None and vs[i-1] is not None and vs[i-1] != 0:
                            out.append(f'{(vs[i]-vs[i-1])/abs(vs[i-1])*100:+.1f}%')
                        else:
                            out.append('-')
                    return ' | '.join(out)

                return {
                    'Ticker': t,
                    'EPS (近4季)': fv(ev),
                    'EPS Growth (QoQ)': fg(ev),
                    'Sales (近4季)': fv(sv, True),
                    'Sales Growth (QoQ)': fg(sv)
                }

            except Exception:
                time.sleep(1)

        return {
            'Ticker': t,
            'EPS (近4季)': 'N/A',
            'EPS Growth (QoQ)': 'N/A',
            'Sales (近4季)': 'N/A',
            'Sales Growth (QoQ)': 'N/A'
        }

    empty_df = pd.DataFrame(columns=['Ticker', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'])

    if not tickers:
        return empty_df

    results, done = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(fetch_single, t): t for t in tickers}
        for f in concurrent.futures.as_completed(futs):
            r = f.result()
            if r:
                results.append(r)
            done += 1
            if _status_text:
                _status_text.markdown(f'**階段 3/3**: 攞緊最新財報數據... (`{done}` / `{len(tickers)}`)')
            if _progress_bar:
                _progress_bar.progress(min(1.0, done / max(len(tickers), 1)))

    return pd.DataFrame(results) if results else empty_df

# ==========================================
# 5. 模組 B：AI 新聞分析引擎
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_top_news():
    news_items, seen = [], set()

    try:
        for t in ['SPY', 'QQQ']:
            news = finvizfinance(t).ticker_news()
            if not news.empty:
                for _, row in news.head(15).iterrows():
                    title = row.get('Title', '')
                    if title and title not in seen:
                        seen.add(title)
                        news_items.append({
                            '來源': row.get('Source', 'Finviz'),
                            '新聞標題': title,
                            '內文摘要': '（來自 Finviz 標題）'
                        })
    except Exception:
        pass

    try:
        for t in ['SPY', 'QQQ', 'NVDA', 'AAPL']:
            tkr = yf.Ticker(t)
            if hasattr(tkr, 'news') and isinstance(tkr.news, list):
                for item in tkr.news[:5]:
                    title = item.get('content', {}).get('title', item.get('title', ''))
                    if title and title not in seen:
                        seen.add(title)
                        summary = item.get('content', {}).get('summary', item.get('summary', '無內文'))
                        news_items.append({
                            '來源': item.get('publisher', 'Finance News'),
                            '新聞標題': title,
                            '內文摘要': str(summary)[:240]
                        })
    except Exception:
        pass

    return news_items

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list:
        return '⚠️ 目前攞唔到新聞數據，請遲啲再試下。'

    news_text = '\n'.join([
        f"{i+1}. [{x['來源']}] 標題：{x['新聞標題']}\n摘要：{x['內文摘要']}\n"
        for i, x in enumerate(news_list)
    ])

    system_prompt = """
You are a Hong Kong financial analyst.

規則：
1. 全文必須用香港廣東話 + 繁體中文。
2. 唔可以輸出 JSON、XML、markdown code block。
3. 唔可以輸出 reasoning、thoughts、reasoning_content。
4. 篇幅不限，但只輸出最終報告。
5. 直接由標題開始寫。

格式：
【📉 近月市場焦點總結】
（篇幅不限）

【🚀 潛力爆發股全面掃描】
（篇幅不限）
""".strip()

    user_prompt = f"""
請根據以下財經新聞，寫一份自然、完整、香港廣東話分析。

新聞：
{news_text}

記住：
- 唔好輸出任何分析過程
- 唔好輸出 JSON
- 只輸出最終報告
""".strip()

    result = call_pollinations(
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt}
        ],
        model='openai-fast',
        timeout=60
    )

    cleaned = clean_ai_response(result)
    if "【📉 近月市場焦點總結】" not in cleaned:
        cleaned = f"【📉 近月市場焦點總結】\n\n{cleaned}"
    return cleaned

# ==========================================
# 6. 模組 C：AI 五維交叉博弈分析
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, twits_df, x_df, insider_df, congress_df):
    r_str = safe_to_string(reddit_df, rows=8)
    t_str = safe_to_string(twits_df, rows=8)
    x_str = safe_to_string(x_df, rows=8)
    i_str = safe_to_string(insider_df, rows=8)
    c_str = safe_to_string(congress_df, rows=8)

    system_prompt = """
You are a Hong Kong financial analyst.

嚴格規則：
1. 全文必須用香港廣東話口語 + 繁體中文。
2. 唔可以輸出 JSON。
3. 唔可以輸出 XML。
4. 唔可以輸出 markdown code block。
5. 唔可以解釋你自己點分析。
6. 唔可以輸出 reasoning、thoughts、reasoning_content。
7. 唔可以輸出英語句子，除咗股票代號、人名。
8. 你一定要包含以下四個詞語：
   瘋狂吸籌
   探氪
   春江鴨
   人踩人風險

你只可以輸出以下格式：

【🕵️ 另類數據 AI 偵測深度報告】

【🔥 社交熱度雙引擎：Reddit、StockTwits、X 正喺度推高邊啲股票？】
（篇幅不限，按數據自然分析）

【🏛️ 聰明錢與政客追蹤：終極內幕買緊乜？】
（篇幅不限，按數據自然分析）

【🎯 終極五維共振：最強爆發潛力股與高危陷阱】
（篇幅不限，按數據自然分析）
""".strip()

    user_prompt = f"""
請根據以下數據直接寫報告：

Reddit:
{r_str}

StockTwits:
{t_str}

X / FinTwit:
{x_str}

Insiders:
{i_str}

Congress:
{c_str}

記住：
- 只輸出最終報告正文
- 唔好輸出任何解釋、JSON、前言、思考過程
""".strip()

    result = call_pollinations(
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt}
        ],
        model='openai-fast',
        timeout=80
    )

    return extract_cantonese_report(result)

# ==========================================
# 7. 模組 D：個股驗證模式 Bottom-Up
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_single_stock_news(ticker):
    news_items = []

    try:
        tkr = yf.Ticker(ticker)
        if hasattr(tkr, 'news') and isinstance(tkr.news, list):
            for item in tkr.news[:10]:
                title = item.get('content', {}).get('title', item.get('title', ''))
                summary = item.get('content', {}).get('summary', item.get('summary', '無摘要'))
                if title:
                    news_items.append(f"標題: {title} | 摘要: {str(summary)[:220]}")
    except Exception:
        pass

    if not news_items:
        try:
            news = finvizfinance(ticker).ticker_news()
            if not news.empty:
                for _, row in news.head(10).iterrows():
                    news_items.append(f"標題: {row.get('Title', '')} | 來源: {row.get('Source', '')}")
        except Exception:
            pass

    return news_items

def analyze_single_stock_sentiment(ticker, news_items):
    fallback_label = "【⚖️ 中性觀望】"
    fallback_body = "市場消息面暫時未有一面倒優勢，利好與風險並存，現階段較適合保持審慎，等待更多業績、指引或催化消息再判斷後續方向。"

    if not news_items:
        return f"{fallback_label}\n\n缺乏近期專屬新聞，暫時未見足夠利好或利淡催化，現階段較適合先觀望。"

    news_str = "\n".join(news_items)

    system_prompt = """
You are a Hong Kong financial AI.

嚴格規則：
1. 第一行必須完全等於以下其中一個：
【🔥 極度看好】
【📈 偏向樂觀】
【⚖️ 中性觀望】
【📉 偏向悲觀】
【🧊 極度看淡】
2. 第一行之後，內容必須用繁體中文香港廣東話自然分析。
3. 唔可以輸出 JSON、XML、markdown code block、reasoning_content、分析過程。
4. 篇幅不限，但只輸出最終答案。
5. 如果好淡因素混雜，以【⚖️ 中性觀望】為優先。
""".strip()

    user_prompt = f"""
請分析 {ticker} 近期新聞。

新聞如下：
{news_str}

只輸出最終答案。
""".strip()

    result = call_pollinations(
        messages=[
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt}
        ],
        model='openai-fast',
        timeout=25
    )

    label, body = extract_stock_sentiment_output(result)

    if not label:
        label = fallback_label
    if not body:
        body = fallback_body

    return f"{label}\n\n{body}"

# ==========================================
# 8. 模組 E：終極雙劍合璧
# ==========================================
def run_full_integration(final_df, progress_bar, status_text):
    if final_df.empty:
        return pd.DataFrame()

    breakout_df = final_df[
        final_df['RS_階段'].isin(['🚀 啱啱突破', '🔥 已經突破']) |
        final_df['MACD_階段'].isin(['🚀 啱啱突破', '🔥 已經突破'])
    ].copy()

    if breakout_df.empty:
        return pd.DataFrame()

    total_stocks = min(15, len(breakout_df))
    breakout_df = breakout_df.head(total_stocks)

    sentiments = []
    reasons = []

    for _, row in breakout_df.iterrows():
        ticker = row['Ticker']
        status_text.markdown(f"**終極驗證中...** 正在用 AI 掃描 `{ticker}` 嘅新聞基本面 ({len(sentiments)+1}/{total_stocks})")
        progress_bar.progress((len(sentiments)+1) / total_stocks)

        news = fetch_single_stock_news(ticker)
        if news:
            ai_res = analyze_single_stock_sentiment(ticker, news)
            lines = [x.strip() for x in ai_res.split('\n') if x.strip()]
            sentiment = lines[0] if len(lines) > 0 else "【⚖️ 中性觀望】"
            reason = "\n\n".join(lines[1:]) if len(lines) > 1 else "無具體解釋。"
        else:
            sentiment = "【⚖️ 中性觀望】"
            reason = "無新聞數據。"

        sentiments.append(sentiment)
        reasons.append(reason)
        time.sleep(1)

    breakout_df['AI 消息情緒'] = sentiments
    breakout_df['AI 深度分析'] = reasons

    golden_df = breakout_df[
        ~breakout_df['AI 消息情緒'].str.contains('悲觀|看淡|中性', na=False)
    ]
    return golden_df

# ==========================================
# 9. Sidebar
# ==========================================
with st.sidebar:
    st.title('🧰 投資雙引擎')
    app_mode = st.radio('可用模組', [
        '🎯 RS x MACD 動能狙擊手',
        '📰 近月 AI 洞察 (廣東話版)',
        '🕵️ 另類數據雷達 (5大維度)',
        '🔍 個股驗證模式 (Bottom-Up)',
        '⚔️ 終極雙劍合璧 (Full Integration)'
    ])
    st.markdown('---')
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ==========================================
# 10. 模組 A
# ==========================================
if app_mode == '🎯 RS x MACD 動能狙擊手':
    st.title('🎯 美股 RS x MACD x 趨勢 狙擊手')
    st.markdown('幫你搵市場上動能最強、財報增長緊嘅爆發潛力股。')

    with st.expander('⚙️ 展開設定篩選參數', expanded=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown('#### 1️⃣ 基礎與趨勢')
            min_mcap = st.number_input('最低市值 (百萬 USD)', min_value=0.0, value=500.0, step=50.0)
            enable_sma = st.checkbox('啟動 【趨勢排列】 過濾', value=True)
            if enable_sma:
                sub1, sub2 = st.columns(2)
                sma_short = sub1.selectbox('短期 SMA', [10, 20, 25, 50], index=2)
                sma_long = sub2.selectbox('長期 SMA', [50, 100, 125, 150, 200], index=2)
                close_condition = st.selectbox(
                    '額外 Close 條件',
                    ['唔揀', 'Close > 短期 SMA', 'Close > 長期 SMA', 'Close > 短期及長期 SMA'],
                    index=1
                )
            else:
                sma_short, sma_long, close_condition = 25, 125, '唔揀'

        with col2:
            st.markdown('#### 2️⃣ RS 動能')
            enable_rs = st.checkbox('啟動 【RS】 過濾', value=True)
            selected_rs = st.multiselect(
                '顯示 RS 階段:',
                ['🚀 啱啱突破', '🔥 已經突破', '🎯 就快突破 (<5%)'],
                default=['🚀 啱啱突破']
            ) if enable_rs else []

        with col3:
            st.markdown('#### 3️⃣ MACD 爆發點')
            enable_macd = st.checkbox('啟動 【MACD】 過濾', value=True)
            selected_macd = st.multiselect(
                '顯示 MACD 階段:',
                ['🚀 啱啱突破', '🔥 已經突破', '🎯 就快突破 (<5%)'],
                default=['🚀 啱啱突破']
            ) if enable_macd else []

        start_scan = st.button('🚀 開始全市場精確掃描', use_container_width=True, type='primary')

    if start_scan:
        status_text, progress_bar = st.empty(), st.progress(0)
        status_text.markdown('**階段 1/3**: 搵緊 Finviz 基礎股票名單...')
        raw_data = fetch_finviz_data()
        progress_bar.progress(100)

        if not raw_data.empty:
            df_processed = raw_data.copy()
            df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
            final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].copy()

            if enable_rs or enable_macd or enable_sma:
                progress_bar.progress(0)
                indicators = calculate_all_indicators(
                    final_df['Ticker'].tolist(),
                    sma_short,
                    sma_long,
                    close_condition,
                    _progress_bar=progress_bar,
                    _status_text=status_text
                )

                final_df['RS_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('RS', '無'))
                final_df['MACD_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('MACD', '無'))
                final_df['SMA多頭'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('SMA_Trend', False))

                if enable_sma:
                    final_df = final_df[final_df['SMA多頭'] == True]
                if enable_rs:
                    final_df = final_df[final_df['RS_階段'].isin(selected_rs)]
                if enable_macd:
                    final_df = final_df[final_df['MACD_階段'].isin(selected_macd)]

                if len(final_df) > 0:
                    progress_bar.progress(0)
                    fund_df = fetch_fundamentals(
                        final_df['Ticker'].tolist(),
                        _progress_bar=progress_bar,
                        _status_text=status_text
                    )
                    final_df = pd.merge(final_df, fund_df, on='Ticker', how='left')
                    status_text.markdown('✅ **全市場掃描搞掂！**')
                    progress_bar.progress(100)

                    st.success(f'成功搵到 {len(final_df)} 隻潛力股票。')
                    cols = ['Ticker'] + [
                        c for c in [
                            'RS_階段', 'MACD_階段', 'Company', 'Sector', 'Industry',
                            'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)',
                            'Sales (近4季)', 'Sales Growth (QoQ)'
                        ] if c in final_df.columns
                    ]
                    st.dataframe(final_df[cols], use_container_width=True, hide_index=True, height=600)
                else:
                    status_text.markdown('✅ **全市場掃描搞掂！**')
                    progress_bar.progress(100)
                    st.warning('⚠️ 搵唔到完全滿足條件嘅股票。')

# ==========================================
# 11. 模組 B
# ==========================================
elif app_mode == '📰 近月 AI 洞察 (廣東話版)':
    st.title('📰 近月 AI 新聞深度分析')

    if st.button('🚀 攞今日 AI 報告', type='primary', use_container_width=True):
        with st.spinner('⏳ 嘗試緊從多個渠道攞歷史財經頭條同摘要...'):
            news_list = fetch_top_news()

        if news_list:
            with st.spinner('🧠 AI 認真睇緊內文，掃描所有潛力股票...'):
                report = analyze_news_ai(news_list)
                st.markdown('### 🤖 華爾街 AI 深度洞察報告')
                with st.container(border=True):
                    st.markdown(report)
        else:
            st.warning("⚠️ 暫時攞唔到新聞資料。")

# ==========================================
# 12. 模組 C
# ==========================================
elif app_mode == '🕵️ 另類數據雷達 (5大維度)':
    st.title('🕵️ 另類數據雷達 (5大維度)')

    c1, c2 = st.columns(2)
    with c1:
        st.markdown('**1. Reddit WSB 討論熱度 (過去24h)**')
        r_df, r_msg = fetch_reddit_sentiment()
        st.caption(r_msg)
        st.dataframe(r_df.head(8), use_container_width=True, hide_index=True)

    with c2:
        st.markdown('**2. StockTwits 全美熱搜榜 (即時)**')
        t_df, t_msg = fetch_stocktwits_trending()
        st.caption(t_msg)
        st.dataframe(t_df.head(8), use_container_width=True, hide_index=True)

    c3, c4 = st.columns(2)
    with c3:
        st.markdown('**3. X / FinTwit 社交情緒熱度 (過去7日)**')
        x_df, x_msg = fetch_x_sentiment()
        st.caption(x_msg)
        st.dataframe(x_df.head(8), use_container_width=True, hide_index=True)

    with c4:
        st.markdown('**4. 高層 Insider 真金白銀買入 (過去30日)**')
        i_df = fetch_insider_buying()
        st.dataframe(i_df.head(8), use_container_width=True, hide_index=True)

    st.markdown('**5. 國會議員交易 (過去45日申報)**')
    c_df, c_msg = fetch_congress_trades()
    st.caption(c_msg)
    st.dataframe(c_df.head(8), use_container_width=True, hide_index=True)

    if st.button('🚀 啟動 AI 五維交叉博弈分析', type='primary', use_container_width=True):
        with st.spinner('🧠 AI 正在進行 Reddit + StockTwits + X + Insider + Congress 五維度深度分析...'):
            res = analyze_alt_data_ai(r_df, t_df, x_df, i_df, c_df)
            st.markdown('### 🤖 另類數據 AI 偵測深度報告')
            with st.container(border=True):
                st.markdown(res)

# ==========================================
# 13. 模組 D
# ==========================================
elif app_mode == '🔍 個股驗證模式 (Bottom-Up)':
    st.title('🔍 個股驗證模式 (Bottom-Up)')
    st.markdown('當你見到一隻股票，輸入 Ticker 讓 AI 即時睇下佢背後有無新聞利好支撐。')

    target_ticker = st.text_input("輸入美股代號 (例如 TSLA, NVDA, ASTS):").upper().strip()

    if st.button('🧠 立即驗證', type='primary') and target_ticker:
        with st.spinner(f'抓取緊 {target_ticker} 嘅最新新聞並交由 AI 分析...'):
            news = fetch_single_stock_news(target_ticker)

            if news:
                res = analyze_single_stock_sentiment(target_ticker, news)
                st.subheader(f"📊 {target_ticker} 驗證結果")

                lines = [x.strip() for x in res.split('\n') if x.strip()]
                if lines:
                    st.markdown(f"### {lines[0]}")
                    body = "\n\n".join(lines[1:]) if len(lines) > 1 else "暫時無補充分析。"
                    with st.container(border=True):
                        st.markdown(body)
                else:
                    with st.container(border=True):
                        st.markdown(res)

                with st.expander("📄 點擊查看 AI 參考嘅原始新聞"):
                    for n in news:
                        st.caption(n)
            else:
                st.warning(f"⚠️ 搵唔到 {target_ticker} 嘅近期新聞。")

# ==========================================
# 14. 模組 E
# ==========================================
elif app_mode == '⚔️ 終極雙劍合璧 (Full Integration)':
    st.title('⚔️ 終極雙劍合璧 (Full Integration)')
    st.markdown('**全自動 Pipeline**：先用 RS x MACD 掃描全市場搵出突破股，再自動將名單送入 AI 新聞引擎，剔除壞消息/無消息嘅假突破，只留低「技術 + 消息」黃金共振股！')

    st.info("💡 呢個功能會消耗較多時間 (大約 2-3 分鐘)，請耐心等候。")

    if st.button('🚀 啟動終極掃描', type='primary', use_container_width=True):
        status_text, progress_bar = st.empty(), st.progress(0)

        status_text.markdown('**階段 1/2**: 正在執行全市場 RS x MACD 掃描 (強制設定市值 > 20億以加快速度)...')
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Mid (over $2bln)'})
        raw_data = f_screener.screener_view()

        if not raw_data.empty:
            df_processed = raw_data.copy()
            df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)

            indicators = calculate_all_indicators(
                df_processed['Ticker'].tolist(),
                25,
                125,
                'Close > 短期及長期 SMA',
                _progress_bar=progress_bar,
                _status_text=status_text
            )

            df_processed['RS_階段'] = df_processed['Ticker'].map(lambda x: indicators.get(x, {}).get('RS', '無'))
            df_processed['MACD_階段'] = df_processed['Ticker'].map(lambda x: indicators.get(x, {}).get('MACD', '無'))
            df_processed['SMA多頭'] = df_processed['Ticker'].map(lambda x: indicators.get(x, {}).get('SMA_Trend', False))

            tech_df = df_processed[
                (df_processed['SMA多頭'] == True) &
                (df_processed['RS_階段'].isin(['🚀 啱啱突破', '🔥 已經突破'])) &
                (df_processed['MACD_階段'].isin(['🚀 啱啱突破', '🔥 已經突破']))
            ].copy()

            if not tech_df.empty:
                st.success(f"✅ 技術面掃描完成！搵到 {len(tech_df)} 隻技術突破股。準備交由 AI 驗證基本面...")

                golden_df = run_full_integration(tech_df, progress_bar, status_text)

                status_text.markdown('✅ **終極掃描完成！**')
                progress_bar.progress(100)

                if not golden_df.empty:
                    st.balloons()
                    st.subheader(f"🏆 終極黃金共振名單 (共 {len(golden_df)} 隻)")
                    st.markdown("呢啲股票符合 **技術面突破** 加上 **AI 判定新聞偏正面**，係較高勝率嘅潛力股：")

                    display_cols = ['Ticker', 'Company', 'Sector', 'RS_階段', 'MACD_階段', 'AI 消息情緒']
                    existing_cols = [c for c in display_cols if c in golden_df.columns]
                    st.dataframe(golden_df[existing_cols], use_container_width=True, hide_index=True)

                    st.markdown("### 🧠 AI 深度分析逐隻睇")
                    for _, row in golden_df.iterrows():
                        with st.expander(f"{row.get('Ticker', 'N/A')} | {row.get('AI 消息情緒', 'N/A')}"):
                            st.markdown(row.get('AI 深度分析', '無分析內容。'))
                else:
                    st.warning('⚠️ 技術突破股經過 AI 驗證後，發現大部分都未有足夠消息面支持，為安全起見，本次無黃金名單輸出。')
            else:
                status_text.markdown('✅ 掃描完成。')
                st.warning("市場上暫時無股票同時符合嚴格嘅 RS 同 MACD 雙突破條件。")
        else:
            status_text.markdown('⚠️ 暫時攞唔到 Finviz 股票清單。')
