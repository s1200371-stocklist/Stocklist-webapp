
import os, re, json, time, random, datetime, requests
import pandas as pd
import streamlit as st
import yfinance as yf
import concurrent.futures
from datetime import timedelta
from pathlib import Path
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance

# ── Fast Mode: pre-computed scan cache helpers ──────────────────────────────
DEFAULT_SCAN_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'latest_scan.parquet')

def load_latest_scan_cache(path: str = DEFAULT_SCAN_PATH):
    """
    Load pre-computed scan cache from Parquet (preferred) or CSV fallback.
    Returns (df, scan_timestamp_str, status_message).
    df is empty DataFrame if no cache found.
    """
    base = Path(path)
    parquet_path = base.with_suffix('.parquet') if base.suffix != '.parquet' else base
    csv_path = base.with_suffix('.csv')

    for p in [parquet_path, csv_path]:
        if p.exists():
            try:
                df = pd.read_parquet(str(p)) if p.suffix == '.parquet' else pd.read_csv(str(p))
                if df.empty:
                    continue
                ts = str(df['scan_timestamp'].iloc[0]) if 'scan_timestamp' in df.columns else None
                try:
                    mtime = datetime.datetime.fromtimestamp(p.stat().st_mtime)
                    age = datetime.datetime.now() - mtime
                    h, m = int(age.total_seconds() // 3600), int((age.total_seconds() % 3600) // 60)
                    age_str = f'{h}h {m}m 前' if h else f'{m}m 前'
                except Exception:
                    age_str = '未知'
                status = f'✅ 快取載入成功 ({p.name}) — {len(df)} 筆 — 更新於 {age_str}'
                return df, ts, status
            except Exception:
                continue
    return pd.DataFrame(), None, '⚠️ 尚未有掃描快取。請運行 `python scanner.py` 生成。'

def _cache_age_hours(path: str = DEFAULT_SCAN_PATH) -> float:
    """Return age of cache file in hours, or 999 if not found."""
    base = Path(path)
    for p in [base.with_suffix('.parquet'), base.with_suffix('.csv')]:
        if p.exists():
            try:
                return (datetime.datetime.now() - datetime.datetime.fromtimestamp(p.stat().st_mtime)).total_seconds() / 3600
            except Exception:
                pass
    return 999

st.set_page_config(page_title='🚀 美股全方位量化與 AI 平台', page_icon='📈', layout='wide')


# ==========================================
# Global CSS / Dark Theme Polish
# ==========================================
st.markdown("""
<style>
/* ── Radar table improvements ── */
.rdr-tbl { width:100%; border-collapse:collapse; font-family:'Inter',sans-serif; font-size:0.79rem; }
.rdr-tbl th { background:#141824; color:#8a9bb0; padding:8px 10px; text-align:left;
    border-bottom:2px solid #252d3d; white-space:nowrap; font-size:0.73rem;
    font-weight:600; letter-spacing:0.3px; }
.rdr-tbl td { padding:7px 10px; vertical-align:middle; border-bottom:1px solid #1a1e28; }
.rdr-tbl tr:nth-child(even) { background:#0c0f18; }
.rdr-tbl tr:nth-child(odd)  { background:#101420; }
.rdr-tbl tr:hover { background:#1c2535 !important; transition:background 0.15s; }

/* ── Theme rank table ── */
.theme-rank { width:100%; border-collapse:collapse; font-family:'Inter',sans-serif; font-size:0.78rem; }
.theme-rank th { background:#181e2e; color:#8a9bb0; padding:7px 10px; text-align:left;
    border-bottom:2px solid #252d3d; white-space:nowrap; font-size:0.72rem; }
.theme-rank td { padding:6px 10px; vertical-align:middle; border-bottom:1px solid #1a1e28; }
.theme-rank tr:hover { background:#1c2535; transition:background 0.15s; }

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background:#101420; border:1px solid #252d3d;
    border-radius:8px; padding:10px 14px; }
[data-testid="metric-container"] label { color:#8a9bb0 !important; font-size:0.72rem !important; }
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size:1.3rem !important; font-weight:700 !important; color:#e0e8f0 !important; }

/* ── Section headers ── */
.section-divider {
    border:none; border-top:1px solid #252d3d;
    margin:14px 0; opacity:1; }

/* ── Streamlit expander ── */
[data-testid="stExpander"] {
    border:1px solid #252d3d !important; border-radius:8px !important;
    background:#0d1018 !important; }

/* ── Buttons ── */
[data-testid="stButton"] > button {
    border-radius:7px !important; font-weight:600 !important;
    transition:all 0.15s ease !important; }

/* ── Sidebar ── */
[data-testid="stSidebar"] { background:#0a0d14 !important; }
[data-testid="stSidebar"] [data-testid="stExpander"] {
    background:#0f1219 !important; border-color:#1e2430 !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width:6px; height:6px; }
::-webkit-scrollbar-track { background:#0a0d14; }
::-webkit-scrollbar-thumb { background:#2a3a4a; border-radius:3px; }
::-webkit-scrollbar-thumb:hover { background:#3a5a7a; }
</style>
""", unsafe_allow_html=True)


# ==========================================
# 工具函數
# ==========================================
def get_headers():
    ua = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36',
    ]
    return {'User-Agent': random.choice(ua), 'Accept': 'text/html,*/*;q=0.8', 'Accept-Language': 'en-US,en;q=0.9'}

def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except: return 0.0

def safe_to_string(df, rows=8):
    try:
        return "無數據" if df is None or df.empty else df.head(rows).to_string(index=False)
    except: return "無數據"

BAD_PATTERNS = [
    r'^\s*we must\b.*$', r'^\s*lets\b.*$', r'^\s*probably\b.*$',
    r'^\s*need to\b.*$', r'^\s*add insights\b.*$', r'^\s*also not use\b.*$',
    r'^\s*use plain text\b.*$', r'^\s*ensure we do not\b.*$',
    r'^\s*only the final report\b.*$', r'^\s*json\b.*$',
    r'^\s*role\b.*$', r'^\s*assistant\b.*$',
    r'^\s*reasoning_content\b.*$', r'^\s*tool_calls\b.*$',
]

def clean_ai_response(text):
    if not isinstance(text, str): return str(text)
    raw = text.strip()
    raw = re.sub(r'\s*```$', '', raw)
    raw = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL | re.IGNORECASE)
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            if "choices" in parsed and parsed["choices"]:
                msg = parsed["choices"][0].get("message", {})
                if isinstance(msg, dict):
                    c = msg.get("content")
                    if isinstance(c, str) and c.strip(): return c.strip()
            if "content" in parsed and isinstance(parsed["content"], str): return parsed["content"].strip()
            if parsed.get("role") == "assistant" and isinstance(parsed.get("content"), str): return parsed["content"].strip()
            if isinstance(parsed.get("final"), str) and parsed["final"].strip(): return parsed["final"].strip()
    except: pass
    raw = re.sub(r'"reasoning_content"\s*:\s*".*?"\s*,?', '', raw, flags=re.DOTALL)
    raw = re.sub(r'"role"\s*:\s*"assistant"\s*,?', '', raw, flags=re.DOTALL)
    raw = re.sub(r'"content"\s*:\s*', '', raw, flags=re.DOTALL)
    raw = re.sub(r',?\s*"tool_calls"\s*:\s*\[.*?\]\s*', '', raw, flags=re.DOTALL)
    raw = re.sub(r',?\s*"function_call"\s*:\s*\{.*?\}\s*', '', raw, flags=re.DOTALL)
    raw = re.sub(r',?\s*"finish_reason"\s*:\s*"tool_calls"\s*', '', raw, flags=re.DOTALL)
    raw = raw.replace('\\"', '"').replace('\\n', '\n').strip()
    raw = re.sub(r'^\{+', '', raw).strip()
    raw = re.sub(r'\}+$', '', raw).strip()
    lines = []
    for line in raw.splitlines():
        ls = line.strip()
        if not ls: lines.append(''); continue
        skip = any(re.match(p, ls, flags=re.IGNORECASE) for p in BAD_PATTERNS)
        if not skip: lines.append(line)
    raw = '\n'.join(lines)
    raw = raw.replace('","tool_calls":[]', '').replace('"tool_calls":[]', '')
    return re.sub(r'\n{3,}', '\n\n', raw).strip()

def final_text_sanitize(text):
    if not isinstance(text, str): return str(text)
    t = clean_ai_response(text)
    for p in [
        r'",\s*tool_calls"\s*:\s*\[.*?\]\s*$', r',\s*"tool_calls"\s*:\s*\[.*?\]\s*$',
        r'",\s*reasoning_content"\s*:\s*".*?$', r',\s*"reasoning_content"\s*:\s*".*?$',
        r'",\s*role"\s*:\s*"assistant".*?$', r',\s*"role"\s*:\s*"assistant".*?$',
    ]:
        t = re.sub(p, '', t, flags=re.DOTALL | re.IGNORECASE)
    t = t.replace('","tool_calls":[]', '').replace('"tool_calls":[]', '')
    return re.sub(r'\n{3,}', '\n\n', t).strip()

def call_pollinations(messages, model='openai', timeout=60):
    """呼叫 Pollinations AI，自動 retry + 429 fallback"""
    import time
    models_to_try = [model, 'openai', 'openai-large']
    for attempt, m in enumerate(models_to_try):
        try:
            r = requests.post(
                'https://text.pollinations.ai/',
                json={'messages': messages, 'model': m},
                timeout=timeout
            )
            if r.status_code == 429:
                time.sleep(3 + attempt * 2)
                continue
            if r.status_code == 200 and r.text.strip():
                return final_text_sanitize(r.text)
        except requests.exceptions.Timeout:
            if attempt < len(models_to_try) - 1:
                time.sleep(2)
                continue
            return f"⚠️ AI 逾時"
        except Exception as e:
            return f"⚠️ AI 發生錯誤: {e}"
    return f"⚠️ AI 暫時繁忙，請稍後重試"

def extract_cantonese_report(text):
    cleaned = final_text_sanitize(text)
    anchor = "【🕵️ 另類數據 AI 偵測深度報告】"
    idx = cleaned.find(anchor)
    if idx != -1: cleaned = cleaned[idx:].strip()
    headings = [
        "【🔥 社交熱度雙引擎：Reddit、StockTwits、X 正喺度推高邊啲股票？】",
        "【🏛️ 聰明錢與政客追蹤：終極內幕買緊乜？】",
        "【🎯 終極五維共振：最強爆發潛力股與高危陷阱】"
    ]
    found = sorted([(cleaned.find(h), h) for h in headings if cleaned.find(h) != -1])
    if found:
        rebuilt = [anchor]
        for i, (pos, h) in enumerate(found):
            end = found[i+1][0] if i+1 < len(found) else len(cleaned)
            rebuilt.append(final_text_sanitize(cleaned[pos:end].strip()))
        return '\n\n'.join(rebuilt).strip()
    fb = final_text_sanitize(cleaned)
    return f"{anchor}\n\n{fb}" if fb else f"{anchor}\n\n⚠️ AI 回傳格式異常，建議重新生成一次。"

def extract_stock_sentiment_output(text):
    LABELS = ["【🔥 極度看好】", "【📈 偏向樂觀】", "【⚖️ 中性觀望】", "【📉 偏向悲觀】", "【🧊 極度看淡】"]
    FB_BODY = "市場消息面暫時未有一一面倒優勢，利好與風險並存，現階段較適合保持審慎，等待更多業績、指引或催化消息再判斷後續方向。"
    cleaned = final_text_sanitize(text)
    
    # 強制截斷：尋找第一個出現的標籤，將前面的所有 AI 思考過程全部刪除
    first_label_idx = -1
    for l in LABELS:
        idx = cleaned.find(l)
        if idx != -1 and (first_label_idx == -1 or idx < first_label_idx):
            first_label_idx = idx
            
    if first_label_idx != -1:
        cleaned = cleaned[first_label_idx:]

    lines = [l.strip() for l in cleaned.split('\n') if l.strip()]
    label, body_lines = "【⚖️ 中性觀望】", []
    label_found = False
    
    for line in lines:
        if line in LABELS or any(l in line for l in LABELS):
            for l in LABELS:
                if l in line:
                    label = l
                    label_found = True
                    break
            continue
            
        if not label_found: continue
        
        low = line.lower()
        if any(k in low for k in ["reasoning_content", "tool_calls", '"role"', '"content"', "let's", "i will"]): continue
        if line.startswith("{") and line.endswith("}"): continue
        body_lines.append(line)
        
    body = final_text_sanitize('\n\n'.join(body_lines).strip())
    return label, body if body else FB_BODY

# ==========================================
# 新聞
# ==========================================
def parse_rss_items(xml_text, source_name, limit=10):
    items = []
    try:
        for block in re.findall(r'<item>(.*?)</item>', xml_text, flags=re.DOTALL | re.IGNORECASE)[:limit]:
            tm = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>|<title>(.*?)</title>', block, flags=re.DOTALL | re.IGNORECASE)
            dm = re.search(r'<description><!\[CDATA\[(.*?)\]\]></description>|<description>(.*?)</description>', block, flags=re.DOTALL | re.IGNORECASE)
            title = re.sub(r'<.*?>', '', (tm.group(1) or tm.group(2) or '') if tm else '').strip()
            desc = re.sub(r'<.*?>', '', (dm.group(1) or dm.group(2) or '') if dm else '').strip()
            if title: items.append({'來源': source_name, '新聞標題': title, '內文摘要': desc[:240] if desc else '（RSS 摘要）'})
    except: pass
    return items

def fetch_rss_market_news():
    sources = [('CNBC', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?profile=120000000&id=10000664'), ('MarketWatch', 'https://feeds.content.dowjones.io/public/rss/mw_topstories')]
    items, seen = [], set()
    for name, url in sources:
        try:
            res = requests.get(url, headers=get_headers(), timeout=10)
            if res.status_code == 200:
                for item in parse_rss_items(res.text, name, 8):
                    if item['新聞標題'] not in seen:
                        seen.add(item['新聞標題']); items.append(item)
        except: pass
    return items

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_top_news():
    items, seen = [], set()
    try:
        for t in ['SPY', 'QQQ']:
            news = finvizfinance(t).ticker_news()
            if news is not None and not news.empty:
                for _, row in news.head(15).iterrows():
                    title = str(row.get('Title', '')).strip()
                    if title and title not in seen:
                        seen.add(title); items.append({'來源': row.get('Source', 'Finviz'), '新聞標題': title, '內文摘要': '（來自 Finviz 標題）'})
    except: pass
    try:
        for t in ['SPY', 'QQQ', 'NVDA', 'AAPL']:
            tkr = yf.Ticker(t)
            if hasattr(tkr, 'news') and isinstance(tkr.news, list):
                for item in tkr.news[:5]:
                    content = item.get('content', {}) if isinstance(item, dict) else {}
                    title = str(content.get('title', item.get('title', ''))).strip()
                    summary = content.get('summary', item.get('summary', '無內文'))
                    if title and title not in seen:
                        seen.add(title); items.append({'來源': item.get('publisher', 'Yahoo'), '新聞標題': title, '內文摘要': str(summary)[:240]})
    except: pass
    if len(items) < 8:
        for item in fetch_rss_market_news():
            if item['新聞標題'] not in seen:
                seen.add(item['新聞標題']); items.append(item)
    if not items:
        items = [
            {'來源': 'System Mock', '新聞標題': '大型科技股進入財報前觀望期，市場聚焦 AI 資本開支與指引', '內文摘要': '投資者正觀望雲端、晶片與廣告平台巨頭對 AI 投資回報的最新說法。'},
            {'來源': 'System Mock', '新聞標題': '聯儲局政策預期反覆，成長股波動加劇', '內文摘要': '市場對減息時間表仍有分歧，高估值板塊短線走勢受壓。'},
            {'來源': 'System Mock', '新聞標題': '半導體與算力需求持續受關注，資金重新審視估值合理性', '內文摘要': 'AI 相關股份熱度未完全退潮，但市場開始區分基本面與題材炒作。'},
        ]
    return items

# ==========================================
# 另類數據 (6 大維度)
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_sentiment():
    try:
        res = requests.get('https://apewisdom.io/api/v1.0/filter/all-stocks/page/1', headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        if res.status_code == 200:
            results = res.json().get('results', [])
            if results:
                rows = []
                for i in results[:10]:
                    now_m = i.get('mentions', 0)
                    prev_m = i.get('mentions_24h_ago', now_m)
                    now_r = i.get('rank', 99)
                    prev_r = i.get('rank_24h_ago', now_r)
                    pct = (now_m - prev_m) / prev_m * 100 if prev_m > 0 else 0
                    if pct >= 20 or now_r < prev_r:
                        trend = f'📈 上升 (+{pct:.0f}%)'
                    elif pct <= -20 or now_r > prev_r:
                        trend = f'📉 下跌 ({pct:.0f}%)'
                    else:
                        trend = f'▶ 平穩 ({pct:+.0f}%)'
                    rows.append({'Ticker': str(i.get('ticker', '')).upper(), 'Sentiment': 'Bullish' if now_m > 30 else 'Neutral', 'Mentions': now_m, 'Trend': trend})
                df = pd.DataFrame(rows)
                return df, '🟢 ApeWisdom (過去24h數據)'
    except: pass
    return pd.DataFrame([
        {'Ticker': 'SPY',  'Sentiment': 'Bullish', 'Mentions': 2420, 'Trend': '📈 上升 (+45%)'},
        {'Ticker': 'NVDA', 'Sentiment': 'Bullish', 'Mentions': 1965, 'Trend': '📈 上升 (+32%)'},
        {'Ticker': 'TSLA', 'Sentiment': 'Bullish', 'Mentions': 1540, 'Trend': '▶ 平穩 (+5%)'},
        {'Ticker': 'AAPL', 'Sentiment': 'Neutral', 'Mentions': 1120, 'Trend': '📉 下跌 (-18%)'},
        {'Ticker': 'AMD',  'Sentiment': 'Bullish', 'Mentions': 890,  'Trend': '📈 上升 (+28%)'},
        {'Ticker': 'PLTR', 'Sentiment': 'Bullish', 'Mentions': 780,  'Trend': '📈 上升 (+61%)'},
        {'Ticker': 'MSFT', 'Sentiment': 'Neutral', 'Mentions': 650,  'Trend': '▶ 平穩 (-3%)'},
        {'Ticker': 'META', 'Sentiment': 'Bullish', 'Mentions': 530,  'Trend': '📈 上升 (+22%)'},
        {'Ticker': 'COIN', 'Sentiment': 'Bullish', 'Mentions': 480,  'Trend': '📉 下跌 (-25%)'},
        {'Ticker': 'MARA', 'Sentiment': 'Bullish', 'Mentions': 370,  'Trend': '▶ 平穩 (+8%)'},
    ]), '🔴 離線備援 (WSB)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_stocktwits_trending():
    try:
        res = requests.get('https://api.stocktwits.com/api/2/trending/symbols.json', headers=get_headers(), timeout=8)
        if res.status_code == 200:
            symbols = res.json().get('symbols', [])
            if symbols:
                rows = []
                for s in symbols[:10]:
                    score = float(s.get('trending_score') or 0)
                    rank = int(s.get('rank') or 99)
                    if score >= 15 or rank <= 5:
                        trend = f'📈 熱烈上升 (Score:{score:.1f})'
                    elif score >= 5:
                        trend = f'▶ 溫和上升 (Score:{score:.1f})'
                    else:
                        trend = f'📉 熱度下降 (Score:{score:.1f})'
                    rows.append({'Ticker': s.get('symbol', ''), 'Name': s.get('title', ''), 'Trend': trend})
                return pd.DataFrame(rows), '🟢 StockTwits 正常'
    except: pass
    return pd.DataFrame([
        {'Ticker': 'CAR',  'Name': 'Avis Budget Group',       'Trend': '📈 熱烈上升 (Score:28.4)'},
        {'Ticker': 'UNH',  'Name': 'UnitedHealth Group',      'Trend': '📉 熱度下降 (Score:3.2)'},
        {'Ticker': 'NVDA', 'Name': 'NVIDIA Corporation',      'Trend': '📈 熱烈上升 (Score:24.4)'},
        {'Ticker': 'TSLA', 'Name': 'Tesla Inc',               'Trend': '📈 熱烈上升 (Score:21.7)'},
        {'Ticker': 'AAPL', 'Name': 'Apple Inc',               'Trend': '▶ 溫和上升 (Score:9.1)'},
        {'Ticker': 'AMD',  'Name': 'Advanced Micro Devices',  'Trend': '📈 熱烈上升 (Score:19.4)'},
        {'Ticker': 'PLTR', 'Name': 'Palantir Technologies',   'Trend': '📈 熱烈上升 (Score:17.8)'},
        {'Ticker': 'MSTR', 'Name': 'MicroStrategy',           'Trend': '▶ 溫和上升 (Score:8.3)'},
        {'Ticker': 'COIN', 'Name': 'Coinbase Global',         'Trend': '▶ 溫和上升 (Score:6.5)'},
        {'Ticker': 'CRWD', 'Name': 'CrowdStrike Holdings',    'Trend': '▶ 溫和上升 (Score:7.2)'},
    ]), '🔴 離線備援 (StockTwits)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_x_sentiment():
    api_key = os.getenv("X_SENTIMENT_API_KEY")
    if api_key:
        try:
            res = requests.get("https://api.adanos.org/x-stocks/sentiment", headers={"Authorization": f"Bearer {api_key}"}, params={"limit": 10}, timeout=12)
            if res.status_code == 200:
                stocks = res.json().get("stocks", [])
                if stocks:
                    rows = []
                    for item in stocks[:10]:
                        score = item.get("sentiment_score", 0)
                        rows.append({"Ticker": str(item.get("ticker", "")).upper(), "Sentiment": "Bullish" if score >= 0.25 else ("Bearish" if score <= -0.25 else "Neutral"), "Mentions": item.get("mentions", 0), "Bullish %": item.get("bullish_pct", 50), "Trend": item.get("trend", "N/A")})
                    return pd.DataFrame(rows), "🟢 X / FinTwit API 正常"
        except: pass
    return pd.DataFrame([
        {"Ticker": "TSLA", "Sentiment": "Bullish", "Mentions": 4820, "Bullish %": 68, "Trend": "Rising"},
        {"Ticker": "NVDA", "Sentiment": "Bullish", "Mentions": 3910, "Bullish %": 72, "Trend": "Rising"},
        {"Ticker": "PLTR", "Sentiment": "Bullish", "Mentions": 2440, "Bullish %": 66, "Trend": "Stable"},
        {"Ticker": "AAPL", "Sentiment": "Neutral", "Mentions": 2210, "Bullish %": 52, "Trend": "Stable"},
        {"Ticker": "AMD",  "Sentiment": "Bullish", "Mentions": 1980, "Bullish %": 61, "Trend": "Rising"},
        {"Ticker": "META", "Sentiment": "Bullish", "Mentions": 1750, "Bullish %": 64, "Trend": "Rising"},
        {"Ticker": "MSFT", "Sentiment": "Neutral", "Mentions": 1620, "Bullish %": 55, "Trend": "Stable"},
        {"Ticker": "COIN", "Sentiment": "Bullish", "Mentions": 1430, "Bullish %": 63, "Trend": "Rising"},
        {"Ticker": "MSTR", "Sentiment": "Bullish", "Mentions": 1280, "Bullish %": 70, "Trend": "Rising"},
        {"Ticker": "CRWD", "Sentiment": "Bullish", "Mentions": 1090, "Bullish %": 59, "Trend": "Stable"},
    ]), "🔴 離線備援 (X / FinTwit)"

# ==========================================
# Part 4: 全市場真實 Insider 買入 (全新 OpenInsider 抓取器)
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    # 首選方案：OpenInsider (市場公認最準確、最快更新的全市場高管買入追蹤)
    try:
        url = 'http://openinsider.com/insider-purchases-25k' # 專門抓 25K 美金以上嘅大額買入
        res = requests.get(url, headers=get_headers(), timeout=12)
        if res.status_code == 200:
            dfs = pd.read_html(res.text)
            for df in dfs:
                if 'Ticker' in df.columns and 'Value' in df.columns:
                    df = df.dropna(subset=['Ticker', 'Value'])
                    df = df[['Trade Date', 'Ticker', 'Insider Name', 'Title', 'Price', 'Value']]
                    df = df.rename(columns={'Trade Date': 'Date', 'Insider Name': 'Insider'})
                    df['Ticker'] = df['Ticker'].astype(str).str.upper()
                    if not df.empty:
                        return df.head(10), '🟢 OpenInsider 真實數據 (最新 >$25k 買入)'
    except Exception as e: 
        pass

    # 備援方案：Finviz Insider
    try:
        from finvizfinance.insider import Insider
        insider = Insider(option='top owner buys')
        df = insider.get_insider()
        if not df.empty:
            df = df[['Date', 'Ticker', 'Owner', 'Relationship', 'Cost', 'Value ($)']]
            df = df.rename(columns={'Owner': 'Insider', 'Relationship': 'Title', 'Cost': 'Price', 'Value ($)': 'Value'})
            df['Value'] = df['Value'].apply(lambda x: f"${x:,.0f}" if isinstance(x, (int, float)) else str(x))
            return df.head(10), '🟢 Finviz 真實數據 (Top Owner Buys)'
    except: 
        pass

    # 靜態備援 (避免系統崩潰)
    return pd.DataFrame([
        {'Date': datetime.datetime.now().strftime('%Y-%m-%d'), 'Ticker': 'ASTS', 'Insider': 'Abel Avellan', 'Title': 'CEO', 'Price': '$24.50', 'Value': '$2,500,000'},
        {'Date': (datetime.datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'), 'Ticker': 'PLTR', 'Insider': 'Peter Thiel', 'Title': 'Director', 'Price': '$82.00', 'Value': '$1,230,000'},
    ]), '🔴 離線備援數據'

# ==========================================
# Part 5: 真實國會交易 (QuiverQuant API)
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_congress_trades():
    cutoff = (datetime.datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    try:
        r = requests.get(
            'https://api.quiverquant.com/beta/live/congresstrading',
            headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'},
            timeout=12
        )
        if r.status_code == 200:
            data = r.json()
            purchases = []
            SMALL_RANGES = {'$1,001 - $15,000', '$201 - $1,000', '$0 - $200', ''}
            for d in data:
                if not isinstance(d, dict): continue
                if d.get('Transaction') != 'Purchase': continue
                desc = str(d.get('Description') or '')
                if 'dividend' in desc.lower(): continue
                ticker = str(d.get('Ticker') or '')
                if not ticker or len(ticker) > 5 or not ticker.replace('.', '').isalpha(): continue
                rng = str(d.get('Range') or '')
                if rng in SMALL_RANGES: continue  # filter out small trades
                tx_date = str(d.get('TransactionDate') or '')
                if tx_date < cutoff: continue
                amt = float(d.get('Amount') or 0)
                purchases.append({
                    'Date': tx_date,
                    'Politician': d.get('Representative', 'N/A'),
                    'Party': d.get('Party', ''),
                    'Ticker': ticker,
                    'Amount': rng,
                    'House': d.get('House', ''),
                    '_amt': amt,
                })
            if purchases:
                purchases.sort(key=lambda x: (x['Date'], x['_amt']), reverse=True)
                df = pd.DataFrame(purchases).head(10)[['Date', 'Politician', 'Party', 'Ticker', 'Amount', 'House']]
                return df.reset_index(drop=True), f'🟢 QuiverQuant 真實數據 (過去60日, {len(purchases)} 筆)'
    except: pass
    return pd.DataFrame([
        {'Date': '2026-03-27', 'Politician': 'Josh Gottheimer',      'Party': 'D', 'Ticker': 'MSFT', 'Amount': '$500,001 - $1,000,000', 'House': 'Representatives'},
        {'Date': '2026-03-25', 'Politician': 'Josh Gottheimer',      'Party': 'D', 'Ticker': 'MSFT', 'Amount': '$50,001 - $100,000',    'House': 'Representatives'},
        {'Date': '2026-03-24', 'Politician': 'Maria Elvira Salazar', 'Party': 'R', 'Ticker': 'HON',  'Amount': '$15,001 - $50,000',     'House': 'Representatives'},
        {'Date': '2026-03-24', 'Politician': 'Maria Elvira Salazar', 'Party': 'R', 'Ticker': 'AMGN', 'Amount': '$15,001 - $50,000',     'House': 'Representatives'},
        {'Date': '2026-03-23', 'Politician': 'Tim Moore',            'Party': 'R', 'Ticker': 'CBRL', 'Amount': '$15,001 - $50,000',     'House': 'Representatives'},
        {'Date': '2026-03-20', 'Politician': 'Gilbert Cisneros',     'Party': 'D', 'Ticker': 'MIAX', 'Amount': '$15,001 - $50,000',     'House': 'Representatives'},
        {'Date': '2026-03-20', 'Politician': 'Tim Moore',            'Party': 'R', 'Ticker': 'LGIH', 'Amount': '$15,001 - $50,000',     'House': 'Representatives'},
        {'Date': '2026-03-19', 'Politician': 'Maria Elvira Salazar', 'Party': 'R', 'Ticker': 'RH',   'Amount': '$15,001 - $50,000',     'House': 'Representatives'},
        {'Date': '2026-03-19', 'Politician': 'Maria Elvira Salazar', 'Party': 'R', 'Ticker': 'GS',   'Amount': '$15,001 - $50,000',     'House': 'Representatives'},
        {'Date': '2026-03-19', 'Politician': 'Maria Elvira Salazar', 'Party': 'R', 'Ticker': 'CSCO', 'Amount': '$15,001 - $50,000',     'House': 'Representatives'},
    ]), '🔴 離線備援 (Congress)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_5ch_sentiment():
    data = [
        {"Ticker": "NVDA", "Name": "エヌビディア",        "Sentiment": "🚀 極度狂熱",  "Trend": "📈 上升",  "Source": "5ch/YahooJP"},
        {"Ticker": "AMD",  "Name": "AMD",                 "Sentiment": "📈 偏向樂觀",  "Trend": "📈 上升",  "Source": "5ch/YahooJP"},
        {"Ticker": "TSLA", "Name": "テスラ",               "Sentiment": "📉 悲觀/做空", "Trend": "📉 下跌",  "Source": "5ch/YahooJP"},
        {"Ticker": "AAPL", "Name": "アップル",             "Sentiment": "⚖️ 中立",     "Trend": "▶ 平穩",  "Source": "5ch/YahooJP"},
        {"Ticker": "PLTR", "Name": "パランティア",         "Sentiment": "📈 偏向樂觀",  "Trend": "📈 上升",  "Source": "5ch/YahooJP"},
        {"Ticker": "MSFT", "Name": "マイクロソフト",       "Sentiment": "⚖️ 中立",     "Trend": "▶ 平穩",  "Source": "5ch/YahooJP"},
        {"Ticker": "META", "Name": "メタ",                 "Sentiment": "📈 偏向樂觀",  "Trend": "📈 上升",  "Source": "5ch/YahooJP"},
        {"Ticker": "COIN", "Name": "コインベース",         "Sentiment": "🚀 極度狂熱",  "Trend": "📈 上升",  "Source": "5ch/YahooJP"},
        {"Ticker": "MSTR", "Name": "マイクロストラテジー", "Sentiment": "🚀 極度狂熱",  "Trend": "📈 上升",  "Source": "5ch/YahooJP"},
        {"Ticker": "INTC", "Name": "インテル",             "Sentiment": "📉 悲觀/做空", "Trend": "📉 下跌",  "Source": "5ch/YahooJP"},
    ]
    return pd.DataFrame(data), "🟢 日本 2ch/5ch 海外板塊情緒 (示意)"

# ==========================================
# 量化技術與財報
# ==========================================
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f = Overview()
        f.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f.screener_view()
    except: return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, close_condition, batch_size=200, _progress_bar=None, _status_text=None):
    results = {}
    bench_data, used_bench = pd.DataFrame(), ''
    for b in ['QQQ', '^NDX', 'QQQM']:
        try:
            tmp = yf.download(b, period='2y', progress=False, group_by='column', auto_adjust=False)
            if not tmp.empty and 'Close' in tmp.columns:
                bench_data = tmp['Close'].to_frame(name=b) if isinstance(tmp['Close'], pd.Series) else tmp['Close']
                used_bench = b; break
        except: continue
    if bench_data.empty: return results
    if getattr(bench_data.index, 'tz', None) is not None: bench_data.index = bench_data.index.tz_localize(None)
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]
    for i in range(0, len(tickers), batch_size):
        bt = tickers[i:i + batch_size]
        if _status_text: _status_text.markdown(f'**階段 2/3**: 計緊技術指標... (`{min(i+batch_size, len(tickers))}` / `{len(tickers)}`)')
        if _progress_bar: _progress_bar.progress(min(1.0, (i+batch_size) / max(len(tickers), 1)))
        try:
            data = yf.download(bt, period='2y', progress=False, group_by='column', auto_adjust=False)
            if data.empty or 'Close' not in data.columns: raise ValueError()
            cp = data['Close']
            if isinstance(cp, pd.Series): cp = cp.to_frame(name=bt[0])
            cp = cp.ffill().dropna(how='all')
            if getattr(cp.index, 'tz', None) is not None: cp.index = cp.index.tz_localize(None)
            for ticker in bt:
                rs, ms, st = '無', '無', False
                if ticker in cp.columns and not cp[ticker].dropna().empty:
                    sp = cp[ticker].dropna()
                    if len(sp) > max(sma_short, sma_long) + 1:
                        sn = sp / sp.iloc[0]
                        rl = sn / bench_norm.reindex(sn.index).ffill() * 100
                        rma = rl.rolling(25).mean()
                        if float(rl.iloc[-1]) > float(rma.iloc[-1]): rs = '🚀 啱啱突破' if float(rl.iloc[-2]) <= float(rma.iloc[-2]) else '🔥 已經突破'
                        elif float(rl.iloc[-1]) >= float(rma.iloc[-1]) * 0.95: rs = '🎯 就快突破 (<5%)'
                        e12, e26 = sp.ewm(span=12, adjust=False).mean(), sp.ewm(span=26, adjust=False).mean()
                        ml = e12 - e26; sl = ml.ewm(span=9, adjust=False).mean()
                        if float(ml.iloc[-1]) > float(sl.iloc[-1]): ms = '🚀 啱啱突破' if float(ml.iloc[-2]) <= float(sl.iloc[-2]) else '🔥 已經突破'
                        elif abs(float(ml.iloc[-1]) - float(sl.iloc[-1])) <= max(abs(float(sl.iloc[-1])) * 0.05, 1e-9): ms = '🎯 就快突破 (<5%)'
                        ss, ls = sp.rolling(sma_short).mean(), sp.rolling(sma_long).mean()
                        lc, lss, lls = float(sp.iloc[-1]), float(ss.iloc[-1]), float(ls.iloc[-1])
                        tok = lss > lls
                        if close_condition == 'Close > 短期 SMA': tok = tok and lc > lss
                        elif close_condition == 'Close > 長期 SMA': tok = tok and lc > lls
                        elif close_condition == 'Close > 短期及長期 SMA': tok = tok and lc > lss and lc > lls
                        st = tok
                results[ticker] = {'RS': rs, 'MACD': ms, 'SMA_Trend': st}
        except:
            for t in bt: results[t] = {'RS': '無', 'MACD': '無', 'SMA_Trend': False}
        time.sleep(0.5)
    return results

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    def fetch_single(t):
        for _ in range(2):
            try:
                tkr = yf.Ticker(t)
                q = tkr.quarterly_financials
                if q is None or q.empty: q = tkr.quarterly_income_stmt
                if q is None or q.empty: continue
                cols = sorted(list(q.columns)[:4]) if len(q.columns) >= 4 else list(q.columns)
                er = next((q.loc[r] for r in ['Diluted EPS', 'Basic EPS', 'Normalized EPS'] if r in q.index), None)
                sr = next((q.loc[r] for r in ['Total Revenue', 'Operating Revenue'] if r in q.index), None)
                ev = [float(er[c]) if er is not None and pd.notna(er[c]) else None for c in cols]
                sv = [float(sr[c]) if sr is not None and pd.notna(sr[c]) else None for c in cols]
                def fv(vs, s=False): return ' | '.join(['-' if v is None else (f'{v/1e9:.2f}B' if s and v>=1e9 else (f'{v/1e6:.2f}M' if s and v>=1e6 else f'{v:.2f}')) for v in vs])
                def fg(vs): return ' | '.join(['-'] + [f'{(vs[i]-vs[i-1])/abs(vs[i-1])*100:+.1f}%' if vs[i] is not None and vs[i-1] is not None and vs[i-1]!=0 else '-' for i in range(1, len(vs))])
                return {'Ticker': t, 'EPS (近4季)': fv(ev), 'EPS Growth (QoQ)': fg(ev), 'Sales (近4季)': fv(sv, True), 'Sales Growth (QoQ)': fg(sv)}
            except: time.sleep(1)
        return {'Ticker': t, 'EPS (近4季)': 'N/A', 'EPS Growth (QoQ)': 'N/A', 'Sales (近4季)': 'N/A', 'Sales Growth (QoQ)': 'N/A'}
    empty = pd.DataFrame(columns=['Ticker', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'])
    if not tickers: return empty
    res, done = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        for f in concurrent.futures.as_completed({ex.submit(fetch_single, t): t for t in tickers}):
            if f.result(): res.append(f.result())
            done += 1
            if _status_text: _status_text.markdown(f'**階段 3/3**: 攞緊最新財報數據... (`{done}` / `{len(tickers)}`)')
            if _progress_bar: _progress_bar.progress(min(1.0, done / max(len(tickers), 1)))
    return pd.DataFrame(res) if res else empty

# ==========================================
# AI 分析 (加入板塊與受惠名單)
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list: return '⚠️ 目前攞唔到新聞數據，請遲啲再試下。'
    news_text = '\n'.join([f"{i+1}. [{x['來源']}] 標題：{x['新聞標題']}\n摘要：{x['內文摘要']}\n" for i, x in enumerate(news_list)])
    
    sys_p = "你是一位資深的香港華爾街財經專家。你的任務是直接撰寫市場報告，全程使用「香港廣東話（口語）」及繁體中文。請直接輸出最終的報告內容，絕對禁止包含任何思考過程或英文邏輯推理。"
    
    user_p = f"""請根據以下最新財經新聞，寫一份香港廣東話版本的市場分析報告。

新聞數據：
{news_text}

[嚴重警告：請直接由標題開始寫，不要輸出任何 "Let's produce", "In the first section" 等思考過程。如果輸出任何分析計劃，系統會崩潰！]

必須嚴格包含以下三個標題，並由標題開始按順序輸出：

【📉 近月市場焦點總結】
（用廣東話總結大市氣氛同宏觀焦點）

【🎯 焦點板塊與受惠名單】
（請明確提取新聞中提及或隱含嘅「強勢/弱勢板塊」，並必定要標明相關嘅「美股代號 Ticker」或「指標性公司名稱」，例如：板塊：半導體 👉 相關個股：NVDA, AMD。請用清晰嘅點列形式列出）

【🚀 潛力爆發股全面掃描】
（根據板塊同資金流向，用廣東話做深度分析同前瞻）"""

    r = call_pollinations([{'role': 'system', 'content': sys_p}, {'role': 'user', 'content': user_p}], timeout=60)
    c = final_text_sanitize(r)
    
    # 🔪 終極殺手鐧：尋找預設標題，將標題前面的所有「推理廢話」直接切斷拋棄
    anchor = "【📉 近月市場焦點總結】"
    idx = c.find(anchor)
    if idx != -1:
        c = c[idx:]  # 只保留標題及之後的內容
    else:
        # 容錯：如果 AI 稍微改了標題
        alt_idx = c.find("近月市場焦點總結")
        if alt_idx != -1:
            c = anchor + "\n" + c[alt_idx + len("近月市場焦點總結"):]
        else:
            c = f"{anchor}\n\n{c}"

    return c

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, twits_df, x_df, insider_df, congress_df):
    sys_p = "你是一位資深的香港華爾街財經專家。請直接使用「香港廣東話（口語）」及繁體中文輸出深度報告。絕對禁止輸出任何英文思考過程。請直接給出純文字分析結果。"
    usr_p = f"""請根據以下另類數據直接寫一份純文字的廣東話報告：

Reddit:
{safe_to_string(reddit_df)}

StockTwits:
{safe_to_string(twits_df)}

X:
{safe_to_string(x_df)}

Insiders:
{safe_to_string(insider_df)}

Congress:
{safe_to_string(congress_df)}

請務必包含以下標題（直接由標題開始寫，不要有任何英文開場白）：
【🕵️ 另類數據 AI 偵測深度報告】
【🔥 社交熱度雙引擎：Reddit、StockTwits、X 正喺度推高邊啲股票？】
【🏛️ 聰明錢與政客追蹤：終極內幕買緊乜？】
【🎯 終極五維共振：最強爆發潛力股與高危陷阱】"""

    return extract_cantonese_report(call_pollinations([{'role': 'system', 'content': sys_p}, {'role': 'user', 'content': usr_p}], timeout=80))

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_single_stock_news(ticker):
    items = []
    try:
        tkr = yf.Ticker(ticker)
        if hasattr(tkr, 'news') and isinstance(tkr.news, list):
            for item in tkr.news[:10]:
                content = item.get('content', {}) if isinstance(item, dict) else {}
                title = content.get('title', item.get('title', ''))
                if title: items.append(f"標題: {title} | 摘要: {str(content.get('summary', item.get('summary', '')))[:220]}")
    except: pass
    if not items:
        try:
            news = finvizfinance(ticker).ticker_news()
            if not news.empty:
                for _, row in news.head(10).iterrows():
                    items.append(f"標題: {row.get('Title', '')} | 來源: {row.get('Source', '')}")
        except: pass
    return items

def analyze_single_stock_sentiment(ticker, news_items):
    if not news_items: return "【⚖️ 中性觀望】\n\n缺乏近期專屬新聞，暫時未見足夠催化劑，較適合先觀望。"
    sys_p = "你是一位專業的香港華爾街財經AI。請直接使用香港廣東話（口語）進行分析。嚴禁輸出任何英文思考過程。"
    usr_p = f"""分析 {ticker} 近期新聞：
{chr(10).join(news_items)}

規則：
1. 第一行必須完全等於以下其中一個，不要加其他字：【🔥 極度看好】或【📈 偏向樂觀】或【⚖️ 中性觀望】或【📉 偏向悲觀】或【🧊 極度看淡】。
2. 第一行之後，請用廣東話自然分析並解釋原因。

請直接輸出最終答案，不要任何英文或思考廢話。"""
    r = call_pollinations([{'role': 'system', 'content': sys_p}, {'role': 'user', 'content': usr_p}], timeout=25)
    label, body = extract_stock_sentiment_output(r)
    return f"{label}\n\n{body}"

# ==========================================
# Full Integration
# ==========================================
def run_full_integration(final_df, progress_bar, status_text):
    if final_df.empty: return pd.DataFrame()
    bdf = final_df[final_df['RS_階段'].isin(['🚀 啱啱突破', '🔥 已經突破']) | final_df['MACD_階段'].isin(['🚀 啱啱突破', '🔥 已經突破'])].copy()
    if bdf.empty: return pd.DataFrame()
    total = min(15, len(bdf))
    bdf = bdf.head(total)
    sentiments, reasons = [], []
    for _, row in bdf.iterrows():
        ticker = row['Ticker']
        status_text.markdown(f"**終極驗證中...** 正在用 AI 掃描 `{ticker}` 嘅新聞基本面 ({len(sentiments)+1}/{total})")
        progress_bar.progress((len(sentiments)+1) / total)
        news = fetch_single_stock_news(ticker)
        if news:
            ai_res = final_text_sanitize(analyze_single_stock_sentiment(ticker, news))
            lines = [x.strip() for x in ai_res.split('\n') if x.strip()]
            sentiments.append(lines[0] if lines else "【⚖️ 中性觀望】")
            reasons.append(final_text_sanitize('\n\n'.join(lines[1:]) if len(lines) > 1 else '無具體解釋。'))
        else:
            sentiments.append("【⚖️ 中性觀望】"); reasons.append("無新聞數據。")
        time.sleep(1)
    bdf['AI 消息情緒'] = sentiments
    bdf['AI 深度分析'] = reasons
    return bdf[~bdf['AI 消息情緒'].str.contains('悲觀|看淡|中性', na=False)]


# ==========================================
# 熱門板塊關係圖 模組 (方案B: AI全動態)
# ==========================================

# ==========================================
# 熱門板塊關係圖 模組 (方案B: AI全動態)
# ==========================================

# 顏色池（板塊用）
SECTOR_COLORS = [
    '#FF6B6B','#4ECDC4','#45B7D1','#96CEB4','#FFEAA7',
    '#DDA0DD','#F0A500','#98D8C8','#FFB6C1','#B8860B',
    '#87CEEB','#FFA07A','#90EE90','#DEB887','#ADD8E6',
]

# 固定備用板塊（當AI失敗時用）
FALLBACK_SECTOR_STOCKS = {
    '🤖 人工智能 AI': {'stocks': {'PLTR':'Palantir','AI':'C3.ai','MSFT':'Microsoft','GOOGL':'Google','SOUN':'SoundHound'}},
    '⚡ 晶片/半導體':  {'stocks': {'NVDA':'NVIDIA','AMD':'AMD','AVGO':'Broadcom','AMAT':'Applied Materials','ARM':'ARM'}},
    '🗄️ 數據儲存':    {'stocks': {'MU':'Micron','WDC':'Western Digital','SNDK':'SanDisk','STX':'Seagate','PSTG':'Pure Storage'}},
    '❄️ 冷卻/電力基建':{'stocks': {'VRT':'Vertiv','SMCI':'SuperMicro','ETN':'Eaton','GEV':'GE Vernova','HUBB':'Hubbell'}},
    '☁️ 雲端/數據中心':{'stocks': {'AMZN':'Amazon','SNOW':'Snowflake','DDOG':'Datadog','NET':'Cloudflare','CRM':'Salesforce'}},
    '🛡️ 網絡安全':    {'stocks': {'CRWD':'CrowdStrike','PANW':'Palo Alto','ZS':'Zscaler','S':'SentinelOne','OKTA':'Okta'}},
    '⚛️ 核能/新能源': {'stocks': {'CEG':'Constellation','VST':'Vistra','CCJ':'Cameco','OKLO':'Oklo','NNE':'Nano Nuclear'}},
    '🚀 太空/國防':   {'stocks': {'RKLB':'Rocket Lab','ASTS':'AST SpaceMobile','KTOS':'Kratos','PLTR':'Palantir','LMT':'Lockheed'}},
}

def _parse_ai_sector_json(raw_text):
    """解析 AI 返回的板塊 JSON，容錯處理"""
    import re, json
    text = raw_text.strip()
    # 提取 JSON block
    for pattern in [r'```json\s*(.*?)\s*```', r'```\s*(.*?)\s*```', r'(\[.*\])', r'(\{.*\})']:
        m = re.search(pattern, text, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
                if isinstance(data, list):
                    return data
                if isinstance(data, dict) and 'sectors' in data:
                    return data['sectors']
            except:
                continue
    # 直接 parse 整段
    try:
        data = json.loads(text)
        if isinstance(data, list): return data
        if isinstance(data, dict) and 'sectors' in data: return data['sectors']
    except:
        pass
    return None

@st.cache_data(ttl=3600, show_spinner=False)
def ai_generate_hot_sectors(news_headlines: str):
    """
    叫 AI 根據最新新聞，自動生成今日最熱門板塊 + 代表股票 (JSON格式)
    返回: list of dict [{name, emoji, desc, stocks: {TICKER: name, ...}}, ...]
    """
    sys_p = """你係一位美股板塊專家。根據用戶提供的最新市場新聞標題，識別今日最熱門的5-7個美股板塊。
請用以下 JSON 格式回覆，只需返回 JSON，不要其他文字：
[
  {
    "name": "板塊名稱（中文）",
    "emoji": "一個相關emoji",
    "desc": "簡短描述（10字內）",
    "stocks": {
      "TICKER1": "公司名",
      "TICKER2": "公司名",
      "TICKER3": "公司名",
      "TICKER4": "公司名",
      "TICKER5": "公司名",
      "TICKER6": "公司名"
    }
  }
]
要求：
- 每個板塊包含5-7隻最具代表性的美股 ticker（必須係真實存在的美股代號）
- 板塊要反映當前新聞熱點，例如AI、晶片、核能、機器人、國防等
- stocks 裡面必須係有效的美股ticker，例如NVDA、PLTR、AMZN等
- 只返回純 JSON，不要markdown，不要解釋"""

    usr_p = f"最新市場新聞標題：\n{news_headlines}"

    raw = call_pollinations([
        {'role': 'system', 'content': sys_p},
        {'role': 'user', 'content': usr_p}
    ], timeout=60)

    sectors = _parse_ai_sector_json(raw)
    if sectors and len(sectors) >= 3:
        return sectors, True  # True = AI生成成功
    return None, False

def build_sector_stocks_from_ai(ai_sectors):
    """將 AI 返回的 list 轉換成 SECTOR_STOCKS 格式"""
    result = {}
    for i, s in enumerate(ai_sectors):
        emoji = s.get('emoji', '📊')
        name = s.get('name', f'板塊{i+1}')
        key = f"{emoji} {name}"
        result[key] = {
            'color': SECTOR_COLORS[i % len(SECTOR_COLORS)],
            'desc': s.get('desc', ''),
            'stocks': {k.upper().strip(): v for k, v in s.get('stocks', {}).items()}
        }
    return result

def build_sector_stocks_fallback():
    """備用：使用預設板塊，加顏色"""
    result = {}
    for i, (key, val) in enumerate(FALLBACK_SECTOR_STOCKS.items()):
        result[key] = {
            'color': SECTOR_COLORS[i % len(SECTOR_COLORS)],
            'desc': '',
            'stocks': val['stocks']
        }
    return result

@st.cache_data(ttl=300, show_spinner=False)
def fetch_sector_performance_dynamic(ticker_list_key: str, tickers_tuple):
    """抓取指定股票近期表現，用 ticker_list_key 作 cache key"""
    tickers = list(tickers_tuple)
    perf = {}
    if not tickers:
        return perf
    try:
        raw = yf.download(tickers, period='5d', interval='1d', progress=False, auto_adjust=True)
        if hasattr(raw.columns, 'levels'):
            closes = raw['Close']
        else:
            closes = raw
        for ticker in tickers:
            try:
                prices = closes[ticker].dropna() if ticker in closes.columns else pd.Series(dtype=float)
                if len(prices) >= 2:
                    chg1d = (float(prices.iloc[-1]) - float(prices.iloc[-2])) / float(prices.iloc[-2]) * 100
                    chg5d = (float(prices.iloc[-1]) - float(prices.iloc[0]))  / float(prices.iloc[0])  * 100
                    perf[ticker] = {'1d': round(chg1d, 2), '5d': round(chg5d, 2), 'price': round(float(prices.iloc[-1]), 2)}
                elif len(prices) == 1:
                    perf[ticker] = {'1d': 0.0, '5d': 0.0, 'price': round(float(prices.iloc[-1]), 2)}
            except:
                perf[ticker] = {'1d': 0.0, '5d': 0.0, 'price': 0.0}
    except:
        pass
    return perf

def get_sector_avg_perf_dynamic(sector_data, perf_data):
    stocks = sector_data.get('stocks', {})
    vals = [perf_data.get(t, {}).get('5d', 0) for t in stocks]
    return round(sum(vals) / len(vals), 2) if vals else 0.0

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_hot_sectors_ai_dynamic(perf_summary: str):
    sys_p = """你係一位專業美股板塊分析師。請用廣東話分析以下各板塊近5日表現，指出：
1. 最熱門嘅2-3個板塊係咩，點解佢哋咁熱？
2. 背後有咩宏觀催化劑？
3. 有冇板塊輪動訊號？
4. 投資者點樣部署？
請用清晰廣東話加bullet points，唔超過400字。"""
    usr_p = f"各板塊近5日平均表現：\n{perf_summary}"
    return call_pollinations([
        {'role': 'system', 'content': sys_p},
        {'role': 'user', 'content': usr_p}
    ], timeout=60)

def _perf_badge(val):
    """根據漲跌幅返回帶顏色的 HTML badge"""
    if val > 5:    bg, txt = '#00573A', '#00C851'
    elif val > 0:  bg, txt = '#1A3A2A', '#26A69A'
    elif val > -5: bg, txt = '#3A2000', '#FF6D00'
    else:          bg, txt = '#3A0000', '#FF4444'
    arrow = '▲' if val >= 0 else '▼'
    return (f"<span style='background:{bg};color:{txt};padding:1px 5px;"
            f"border-radius:4px;font-size:0.78rem;font-weight:bold'>"
            f"{arrow}{abs(val):.1f}%</span>")

# ============================================================
# 板塊 ETF 定義 - 用真實 ETF 5日表現判斷熱門板塊
# ============================================================
SECTOR_ETF_MAP = {
    '🤖 人工智能 AI':    {
        'etf': 'AIQ', 'keywords': ['ai','artificial intelligence','openai','llm','copilot','gemini','claude','generative','palantir','chatgpt'],
        'stocks': {'PLTR':'Palantir','AI':'C3.ai','MSFT':'Microsoft','GOOGL':'Google','META':'Meta','SOUN':'SoundHound','BBAI':'BigBear.ai'}
    },
    '⚡ 晶片/半導體': {
        'etf': 'SOXX', 'keywords': ['chip','semiconductor','nvidia','gpu','h100','blackwell','wafer','tsmc','qualcomm','arm'],
        'stocks': {'NVDA':'NVIDIA','AMD':'AMD','AVGO':'Broadcom','AMAT':'Applied Materials','ARM':'ARM','QCOM':'Qualcomm','INTC':'Intel'}
    },
    '🗄️ 數據儲存/SSD': {
        'etf': 'MU', 'keywords': ['storage','ssd','nand','flash','memory','hdd','western digital','micron','seagate'],
        'stocks': {'MU':'Micron','WDC':'Western Digital','SNDK':'SanDisk','STX':'Seagate','PSTG':'Pure Storage','NTAP':'NetApp'}
    },
    '❄️ 冷卻/電力基建': {
        'etf': 'VRT', 'keywords': ['cooling','data center power','vertiv','supermicro','eaton','ge vernova','liquid cooling','power infrastructure'],
        'stocks': {'VRT':'Vertiv','SMCI':'SuperMicro','ETN':'Eaton','GEV':'GE Vernova','PWR':'Quanta','HUBB':'Hubbell'}
    },
    '☁️ 雲端/數據中心': {
        'etf': 'SKYY', 'keywords': ['cloud','aws','azure','google cloud','data center','snowflake','datadog','cloudflare','saas'],
        'stocks': {'AMZN':'Amazon','MSFT':'Microsoft','GOOGL':'Google','SNOW':'Snowflake','DDOG':'Datadog','NET':'Cloudflare','CRM':'Salesforce'}
    },
    '🛡️ 網絡安全': {
        'etf': 'CIBR', 'keywords': ['cybersecurity','security','crowdstrike','hacker','breach','ransomware','firewall','zero trust'],
        'stocks': {'CRWD':'CrowdStrike','PANW':'Palo Alto','ZS':'Zscaler','S':'SentinelOne','OKTA':'Okta','FTNT':'Fortinet','CYBR':'CyberArk'}
    },
    '🤖 人形機器人': {
        'etf': 'BOTZ', 'keywords': ['robot','humanoid','autonomous','tesla optimus','automation','robotics','servo','mechanical arm'],
        'stocks': {'TSLA':'Tesla','NVDA':'NVIDIA','ABB':'ABB','HON':'Honeywell','TER':'Teradyne','ISRG':'Intuitive','FANUY':'Fanuc'}
    },
    '⚛️ 核能/新能源': {
        'etf': 'NLR', 'keywords': ['nuclear','uranium','constellation','vistra','cameco','oklo','smr','fission','reactor','clean energy'],
        'stocks': {'CEG':'Constellation','VST':'Vistra','CCJ':'Cameco','OKLO':'Oklo','NNE':'Nano Nuclear','DNN':'Denison'}
    },
    '🚀 太空/國防': {
        'etf': 'ITA', 'keywords': ['space','rocket','satellite','defense','pentagon','military','spacex','rocketlab','asts','starlink','drone'],
        'stocks': {'RKLB':'Rocket Lab','ASTS':'AST SpaceMobile','KTOS':'Kratos','LMT':'Lockheed','NOC':'Northrop','PLTR':'Palantir','BA':'Boeing'}
    },
    '💊 生物科技': {
        'etf': 'XBI', 'keywords': ['biotech','pharma','drug','fda','clinical','cancer','crispr','genomics','glp-1','ozempic','mrna'],
        'stocks': {'RXRX':'Recursion','CRSP':'CRISPR','ILMN':'Illumina','MRNA':'Moderna','GILD':'Gilead','REGN':'Regeneron','NVAX':'Novavax'}
    },
}

# 完整關係庫 - 30條，動態篩選
FULL_RELATIONS_DB = [
    {"company_a":"NVDA","company_b":"MSFT","type":"合作","desc":"Azure H100/B200 GPU 供應","strength":"強"},
    {"company_a":"NVDA","company_b":"GOOGL","type":"合作","desc":"GCP GPU 雲端合作","strength":"強"},
    {"company_a":"NVDA","company_b":"AMZN","type":"合作","desc":"AWS GPU 實例供應","strength":"強"},
    {"company_a":"NVDA","company_b":"META","type":"客戶","desc":"META 大量採購 H100/B200","strength":"強"},
    {"company_a":"NVDA","company_b":"PLTR","type":"合作","desc":"AI 平台聯合部署","strength":"中"},
    {"company_a":"NVDA","company_b":"TSLA","type":"供應商","desc":"FSD 訓練晶片供應商","strength":"中"},
    {"company_a":"AMD","company_b":"NVDA","type":"競爭","desc":"GPU 市場直接競爭","strength":"強"},
    {"company_a":"AMD","company_b":"MSFT","type":"合作","desc":"Azure MI300X 部署","strength":"中"},
    {"company_a":"AVGO","company_b":"GOOGL","type":"合作","desc":"Google TPU 晶片設計","strength":"強"},
    {"company_a":"AVGO","company_b":"META","type":"客戶","desc":"Meta 自研 AI 晶片 MTIA","strength":"強"},
    {"company_a":"VRT","company_b":"NVDA","type":"客戶","desc":"數據中心液冷系統","strength":"強"},
    {"company_a":"VRT","company_b":"MSFT","type":"客戶","desc":"Azure 數據中心冷卻","strength":"強"},
    {"company_a":"VRT","company_b":"AMZN","type":"客戶","desc":"AWS 數據中心設備","strength":"強"},
    {"company_a":"SMCI","company_b":"NVDA","type":"合作","desc":"GPU 伺服器整合商","strength":"強"},
    {"company_a":"MU","company_b":"NVDA","type":"供應商","desc":"HBM3 記憶體供應","strength":"強"},
    {"company_a":"MU","company_b":"AMD","type":"供應商","desc":"DDR5/GDDR7 供應","strength":"中"},
    {"company_a":"AMZN","company_b":"MSFT","type":"競爭","desc":"雲端市場主要競爭","strength":"強"},
    {"company_a":"GOOGL","company_b":"MSFT","type":"競爭","desc":"AI 及雲端全面競爭","strength":"強"},
    {"company_a":"SNOW","company_b":"NVDA","type":"合作","desc":"GPU 加速數據分析","strength":"中"},
    {"company_a":"NET","company_b":"MSFT","type":"合作","desc":"Azure 邊緣安全整合","strength":"中"},
    {"company_a":"CEG","company_b":"MSFT","type":"合作","desc":"核電數據中心供電協議","strength":"強"},
    {"company_a":"CEG","company_b":"GOOGL","type":"合作","desc":"核能購電長期合約","strength":"強"},
    {"company_a":"CCJ","company_b":"CEG","type":"供應商","desc":"鈾燃料供應商","strength":"強"},
    {"company_a":"VST","company_b":"AMZN","type":"合作","desc":"AWS 電力供應協議","strength":"強"},
    {"company_a":"CRWD","company_b":"MSFT","type":"合作","desc":"Azure 安全整合","strength":"中"},
    {"company_a":"PANW","company_b":"GOOGL","type":"合作","desc":"GCP 安全服務合作","strength":"中"},
    {"company_a":"CRWD","company_b":"PANW","type":"競爭","desc":"SIEM/XDR 市場競爭","strength":"強"},
    {"company_a":"RKLB","company_b":"ASTS","type":"合作","desc":"衛星發射合作夥伴","strength":"中"},
    {"company_a":"PLTR","company_b":"MSFT","type":"合作","desc":"Azure AI 政府雲合作","strength":"中"},
    {"company_a":"WDC","company_b":"SNDK","type":"合作","desc":"NAND 閃存業務分拆","strength":"強"},
]


@st.cache_data(ttl=1800, show_spinner=False)
def get_hot_sectors_by_etf_performance(headlines: str = ""):
    """
    核心邏輯：
    1. 用 yfinance 抓每個板塊對應 ETF 嘅5日表現（真實市場數據）
    2. 用新聞標題關鍵字做額外 boost（新聞熱度加分）
    3. 兩者合併排序，選出最熱門6個板塊
    4. 對每個板塊，再從個股裡面挑出5日漲幅最強嘅5隻
    5. 動態篩選相關關係
    返回: (sector_stocks_dict, relations_list, scores_dict)
    """
    import yfinance as yf

    hl_lower = headlines.lower()

    # Step 1: 抓 ETF 5日表現
    etf_list = [v['etf'] for v in SECTOR_ETF_MAP.values()]
    try:
        etf_data = yf.download(etf_list, period='7d', progress=False, auto_adjust=True, 
                               group_by='ticker', threads=True)['Close']
    except Exception:
        etf_data = None

    # Step 2: 計算每個板塊分數 = ETF漲幅(%) + 新聞關鍵字命中數*2
    scored = []
    for sname, sdata in SECTOR_ETF_MAP.items():
        # ETF performance score
        etf_score = 0.0
        if etf_data is not None:
            try:
                col = etf_data[sdata['etf']].dropna()
                if len(col) >= 2:
                    n = min(5, len(col)-1)
                    etf_score = float((col.iloc[-1] / col.iloc[-n-1] - 1) * 100)
            except Exception:
                pass

        # News keyword score
        news_score = sum(2 for kw in sdata.get('keywords', []) if kw in hl_lower)

        total_score = etf_score + news_score
        scored.append((total_score, etf_score, news_score, sname, sdata))

    # Step 3: 排序，選 top 6
    scored.sort(key=lambda x: x[0], reverse=True)
    top6 = scored[:6]

    # Step 4: 對每個板塊，抓個股表現，挑最強5隻
    all_individual = list(set(
        t for _, _, _, _, sd in top6
        for t in sd['stocks'].keys()
    ))
    try:
        stock_data = yf.download(all_individual, period='7d', progress=False,
                                  auto_adjust=True, group_by='ticker', threads=True)['Close']
    except Exception:
        stock_data = None

    sector_stocks = {}
    scores_info = {}
    for i, (total, etf_perf, news_sc, sname, sdata) in enumerate(top6):
        # 為每隻股票計算5日表現
        stock_perfs = {}
        for ticker in sdata['stocks'].keys():
            try:
                col = (stock_data[ticker] if stock_data is not None else None)
                if col is not None:
                    col = col.dropna()
                    if len(col) >= 2:
                        n = min(5, len(col)-1)
                        stock_perfs[ticker] = float((col.iloc[-1] / col.iloc[-n-1] - 1) * 100)
            except Exception:
                stock_perfs[ticker] = 0.0

        # 按5日表現排序，選最強5隻
        sorted_stocks = sorted(sdata['stocks'].items(),
                                key=lambda kv: stock_perfs.get(kv[0], 0.0), reverse=True)
        top5_stocks = dict(sorted_stocks[:5])

        sector_stocks[sname] = {
            'color': SECTOR_COLORS[i % len(SECTOR_COLORS)],
            'desc': f"ETF {sdata['etf']} {etf_perf:+.1f}%",
            'stocks': top5_stocks,
            'etf_perf': etf_perf,
        }
        scores_info[sname] = {
            'total': total, 'etf': etf_perf, 'news': news_sc,
            'etf_ticker': sdata['etf']
        }

    # Step 5: 動態篩選關係
    active_tickers = set(
        t for sd in sector_stocks.values() for t in sd['stocks'].keys()
    )
    relations = [
        r for r in FULL_RELATIONS_DB
        if r.get('company_a') in active_tickers and r.get('company_b') in active_tickers
    ]
    if len(relations) < 8:
        relations = [
            r for r in FULL_RELATIONS_DB
            if r.get('company_a') in active_tickers or r.get('company_b') in active_tickers
        ]
    if len(relations) < 5:
        relations = FULL_RELATIONS_DB[:20]

    return sector_stocks, relations, scores_info


# Legacy function wrappers (保持 cache.clear() 兼容)
@st.cache_data(ttl=1800, show_spinner=False)
def ai_generate_sectors_only(headlines: str):
    return None, False

@st.cache_data(ttl=1800, show_spinner=False)
def ai_generate_relations_only(headlines: str, ticker_list: str):
    return None, False

def ai_generate_company_relations(headlines: str):
    """主入口：用 ETF 表現 + 新聞關鍵字選板塊，完全唔依賴 AI"""
    sector_stocks, relations, scores = get_hot_sectors_by_etf_performance(headlines)
    result = {
        'sector_stocks': sector_stocks,
        'relations': relations,
        'scores': scores,
        '_source': 'etf_realtime',
    }
    return result, True


# 備用關係數據
FALLBACK_RELATIONS = [
    {"company_a":"NVDA","company_b":"MSFT","type":"合作","desc":"Azure AI加速器供應商","strength":"強"},
    {"company_a":"NVDA","company_b":"GOOGL","type":"合作","desc":"GCP GPU雲端合作","strength":"強"},
    {"company_a":"NVDA","company_b":"AMZN","type":"合作","desc":"AWS GPU實例供應","strength":"強"},
    {"company_a":"NVDA","company_b":"META","type":"客戶","desc":"META大量採購H100","strength":"強"},
    {"company_a":"NVDA","company_b":"TSLA","type":"供應商","desc":"FSD訓練晶片供應商","strength":"中"},
    {"company_a":"NVDA","company_b":"PLTR","type":"合作","desc":"AI平台聯合部署","strength":"中"},
    {"company_a":"AVGO","company_b":"GOOGL","type":"合作","desc":"Google TPU晶片設計","strength":"強"},
    {"company_a":"AVGO","company_b":"META","type":"客戶","desc":"Meta自研AI晶片","strength":"強"},
    {"company_a":"AMD","company_b":"MSFT","type":"合作","desc":"Azure MI300X部署","strength":"中"},
    {"company_a":"AMD","company_b":"NVDA","type":"競爭","desc":"GPU市場直接競爭","strength":"強"},
    {"company_a":"VRT","company_b":"NVDA","type":"客戶","desc":"數據中心冷卻系統","strength":"強"},
    {"company_a":"VRT","company_b":"MSFT","type":"客戶","desc":"Azure數據中心冷卻","strength":"強"},
    {"company_a":"VRT","company_b":"AMZN","type":"客戶","desc":"AWS數據中心設備","strength":"強"},
    {"company_a":"SMCI","company_b":"NVDA","type":"合作","desc":"GPU伺服器整合商","strength":"強"},
    {"company_a":"MU","company_b":"NVDA","type":"供應商","desc":"HBM3記憶體供應","strength":"強"},
    {"company_a":"PLTR","company_b":"MSFT","type":"合作","desc":"Azure平台整合","strength":"中"},
    {"company_a":"CEG","company_b":"MSFT","type":"合作","desc":"核電數據中心供電協議","strength":"強"},
    {"company_a":"CEG","company_b":"GOOGL","type":"合作","desc":"核能購電長期合約","strength":"強"},
    {"company_a":"CRWD","company_b":"MSFT","type":"合作","desc":"Azure安全整合","strength":"中"},
    {"company_a":"CRWD","company_b":"NVDA","type":"合作","desc":"AI驅動威脅偵測","strength":"中"},
    {"company_a":"AMZN","company_b":"MSFT","type":"競爭","desc":"雲端市場主要競爭","strength":"強"},
    {"company_a":"GOOGL","company_b":"MSFT","type":"競爭","desc":"AI及雲端全面競爭","strength":"強"},
    {"company_a":"RKLB","company_b":"ASTS","type":"合作","desc":"衛星發射合作夥伴","strength":"中"},
    {"company_a":"CCJ","company_b":"CEG","type":"供應商","desc":"鈾燃料供應商","strength":"強"},
    {"company_a":"SNOW","company_b":"NVDA","type":"合作","desc":"GPU加速數據分析","strength":"中"},
]

RELATION_TYPE_COLOR = {
    '合作': ('#1A3A5A', '#4FC3F7', '🤝'),
    '供應商': ('#2A3A1A', '#AED581', '🔗'),
    '客戶': ('#3A2A1A', '#FFB74D', '💰'),
    '競爭': ('#3A1A1A', '#EF9A9A', '⚔️'),
    '投資': ('#2A1A3A', '#CE93D8', '💼'),
}

def render_relations_html(relations, perf_data):
    """用 HTML 表格顯示公司合作關係"""
    type_filter = list(RELATION_TYPE_COLOR.keys())

    rows_html = []
    for r in relations:
        ca = r.get('company_a','').upper()
        cb = r.get('company_b','').upper()
        rtype = r.get('type','合作')
        desc  = r.get('desc','')
        strength = r.get('strength','中')

        tc = RELATION_TYPE_COLOR.get(rtype, ('#1A1A2A','#aaa','🔗'))
        bg_c, txt_c, emoji = tc

        pa = perf_data.get(ca, {})
        pb = perf_data.get(cb, {})
        pa5 = pa.get('5d', 0)
        pb5 = pb.get('5d', 0)

        str_stars = {'強':'⭐⭐⭐','中':'⭐⭐','弱':'⭐'}.get(strength,'⭐⭐')

        rows_html.append(
            f"<tr>"
            f"<td style='padding:6px 8px;font-weight:bold;color:white'>{ca}</td>"
            f"<td style='padding:6px 8px'>{_perf_badge(pa5)}</td>"
            f"<td style='padding:6px 8px;text-align:center'>"
            f"<span style='background:{bg_c};color:{txt_c};padding:2px 8px;border-radius:12px;font-size:0.8rem'>"
            f"{emoji} {rtype}</span></td>"
            f"<td style='padding:6px 8px;font-weight:bold;color:white'>{cb}</td>"
            f"<td style='padding:6px 8px'>{_perf_badge(pb5)}</td>"
            f"<td style='padding:6px 8px;color:#ccc;font-size:0.82rem'>{desc}</td>"
            f"<td style='padding:6px 8px;text-align:center;font-size:0.78rem'>{str_stars}</td>"
            f"</tr>"
        )

    table_html = f"""
<style>
.rel-table {{ width:100%;border-collapse:collapse;font-family:sans-serif }}
.rel-table th {{ background:#1E1E2E;color:#aaa;padding:8px;font-size:0.8rem;text-align:left;border-bottom:1px solid #333 }}
.rel-table tr:nth-child(even) {{ background:#0E1117 }}
.rel-table tr:nth-child(odd)  {{ background:#161B22 }}
.rel-table tr:hover           {{ background:#1F2937 }}
</style>
<table class="rel-table">
<thead><tr>
<th>公司 A</th><th>5日表現</th><th style='text-align:center'>關係類型</th>
<th>公司 B</th><th>5日表現</th><th>關係描述</th><th style='text-align:center'>強度</th>
</tr></thead>
<tbody>{''.join(rows_html)}</tbody>
</table>"""
    return table_html

def render_hot_sectors_module():
    st.title('🔥 熱門板塊 & 公司合作關係')
    st.caption('AI 自動識別今日熱門板塊及公司間合作/供應鏈/競爭關係，每小時更新')

    col_ctrl1, col_ctrl2 = st.columns([4, 1])
    with col_ctrl2:
        st.markdown('<br>', unsafe_allow_html=True)
        if st.button('🔄 強制刷新', use_container_width=True, key='refresh_sectors'):
            get_hot_sectors_by_etf_performance.clear()
            analyze_hot_sectors_ai_dynamic.clear()
            fetch_sector_performance_dynamic.clear()
            st.rerun()

    # ── Step 1: 抓新聞 ──
    with st.spinner('📰 抓取最新市場新聞...'):
        news_list = fetch_top_news()
    headlines = '\n'.join([
        f"- {n.get('新聞標題','')}" for n in news_list[:20]
        if n.get('新聞標題','')
    ]) if news_list else "AI、晶片、數據中心、核能持續受關注"

    # ── Step 2: AI 生成板塊 + 合作關係 ──
    with st.spinner('🤖 AI 分析板塊及公司關係...'):
        ai_data, ai_ok = ai_generate_company_relations(headlines)

    # 從新結果格式提取數據
    sector_stocks = ai_data.get('sector_stocks', {}) if ai_data else {}
    relations     = ai_data.get('relations', [])     if ai_data else []
    scores_info   = ai_data.get('scores', {})        if ai_data else {}

    if not sector_stocks:
        sector_stocks = build_sector_stocks_fallback()
    if not relations:
        relations = FALLBACK_RELATIONS[:20]

    n_sec = len(sector_stocks)
    n_rel = len(relations)
    st.success(f"📊 📡 實時 ETF 數據選出 **{n_sec}** 個熱門板塊　｜　🔗 動態篩選 **{n_rel}** 條關係")

    # 板塊排名展示
    if scores_info:
        cols_score = st.columns(min(n_sec, 6))
        for col, (sname, info) in zip(cols_score, sorted(scores_info.items(), key=lambda x: x[1]['total'], reverse=True)):
            etf_p = info.get('etf', 0)
            col.metric(
                label=sname.split(' ',1)[-1][:12],
                value=f"{info.get('etf_ticker','')}",
                delta=f"{etf_p:+.1f}%"
            )

    with st.expander("🔍 板塊選取詳情（ETF 表現 + 新聞評分）", expanded=False):
        if scores_info:
            import pandas as pd
            df_scores = pd.DataFrame([
                {
                    '板塊': sname,
                    'ETF': info.get('etf_ticker',''),
                    'ETF 5日%': f"{info.get('etf',0):+.2f}%",
                    '新聞分': info.get('news', 0),
                    '總分': f"{info.get('total',0):.2f}",
                }
                for sname, info in sorted(scores_info.items(), key=lambda x: x[1]['total'], reverse=True)
            ])
            st.dataframe(df_scores, use_container_width=True, hide_index=True)
        st.caption(f"關係條數：{len(relations)}　活躍 Ticker 數：{len(set(t for sd in sector_stocks.values() for t in sd['stocks'].keys()))}")

    # ── Step 3: 抓股票表現 ──
    all_tickers = list(set(
        t for sd in sector_stocks.values() for t in sd['stocks'].keys()
    ) | set(
        r.get('company_a','') for r in relations
    ) | set(
        r.get('company_b','') for r in relations
    ))
    all_tickers = [t for t in all_tickers if t]
    ticker_key  = ','.join(sorted(all_tickers))
    with st.spinner('📡 抓取股票實時表現...'):
        perf_data = fetch_sector_performance_dynamic(ticker_key, tuple(sorted(all_tickers)))

    # ── Step 4: AI 板塊輪動分析 ──
    st.markdown('---')
    st.markdown('### 🤖 AI 板塊輪動分析（廣東話）')
    sector_perf_list, perf_lines = [], []
    for sname, sdata in sector_stocks.items():
        avg = get_sector_avg_perf_dynamic(sdata, perf_data)
        sector_perf_list.append((sname, avg))
        perf_lines.append(f"• {sname}: {avg:+.1f}%")
    sector_perf_list.sort(key=lambda x: x[1], reverse=True)

    ai_col1, ai_col2 = st.columns([2, 1])
    with ai_col1:
        with st.spinner('🧠 AI 分析緊板塊輪動...'):
            ai_analysis = final_text_sanitize(analyze_hot_sectors_ai_dynamic('\n'.join(perf_lines)))
        with st.container(border=True):
            st.markdown(ai_analysis)
    with ai_col2:
        st.markdown('#### 📊 板塊5日表現排名')
        for rank, (sname, avg) in enumerate(sector_perf_list, 1):
            st.markdown(
                f"<div style='display:flex;justify-content:space-between;padding:2px 0;font-size:0.82rem'>"
                f"<span>{rank}. {sname.split()[0]} {sname.split(' ',1)[-1][:12]}</span>"
                f"<span style='color:{'#00C851' if avg>=0 else '#FF4444'}'><b>{avg:+.1f}%</b></span>"
                f"</div>", unsafe_allow_html=True)

    # ── Step 5: 公司合作關係表 ──
    st.markdown('---')
    st.markdown('### 🔗 公司合作 / 供應鏈 / 競爭關係')

    # 圖例
    leg_cols = st.columns(5)
    for col, (rtype, (bg, tc, emoji)) in zip(leg_cols, RELATION_TYPE_COLOR.items()):
        col.markdown(
            f"<span style='background:{bg};color:{tc};padding:2px 8px;border-radius:10px;font-size:0.78rem'>"
            f"{emoji} {rtype}</span>", unsafe_allow_html=True)

    # 關係類型篩選
    st.markdown('<br>', unsafe_allow_html=True)
    filter_types = st.multiselect(
        '篩選關係類型:',
        list(RELATION_TYPE_COLOR.keys()),
        default=list(RELATION_TYPE_COLOR.keys()),
        key='rel_type_filter'
    )

    filtered_rels = [r for r in relations if r.get('type','合作') in filter_types]

    st.markdown(
        f"<div style='color:#888;font-size:0.8rem;margin-bottom:8px'>"
        f"顯示 {len(filtered_rels)} / {len(relations)} 條關係</div>",
        unsafe_allow_html=True
    )

    if filtered_rels:
        st.markdown(render_relations_html(filtered_rels, perf_data), unsafe_allow_html=True)
    else:
        st.warning('請至少選擇一種關係類型')

    # ── Step 6: 板塊個股表 ──
    st.markdown('---')
    st.markdown('### 📋 板塊個股詳細數據')
    tab_names = [s.split(' ', 1)[-1][:14] for s in sector_stocks.keys()]
    tabs = st.tabs(tab_names)
    for tab, (sector_name, sdata) in zip(tabs, sector_stocks.items()):
        with tab:
            rows = []
            for ticker, name in sdata['stocks'].items():
                p = perf_data.get(ticker, {})
                rows.append({
                    'Ticker': ticker,
                    '名稱': name,
                    '現價 (USD)': f"${p.get('price', 0):.2f}",
                    '1日變幅': f"{p.get('1d', 0):+.1f}%",
                    '5日變幅': f"{p.get('5d', 0):+.1f}%",
                })
            st.dataframe(rows, use_container_width=True, hide_index=True)



# ==========================================
# 產業故事 Radar / Scorecard 模組
# ==========================================

# 評分標準：0 = 最弱，5 = 最強
MARKET_RADAR_THEMES = [
    {
        "rank": 1,
        "theme": "AI 算力基建",
        "desc": "超大規模數據中心、GPU 需求、液冷系統",
        "etfs": ["SOXX", "AIQ", "SMH"],
        "stocks": ["NVDA", "AVGO", "AMD", "VRT", "SMCI"],
        "status": "🔥 極度活躍",
        "confirmation": [
            "NVDA 股價維持在 200DMA 以上",
            "SOX 指數相對 SPY 持續跑贏",
            "CSP/雲端業者資本開支指引上調",
        ],
        "invalidation": [
            "NVDA 業績 / 指引低於市場預期",
            "美國對華晶片出口限制大幅收緊",
            "聯儲局激進加息令成長股估值重估",
        ],
        "score": 5,
    },
    {
        "rank": 2,
        "theme": "AI 軟件 / 企業 AI 落地",
        "desc": "AI Copilot、代碼生成、企業 SaaS AI 化",
        "etfs": ["AIQ", "IGV", "WCLD"],
        "stocks": ["MSFT", "PLTR", "CRM", "NOW", "GOOGL"],
        "status": "📈 強勢上升",
        "confirmation": [
            "Microsoft Azure AI 季度收入加速增長",
            "Palantir AIP 商業合約數量持續突破",
            "PLTR / CRM RPO 同比 >20%",
        ],
        "invalidation": [
            "企業 IT 預算削減，SaaS 客戶流失率上升",
            "Open-source 模型大幅壓低 AI 授權費",
            "MSFT / GOOGL 指引遜預期",
        ],
        "score": 4,
    },
    {
        "rank": 3,
        "theme": "網絡安全",
        "desc": "AI 驅動威脅偵測、零信任架構、SIEM/XDR",
        "etfs": ["CIBR", "HACK", "BUG"],
        "stocks": ["CRWD", "PANW", "ZS", "S", "CYBR"],
        "status": "📈 強勢上升",
        "confirmation": [
            "CRWD ARR 同比增長 >30%",
            "政府/國防採購訂單持續增加",
            "CIBR ETF 突破前高",
        ],
        "invalidation": [
            "CRWD / PANW 業績指引下調",
            "大型安全事件令市場對產品信心下降",
            "市場資金全面轉向防守性板塊",
        ],
        "score": 4,
    },
    {
        "rank": 4,
        "theme": "核能 / 清潔電力",
        "desc": "小型模組反應堆 (SMR)、鈾礦、數據中心供電",
        "etfs": ["NLR", "URA", "URNM"],
        "stocks": ["CEG", "VST", "CCJ", "OKLO", "NNE"],
        "status": "⚖️ 整固等待催化劑",
        "confirmation": [
            "美國政府通過核電立法或補貼政策",
            "大型科技公司核電 PPA 簽訂數量增加",
            "鈾現貨價格突破 $100/lb",
        ],
        "invalidation": [
            "核電監管收緊或許可證申請受阻",
            "再生能源成本急跌令核電失去競爭力",
            "鈾價持續回落",
        ],
        "score": 3,
    },
    {
        "rank": 5,
        "theme": "太空 / 國防科技",
        "desc": "衛星互聯網、低軌衛星、國防 AI 合約",
        "etfs": ["ITA", "ROKT", "UFO"],
        "stocks": ["RKLB", "ASTS", "KTOS", "LMT", "PLTR"],
        "status": "⚖️ 整固等待催化劑",
        "confirmation": [
            "ASTS 商業衛星服務正式開通",
            "美國國防預算持續增加",
            "RKLB 發射成功率維持 >90%",
        ],
        "invalidation": [
            "國防開支削減或繼續解除",
            "衛星發射失敗令市場信心受損",
            "地緣政治風險下降令防禦股回落",
        ],
        "score": 3,
    },
    {
        "rank": 6,
        "theme": "人形機器人 / 自動化",
        "desc": "工廠自動化、AI 驅動機器人、Tesla Optimus",
        "etfs": ["BOTZ", "ROBO", "IRBO"],
        "stocks": ["TSLA", "NVDA", "ABB", "TER", "ISRG"],
        "status": "🚀 早期爆發",
        "confirmation": [
            "Tesla Optimus 量產出貨確認",
            "工廠訂單數量超預期",
            "BOTZ ETF 突破52週高位",
        ],
        "invalidation": [
            "Tesla Optimus 量產延遲",
            "製造商資本開支削減",
            "TSLA 股價大幅低於 200DMA",
        ],
        "score": 3,
    },
    {
        "rank": 7,
        "theme": "生物科技 / GLP-1",
        "desc": "減肥藥、AI 藥物研發、基因編輯",
        "etfs": ["XBI", "IBB", "ARKG"],
        "stocks": ["LLY", "NVO", "RXRX", "CRSP", "MRNA"],
        "status": "📉 觀望/回調",
        "confirmation": [
            "LLY / NVO GLP-1 季度銷售超預期",
            "FDA 加速批准新藥",
            "XBI ETF 突破整固區",
        ],
        "invalidation": [
            "GLP-1 安全問題或副作用報告",
            "FDA 拒絕重要新藥申請",
            "醫保談判令藥企定價能力受壓",
        ],
        "score": 2,
    },
    {
        "rank": 8,
        "theme": "加密貨幣 / Web3",
        "desc": "Bitcoin ETF、DeFi、穩定幣立法",
        "etfs": ["IBIT", "FBTC", "BITO"],
        "stocks": ["COIN", "MSTR", "MARA", "RIOT", "CLSK"],
        "status": "⚖️ 整固等待催化劑",
        "confirmation": [
            "Bitcoin 突破前歷史高位",
            "美國穩定幣/加密監管法案通過",
            "機構 BTC ETF 持續淨流入",
        ],
        "invalidation": [
            "Bitcoin 跌穿 200DMA",
            "監管收緊或交易所重大黑客事件",
            "宏觀風險偏好急速下降",
        ],
        "score": 3,
    },
]

SCORE_COLOR_MAP = {
    5: ("#00573A", "#00C851", "⭐⭐⭐⭐⭐"),
    4: ("#1A3A2A", "#26A69A", "⭐⭐⭐⭐"),
    3: ("#2A2A00", "#F9A825", "⭐⭐⭐"),
    2: ("#3A2000", "#FF6D00", "⭐⭐"),
    1: ("#3A0000", "#FF4444", "⭐"),
    0: ("#1A1A1A", "#888888", "—"),
}

STATUS_COLOR_MAP = {
    "🔥 極度活躍":      "#e74c3c",
    "🚀 早期爆發":      "#9b59b6",
    "📈 強勢上升":      "#27ae60",
    "⚖️ 整固等待催化劑": "#f39c12",
    "📉 觀望/回調":     "#7f8c8d",
}


def render_market_radar_module():
    """渲染 產業故事 Radar / Scorecard 主頁面"""
    st.title("🎯 產業故事 Radar / Scorecard")
    st.caption(
        "依市值重要性、產業敘事、代表 ETF/股票、現況、確認指標及失效條件，"
        "對美股主要主題進行綜合排名評分 (0–5分)。每次刷新時自動依最新 ETF 表現微調分數。"
    )

    # ── 頂部控制列 ──────────────────────────────────────────────────────
    ctrl_l, ctrl_r = st.columns([5, 1])
    with ctrl_r:
        st.markdown("<br>", unsafe_allow_html=True)
        refresh_radar = st.button("🔄 刷新評分", use_container_width=True, key="refresh_radar")

    # ── 動態評分調整：拉取各主題代表 ETF 近5日表現，疊加到靜態分數上 ──
    @st.cache_data(ttl=600, show_spinner=False)
    def _fetch_radar_etf_scores():
        """抓所有主題第一隻 ETF 的5日漲跌，返回 {etf: pct} dict"""
        all_etfs = list({
            theme["etfs"][0]
            for theme in MARKET_RADAR_THEMES
            if theme["etfs"]
        })
        try:
            raw = yf.download(all_etfs, period="7d", progress=False, auto_adjust=True)
            closes = raw["Close"] if "Close" in raw.columns.get_level_values(0) else raw
            out = {}
            for etf in all_etfs:
                try:
                    col = closes[etf].dropna()
                    if len(col) >= 2:
                        n = min(5, len(col) - 1)
                        out[etf] = round(float((col.iloc[-1] / col.iloc[-n - 1] - 1) * 100), 2)
                except:
                    pass
            return out
        except:
            return {}

    if refresh_radar:
        try:
            _fetch_radar_etf_scores.clear()
        except:
            pass

    with ctrl_l:
        st.markdown("載入代表 ETF 近5日表現調整評分...")

    with st.spinner("📡 抓取 ETF 實時數據..."):
        etf_perfs = _fetch_radar_etf_scores()

    # ── 計算動態分數（靜態分 + ETF表現微調，上限5，下限0）──
    def _dynamic_score(theme, etf_perfs):
        base = theme["score"]
        etf = theme["etfs"][0] if theme["etfs"] else None
        if etf and etf in etf_perfs:
            pct = etf_perfs[etf]
            if pct >= 5:
                adj = 0.5
            elif pct >= 2:
                adj = 0.25
            elif pct <= -5:
                adj = -0.5
            elif pct <= -2:
                adj = -0.25
            else:
                adj = 0
            return min(5, max(0, round(base + adj, 2)))
        return float(base)

    # ── 按動態分數重排 ──
    themes_sorted = sorted(
        MARKET_RADAR_THEMES,
        key=lambda t: _dynamic_score(t, etf_perfs),
        reverse=True,
    )
    for new_rank, t in enumerate(themes_sorted, 1):
        t["_rank"] = new_rank
        t["_dyn_score"] = _dynamic_score(t, etf_perfs)

    # ── 摘要卡片列（最多顯示4個） ──
    st.markdown("---")
    st.markdown("#### 🏆 頂級主題快覽")
    top_cols = st.columns(min(4, len(themes_sorted)))
    for col, t in zip(top_cols, themes_sorted[:4]):
        sc = int(round(t["_dyn_score"]))
        sc = max(0, min(5, sc))
        bg, fg, _ = SCORE_COLOR_MAP.get(sc, SCORE_COLOR_MAP[0])
        etf_pct = etf_perfs.get(t["etfs"][0], None) if t["etfs"] else None
        delta_str = f"{etf_pct:+.1f}% (5d)" if etf_pct is not None else "ETF N/A"
        col.metric(
            label=f"#{t['_rank']} {t['theme']}",
            value=f"{t['_dyn_score']:.1f} / 5",
            delta=delta_str,
        )

    # ── 主雷達表格 ──
    st.markdown("---")
    st.markdown("### 📊 完整 Radar 評分表")

    # 構建 HTML 表格
    header_html = """
<style>
.radar-tbl { width:100%; border-collapse:collapse; font-family:sans-serif; font-size:0.82rem; }
.radar-tbl th { background:#1E1E2E; color:#aaa; padding:8px 10px; text-align:left;
                border-bottom:2px solid #333; white-space:nowrap; }
.radar-tbl td { padding:7px 10px; vertical-align:top; border-bottom:1px solid #222; }
.radar-tbl tr:nth-child(even) { background:#0E1117; }
.radar-tbl tr:nth-child(odd)  { background:#161B22; }
.radar-tbl tr:hover           { background:#1F2937; }
.score-badge { padding:2px 9px; border-radius:12px; font-weight:bold; font-size:0.8rem; }
.status-chip { padding:2px 8px; border-radius:10px; font-size:0.78rem; font-weight:bold;
               white-space:nowrap; }
.ticker-chip { background:#1A2A3A; color:#4FC3F7; padding:1px 6px; border-radius:6px;
               margin:1px; display:inline-block; font-size:0.76rem; }
.confirm-dot::before { content:"✅ "; }
.invalid-dot::before { content:"❌ "; }
</style>
<table class="radar-tbl">
<thead><tr>
  <th>#</th>
  <th>主題</th>
  <th>代表 ETF</th>
  <th>代表股票</th>
  <th>現況</th>
  <th>確認訊號</th>
  <th>失效條件</th>
  <th>評分</th>
</tr></thead>
<tbody>
"""

    rows = []
    for t in themes_sorted:
        sc = int(round(t["_dyn_score"]))
        sc = max(0, min(5, sc))
        bg, fg, stars = SCORE_COLOR_MAP.get(sc, SCORE_COLOR_MAP[0])

        status_c = STATUS_COLOR_MAP.get(t["status"], "#888")

        etf_chips = " ".join(
            f"<span class='ticker-chip'>{e}</span>" for e in t["etfs"]
        )
        stock_chips = " ".join(
            f"<span class='ticker-chip'>{s}</span>" for s in t["stocks"]
        )

        conf_lines = "".join(
            f"<div class='confirm-dot' style='color:#aaa;margin:1px 0'>{c}</div>"
            for c in t["confirmation"]
        )
        inv_lines = "".join(
            f"<div class='invalid-dot' style='color:#e07070;margin:1px 0'>{c}</div>"
            for c in t["invalidation"]
        )

        etf_pct = etf_perfs.get(t["etfs"][0], None) if t["etfs"] else None
        pct_str = (
            f" <span style='color:{'#00C851' if etf_pct >= 0 else '#FF4444'};font-size:0.72rem'>" 
            f"({'▲' if etf_pct >= 0 else '▼'}{abs(etf_pct):.1f}%)</span>"
            if etf_pct is not None else ""
        )

        rows.append(
            f"<tr>"
            f"<td style='color:#888;font-size:0.75rem'>#{t['_rank']}</td>"
            f"<td><b style='color:white'>{t['theme']}</b><br>"
            f"<span style='color:#666;font-size:0.73rem'>{t['desc']}</span></td>"
            f"<td>{etf_chips}{pct_str}</td>"
            f"<td>{stock_chips}</td>"
            f"<td><span class='status-chip' style='background:{status_c}33;color:{status_c}'>"
            f"{t['status']}</span></td>"
            f"<td>{conf_lines}</td>"
            f"<td>{inv_lines}</td>"
            f"<td><span class='score-badge' style='background:{bg};color:{fg}'>"
            f"{t['_dyn_score']:.1f} {stars}</span></td>"
            f"</tr>"
        )

    table_html = header_html + "\n".join(rows) + "\n</tbody></table>"
    st.markdown(table_html, unsafe_allow_html=True)

    # ── 詳細展開卡片 ──
    st.markdown("---")
    st.markdown("### 🔍 主題詳細分析")
    for t in themes_sorted:
        sc = int(round(t["_dyn_score"]))
        sc = max(0, min(5, sc))
        bg, fg, stars = SCORE_COLOR_MAP.get(sc, SCORE_COLOR_MAP[0])
        with st.expander(
            f"#{t['_rank']}  {t['theme']}  ｜  評分 {t['_dyn_score']:.1f}/5  {stars}",
            expanded=False,
        ):
            dc1, dc2, dc3 = st.columns([1, 1, 1])
            with dc1:
                st.markdown("**代表 ETF**")
                for e in t["etfs"]:
                    pct = etf_perfs.get(e)
                    pct_label = (
                        f" ({'+' if pct >= 0 else ''}{pct:.1f}% 5d)"
                        if pct is not None else ""
                    )
                    color = (
                        "#00C851" if pct is not None and pct >= 0
                        else ("#FF4444" if pct is not None else "#aaa")
                    )
                    st.markdown(
                        f"<span style='background:#1A2A3A;color:#4FC3F7;padding:2px 8px;"
                        f"border-radius:6px'>{e}</span>"
                        f"<span style='color:{color};font-size:0.78rem'>{pct_label}</span>",
                        unsafe_allow_html=True,
                    )
            with dc2:
                st.markdown("**代表股票**")
                for s in t["stocks"]:
                    st.markdown(
                        f"<span style='background:#1A2A3A;color:#4FC3F7;padding:2px 8px;"
                        f"border-radius:6px;margin:2px;display:inline-block'>{s}</span>",
                        unsafe_allow_html=True,
                    )
            with dc3:
                status_c = STATUS_COLOR_MAP.get(t["status"], "#888")
                st.markdown(
                    f"**現況：** <span style='background:{status_c}33;color:{status_c};"
                    f"padding:2px 8px;border-radius:8px'>{t['status']}</span>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"<span class='score-badge' style='background:{bg};color:{fg};"
                    f"padding:3px 10px;border-radius:10px;font-size:0.85rem'>"
                    f"評分：{t['_dyn_score']:.1f} / 5 &nbsp; {stars}</span>",
                    unsafe_allow_html=True,
                )

            st.markdown("**✅ 確認訊號（以下出現 → 主題延續）**")
            for c in t["confirmation"]:
                st.markdown(f"- {c}")
            st.markdown("**❌ 失效條件（以下出現 → 謹慎或離場）**")
            for c in t["invalidation"]:
                st.markdown(f"- {c}")

    # ── 評分說明 ──
    st.markdown("---")
    with st.expander("📖 評分說明", expanded=False):
        st.markdown("""
| 分數 | 含義 |
|------|------|
| 5 / 5 | 市值最重要、主題敘事最強、技術突破、資金全面湧入 |
| 4 / 5 | 強勢上升趨勢，確認訊號充分，短線動能極佳 |
| 3 / 5 | 中性至偏強，整固等待催化劑，存在一定不確定性 |
| 2 / 5 | 偏弱，敘事受壓或技術面走弱，需觀望更多確認 |
| 1–0 / 5 | 整體走弱，失效條件已觸發或即將觸發 |

> **注意**：靜態評分由人工設定，實時動態調整由代表 ETF 近5日漲跌 (±0.25–0.5分) 自動疊加。
        """)


# ==========================================
# 宏觀燈號 Macro Signal – 評分邏輯
# ==========================================
# Logic overview (transparent assumptions):
#   就業 Employment:
#     GREEN  = UNRATE ≤ 4.5% AND JOBLESS ≤ 230k AND payrolls change ≥ 0
#     YELLOW = mixed signals
#     RED    = UNRATE > 5.5% OR JOBLESS > 260k OR payrolls change < -50k
#   通脹 Inflation:
#     GREEN  = CPI_YOY ≤ 2.5% AND CORE_CPI ≤ 2.5%
#     YELLOW = CPI ≤ 3.5% OR CoreCPI ≤ 3.5%
#     RED    = CPI > 3.5% OR CoreCPI > 3.5%
#   利率 Rates:
#     GREEN  = Fed Funds ≤ 3.0% OR yield curve not inverted (spread > 0)
#     YELLOW = Fed Funds 3-4.5% AND curve near flat
#     RED    = Fed Funds > 4.5% AND curve inverted (spread < -0.5)
#   風險偏好 Risk Appetite:
#     GREEN  = SPY 1d ≥ 0% AND QQQ 1d ≥ 0% AND VIX < 20
#     YELLOW = mixed signals
#     RED    = VIX ≥ 30 OR (SPY < -1% AND QQQ < -1%)
#
# Each category: green=2pts, yellow=1pt, red=0pts → total 0-8, map to 0-100

def compute_macro_signal(m: dict) -> dict:
    """
    Given the macro data dict from fetch_sidebar_market_data(),
    compute per-category traffic-light status and an overall score (0-100).
    Returns: {
      'employment': {'status': 'green'|'yellow'|'red', 'score': 0|1|2, 'detail': str},
      'inflation':  {'status': ..., 'score': ..., 'detail': ...},
      'rates':      {'status': ..., 'score': ..., 'detail': ...},
      'risk':       {'status': ..., 'score': ..., 'detail': ...},
      'total_score': int,   # 0-100
      'total_label': str,
    }
    """
    result = {}

    # ── 就業 Employment ──────────────────────────────────────────────
    try:
        unemp = m.get('UNRATE')
        jl    = m.get('JOBLESS')
        pay   = m.get('PAYEMS')
        u_val   = unemp['value']        if unemp else None
        jl_val  = jl['value']           if jl    else None
        pay_chg = pay.get('change', 0)  if pay   else None

        details = []
        if u_val   is not None: details.append(f"失業率 {u_val:.1f}%")
        if jl_val  is not None: details.append(f"申領 {jl_val:,}")
        if pay_chg is not None: details.append(f"新增就業 {'+' if pay_chg >= 0 else ''}{pay_chg:,}")

        bad_u   = (u_val   is not None and u_val   > 5.5)
        bad_jl  = (jl_val  is not None and jl_val  > 260000)
        bad_pay = (pay_chg is not None and pay_chg < -50000)
        ok_u    = (u_val   is None or u_val   <= 4.5)
        ok_jl   = (jl_val  is None or jl_val  <= 230000)
        ok_pay  = (pay_chg is None or pay_chg >= 0)

        if bad_u or bad_jl or bad_pay:
            status, score = 'red', 0
        elif ok_u and ok_jl and ok_pay:
            status, score = 'green', 2
        else:
            status, score = 'yellow', 1

        result['employment'] = {
            'status': status, 'score': score,
            'detail': ' | '.join(details) if details else 'N/A'
        }
    except Exception:
        result['employment'] = {'status': 'yellow', 'score': 1, 'detail': '數據不可用'}

    # ── 通脹 Inflation ────────────────────────────────────────────────
    try:
        cpi      = m.get('CPI_YOY')
        core_cpi = m.get('CORE_CPI')
        cpi_val  = cpi['value']      if cpi      else None
        core_val = core_cpi['value'] if core_cpi else None

        details = []
        if cpi_val  is not None: details.append(f"CPI {cpi_val:.1f}%")
        if core_val is not None: details.append(f"Core {core_val:.1f}%")

        bad_cpi  = (cpi_val  is not None and cpi_val  > 3.5)
        bad_core = (core_val is not None and core_val > 3.5)
        ok_cpi   = (cpi_val  is None or cpi_val  <= 2.5)
        ok_core  = (core_val is None or core_val <= 2.5)

        if bad_cpi or bad_core:
            status, score = 'red', 0
        elif ok_cpi and ok_core:
            status, score = 'green', 2
        else:
            status, score = 'yellow', 1

        result['inflation'] = {
            'status': status, 'score': score,
            'detail': ' | '.join(details) if details else 'N/A'
        }
    except Exception:
        result['inflation'] = {'status': 'yellow', 'score': 1, 'detail': '數據不可用'}

    # ── 利率 Rates ───────────────────────────────────────────────────
    try:
        ff  = m.get('FEDFUNDS')
        yr  = m.get('YIELD_SPREAD_RAW')
        ff_val = ff['value'] if ff else None
        spread = None
        if yr:
            spread = yr.get('tnx', 0) - yr.get('irx', 0)

        details = []
        if ff_val is not None: details.append(f"Fed Funds {ff_val:.2f}%")
        if spread  is not None: details.append(f"10Y-短端 {spread:+.2f}%")

        bad_ff = (ff_val is not None and ff_val > 4.5)
        bad_sp = (spread is not None and spread < -0.5)
        ok_ff  = (ff_val is None or ff_val <= 3.0)
        ok_sp  = (spread is None or spread > 0)

        if bad_ff and bad_sp:
            status, score = 'red', 0
        elif ok_ff or ok_sp:
            status, score = 'green', 2
        else:
            status, score = 'yellow', 1

        result['rates'] = {
            'status': status, 'score': score,
            'detail': ' | '.join(details) if details else 'N/A'
        }
    except Exception:
        result['rates'] = {'status': 'yellow', 'score': 1, 'detail': '數據不可用'}

    # ── 風險偏好 Risk Appetite ────────────────────────────────────────
    try:
        vix = m.get('VIX', {})
        spy = m.get('SPY', {})
        qqq = m.get('QQQ', {})
        fg  = m.get('FEAR_GREED', {})

        vix_v = vix.get('price') if vix else None
        spy_p = spy.get('pct')   if spy else None
        qqq_p = qqq.get('pct')   if qqq else None
        fg_sc = fg.get('score')  if fg  else None

        details = []
        if vix_v is not None: details.append(f"VIX {vix_v:.1f}")
        if spy_p is not None: details.append(f"SPY {spy_p:+.1f}%")
        if qqq_p is not None: details.append(f"QQQ {qqq_p:+.1f}%")
        if fg_sc is not None: details.append(f"F&G {fg_sc:.0f}")

        bad_vix = (vix_v is not None and vix_v >= 30)
        bad_eq  = (spy_p is not None and qqq_p is not None and spy_p < -1.0 and qqq_p < -1.0)
        ok_vix  = (vix_v is None or vix_v < 20)
        ok_eq   = (spy_p is None or spy_p >= 0) and (qqq_p is None or qqq_p >= 0)

        if bad_vix or bad_eq:
            status, score = 'red', 0
        elif ok_vix and ok_eq:
            status, score = 'green', 2
        else:
            status, score = 'yellow', 1

        result['risk'] = {
            'status': status, 'score': score,
            'detail': ' | '.join(details) if details else 'N/A'
        }
    except Exception:
        result['risk'] = {'status': 'yellow', 'score': 1, 'detail': '數據不可用'}

    # ── 總分 0-8 → 0-100 ────────────────────────────────────────────
    total = sum(result[k]['score'] for k in ('employment', 'inflation', 'rates', 'risk'))
    total_score = int(round(total / 8 * 100))

    if total_score >= 75:
        total_label = '🟢 宏觀環境理想'
    elif total_score >= 50:
        total_label = '🟡 宏觀環境混合'
    elif total_score >= 25:
        total_label = '🟠 宏觀環境偏弱'
    else:
        total_label = '🔴 宏觀環境惡劣'

    result['total_score'] = total_score
    result['total_label'] = total_label
    return result


_SIGNAL_COLORS = {
    'green':  ('#00573A', '#00C851', '🟢'),
    'yellow': ('#3A3000', '#F9A825', '🟡'),
    'red':    ('#3A0000', '#FF4444', '🔴'),
}

_SIGNAL_LABELS_ZH = {
    'employment': '就業 Employment',
    'inflation':  '通脹 Inflation',
    'rates':      '利率 Rates',
    'risk':       '風險偏好 Risk Appetite',
}


def render_macro_signal_sidebar(m: dict):
    """Compact sidebar section: 宏觀燈號 Macro Signal."""
    st.markdown(
        "<div style='font-size:0.8rem;font-weight:700;color:#aaa;margin:8px 0 4px'>"
        "🚦 宏觀燈號 Macro Signal</div>",
        unsafe_allow_html=True
    )

    try:
        sig = compute_macro_signal(m)
    except Exception:
        st.caption('⚠️ 燈號計算失敗')
        return

    ts   = sig['total_score']
    tlbl = sig['total_label']
    bar_color = '#00C851' if ts >= 75 else ('#F9A825' if ts >= 50 else ('#FF6D00' if ts >= 25 else '#FF4444'))
    st.markdown(
        f"<div style='background:#1A1A2E;border-radius:6px;padding:5px 8px;margin-bottom:5px'>"
        f"<div style='display:flex;justify-content:space-between;align-items:center'>"
        f"<span style='font-size:0.74rem;color:#ccc'>{tlbl}</span>"
        f"<span style='font-size:0.8rem;font-weight:bold;color:{bar_color}'>{ts}/100</span>"
        f"</div>"
        f"<div style='background:#333;border-radius:3px;height:5px;margin-top:3px'>"
        f"<div style='background:{bar_color};width:{ts}%;height:5px;border-radius:3px'></div>"
        f"</div></div>",
        unsafe_allow_html=True
    )

    for key, label in _SIGNAL_LABELS_ZH.items():
        cat = sig.get(key, {})
        status = cat.get('status', 'yellow')
        bg, fg, dot = _SIGNAL_COLORS.get(status, _SIGNAL_COLORS['yellow'])
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;align-items:center;"
            f"padding:2px 4px;border-left:3px solid {fg};margin:2px 0;border-radius:3px;"
            f"background:{bg}44'>"
            f"<span style='font-size:0.71rem;color:#ccc'>{label}</span>"
            f"<span style='font-size:0.76rem'>{dot}</span>"
            f"</div>",
            unsafe_allow_html=True
        )

    with st.expander('📖 燈號詳情 & 邏輯', expanded=False):
        for key, label in _SIGNAL_LABELS_ZH.items():
            cat = sig.get(key, {})
            status = cat.get('status', 'yellow')
            detail = cat.get('detail', 'N/A')
            _, fg, dot = _SIGNAL_COLORS.get(status, _SIGNAL_COLORS['yellow'])
            st.markdown(
                f"<span style='color:{fg}'><b>{dot} {label}</b></span>  "
                f"<span style='font-size:0.72rem;color:#aaa'>{detail}</span>",
                unsafe_allow_html=True
            )
        st.markdown(
            "<hr style='opacity:0.2;margin:4px 0'>"
            "<span style='font-size:0.65rem;color:#666'>"
            "判斷邏輯：🟢就業-失業率≤4.5%/申領≤23萬；通脹-CPI≤2.5%；利率-FF≤3%或曲線非倒掛；"
            "風險-VIX&lt;20且SPY/QQQ正；🔴為相反惡化條件；🟡為混合。</span>",
            unsafe_allow_html=True
        )


# ==========================================
# 新聞催化劑 / RS 方法選股模組 – 定義與數據
# ==========================================

CATALYST_THEME_MAP = {
    '🤖 AI 半導體 / 算力': {
        'keywords': ['ai', 'artificial intelligence', 'gpu', 'chip', 'semiconductor',
                     'h100', 'blackwell', 'nvidia', 'openai', 'llm'],
        'etf': 'SOXX',
        'tickers': {
            'NVDA': 'NVIDIA – GPU 主導',
            'AMD':  'AMD – GPU/CPU',
            'AVGO': 'Broadcom – AI ASIC',
            'AMAT': 'Applied Materials',
            'ARM':  'ARM Holdings',
            'QCOM': 'Qualcomm',
            'MU':   'Micron – HBM 記憶體',
        },
        'catalyst_tags': ['AI 資本開支上調', 'H100/B200 需求強勁', 'GPU 出口限制緩和'],
    },
    '🧠 大型 AI 平台 (Mega-cap)': {
        'keywords': ['microsoft', 'google', 'meta', 'amazon', 'azure',
                     'copilot', 'gemini', 'llama', 'cloud'],
        'etf': 'QQQ',
        'tickers': {
            'MSFT':  'Microsoft – Azure AI',
            'GOOGL': 'Google – Gemini AI',
            'META':  'Meta – LLaMA / AI Ads',
            'AMZN':  'Amazon – AWS + Bedrock',
            'AAPL':  'Apple – 端側 AI',
        },
        'catalyst_tags': ['雲端收入加速', 'AI Copilot 訂閱增長', '業績指引上調'],
    },
    '⚡ 數據中心電力 / 電網': {
        'keywords': ['data center', 'power', 'grid', 'nuclear', 'electricity',
                     'cooling', 'vertiv', 'eaton', 'ge vernova'],
        'etf': 'VRT',
        'tickers': {
            'VRT':  'Vertiv – 液冷系統',
            'ETN':  'Eaton – 電力管理',
            'GEV':  'GE Vernova – 渦輪機',
            'PWR':  'Quanta Services',
            'SMCI': 'SuperMicro – AI 伺服器',
            'HUBB': 'Hubbell – 電網設備',
        },
        'catalyst_tags': ['核電 PPA 簽訂', '數據中心供電需求', '電網升級政策'],
    },
    '⚛️ 能源 / 天然氣': {
        'keywords': ['natural gas', 'energy', 'lng', 'oil', 'uranium',
                     'nuclear', 'constellation', 'vistra'],
        'etf': 'XLE',
        'tickers': {
            'CEG':  'Constellation Energy',
            'VST':  'Vistra Energy',
            'CCJ':  'Cameco – 鈾礦',
            'OKLO': 'Oklo – 小型核反應堆',
            'LNG':  'Cheniere Energy – LNG',
            'NNE':  'Nano Nuclear',
        },
        'catalyst_tags': ['核電立法推進', '天然氣價格上升', '電力需求強勁'],
    },
    '🏭 工業 / 再工業化': {
        'keywords': ['industrial', 'manufacturing', 'reshoring', 'infrastructure',
                     'defense', 'construction'],
        'etf': 'XLI',
        'tickers': {
            'CAT': 'Caterpillar',
            'HON': 'Honeywell',
            'DE':  'Deere – 農機',
            'GE':  'GE Aerospace',
            'RTX': 'RTX – 國防/航空',
            'LMT': 'Lockheed Martin',
        },
        'catalyst_tags': ['基建支出法案', '國防預算增加', '回流製造訂單'],
    },
    '🔧 材料 / 銅 / 電氣化': {
        'keywords': ['copper', 'materials', 'mining', 'electrification',
                     'battery', 'lithium', 'fcx'],
        'etf': 'XLB',
        'tickers': {
            'FCX':  'Freeport – 銅礦龍頭',
            'SCCO': 'Southern Copper',
            'ALB':  'Albemarle – 鋰',
            'MP':   'MP Materials – 稀土',
            'NEM':  'Newmont – 黃金',
            'AA':   'Alcoa – 鋁',
        },
        'catalyst_tags': ['電動車需求', '銅礦供應緊張', 'AI 數據中心用銅'],
    },
    '📊 小型股 / 週期輪動': {
        'keywords': ['small cap', 'rate cut', 'fed pivot', 'ipo', 'russell'],
        'etf': 'IWM',
        'tickers': {
            'PLTR': 'Palantir – AI 軟件',
            'SOUN': 'SoundHound AI',
            'BBAI': 'BigBear.ai',
            'RKLB': 'Rocket Lab – 太空',
            'ASTS': 'AST SpaceMobile',
            'IONQ': 'IonQ – 量子計算',
        },
        'catalyst_tags': ['Fed 降息預期', '小型股估值修復', '高 beta 追落後'],
    },
    '🏦 金融 / 銀行': {
        'keywords': ['financials', 'bank', 'earnings', 'interest rate', 'credit',
                     'jpmorgan', 'goldman'],
        'etf': 'XLF',
        'tickers': {
            'JPM': 'JPMorgan Chase',
            'GS':  'Goldman Sachs',
            'MS':  'Morgan Stanley',
            'BAC': 'Bank of America',
            'V':   'Visa',
            'MA':  'Mastercard',
        },
        'catalyst_tags': ['業績超預期', '淨息差改善', '資本回購計劃'],
    },
    '💊 醫療 / 生物科技': {
        'keywords': ['biotech', 'pharma', 'glp-1', 'fda', 'drug', 'cancer',
                     'genomics', 'healthcare'],
        'etf': 'XBI',
        'tickers': {
            'LLY':  'Eli Lilly – GLP-1',
            'NVO':  'Novo Nordisk',
            'RXRX': 'Recursion – AI 藥研',
            'CRSP': 'CRISPR Therapeutics',
            'MRNA': 'Moderna',
            'REGN': 'Regeneron',
        },
        'catalyst_tags': ['GLP-1 銷售超預期', 'FDA 新藥批准', 'AI 藥物研發突破'],
    },
    '🛡️ AI 軟件 / 網絡安全': {
        'keywords': ['cybersecurity', 'saas', 'software', 'crwd', 'crowdstrike',
                     'palantir', 'palo alto', 'sentinel'],
        'etf': 'CIBR',
        'tickers': {
            'CRWD': 'CrowdStrike',
            'PANW': 'Palo Alto Networks',
            'ZS':   'Zscaler',
            'S':    'SentinelOne',
            'NOW':  'ServiceNow',
            'CRM':  'Salesforce',
        },
        'catalyst_tags': ['ARR 增長加速', '政府合約增加', '零信任安全普及'],
    },
    '🛒 消費 / 零售': {
        'keywords': ['consumer', 'retail', 'spending', 'amazon', 'walmart',
                     'holiday', 'ecommerce'],
        'etf': 'XLY',
        'tickers': {
            'AMZN': 'Amazon – 電商 + AWS',
            'COST': 'Costco',
            'WMT':  'Walmart',
            'TSLA': 'Tesla – EV 消費',
            'NKE':  'Nike',
            'LULU': 'Lululemon',
        },
        'catalyst_tags': ['消費者信心改善', '節日銷售超預期', 'EV 需求反彈'],
    },
}


# ── RS 方法：正關鍵字 / 負關鍵字 ──────────────────────────────────
_POS_KW = ['beat', 'raise', 'growth', 'ai', 'contract', 'upgrade', 'bullish',
            'record', 'demand', 'approval', 'surpass', 'outperform', 'accelerat',
            'expand', 'win', 'rally', 'surge', 'breakout', 'strong', 'profit']
_NEG_KW = ['miss', 'cut', 'downgrade', 'investigation', 'delay', 'weak',
            'lawsuit', 'warning', 'tariff', 'slump', 'loss', 'decline',
            'recall', 'fine', 'probe', 'fraud', 'shortfall',
            'disappoint', 'bearish', 'plunge', 'crash', 'layoff', 'reduce']


def classify_news_sentiment(headlines):
    pos, neg = 0, 0
    matched_pos, matched_neg = set(), set()
    for h in headlines:
        hl = h.lower()
        for kw in _POS_KW:
            if kw in hl:
                pos += 1
                matched_pos.add(kw)
        for kw in _NEG_KW:
            if kw in hl:
                neg += 1
                matched_neg.add(kw)
    if pos > neg and pos > 0:
        label = '🟢 正面'
    elif neg > pos and neg > 0:
        label = '🔴 負面'
    else:
        label = '⚪ 中性'
    return label, pos, neg, sorted(matched_pos)[:3], sorted(matched_neg)[:3]


@st.cache_data(ttl=600, show_spinner=False)
def fetch_catalyst_rs_data(tickers_tuple, benchmark='SPY'):
    tickers = list(tickers_tuple)
    all_t   = list(set(tickers + [benchmark]))
    result  = {}
    try:
        raw = yf.download(all_t, period='1y', interval='1d', progress=False,
                          auto_adjust=True, group_by='column', threads=True)
        if raw.empty:
            return result
        if isinstance(raw.columns, pd.MultiIndex):
            closes = raw['Close']
        else:
            closes = raw[['Close']] if 'Close' in raw.columns else raw
        today = closes.index[-1]

        def _offset_price(col, days):
            try:
                idx = max(0, len(col) - days - 1)
                return float(col.iloc[idx])
            except Exception:
                return None

        def _ytd_start_price(col):
            try:
                yr_idx = col.index[col.index.year == today.year]
                return float(col[yr_idx[0]]) if len(yr_idx) > 0 else None
            except Exception:
                return None

        def _ret(col, days=None, ytd=False):
            try:
                curr = float(col.dropna().iloc[-1])
                base = _ytd_start_price(col.dropna()) if ytd else _offset_price(col.dropna(), days)
                if base and base != 0:
                    return round((curr - base) / base * 100, 2)
            except Exception:
                pass
            return None

        for t in all_t:
            try:
                col = closes[t].dropna() if t in closes.columns else pd.Series(dtype=float)
                if col.empty:
                    result[t] = {}
                    continue
                result[t] = {
                    'price': round(float(col.iloc[-1]), 2),
                    '1d':   _ret(col, 1),
                    '5d':   _ret(col, 5),
                    '1m':   _ret(col, 21),
                    '3m':   _ret(col, 63),
                    'ytd':  _ret(col, ytd=True),
                }
            except Exception:
                result[t] = {}
    except Exception:
        pass
    return result


def compute_rs_category(ticker, perf_data, benchmark='SPY'):
    """Return one of four RS categories:
    🟢 跑贏指數 – confirmed outperformer (1M & 3M both > 0)
    🟠 接近突破 – close to outperforming: 1M near/above 0, 3M slightly negative
    🟡 剛轉強  – just starting to turn: 5D relative > 0 but 1M still negative
    🔴 跑輸指數 – lagging on all timeframes
    """
    t = perf_data.get(ticker, {})
    b = perf_data.get(benchmark, {})

    def _rel(h):
        tv = t.get(h)
        bv = b.get(h)
        if tv is None or bv is None:
            return None
        return tv - bv

    r5d = _rel('5d')
    r1m = _rel('1m')
    r3m = _rel('3m')

    # ── 🟢 跑贏指數: confirmed on BOTH 1M and 3M ─────────────────────────
    if r1m is not None and r3m is not None:
        if r1m > 0 and r3m > 0:
            return '🟢 跑贏指數'

    # ── 🟠 接近突破: 1M near/above 0, 3M slightly negative or improving ──
    # Stronger than 剛轉強 but not yet confirmed on both timeframes
    near_breakout = False
    if r1m is not None and r1m >= -1.5:      # 1M relative very close to or above 0
        if r3m is not None and r3m >= -5:   # 3M not deeply negative
            near_breakout = True
    if r1m is not None and r1m > 0:          # 1M already outperforming
        if r3m is not None and r3m <= 0:     # but 3M still slightly behind
            near_breakout = True
    if near_breakout:
        return '🟠 接近突破'

    # ── 🟡 剛轉強: 5D relative turns positive, early-stage momentum turn ─
    # 5D > 0 but 1M still negative; or 5D meaningfully better than 1M
    just_turned = False
    if r5d is not None and r5d > 0:
        if r1m is None or r1m <= 0:          # 5D positive but 1M not yet
            just_turned = True
    if r5d is not None and r1m is not None:
        if r5d - r1m >= 2:                   # 5D meaningfully outpacing 1M (momentum turning)
            just_turned = True
    if just_turned:
        return '🟡 剛轉強'

    return '🔴 跑輸指數'


def _rs_status_badge(status):
    colors = {
        '🟢 跑贏指數':  ('#003820', '#00C851'),
        '🟠 接近突破': ('#2D1800', '#FF8C00'),
        '🟡 剛轉強':   ('#3A3000', '#F9A825'),
        '🔴 跑輸指數':  ('#3A0000', '#FF4444'),
    }
    bg, fg = colors.get(status, ('#1A1A2E', '#aaa'))
    return (f"<span style='background:{bg};color:{fg};padding:2px 8px;"
            f"border-radius:10px;font-size:0.72rem;font-weight:bold'>{status}</span>")


def _sentiment_badge(label):
    colors = {
        '🟢 正面': ('#003820', '#00C851'),
        '🔴 負面': ('#3A0000', '#FF4444'),
        '⚪ 中性':     ('#1A1A2E', '#aaa'),
    }
    bg, fg = colors.get(label, ('#1A1A2E', '#aaa'))
    return (f"<span style='background:{bg};color:{fg};padding:2px 7px;"
            f"border-radius:8px;font-size:0.70rem;font-weight:bold'>{label}</span>")


def _ret_cell(val):
    if val is None:
        return "<span style='color:#555'>N/A</span>"
    color = '#00C851' if val >= 0 else '#FF4444'
    arrow = '▲' if val >= 0 else '▼'
    return f"<span style='color:{color}'>{arrow}{abs(val):.1f}%</span>"


@st.cache_data(ttl=900, show_spinner=False)
def fetch_ticker_news_sentiment(tickers_tuple):
    result = {}
    for ticker in list(tickers_tuple):
        headlines = []
        try:
            tkr = yf.Ticker(ticker)
            news_list = tkr.news if hasattr(tkr, 'news') and isinstance(tkr.news, list) else []
            for item in news_list[:8]:
                content = item.get('content', {}) if isinstance(item, dict) else {}
                title = str(content.get('title', item.get('title', ''))).strip()
                if title:
                    headlines.append(title)
        except Exception:
            pass
        if not headlines:
            try:
                news_df = finvizfinance(ticker).ticker_news()
                if not news_df.empty:
                    for _, row in news_df.head(8).iterrows():
                        t = str(row.get('Title', '')).strip()
                        if t:
                            headlines.append(t)
            except Exception:
                pass
        label, pos, neg, pos_kw, neg_kw = classify_news_sentiment(headlines)
        result[ticker] = {
            'label': label, 'pos': pos, 'neg': neg,
            'pos_kw': pos_kw, 'neg_kw': neg_kw,
            'count': len(headlines),
        }
    return result


def build_rs_candidate_table(selected_themes, perf_data, sentiment_data, benchmark='SPY'):
    seen = set()
    rows = []
    for theme in selected_themes:
        tdata = CATALYST_THEME_MAP.get(theme, {})
        curated_tags = tdata.get('catalyst_tags', [])
        for ticker, name in tdata.get('tickers', {}).items():
            if ticker in seen:
                continue
            seen.add(ticker)
            p = perf_data.get(ticker, {})
            b = perf_data.get(benchmark, {})
            rs_cat = compute_rs_category(ticker, perf_data, benchmark)
            sent = sentiment_data.get(ticker, {})
            sent_label = sent.get('label', '⚪ 中性')
            pos_kw = sent.get('pos_kw', [])
            all_tags = list(dict.fromkeys(curated_tags[:2] + pos_kw[:1]))[:3]
            tag_str = ' | '.join(all_tags) if all_tags else '—'
            comment = ''
            if rs_cat == '🟢 跑贏指數' and sent_label == '🟢 正面':
                comment = '⭐ 技術+情緒雙強'
            elif rs_cat == '🟠 接近突破' and sent_label == '🟢 正面':
                comment = '🔥 接近突破+正面消息'
            elif rs_cat == '🟡 剛轉強' and sent_label == '🟢 正面':
                comment = '📈 剛轉強+正面消息'

            def _rel_safe(h):
                tv = p.get(h)
                bv = b.get(h)
                if tv is None or bv is None:
                    return None
                return round(tv - bv, 2)

            rows.append({
                'theme':      theme.split(' ', 1)[-1][:18] if ' ' in theme else theme[:18],
                'ticker':     ticker,
                'name':       name,
                'sent_label': sent_label,
                'tags':       tag_str,
                'price':      p.get('price'),
                'rel_5d':     _rel_safe('5d'),
                'rel_1m':     _rel_safe('1m'),
                'rel_3m':     _rel_safe('3m'),
                'rel_ytd':    _rel_safe('ytd'),
                'rs_cat':     rs_cat,
                'comment':    comment,
            })
    order = {'🟢 跑贏指數': 0, '🟠 接近突破': 1, '🟡 剛轉強': 2, '🔴 跑輸指數': 3}
    rows.sort(key=lambda r: (order.get(r['rs_cat'], 9), -(r['rel_1m'] or -999)))
    return rows


def render_rs_candidate_table_html(rows, benchmark):
    header = (
        "<style>"
        ".rs-tbl{width:100%;border-collapse:collapse;font-family:sans-serif;font-size:0.79rem}"
        ".rs-tbl th{background:#1E1E2E;color:#aaa;padding:7px 8px;text-align:left;"
        "border-bottom:2px solid #333;white-space:nowrap}"
        ".rs-tbl td{padding:6px 8px;vertical-align:middle;border-bottom:1px solid #1E1E2E}"
        ".rs-tbl tr.win{background:#061810!important}"
        ".rs-tbl tr.near-orange{background:#1A0E00!important}"
        ".rs-tbl tr.near-yellow{background:#1A1500!important}"
        ".rs-tbl tr.lose{background:#0E1117}"
        ".rs-tbl tr:hover{background:#1F2937}"
        ".rtk{font-weight:bold;color:#4FC3F7}"
        ".rtag{background:#1A2A3A;color:#7EC8E3;padding:1px 5px;border-radius:4px;"
        "font-size:0.68rem;margin:1px;display:inline-block}"
        ".rcomment{background:#004020;color:#00C851;padding:1px 6px;border-radius:8px;"
        "font-size:0.67rem;font-weight:bold}"
        "</style>"
        '<table class="rs-tbl">'
        "<thead><tr>"
        "<th>板塊/Theme</th><th>Ticker</th><th>公司</th>"
        "<th>新聞情緒</th><th>催化劑標籤</th>"
        "<th>現價</th>"
        f"<th>5D相對({benchmark})</th><th>1M相對({benchmark})</th>"
        f"<th>3M相對({benchmark})</th><th>YTD相對({benchmark})</th>"
        "<th>RS狀態</th><th>備注</th>"
        "</tr></thead><tbody>"
    )
    body_rows = []
    for r in rows:
        rs = r['rs_cat']
        if rs == '🟢 跑贏指數':
            row_cls = 'win'
        elif rs == '🟠 接近突破':
            row_cls = 'near-orange'
        elif rs == '🟡 剛轉強':
            row_cls = 'near-yellow'
        else:
            row_cls = 'lose'

        price_str = f"${r['price']:.2f}" if r['price'] else 'N/A'
        tag_html = (''.join(
            f"<span class='rtag'>{t}</span>"
            for t in r['tags'].split(' | ') if t and t != '—'
        ) or '—')
        comment_html = f"<span class='rcomment'>{r['comment']}</span>" if r['comment'] else ''

        body_rows.append(
            f"<tr class='{row_cls}'>"
            f"<td style='color:#888;font-size:0.73rem'>{r['theme']}</td>"
            f"<td><span class='rtk'>{r['ticker']}</span></td>"
            f"<td style='color:#ccc;font-size:0.74rem'>{r['name']}</td>"
            f"<td>{_sentiment_badge(r['sent_label'])}</td>"
            f"<td>{tag_html}</td>"
            f"<td style='color:#ccc'>{price_str}</td>"
            f"<td>{_ret_cell(r['rel_5d'])}</td>"
            f"<td>{_ret_cell(r['rel_1m'])}</td>"
            f"<td>{_ret_cell(r['rel_3m'])}</td>"
            f"<td>{_ret_cell(r['rel_ytd'])}</td>"
            f"<td>{_rs_status_badge(rs)}</td>"
            f"<td>{comment_html}</td>"
            f"</tr>"
        )
    return header + '\n'.join(body_rows) + '\n</tbody></table>'


def render_catalyst_screener_module():
    st.title('📡 新聞催化劑 / RS 方法選股')
    st.caption(
        '流程：📰 新聞催化劑 → 🔥 熱門板塊 → 📋 潛在股票 → 📊 RS方法分類 '
        '（跑贏指數 / 接近突破 / 剛轉強 / 跑輸指數）'
        '  ⚠️ 免責聲明：本工具僅供篩選/觀察清單用途，並非投資建議。'
    )

    ctrl_l, ctrl_m, ctrl_r = st.columns([3, 2, 1])
    with ctrl_l:
        all_themes = list(CATALYST_THEME_MAP.keys())
        selected_themes = st.multiselect(
            '選擇催化劑主題 / 板塊 (可多選):',
            all_themes,
            default=all_themes[:4],
            key='cat_theme_select',
            help='選擇你感興趣的催化劑主題，下方將顯示該主題所有候選股票。'
        )
    with ctrl_m:
        benchmark = st.selectbox(
            'RS 比較基準:',
            ['SPY', 'QQQ', 'IWM'],
            key='cat_bench',
            help='RS 方法相對於此指數計算相對回報。預設 SPY。'
        )
        rs_filter = st.multiselect(
            '篩選 RS 狀態:',
            ['🟢 跑贏指數', '🟠 接近突破', '🟡 剛轉強', '🔴 跑輸指數'],
            default=['🟢 跑贏指數', '🟠 接近突破', '🟡 剛轉強', '🔴 跑輸指數'],
            key='rs_status_filter',
        )
        sent_filter = st.multiselect(
            '篩選新聞情緒:',
            ['🟢 正面', '⚪ 中性', '🔴 負面'],
            default=['🟢 正面', '⚪ 中性', '🔴 負面'],
            key='rs_sent_filter',
        )
    with ctrl_r:
        st.markdown('<br>', unsafe_allow_html=True)
        refresh_cat = st.button('🔄 刷新數據', use_container_width=True, key='refresh_catalyst')

    if refresh_cat:
        try:
            fetch_catalyst_rs_data.clear()
            fetch_ticker_news_sentiment.clear()
        except Exception:
            pass
        st.rerun()

    if not selected_themes:
        st.info('請至少選擇一個催化劑主題。')
        return

    # Collect ALL candidate tickers (no RS truncation)
    candidate_tickers = []
    for theme in selected_themes:
        for t in CATALYST_THEME_MAP[theme].get('tickers', {}):
            if t not in candidate_tickers:
                candidate_tickers.append(t)

    # Step 1: News headlines
    st.markdown('---')
    st.markdown('### 📰 步驟1：新聞催化劑 Headlines')
    col_news1, col_news2 = st.columns([3, 2])
    with col_news1:
        with st.spinner('抓取市場新聞...'):
            try:
                news_list = fetch_top_news()
            except Exception:
                news_list = []
        if news_list:
            headlines_lower = ' '.join(n.get('新聞標題', '') for n in news_list[:20]).lower()
            matched_themes = [
                theme for theme, tdata in CATALYST_THEME_MAP.items()
                if any(kw in headlines_lower for kw in tdata.get('keywords', []))
            ]
            st.markdown('**📍 新聞命中板塊:**')
            if matched_themes:
                chips = ' '.join(
                    f"<span style='background:#1A3A2A;color:#00C851;padding:2px 7px;"
                    f"border-radius:8px;font-size:0.73rem;margin:2px;display:inline-block'>{t}</span>"
                    for t in matched_themes[:8]
                )
                st.markdown(chips, unsafe_allow_html=True)
            else:
                st.caption('（暫無明確板塊命中，使用預設催化劑手冊）')
            all_headlines = [n.get('新聞標題', '') for n in news_list[:20] if n.get('新聞標題')]
            theme_label, theme_pos, theme_neg, _, _ = classify_news_sentiment(all_headlines)
            st.markdown(
                f"**市場整體新聞情緒：** {_sentiment_badge(theme_label)} "
                f"<span style='font-size:0.72rem;color:#888'>（正面訊號 {theme_pos} | 負面訊號 {theme_neg}）</span>",
                unsafe_allow_html=True
            )
            with st.expander('🔎 最新財經頭條', expanded=False):
                for item in news_list[:12]:
                    src = item.get('來源', '')
                    ttl = item.get('新聞標題', '')
                    if ttl:
                        st.markdown(
                            f"<div style='border-left:2px solid #333;padding:3px 8px;margin:2px 0;"
                            f"font-size:0.78rem'><span style='color:#888;font-size:0.68rem'>[{src}]</span> {ttl}</div>",
                            unsafe_allow_html=True
                        )
        else:
            st.caption('⚠️ 暫時無法取得即時新聞，使用精選催化劑手冊。')
    with col_news2:
        st.markdown('**📋 精選催化劑手冊:**')
        for theme in selected_themes[:5]:
            tdata = CATALYST_THEME_MAP.get(theme, {})
            tags  = tdata.get('catalyst_tags', [])
            if tags:
                icon = theme.split()[0]
                name = theme.split(' ', 1)[-1].split('/')[0].strip()[:14]
                st.markdown(
                    f"<div style='font-size:0.75rem;color:#aaa;margin:3px 0'>"
                    f"<b style='color:#4FC3F7'>{icon} {name}</b>: "
                    f"{' · '.join(tags[:2])}</div>",
                    unsafe_allow_html=True
                )

    # Step 2: Fetch data (include theme ETFs for heatmap)
    theme_etfs = list(THEME_ETF_MAP.values())
    all_fetch_tickers = list(set(candidate_tickers + theme_etfs))
    st.markdown('---')
    st.markdown('### 📊 步驟2：抓取多時段回報數據')
    with st.spinner(f'正在抓取 {len(all_fetch_tickers)} 隻候選股票+ETF 的多時段回報數據...'):
        try:
            perf_data = fetch_catalyst_rs_data(
                tuple(sorted(set(all_fetch_tickers + [benchmark]))),
                benchmark=benchmark
            )
        except Exception:
            perf_data = {}

    with st.spinner('分析個股新聞情緒...'):
        try:
            sentiment_data = fetch_ticker_news_sentiment(tuple(candidate_tickers))
        except Exception:
            sentiment_data = {}

    # Step 3: RS method categorisation
    st.markdown('---')
    st.markdown('### 📋 步驟3：RS 方法分類 — 潛在股票全名單')

    rows = build_rs_candidate_table(selected_themes, perf_data, sentiment_data, benchmark)

    rows_display = [
        r for r in rows
        if r['rs_cat'] in rs_filter and r['sent_label'] in sent_filter
    ]

    n_win   = sum(1 for r in rows if r['rs_cat'] == '🟢 跑贏指數')
    n_near  = sum(1 for r in rows if r['rs_cat'] == '🟠 接近突破')
    n_just  = sum(1 for r in rows if r['rs_cat'] == '🟡 剛轉強')
    n_lose  = sum(1 for r in rows if r['rs_cat'] == '🔴 跑輸指數')
    bench_1d = perf_data.get(benchmark, {}).get('1d')

    mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
    mc1.metric('全部候選股', len(rows))
    mc2.metric('🟢 跑贏指數', n_win)
    mc3.metric('🟠 接近突破', n_near)
    mc4.metric('🟡 剛轉強', n_just)
    mc5.metric('🔴 跑輸指數', n_lose)
    mc6.metric(f'{benchmark} 今日', f"{bench_1d:+.2f}%" if bench_1d is not None else 'N/A')

    if rows_display:
        st.markdown(
            f"<div style='color:#888;font-size:0.78rem;margin:4px 0'>"
            f"顯示 {len(rows_display)} / {len(rows)} 隻候選股票（已套用篩選）。"
            f"</div>",
            unsafe_allow_html=True
        )
        st.markdown(render_rs_candidate_table_html(rows_display, benchmark), unsafe_allow_html=True)
    else:
        st.warning('⚠️ 當前篩選條件下無候選股票，請調整 RS狀態 或 情緒 篩選。')

    # Theme-level sentiment summary
    st.markdown('---')
    st.markdown('### 🗂️ 板塊新聞情緒摘要')
    theme_cols = st.columns(min(4, len(selected_themes)))
    for col, theme in zip(theme_cols, selected_themes):
        tdata = CATALYST_THEME_MAP.get(theme, {})
        tickers_in_theme = list(tdata.get('tickers', {}).keys())
        t_pos = sum(sentiment_data.get(tk, {}).get('pos', 0) for tk in tickers_in_theme)
        t_neg = sum(sentiment_data.get(tk, {}).get('neg', 0) for tk in tickers_in_theme)
        if t_pos > t_neg and t_pos > 0:
            t_lbl = '🟢 正面'
        elif t_neg > t_pos and t_neg > 0:
            t_lbl = '🔴 負面'
        else:
            t_lbl = '⚪ 中性'
        icon = theme.split()[0]
        name = theme.split(' ', 1)[-1][:16]
        with col:
            st.markdown(
                f"<div style='background:#161B22;border-radius:6px;padding:8px 10px;margin-bottom:4px'>"
                f"<div style='font-size:0.78rem;font-weight:bold;color:#4FC3F7'>{icon} {name}</div>"
                f"<div style='margin:4px 0'>{_sentiment_badge(t_lbl)}</div>"
                f"<div style='font-size:0.67rem;color:#888'>正面 {t_pos} | 負面 {t_neg}</div>"
                f"</div>",
                unsafe_allow_html=True
            )

    # ── B. 公司關係發現 ───────────────────────────────────────
    render_relationship_section(perf_data, sentiment_data, benchmark)

    # ── C. 主題熱力圖 & 下一輪潛在爆發 ────────────────────────────
    render_theme_heatmap_section(selected_themes, perf_data, sentiment_data, benchmark)

    with st.expander('📖 RS 方法分類邏輯 & 免責聲明', expanded=False):
        st.markdown(
            f"**RS 方法分類（相對強度法，對比 {benchmark}）：**\n\n"
            "| RS 狀態 | 條件 |\n"
            "|---------|------|\n"
            "| 🟢 跑贏指數 | 個股 1M 相對回報 > 0 **且** 3M 相對回報 > 0（中長線均跑贏）|\n"
            "| 🟠 接近突破 | 1M 相對差距 ≥ -1.5% 且 3M > -5%；或 1M已正但 3M 仍負（更強但尚未全面確認）|\n"
            "| 🟡 剛轉強  | 5D 相對已正但 1M 仍負；或 5D 相對比1M 高出 ≥2%（短線剛開始轉好）|\n"
            "| 🔴 跑輸指數 | 其他情況（各時間段均落後基準）|\n\n"
            f"**相對回報：** 個股各時間段回報 − 基準（{benchmark}）同期回報\n\n"
            "**新聞情緒：** 正面關鍵字（beat/raise/growth/AI/contract/upgrade…）vs 負面（miss/cut/downgrade/tariff/lawsuit…）關鍵字匹配計分。\n\n"
            "⚠️ 本模組純為技術篩選/觀察清單工具，所有數據僅供參考，不構成任何形式的投資建議。"
        )


# ==========================================
# B. 公司關係圖 Company Relationship Engine
# ==========================================
# Curated relationship graph: seed_ticker -> list of (related_ticker, relation_type, why)
COMPANY_RELATIONSHIP_GRAPH = {
    # ── AI Compute / GPU ────────────────────────────────────────────
    'NVDA': [
        ('TSM',  'supplier',     'TSMC 代工 NVIDIA GPU/AI 晶片（最先進製程）'),
        ('ASML', 'supplier',     'ASML 唯一 EUV 光刻機供應商，TSM 擴產必需'),
        ('AMAT', 'supplier',     'Applied Materials 提供晶圓製造設備'),
        ('LRCX', 'supplier',     'Lam Research 蝕刻/沉積設備'),
        ('KLAC', 'supplier',     'KLA 量測/檢測設備'),
        ('AVGO', 'competitor',   'Broadcom ASIC 客製化 AI 晶片（Google TPU/Meta MTIA）'),
        ('AMD',  'competitor',   'AMD MI300 系列直接競爭 H100/H200'),
        ('MU',   'beneficiary',  'Micron HBM 記憶體為 NVIDIA GPU 必需元件'),
        ('ANET', 'beneficiary',  'Arista Networks 數據中心 AI Fabric 網絡'),
        ('SMCI', 'beneficiary',  'SuperMicro 組裝 NVIDIA GPU 伺服器'),
        ('DELL', 'beneficiary',  'Dell 銷售 NVIDIA GPU 伺服器/工作站'),
        ('VRT',  'infrastructure','Vertiv 液冷/電力管理（AI 數據中心散熱）'),
        ('ETN',  'infrastructure','Eaton 電力分配，AI 數據中心用電管理'),
        ('VST',  'infrastructure','Vistra Energy 為數據中心供電'),
        ('CEG',  'infrastructure','Constellation Energy 核電供應 AI 數據中心'),
        ('GEV',  'infrastructure','GE Vernova 電網/渦輪機，數據中心供電基建'),
    ],
    'AMD': [
        ('NVDA', 'competitor',   'NVIDIA 最直接的 GPU 競爭對手'),
        ('TSM',  'supplier',     'TSMC 代工 AMD CPU/GPU'),
        ('AVGO', 'competitor',   'Broadcom AI ASIC 搶雲端客戶'),
        ('MU',   'beneficiary',  'Micron 記憶體為 AMD GPU 配套'),
        ('INTC', 'competitor',   'Intel CPU 市場直接競爭'),
        ('AMAT', 'supplier',     'Applied Materials 晶圓設備供應商'),
        ('SMCI', 'beneficiary',  'SuperMicro AMD GPU 伺服器'),
    ],
    'TSM': [
        ('NVDA', 'customer',     'NVIDIA 最大晶圓代工客戶'),
        ('AAPL', 'customer',     'Apple A 系列/M 系列晶片'),
        ('AMD',  'customer',     'AMD CPU/GPU 製造'),
        ('QCOM', 'customer',     'Qualcomm 手機晶片'),
        ('AVGO', 'customer',     'Broadcom ASIC 客製晶片'),
        ('ASML', 'supplier',     'ASML EUV 設備，TSM 先進製程必需'),
        ('AMAT', 'supplier',     'Applied Materials 設備'),
        ('LRCX', 'supplier',     'Lam Research 設備'),
    ],
    'AVGO': [
        ('NVDA', 'competitor',   'NVDA GPU vs AVGO ASIC，AI 晶片路線之爭'),
        ('GOOGL','customer',     'Google 採購 AVGO TPU/網絡晶片'),
        ('META', 'customer',     'Meta 採購 AVGO MTIA ASIC'),
        ('TSM',  'supplier',     'TSMC 代工 Broadcom ASIC'),
        ('ANET', 'ecosystem',    'Arista 網絡設備使用 AVGO 晶片'),
        ('AMD',  'competitor',   'AI ASIC 市場競爭'),
    ],
    # ── Cloud / AI Platforms ─────────────────────────────────────────
    'MSFT': [
        ('NVDA', 'supplier',     'Azure 大規模採購 NVIDIA GPU'),
        ('AMZN', 'competitor',   'AWS vs Azure 雲端市場競爭'),
        ('GOOGL','competitor',   'Google Cloud / Workspace 競爭'),
        ('ORCL', 'competitor',   'Oracle Cloud 企業雲端競爭'),
        ('AVGO', 'supplier',     'Broadcom 網絡晶片用於 Azure 數據中心'),
        ('ANET', 'supplier',     'Arista 網絡設備用於 Azure'),
        ('CRM',  'ecosystem',    'Salesforce Einstein AI 與 Azure 整合'),
        ('NOW',  'ecosystem',    'ServiceNow 與 Azure AI 深度整合'),
    ],
    'GOOGL': [
        ('NVDA', 'competitor',   'Google TPU 自研 AI 晶片，減少 NVDA 依賴'),
        ('AVGO', 'supplier',     'Broadcom ASIC 為 Google 客製 TPU'),
        ('AMZN', 'competitor',   'AWS vs GCP 雲端競爭'),
        ('MSFT', 'competitor',   'Azure vs GCP，Bing AI vs Gemini'),
        ('META', 'competitor',   '廣告市場直接競爭'),
        ('ANET', 'supplier',     'Arista 數據中心網絡'),
        ('TSM',  'supplier',     'TSMC 代工 Google TPU'),
    ],
    'META': [
        ('NVDA', 'supplier',     'Meta 最大 GPU 買家之一（訓練 LLaMA）'),
        ('AVGO', 'supplier',     'Broadcom MTIA ASIC 客製晶片'),
        ('GOOGL','competitor',   '廣告/社交媒體競爭'),
        ('SNAP', 'competitor',   'Snapchat 社交媒體競爭'),
        ('PINS', 'competitor',   'Pinterest 廣告競爭'),
        ('ANET', 'supplier',     'Arista 數據中心網絡設備'),
    ],
    'AMZN': [
        ('NVDA', 'supplier',     'AWS 採購 NVIDIA GPU + 自研 Trainium'),
        ('MSFT', 'competitor',   'Azure vs AWS 雲端競爭'),
        ('GOOGL','competitor',   'GCP vs AWS 競爭'),
        ('WMT',  'competitor',   '電商市場直接競爭'),
        ('COST', 'competitor',   '零售/會員制競爭'),
        ('ANET', 'supplier',     'AWS 數據中心網絡'),
        ('AVGO', 'supplier',     'Broadcom 晶片用於 AWS 數據中心'),
    ],
    # ── Data Center Power / Grid ─────────────────────────────────────
    'VRT': [
        ('ETN',  'competitor',   'Eaton 電力管理直接競爭'),
        ('NVDA', 'customer',     'NVIDIA GPU 伺服器熱量管理需要 Vertiv'),
        ('SMCI', 'customer',     'SuperMicro 伺服器配套散熱'),
        ('VST',  'ecosystem',    'Vistra 供電，Vertiv 管理電力分配'),
        ('CEG',  'ecosystem',    'Constellation 核電，配套電力基建'),
        ('PWR',  'ecosystem',    'Quanta Services 電網建設'),
        ('GEV',  'ecosystem',    'GE Vernova 電力設備同一生態'),
    ],
    'ETN': [
        ('VRT',  'competitor',   'Vertiv 電力管理直接競爭'),
        ('GEV',  'ecosystem',    'GE Vernova 電網/渦輪機生態'),
        ('PWR',  'ecosystem',    'Quanta Services 電網建設'),
        ('FCX',  'commodity',    'Eaton 耗用大量銅（電氣化）'),
        ('HUBB', 'competitor',   'Hubbell 電網設備競爭'),
    ],
    'CEG': [
        ('VST',  'competitor',   'Vistra 電力市場競爭（核能 vs 天然氣）'),
        ('OKLO', 'ecosystem',    'Oklo 小型核反應堆同一核能賽道'),
        ('NNE',  'ecosystem',    'Nano Nuclear 同一核能題材'),
        ('ETN',  'beneficiary',  'Eaton 電力設備受惠核電擴建'),
        ('VRT',  'beneficiary',  'Vertiv 受惠核電數據中心供電'),
        ('NVDA', 'customer',     'NVIDIA/AI 數據中心需求驅動核電採購'),
    ],
    # ── Copper / Electrification ─────────────────────────────────────
    'FCX': [
        ('SCCO', 'competitor',   'Southern Copper 銅礦直接競爭'),
        ('TECK', 'competitor',   'Teck Resources 銅礦競爭'),
        ('ETN',  'beneficiary',  'Eaton 電氣化需求驅動銅需求'),
        ('PWR',  'beneficiary',  'Quanta 電網建設消耗大量銅'),
        ('TSLA', 'beneficiary',  'Tesla EV 用銅量為傳統車 3-4 倍'),
        ('ALB',  'commodity',    'Albemarle 鋰礦，同屬 EV 電池材料'),
    ],
    # ── Energy / Natural Gas / LNG ───────────────────────────────────
    'LNG': [
        ('EQT',  'supplier',     'EQT 天然氣生產商，Cheniere LNG 出口'),
        ('WMB',  'supplier',     'Williams 管道輸送天然氣給 LNG 設施'),
        ('KMI',  'supplier',     'Kinder Morgan 管道基建'),
        ('XOM',  'competitor',   'ExxonMobil LNG 出口競爭'),
        ('CVX',  'competitor',   'Chevron LNG 出口競爭'),
    ],
    'XOM': [
        ('CVX',  'competitor',   'Chevron 最直接的綜合油氣競爭對手'),
        ('COP',  'competitor',   'ConocoPhillips 上游競爭'),
        ('SLB',  'supplier',     'Schlumberger 油田服務'),
        ('HAL',  'supplier',     'Halliburton 油田服務'),
        ('LNG',  'competitor',   'Cheniere LNG 出口競爭'),
    ],
    # ── Financial / Fintech ──────────────────────────────────────────
    'JPM': [
        ('GS',   'competitor',   'Goldman Sachs 投行/交易業務競爭'),
        ('MS',   'competitor',   'Morgan Stanley 財富管理競爭'),
        ('BAC',  'competitor',   'Bank of America 零售/商業銀行競爭'),
        ('BX',   'ecosystem',    'Blackstone PE 生態，IPO/M&A 合作'),
        ('V',    'ecosystem',    'Visa 支付網絡（JPM 發卡行）'),
        ('COIN', 'ecosystem',    'Coinbase 加密資產整合'),
    ],
    'GS': [
        ('JPM',  'competitor',   'JPMorgan 投行業務競爭'),
        ('MS',   'competitor',   'Morgan Stanley 競爭'),
        ('BX',   'ecosystem',    'Blackstone PE，共同服務機構客戶'),
        ('KKR',  'ecosystem',    'KKR PE，M&A 生態'),
        ('BLK',  'ecosystem',    'BlackRock 資產管理生態'),
    ],
    'COIN': [
        ('HOOD', 'competitor',   'Robinhood 加密交易競爭'),
        ('MSTR', 'beneficiary',  'MicroStrategy 持 BTC，幣價升 COIN 受惠'),
        ('MARA', 'ecosystem',    'Marathon Digital 加密採礦生態'),
        ('BX',   'ecosystem',    'Blackstone 機構加密資產配置'),
    ],
    # ── Healthcare / Obesity ─────────────────────────────────────────
    'LLY': [
        ('NVO',  'competitor',   'Novo Nordisk GLP-1 Ozempic/Wegovy 直接競爭'),
        ('ISRG', 'beneficiary',  'Intuitive Surgical 肥胖症手術需求'),
        ('TMO',  'beneficiary',  'Thermo Fisher GLP-1 藥物研發服務'),
        ('DHR',  'beneficiary',  'Danaher 生命科學工具'),
        ('RXRX', 'ecosystem',    'Recursion AI 藥研，新適應症發現'),
        ('MRNA', 'competitor',   'Moderna mRNA 平台（潛在 GLP-1 競爭）'),
    ],
    'NVO': [
        ('LLY',  'competitor',   'Eli Lilly Mounjaro/Zepbound 直接競爭'),
        ('TMO',  'beneficiary',  'Thermo Fisher 藥物研發/製造服務'),
        ('ISRG', 'beneficiary',  'Intuitive Surgical 肥胖症手術'),
    ],
}


def get_related_tickers(seed_ticker: str):
    """Return list of (related_ticker, relation_type, why) for a seed ticker.
    Also checks reverse lookups (if seed appears as a related ticker)."""
    direct = COMPANY_RELATIONSHIP_GRAPH.get(seed_ticker.upper(), [])
    # reverse: find entries where seed_ticker is the related ticker
    reverse = []
    seed_up = seed_ticker.upper()
    for parent, relations in COMPANY_RELATIONSHIP_GRAPH.items():
        if parent == seed_up:
            continue
        for (rel_t, rel_type, why) in relations:
            if rel_t == seed_up:
                # Add inverse relationship to the seed
                inv_type = {
                    'supplier': 'customer',
                    'customer': 'supplier',
                    'competitor': 'competitor',
                    'beneficiary': 'ecosystem',
                    'ecosystem': 'ecosystem',
                    'infrastructure': 'ecosystem',
                    'commodity': 'ecosystem',
                }.get(rel_type, 'ecosystem')
                reverse.append((parent, inv_type, f'[反向] {why}'))
    # Merge, deduplicate by ticker
    seen = set()
    result = []
    for item in direct + reverse:
        if item[0] not in seen:
            seen.add(item[0])
            result.append(item)
    return result


def render_relationship_section(perf_data, sentiment_data, benchmark):
    """Render the Company Relationship Discovery section inside the RS module."""
    st.markdown('---')
    st.markdown('### 🕸️ 公司關係發現 Company Relationship Engine')
    st.caption(
        '選擇種子股票，查看供應鏈、客戶、競爭對手、生態受益者等相關公司及其 RS 狀態，'
        '幫助捕捉產業鏈輪動機會。⚠️ 僅供篩選參考，不構成投資建議。'
    )

    all_seed_tickers = sorted(COMPANY_RELATIONSHIP_GRAPH.keys())
    seed = st.selectbox(
        '選擇種子股票 Seed Ticker:',
        all_seed_tickers,
        index=all_seed_tickers.index('NVDA') if 'NVDA' in all_seed_tickers else 0,
        key='rel_seed_ticker',
    )

    related = get_related_tickers(seed)
    if not related:
        st.info(f'暫無 {seed} 的關係數據。')
        return

    # Fetch RS data for related tickers not already in perf_data
    rel_tickers = [r[0] for r in related if r[0] not in perf_data]
    if rel_tickers:
        with st.spinner(f'抓取相關股票數據 ({len(rel_tickers)} 隻)...'):
            try:
                extra_perf = fetch_catalyst_rs_data(
                    tuple(sorted(set(rel_tickers + [benchmark]))),
                    benchmark=benchmark
                )
                perf_data = {**perf_data, **extra_perf}
            except Exception:
                pass

    # Fetch sentiment for related tickers not already in sentiment_data
    rel_sent_needed = [r[0] for r in related if r[0] not in sentiment_data]
    if rel_sent_needed:
        with st.spinner('分析相關股票新聞情緒...'):
            try:
                extra_sent = fetch_ticker_news_sentiment(tuple(rel_sent_needed))
                sentiment_data = {**sentiment_data, **extra_sent}
            except Exception:
                pass

    # Build table rows
    rel_type_labels = {
        'supplier':      '🔧 供應商',
        'customer':      '🛒 客戶',
        'competitor':    '⚔️ 競爭對手',
        'beneficiary':   '📈 受益者',
        'ecosystem':     '🌐 生態圈',
        'infrastructure':'⚡ 基建',
        'commodity':     '🪨 原材料',
    }

    header = (
        "<style>"
        ".rel-tbl{width:100%;border-collapse:collapse;font-family:sans-serif;font-size:0.79rem}"
        ".rel-tbl th{background:#1E1E2E;color:#aaa;padding:7px 8px;text-align:left;"
        "border-bottom:2px solid #333;white-space:nowrap}"
        ".rel-tbl td{padding:6px 8px;vertical-align:middle;border-bottom:1px solid #1E1E2E}"
        ".rel-tbl tr:hover{background:#1F2937}"
        ".rtk2{font-weight:bold;color:#FFD700}"
        "</style>"
        '<table class="rel-tbl">'
        "<thead><tr>"
        f"<th>種子<br>Seed</th><th>關係類型</th><th>相關Ticker</th>"
        "<th>為何相關</th><th>新聞情緒</th>"
        f"<th>5D相對</th><th>1M相對</th><th>3M相對</th><th>RS狀態</th>"
        "</tr></thead><tbody>"
    )

    body_rows = []
    bench = perf_data.get(benchmark, {})

    def _rel_v(ticker, period):
        p = perf_data.get(ticker, {})
        tv = p.get(period)
        bv = bench.get(period)
        if tv is None or bv is None:
            return None
        return round(tv - bv, 2)

    for (rel_t, rel_type, why) in related:
        rs_cat = compute_rs_category(rel_t, perf_data, benchmark)
        sent = sentiment_data.get(rel_t, {})
        sent_label = sent.get('label', '⚪ 中性')
        rel_label = rel_type_labels.get(rel_type, rel_type)
        rs_badge = _rs_status_badge(rs_cat)
        sent_badge = _sentiment_badge(sent_label)
        r5d = _rel_v(rel_t, '5d')
        r1m = _rel_v(rel_t, '1m')
        r3m = _rel_v(rel_t, '3m')
        body_rows.append(
            f"<tr>"
            f"<td><span style='font-weight:bold;color:#4FC3F7'>{seed}</span></td>"
            f"<td>{rel_label}</td>"
            f"<td><span class='rtk2'>{rel_t}</span></td>"
            f"<td style='color:#bbb;font-size:0.73rem'>{why}</td>"
            f"<td>{sent_badge}</td>"
            f"<td>{_ret_cell(r5d)}</td>"
            f"<td>{_ret_cell(r1m)}</td>"
            f"<td>{_ret_cell(r3m)}</td>"
            f"<td>{rs_badge}</td>"
            f"</tr>"
        )

    st.markdown(header + '\n'.join(body_rows) + '\n</tbody></table>', unsafe_allow_html=True)

    # Legend
    st.markdown(
        "<div style='font-size:0.68rem;color:#666;margin-top:6px'>"
        "關係類型說明：🔧 供應商 = 向種子提供原料/設備/服務 │ "
        "🛒 客戶 = 購買種子產品/服務 │ "
        "⚔️ 競爭對手 = 同賽道直接競爭 │ "
        "📈 受益者 = 因種子增長而間接受惠 │ "
        "🌐 生態圈 = 同一主題生態 │ "
        "⚡ 基建 = 提供基礎設施 │ "
        "🪨 原材料 = 關鍵商品/原材料"
        "</div>",
        unsafe_allow_html=True
    )


# ==========================================
# C. 主題熱力圖 & 下一輪潛在爆發 Theme Heat Map
# ==========================================
# Theme ETF map for heat scoring
THEME_ETF_MAP = {
    '🤖 AI 半導體 / 算力': 'SOXX',
    '🧠 大型 AI 平台 (Mega-cap)': 'QQQ',
    '⚡ 數據中心電力 / 電網': 'VRT',
    '⚛️ 能源 / 天然氣': 'XLE',
    '🏭 工業 / 再工業化': 'XLI',
    '🔧 材料 / 銅 / 電氣化': 'XLB',
    '📊 小型股 / 週期輪動': 'IWM',
    '🏦 金融 / 銀行': 'XLF',
    '💊 醫療 / 生物科技': 'XBI',
    '🛡️ AI 軟件 / 網絡安全': 'CIBR',
    '🛒 消費 / 零售': 'XLY',
}


def compute_theme_heat(theme_name, perf_data, sentiment_data, benchmark='SPY'):
    """Compute a simple heat score for a theme given preloaded perf/sentiment data.
    Returns dict with etf_rel_5d/1m/3m, breadth counts, sentiment, heat_label."""
    tdata = CATALYST_THEME_MAP.get(theme_name, {})
    etf = THEME_ETF_MAP.get(theme_name, tdata.get('etf', ''))
    tickers = list(tdata.get('tickers', {}).keys())

    # ETF relative performance
    bench = perf_data.get(benchmark, {})
    etf_data = perf_data.get(etf, {})

    def _rel(d, period):
        tv = d.get(period)
        bv = bench.get(period)
        return round(tv - bv, 2) if tv is not None and bv is not None else None

    etf_rel_5d = _rel(etf_data, '5d')
    etf_rel_1m = _rel(etf_data, '1m')
    etf_rel_3m = _rel(etf_data, '3m')

    # Breadth: count RS categories across theme tickers
    n_win = n_near = n_just = n_lose = 0
    for t in tickers:
        cat = compute_rs_category(t, perf_data, benchmark)
        if cat == '🟢 跑贏指數':  n_win  += 1
        elif cat == '🟠 接近突破': n_near += 1
        elif cat == '🟡 剛轉強':   n_just += 1
        else:                      n_lose += 1

    total = max(len(tickers), 1)
    pct_strong = round((n_win + n_near) / total * 100)
    pct_turning = round((n_just) / total * 100)

    # Average news sentiment
    pos_sum = sum(sentiment_data.get(t, {}).get('pos', 0) for t in tickers)
    neg_sum = sum(sentiment_data.get(t, {}).get('neg', 0) for t in tickers)
    if pos_sum > neg_sum and pos_sum > 0:
        theme_sent = '🟢 正面'
    elif neg_sum > pos_sum and neg_sum > 0:
        theme_sent = '🔴 負面'
    else:
        theme_sent = '⚪ 中性'

    catalyst_count = len(tdata.get('catalyst_tags', []))

    # ── Heat label logic ──
    # 當炒主線: ETF relative strong (1M > 0), breadth majority 跑贏/接近突破, sentiment positive
    etf_1m_ok = etf_rel_1m is not None and etf_rel_1m > 0
    etf_5d_ok = etf_rel_5d is not None and etf_rel_5d > 0

    if etf_1m_ok and pct_strong >= 40 and theme_sent == '🟢 正面':
        heat_label = '🔥 當炒主線'
    elif (etf_5d_ok or (etf_rel_1m is not None and etf_rel_1m > -2)) and \
         (pct_turning + pct_strong) >= 35 and theme_sent in ('🟢 正面', '⚪ 中性'):
        heat_label = '🚀 下一輪潛在'
    else:
        heat_label = '👀 觀察/未確認'

    return {
        'etf': etf,
        'etf_rel_5d': etf_rel_5d,
        'etf_rel_1m': etf_rel_1m,
        'etf_rel_3m': etf_rel_3m,
        'n_win': n_win, 'n_near': n_near, 'n_just': n_just, 'n_lose': n_lose,
        'total': total,
        'pct_strong': pct_strong,
        'pct_turning': pct_turning,
        'theme_sent': theme_sent,
        'catalyst_count': catalyst_count,
        'heat_label': heat_label,
    }


def render_theme_heatmap_section(selected_themes, perf_data, sentiment_data, benchmark):
    """Render the Theme Heat Map & Next-Round Sector Discovery section."""
    st.markdown('---')
    st.markdown('### 🌡️ 主題熱力圖 & 下一輪潛在爆發板塊')
    st.caption(
        '根據主題 ETF 相對表現、板塊廣度（RS 分佈）和新聞情緒，識別 🔥 當炒主線、'
        '🚀 下一輪潛在 和 👀 觀察/未確認 板塊。僅供篩選參考，不構成投資建議。'
    )

    themes_to_analyze = selected_themes if selected_themes else list(CATALYST_THEME_MAP.keys())

    heat_results = []
    for theme in themes_to_analyze:
        h = compute_theme_heat(theme, perf_data, sentiment_data, benchmark)
        heat_results.append((theme, h))

    # Sort: 當炒主線 first, then 下一輪潛在, then 觀察/未確認
    heat_order = {'🔥 當炒主線': 0, '🚀 下一輪潛在': 1, '👀 觀察/未確認': 2}
    heat_results.sort(key=lambda x: (heat_order.get(x[1]['heat_label'], 9),
                                      -(x[1]['etf_rel_1m'] or -99)))

    # Summary buckets
    bucket_hot    = [t for t, h in heat_results if h['heat_label'] == '🔥 當炒主線']
    bucket_next   = [t for t, h in heat_results if h['heat_label'] == '🚀 下一輪潛在']
    bucket_watch  = [t for t, h in heat_results if h['heat_label'] == '👀 觀察/未確認']

    bcol1, bcol2, bcol3 = st.columns(3)
    with bcol1:
        st.markdown(
            "<div style='background:#0D2010;border:1px solid #00C851;border-radius:8px;padding:10px'>"
            "<div style='font-size:0.85rem;font-weight:bold;color:#00C851'>🔥 當炒主線</div>"
            "<div style='font-size:0.75rem;color:#888;margin-top:4px'>ETF+廣度+情緒全面強勢</div>"
            f"<div style='margin-top:6px'>" +
            ''.join(f"<div style='font-size:0.8rem;color:#ccc;padding:2px 0'>• {t.split(' ',1)[-1][:22]}</div>" for t in bucket_hot) +
            ("<div style='color:#555;font-size:0.75rem'>（暫無符合條件板塊）</div>" if not bucket_hot else '') +
            "</div></div>",
            unsafe_allow_html=True
        )
    with bcol2:
        st.markdown(
            "<div style='background:#0D1020;border:1px solid #FF8C00;border-radius:8px;padding:10px'>"
            "<div style='font-size:0.85rem;font-weight:bold;color:#FF8C00'>🚀 下一輪潛在</div>"
            "<div style='font-size:0.75rem;color:#888;margin-top:4px'>剛轉強/接近突破為主，ETF動能改善</div>"
            f"<div style='margin-top:6px'>" +
            ''.join(f"<div style='font-size:0.8rem;color:#ccc;padding:2px 0'>• {t.split(' ',1)[-1][:22]}</div>" for t in bucket_next) +
            ("<div style='color:#555;font-size:0.75rem'>（暫無符合條件板塊）</div>" if not bucket_next else '') +
            "</div></div>",
            unsafe_allow_html=True
        )
    with bcol3:
        st.markdown(
            "<div style='background:#101010;border:1px solid #555;border-radius:8px;padding:10px'>"
            "<div style='font-size:0.85rem;font-weight:bold;color:#888'>👀 觀察/未確認</div>"
            "<div style='font-size:0.75rem;color:#666;margin-top:4px'>混合 RS 或情緒未明朗</div>"
            f"<div style='margin-top:6px'>" +
            ''.join(f"<div style='font-size:0.8rem;color:#888;padding:2px 0'>• {t.split(' ',1)[-1][:22]}</div>" for t in bucket_watch) +
            ("<div style='color:#555;font-size:0.75rem'>（暫無符合條件板塊）</div>" if not bucket_watch else '') +
            "</div></div>",
            unsafe_allow_html=True
        )

    st.markdown('<br>', unsafe_allow_html=True)

    # Detailed heat table
    tbl_header = (
        "<style>"
        ".heat-tbl{width:100%;border-collapse:collapse;font-family:sans-serif;font-size:0.78rem}"
        ".heat-tbl th{background:#1E1E2E;color:#aaa;padding:6px 8px;text-align:left;"
        "border-bottom:2px solid #333;white-space:nowrap}"
        ".heat-tbl td{padding:5px 8px;vertical-align:middle;border-bottom:1px solid #1A1A2E}"
        ".heat-tbl tr:hover{background:#1F2937}"
        "</style>"
        '<table class="heat-tbl">'
        "<thead><tr>"
        "<th>板塊/Theme</th><th>ETF</th>"
        f"<th>ETF 5D相對({benchmark})</th>"
        f"<th>ETF 1M相對({benchmark})</th>"
        f"<th>ETF 3M相對({benchmark})</th>"
        "<th>廣度 🟢跑贏</th><th>廣度 🟠接近</th><th>廣度 🟡剛轉</th>"
        "<th>板塊情緒</th><th>催化劑</th><th>🌡️ 熱力判斷</th>"
        "</tr></thead><tbody>"
    )
    tbl_rows = []
    heat_colors = {
        '🔥 當炒主線':   ('#0D2010', '#00C851'),
        '🚀 下一輪潛在': ('#1A0E00', '#FF8C00'),
        '👀 觀察/未確認': ('#0E1117', '#888'),
    }
    for theme, h in heat_results:
        icon = theme.split()[0]
        name = theme.split(' ', 1)[-1][:20]
        bg, fg = heat_colors.get(h['heat_label'], ('#0E1117', '#888'))
        heat_badge = (
            f"<span style='background:{bg};color:{fg};padding:2px 7px;"
            f"border-radius:8px;font-size:0.71rem;font-weight:bold'>{h['heat_label']}</span>"
        )
        tbl_rows.append(
            f"<tr style='background:{bg}'>"
            f"<td style='color:#ddd'>{icon} {name}</td>"
            f"<td style='color:#4FC3F7;font-weight:bold'>{h['etf']}</td>"
            f"<td>{_ret_cell(h['etf_rel_5d'])}</td>"
            f"<td>{_ret_cell(h['etf_rel_1m'])}</td>"
            f"<td>{_ret_cell(h['etf_rel_3m'])}</td>"
            f"<td style='color:#00C851'>{h['n_win']}/{h['total']}</td>"
            f"<td style='color:#FF8C00'>{h['n_near']}/{h['total']}</td>"
            f"<td style='color:#F9A825'>{h['n_just']}/{h['total']}</td>"
            f"<td>{_sentiment_badge(h['theme_sent'])}</td>"
            f"<td style='color:#7EC8E3'>{h['catalyst_count']}</td>"
            f"<td>{heat_badge}</td>"
            f"</tr>"
        )
    st.markdown(tbl_header + '\n'.join(tbl_rows) + '\n</tbody></table>', unsafe_allow_html=True)
    st.caption(
        '判斷邏輯：🔥 當炒主線 = ETF 1M相對>0 + 跑贏/接近突破股票 ≥40% + 正面情緒；'
        '🚀 下一輪潛在 = ETF 5D改善 + 剛轉強/接近突破股票 ≥35%；'
        '👀 觀察/未確認 = 其他情況。所有數據僅供篩選，不構成投資建議。'
    )


# ==========================================
# Sidebar 市場雷達函數
# ==========================================
@st.cache_data(ttl=300, show_spinner=False)
def fetch_sidebar_market_data():
    data = {}
    tickers = {
        'VIX': '^VIX', 'SPY': 'SPY', 'QQQ': 'QQQ', 'DIA': 'DIA',
        'DXY': 'DX-Y.NYB', 'GOLD': 'GC=F', 'OIL': 'CL=F',
        'NG': 'NG=F', 'BTC': 'BTC-USD', 'ETH': 'ETH-USD',
        'TNX': '^TNX', 'TYX': '^TYX', 'NIKKEI': '^N225', 'HSI': '^HSI',
        'SHCOMP': '000001.SS', 'SILVER': 'SI=F',
    }
    try:
        raw = yf.download(list(tickers.values()), period='2d', interval='1d', progress=False, auto_adjust=True)
        closes = raw['Close'] if 'Close' in raw.columns.get_level_values(0) else raw
        for label, sym in tickers.items():
            try:
                prices = closes[sym].dropna()
                if len(prices) >= 2:
                    prev, curr = float(prices.iloc[-2]), float(prices.iloc[-1])
                    pct = (curr - prev) / prev * 100
                    data[label] = {'price': curr, 'pct': pct}
                elif len(prices) == 1:
                    data[label] = {'price': float(prices.iloc[-1]), 'pct': 0.0}
                else:
                    data[label] = {'price': None, 'pct': 0.0}
            except:
                data[label] = {'price': None, 'pct': 0.0}
    except:
        pass

    # CNN Fear & Greed
    try:
        r = requests.get('https://production.dataviz.cnn.io/index/fearandgreed/graphdata', headers=get_headers(), timeout=8)
        if r.status_code == 200:
            fg = r.json().get('fear_and_greed', {})
            score = fg.get('score')
            data['FEAR_GREED'] = {'score': round(float(score), 1) if score else None, 'rating': fg.get('rating', 'N/A')}
    except:
        data['FEAR_GREED'] = {'score': None, 'rating': 'N/A'}

    # ── FRED helper: parse CSV robustly, multi-strategy fetch ──
    def _fred_csv(series_id, timeout=5):
        """Fetch a FRED CSV with multiple fallback strategies.
        Returns list of (date_str, float_value) tuples, chronological order.
        Last element = most recent. Empty list if all strategies fail."""
        url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}'

        def _parse_csv_text(text):
            rows = []
            for line in text.strip().splitlines():
                parts = line.split(',')
                if len(parts) != 2:
                    continue
                date_s, val_s = parts[0].strip(), parts[1].strip()
                if date_s == 'DATE' or val_s in ('.', '', 'N/A'):
                    continue
                try:
                    rows.append((date_s, float(val_s)))
                except ValueError:
                    continue
            return rows

        # Strategy 1: requests with browser-like headers
        try:
            headers_full = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache',
            }
            resp = requests.get(url, headers=headers_full, timeout=timeout)
            if resp.status_code == 200 and 'DATE' in resp.text:
                rows = _parse_csv_text(resp.text)
                if rows:
                    return rows
        except Exception:
            pass

        # Strategy 2: pandas read_csv (different HTTP stack)
        try:
            df = pd.read_csv(url, storage_options={'User-Agent': 'Mozilla/5.0'})
            if not df.empty and len(df.columns) >= 2:
                rows = []
                for _, row in df.iterrows():
                    date_s = str(row.iloc[0]).strip()
                    val_s  = str(row.iloc[1]).strip()
                    if date_s == 'DATE' or val_s in ('.', '', 'nan', 'N/A'):
                        continue
                    try:
                        rows.append((date_s, float(val_s)))
                    except ValueError:
                        continue
                if rows:
                    return rows
        except Exception:
            pass

        # Strategy 3: minimal curl-style get
        try:
            resp = requests.get(url, timeout=timeout, verify=False)
            if resp.status_code == 200:
                rows = _parse_csv_text(resp.text)
                if rows:
                    return rows
        except Exception:
            pass

        return []  # all strategies failed

    # ── FRED employment series fetcher (with error tracking) ──
    employment_errors = {}

    # FRED: Unemployment Rate
    try:
        rows = _fred_csv('UNRATE')
        if rows:
            date_s, val = rows[-1]
            prior_val = rows[-2][1] if len(rows) >= 2 else val
            data['UNRATE'] = {
                'date': date_s, 'value': val,
                'prior': prior_val, 'change': round(val - prior_val, 2)
            }
        else:
            data['UNRATE'] = {'error': 'FRED 暫時無數據'}
            employment_errors['UNRATE'] = 'FRED 暫時無數據'
    except Exception as _e:
        data['UNRATE'] = {'error': str(_e)[:60]}
        employment_errors['UNRATE'] = str(_e)[:60]

    # FRED: CPI YoY
    try:
        rows = _fred_csv('CPIAUCSL')
        if len(rows) >= 13:
            yoy = (rows[-1][1] - rows[-13][1]) / rows[-13][1] * 100
            data['CPI_YOY'] = {'date': rows[-1][0], 'value': round(yoy, 2)}
        else:
            data['CPI_YOY'] = None
    except Exception:
        data['CPI_YOY'] = None

    # FRED: Fed Funds Rate
    try:
        rows = _fred_csv('FEDFUNDS')
        if rows:
            data['FEDFUNDS'] = {'date': rows[-1][0], 'value': rows[-1][1]}
        else:
            data['FEDFUNDS'] = None
    except Exception:
        data['FEDFUNDS'] = None

    # FRED: Initial Jobless Claims (ICSA)
    try:
        rows = _fred_csv('ICSA')
        if rows:
            date_s, val = rows[-1]
            prior_val = rows[-2][1] if len(rows) >= 2 else val
            data['JOBLESS'] = {
                'date': date_s,
                'value': int(val),
                'prior': int(prior_val),
                'change': int(val - prior_val),
            }
        else:
            data['JOBLESS'] = {'error': 'FRED 暫時無數據'}
            employment_errors['JOBLESS'] = 'FRED 暫時無數據'
    except Exception as _e:
        data['JOBLESS'] = {'error': str(_e)[:60]}
        employment_errors['JOBLESS'] = str(_e)[:60]

    # FRED: Nonfarm Payrolls (PAYEMS) – monthly change (values in thousands)
    try:
        rows = _fred_csv('PAYEMS')
        if len(rows) >= 2:
            date_s, curr_val = rows[-1]
            prev_val = rows[-2][1]
            chg = int(round((curr_val - prev_val) * 1000))  # convert thousands → actual jobs
            data['PAYEMS'] = {
                'date': date_s, 'value': curr_val,
                'prior': prev_val, 'change': chg
            }
        else:
            data['PAYEMS'] = {'error': 'FRED 暫時無數據'}
            employment_errors['PAYEMS'] = 'FRED 暫時無數據'
    except Exception as _e:
        data['PAYEMS'] = {'error': str(_e)[:60]}
        employment_errors['PAYEMS'] = str(_e)[:60]

    # FRED: Labor Force Participation Rate (CIVPART)
    try:
        rows = _fred_csv('CIVPART')
        if len(rows) >= 2:
            date_s, val = rows[-1]
            prior_val = rows[-2][1]
            data['CIVPART'] = {
                'date': date_s, 'value': val,
                'prior': prior_val, 'change': round(val - prior_val, 2)
            }
        else:
            data['CIVPART'] = {'error': 'FRED 暫時無數據'}
            employment_errors['CIVPART'] = 'FRED 暫時無數據'
    except Exception as _e:
        data['CIVPART'] = {'error': str(_e)[:60]}
        employment_errors['CIVPART'] = str(_e)[:60]

    # Store aggregated employment errors
    data['EMPLOYMENT_ERRORS'] = employment_errors

    # FRED: Core CPI (CPILFESL) – ex Food & Energy, YoY
    try:
        rows = _fred_csv('CPILFESL')
        if len(rows) >= 13:
            yoy = (rows[-1][1] - rows[-13][1]) / rows[-13][1] * 100
            data['CORE_CPI'] = {'date': rows[-1][0], 'value': round(yoy, 2)}
        else:
            data['CORE_CPI'] = None
    except Exception:
        data['CORE_CPI'] = None

    # 2-Year Treasury yield via yfinance (^FVX = 5Y but ^IRX = 13-week; use TNX/TYX already fetched)
    # We'll derive 2Y from yfinance ^IRX (13-week) as proxy for short end, or fetch ^FVX
    try:
        twoy = yf.download('^IRX', period='2d', progress=False, auto_adjust=True)
        if not twoy.empty and 'Close' in twoy.columns:
            col = twoy['Close'].dropna()
            if len(col) >= 2:
                data['TWO_Y'] = {'price': float(col.iloc[-1]), 'pct': float((col.iloc[-1] - col.iloc[-2]) / col.iloc[-2] * 100)}
            elif len(col) == 1:
                data['TWO_Y'] = {'price': float(col.iloc[-1]), 'pct': 0.0}
    except:
        data['TWO_Y'] = None

    # Yield curve spread: 10Y - 2Y (derived if both available)
    try:
        tnx = data.get('TNX', {})
        twy = data.get('TWO_Y', {})
        if tnx and tnx.get('price') and twy and twy.get('price'):
            spread = tnx['price'] / 10 - twy['price'] / 100  # TNX is *10, IRX is /100
            # Actually TNX raw value from yfinance is already the yield * 10 for display
            # Let's compute more carefully below in the render function
            data['YIELD_SPREAD_RAW'] = {'tnx': tnx['price'], 'irx': twy['price']}
    except:
        data['YIELD_SPREAD_RAW'] = None

    return data

def _pm(label, emoji, d, fmt='{:.2f}'):
    if d and d.get('price') is not None:
        p, pct = d['price'], d['pct']
        arrow = '▲' if pct >= 0 else '▼'
        color = '#00C851' if pct >= 0 else '#FF4444'
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;align-items:center;padding:2px 0'>"
            f"<span style='font-size:0.78rem'>{emoji} <b>{label}</b></span>"
            f"<span style='font-size:0.78rem'>{fmt.format(p)} "
            f"<span style='color:{color};font-size:0.7rem'>{arrow}{abs(pct):.1f}%</span></span>"
            f"</div>", unsafe_allow_html=True)
    else:
        st.markdown(f"<div style='font-size:0.72rem;color:#666'>{emoji} {label}: N/A</div>", unsafe_allow_html=True)

def _section(title):
    st.markdown(f"<div style='font-size:0.72rem;font-weight:700;color:#888;margin:6px 0 2px'>{title}</div>", unsafe_allow_html=True)

def _divider():
    st.markdown("<hr style='margin:5px 0;opacity:0.2'>", unsafe_allow_html=True)

def _fred_row(label, emoji, value_str, date_str, color='#f1c40f', change_str=None):
    """Compact sidebar row for a FRED macro indicator."""
    change_html = ''
    if change_str:
        change_html = f" <span style='font-size:0.65rem;color:#aaa'>{change_str}</span>"
    st.markdown(
        f"<div style='display:flex;justify-content:space-between;align-items:center;padding:2px 0'>"
        f"<span style='font-size:0.77rem'>{emoji} <b>{label}</b></span>"
        f"<span style='font-size:0.77rem;color:{color}'><b>{value_str}</b>"
        f"<span style='font-size:0.63rem;color:#666'> {date_str}</span>{change_html}</span>"
        f"</div>", unsafe_allow_html=True)


def render_sidebar_employment_expander(m):
    """就業數據 expander – UNRATE, PAYEMS, ICSA, CIVPART (with per-series error status)."""
    # Helper: check if a series dict has a real value or only error
    def _has_value(d):
        return isinstance(d, dict) and d.get('value') is not None

    def _is_error(d):
        return isinstance(d, dict) and 'error' in d and d.get('value') is None

    def _status_dot(d):
        """Return a small coloured status indicator."""
        if _has_value(d):
            return "<span style='color:#2ecc71;font-size:0.65rem'>✅</span>"
        elif _is_error(d):
            return "<span style='color:#e74c3c;font-size:0.65rem'>❌</span>"
        else:
            return "<span style='color:#888;font-size:0.65rem'>—</span>"

    unemp   = m.get('UNRATE')
    payems  = m.get('PAYEMS')
    jl      = m.get('JOBLESS')
    civpart = m.get('CIVPART')
    emp_errors = m.get('EMPLOYMENT_ERRORS', {})

    # Determine overall health
    all_series = [unemp, payems, jl, civpart]
    all_failed = all(not _has_value(d) for d in all_series)
    any_ok     = any(_has_value(d) for d in all_series)

    with st.expander("👷 就業數據 Employment", expanded=False):

        # ── Per-series health bar ───────────────────────────
        status_html = (
            "<div style='display:flex;gap:8px;align-items:center;"
            "font-size:0.68rem;color:#aaa;margin-bottom:6px;flex-wrap:wrap'>"
            f"<span>{_status_dot(unemp)} UNRATE</span>"
            f"<span>{_status_dot(payems)} PAYEMS</span>"
            f"<span>{_status_dot(jl)} ICSA</span>"
            f"<span>{_status_dot(civpart)} CIVPART</span>"
            "</div>"
        )
        st.markdown(status_html, unsafe_allow_html=True)

        # ── All-fail notice ─────────────────────────────────
        if all_failed:
            st.markdown(
                "<div style='background:#1a0a00;border:1px solid #e67e22;"
                "border-radius:6px;padding:8px 10px;margin-bottom:8px'>"
                "<div style='color:#e67e22;font-size:0.77rem;font-weight:700'>"
                "⚠️ FRED 暫時拉唔到，請檢查網絡</div>"
                "<div style='color:#aaa;font-size:0.7rem;margin-top:3px'>"
                "St. Louis FRED API 連線暫時受阻。數據將在下次重試時自動更新。</div>"
                "</div>",
                unsafe_allow_html=True
            )

        # ── Unemployment Rate (UNRATE) ──────────────────────
        if _has_value(unemp):
            v = unemp['value']
            uc = '#e74c3c' if v > 5 else ('#e67e22' if v > 4 else '#2ecc71')
            chg = unemp.get('change')
            chg_str = f"MoM: {'+' if chg >= 0 else ''}{chg:.1f}pp" if chg is not None else None
            _fred_row('失業率 UNRATE', '📊', f"{v:.1f}%", f"({unemp['date'][:7]})",
                      color=uc, change_str=chg_str)
        elif _is_error(unemp):
            err_msg = unemp.get('error', '未知錯誤')[:50]
            st.markdown(
                f"<div style='font-size:0.71rem;color:#888'>📊 失業率 UNRATE: "
                f"<span style='color:#e74c3c'>❌ {err_msg}</span></div>",
                unsafe_allow_html=True)
        else:
            st.markdown("<div style='font-size:0.72rem;color:#666'>📊 失業率 UNRATE: <span style='color:#888'>N/A</span></div>",
                        unsafe_allow_html=True)

        # ── Nonfarm Payrolls (PAYEMS) ───────────────────────
        if _has_value(payems):
            chg = payems.get('change', 0)
            _fred_row(
                '非農就業 PAYEMS', '🏭',
                f"{payems['value'] / 1000:.1f}M",
                f"({payems['date'][:7]})",
                color='#4FC3F7',
                change_str=f"MoM: {'+' if chg > 0 else ''}{chg:,}",
            )
        elif _is_error(payems):
            err_msg = payems.get('error', '未知錯誤')[:50]
            st.markdown(
                f"<div style='font-size:0.71rem;color:#888'>🏭 非農就業 PAYEMS: "
                f"<span style='color:#e74c3c'>❌ {err_msg}</span></div>",
                unsafe_allow_html=True)
        else:
            st.markdown("<div style='font-size:0.72rem;color:#666'>🏭 非農就業 PAYEMS: <span style='color:#888'>N/A</span></div>",
                        unsafe_allow_html=True)

        # ── Initial Jobless Claims (ICSA) ───────────────────
        if _has_value(jl):
            jv = jl['value']
            jc = '#e74c3c' if jv > 250000 else ('#e67e22' if jv > 220000 else '#2ecc71')
            prior = jl.get('prior', jl.get('prev', jv))
            diff = jv - prior
            diff_str = f"WoW: {'+' if diff >= 0 else ''}{diff:,}"
            _fred_row(
                '初領失業金 ICSA', '📋',
                f"{jv:,}",
                f"({jl['date'][:10]})",
                color=jc,
                change_str=diff_str,
            )
        elif _is_error(jl):
            err_msg = jl.get('error', '未知錯誤')[:50]
            st.markdown(
                f"<div style='font-size:0.71rem;color:#888'>📋 初領失業金 ICSA: "
                f"<span style='color:#e74c3c'>❌ {err_msg}</span></div>",
                unsafe_allow_html=True)
        else:
            st.markdown("<div style='font-size:0.72rem;color:#666'>📋 初領失業金 ICSA: <span style='color:#888'>N/A</span></div>",
                        unsafe_allow_html=True)

        # ── Labor Force Participation (CIVPART) ─────────────
        if _has_value(civpart):
            cv = civpart['value']
            prior_c = civpart.get('prior', civpart.get('prev', cv))
            diff = cv - prior_c
            diff_str = f"MoM: {'+' if diff >= 0 else ''}{diff:.1f}pp"
            cc = '#2ecc71' if cv >= 63 else ('#e67e22' if cv >= 61 else '#e74c3c')
            _fred_row(
                '勞動參與率 CIVPART', '👥',
                f"{cv:.1f}%",
                f"({civpart['date'][:7]})",
                color=cc,
                change_str=diff_str,
            )
        elif _is_error(civpart):
            err_msg = civpart.get('error', '未知錯誤')[:50]
            st.markdown(
                f"<div style='font-size:0.71rem;color:#888'>👥 勞動參與率 CIVPART: "
                f"<span style='color:#e74c3c'>❌ {err_msg}</span></div>",
                unsafe_allow_html=True)
        else:
            st.markdown("<div style='font-size:0.72rem;color:#666'>👥 勞動參與率 CIVPART: <span style='color:#888'>N/A</span></div>",
                        unsafe_allow_html=True)

        # ── Footer ──────────────────────────────────────────
        if any_ok:
            st.caption('📌 數據來源: FRED (St. Louis Fed) | 每5分鐘自動更新')
        else:
            st.caption('📌 FRED 連線暫時受阻 | 本 app 每5分鐘自動重試')


def render_sidebar_macro_expander(m):
    """重要宏觀/市場數據 expander – CPI, Core CPI, Fed Rate, yields, VIX, DXY, Oil, SPY/QQQ"""
    with st.expander("🏦 重要宏觀/市場數據", expanded=False):

        # ── 通脹 ──────────────────────────────
        st.markdown("<div style='font-size:0.7rem;font-weight:700;color:#888;margin:4px 0 2px'>📦 通脹 Inflation</div>", unsafe_allow_html=True)

        cpi = m.get('CPI_YOY')
        if cpi:
            cc = '#e74c3c' if cpi['value'] > 3.5 else ('#e67e22' if cpi['value'] > 2.5 else '#2ecc71')
            _fred_row('CPI (YoY)', '📦', f"{cpi['value']:.1f}%", f"({cpi['date'][:7]})", color=cc)

        core_cpi = m.get('CORE_CPI')
        if core_cpi:
            ccc = '#e74c3c' if core_cpi['value'] > 3.5 else ('#e67e22' if core_cpi['value'] > 2.5 else '#2ecc71')
            _fred_row('Core CPI (YoY)', '🎯', f"{core_cpi['value']:.1f}%", f"({core_cpi['date'][:7]})", color=ccc)

        # ── 利率 / 債息 ────────────────────────
        st.markdown("<div style='font-size:0.7rem;font-weight:700;color:#888;margin:6px 0 2px'>🏛️ 利率 & 債息</div>", unsafe_allow_html=True)

        ff = m.get('FEDFUNDS')
        if ff:
            _fred_row('聯儲息率 Fed Funds', '🏛️', f"{ff['value']:.2f}%", f"({ff['date'][:7]})", color='#f1c40f')

        tnx = m.get('TNX', {})
        if tnx and tnx.get('price') is not None:
            p10 = tnx['price']
            pct10 = tnx['pct']
            arrow10 = '▲' if pct10 >= 0 else '▼'
            c10 = '#e74c3c' if pct10 >= 0 else '#2ecc71'  # rising yields = tighter
            _fred_row('10Y 美債息', '📉', f"{p10:.3f}%", f"{arrow10}{abs(pct10):.1f}%", color=c10)

        # 2Y / short end (\ IRX = 13-week T-bill annualised)
        twy = m.get('TWO_Y', {})
        if twy and twy.get('price') is not None:
            p2 = twy['price']
            pct2 = twy.get('pct', 0)
            arrow2 = '▲' if pct2 >= 0 else '▼'
            c2 = '#e74c3c' if pct2 >= 0 else '#2ecc71'
            _fred_row('短端利率 (13W T-Bill)', '🔖', f"{p2:.2f}%", f"{arrow2}{abs(pct2):.1f}%", color=c2)

        # Yield curve spread 10Y - 2Y
        yr = m.get('YIELD_SPREAD_RAW')
        if yr:
            # TNX from yfinance: value is the yield in %, e.g. 4.3 means 4.30%
            # IRX from yfinance: value is the yield in %, e.g. 5.12 means 5.12%
            spread = yr['tnx'] - yr['irx']
            sc = '#2ecc71' if spread > 0 else '#e74c3c'
            sign = '+' if spread >= 0 else ''
            _fred_row('殖利率曲線 10Y-短端', '📐', f"{sign}{spread:.2f}%", '(實時)', color=sc)

        # ── 市場恐慌 / 美元 / 商品 ────────────
        st.markdown("<div style='font-size:0.7rem;font-weight:700;color:#888;margin:6px 0 2px'>📊 市場情緒 & 大類資產</div>", unsafe_allow_html=True)

        vix = m.get('VIX', {})
        if vix and vix.get('price') is not None:
            v = vix['price']
            pctv = vix['pct']
            vl = '極度恐慌' if v >= 30 else ('市場緊張' if v >= 20 else '平靜')
            vc = '#e74c3c' if v >= 30 else ('#e67e22' if v >= 20 else '#2ecc71')
            _fred_row(f'VIX ({vl})', '😱', f"{v:.2f}", f"({'▲' if pctv >= 0 else '▼'}{abs(pctv):.1f}%)", color=vc)

        dxy = m.get('DXY', {})
        if dxy and dxy.get('price') is not None:
            dp = dxy['price']; dpct = dxy['pct']
            dc = '#e67e22' if dpct > 0 else '#2ecc71'  # strong dollar = headwind for risk
            _fred_row('美元指數 DXY', '💵', f"{dp:.2f}", f"({'▲' if dpct >= 0 else '▼'}{abs(dpct):.1f}%)", color=dc)

        oil = m.get('OIL', {})
        if oil and oil.get('price') is not None:
            op = oil['price']; opct = oil['pct']
            oc = '#e74c3c' if opct > 2 else ('#2ecc71' if opct < -2 else '#f1c40f')
            _fred_row('WTI 原油 (USD)', '🛢️', f"${op:.2f}", f"({'▲' if opct >= 0 else '▼'}{abs(opct):.1f}%)", color=oc)

        # SPY & QQQ performance
        spy = m.get('SPY', {})
        if spy and spy.get('price') is not None:
            sp = spy['price']; spct = spy['pct']
            sc2 = '#2ecc71' if spct >= 0 else '#e74c3c'
            _fred_row('S&P500 ETF (SPY)', '📈', f"${sp:.2f}", f"({'▲' if spct >= 0 else '▼'}{abs(spct):.1f}%)", color=sc2)

        qqq = m.get('QQQ', {})
        if qqq and qqq.get('price') is not None:
            qp = qqq['price']; qpct = qqq['pct']
            qc = '#2ecc71' if qpct >= 0 else '#e74c3c'
            _fred_row('納指 ETF (QQQ)', '💻', f"${qp:.2f}", f"({'▲' if qpct >= 0 else '▼'}{abs(qpct):.1f}%)", color=qc)

        st.caption('📌 宏觀數據: FRED | 市場數據: yfinance')



# ==========================================
# 📡 市場實時雷達 — 獨立頁面 (Fast, no blocking)
# ==========================================
def render_market_real_time_radar_page():
    """
    獨立頁面：市場實時雷達。
    快速顯示 SPY/QQQ/VIX/Fear&Greed + 商品/債市/加密 + 亞洲市場。
    使用快取數據，網絡失敗時顯示 N/A card，不阻塞頁面。
    """
    st.title('📡 市場實時雷達')
    st.caption(
        '快速總覽美股大市、宏觀指標、商品、加密、亞洲市場。'
        '數據每5分鐘自動快取，失敗時顯示 N/A 卡片，唔會阻塞頁面。'
    )

    rc1, rc2 = st.columns([5, 1])
    with rc2:
        if st.button('🔄 刷新', use_container_width=True, key='mrr_refresh'):
            try:
                fetch_sidebar_market_data.clear()
            except Exception:
                pass
            st.rerun()

    # Load data (cached, with graceful fallback)
    try:
        m = fetch_sidebar_market_data()
    except Exception:
        m = {}

    # ── Fear & Greed + VIX top strip ──────────────────────────────────
    st.markdown('#### 🌡️ 市場情緒指標')
    fg = m.get('FEAR_GREED', {}) or {}
    vix = m.get('VIX', {}) or {}
    spy = m.get('SPY', {}) or {}
    qqq = m.get('QQQ', {}) or {}

    def _mini_card(label, value_str, pct_str, bg, fg_color):
        return (
            f"<div style='background:{bg};border:1px solid {fg_color}33;border-radius:8px;"
            f"padding:10px 12px;text-align:center;flex:1;min-width:120px'>"
            f"<div style='font-size:0.68rem;color:{fg_color}aa'>{label}</div>"
            f"<div style='font-size:1.1rem;font-weight:bold;color:{fg_color}'>{value_str}</div>"
            f"<div style='font-size:0.65rem;color:{fg_color}88'>{pct_str}</div>"
            f"</div>"
        )

    def _pct_arrow(pct):
        if pct is None: return 'N/A'
        arrow = '▲' if pct >= 0 else '▼'
        c = '#00C851' if pct >= 0 else '#e74c3c'
        return f"<span style='color:{c}'>{arrow}{abs(pct):.2f}%</span>"

    cards = []
    # Fear & Greed
    fgscore = fg.get('score')
    if fgscore is not None:
        if fgscore >= 75: fbg, ffg, flabel = '#3A0A0A', '#e74c3c', '極度貪婪'
        elif fgscore >= 55: fbg, ffg, flabel = '#2A1A0A', '#e67e22', '貪婪'
        elif fgscore >= 45: fbg, ffg, flabel = '#1A1A0A', '#f1c40f', '中立'
        elif fgscore >= 25: fbg, ffg, flabel = '#0A1A2A', '#3498db', '恐懼'
        else: fbg, ffg, flabel = '#0A1020', '#2980b9', '極度恐懼'
        cards.append(_mini_card('CNN 恐貪指數', f'{fgscore:.0f}', flabel, fbg, ffg))
    else:
        cards.append(_mini_card('CNN 恐貪指數', 'N/A', '無法取得', '#111', '#555'))

    # VIX
    vp = vix.get('price')
    if vp is not None:
        if vp >= 30: vbg, vfg, vl = '#3A0A0A', '#e74c3c', '恐慌'
        elif vp >= 20: vbg, vfg, vl = '#2A1000', '#e67e22', '緊張'
        else: vbg, vfg, vl = '#0A1A0A', '#2ecc71', '平靜'
        cards.append(_mini_card('VIX 波動率指數', f'{vp:.1f}', vl, vbg, vfg))
    else:
        cards.append(_mini_card('VIX', 'N/A', '', '#111', '#555'))

    # SPY
    sp = spy.get('price')
    if sp is not None:
        spct = spy.get('pct', 0)
        scol = '#00C851' if spct >= 0 else '#e74c3c'
        cards.append(_mini_card('SPY (S&P500)', f'${sp:.2f}', f"{'▲' if spct >= 0 else '▼'}{abs(spct):.2f}%", '#0A140A' if spct >= 0 else '#1A0A0A', scol))
    else:
        cards.append(_mini_card('SPY', 'N/A', '', '#111', '#555'))

    # QQQ
    qp = qqq.get('price')
    if qp is not None:
        qpct = qqq.get('pct', 0)
        qcol = '#00C851' if qpct >= 0 else '#e74c3c'
        cards.append(_mini_card('QQQ (納指ETF)', f'${qp:.2f}', f"{'▲' if qpct >= 0 else '▼'}{abs(qpct):.2f}%", '#0A140A' if qpct >= 0 else '#1A0A0A', qcol))
    else:
        cards.append(_mini_card('QQQ', 'N/A', '', '#111', '#555'))

    st.markdown(
        "<div style='display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px'>" +
        ''.join(cards) + "</div>",
        unsafe_allow_html=True
    )

    # ── Major Indices table ────────────────────────────────────────────
    def _index_row(label, key, fmt='${:.2f}'):
        d = m.get(key, {}) or {}
        price = d.get('price')
        pct = d.get('pct')
        if price is None:
            return f"<tr><td>{label}</td><td style='color:#555'>N/A</td><td style='color:#555'>—</td></tr>"
        try: pstr = fmt.format(price)
        except: pstr = str(price)
        col = '#00C851' if (pct or 0) >= 0 else '#e74c3c'
        arrow = '▲' if (pct or 0) >= 0 else '▼'
        pct_str = f"{arrow}{abs(pct):.2f}%" if pct is not None else '—'
        return f"<tr><td>{label}</td><td>{pstr}</td><td style='color:{col}'>{pct_str}</td></tr>"

    tbl_style = """<style>
.mkt-tbl{width:100%;border-collapse:collapse;font-size:0.82rem;font-family:Inter,sans-serif}
.mkt-tbl th{background:#141824;color:#8a9bb0;padding:6px 10px;text-align:left;border-bottom:2px solid #252d3d}
.mkt-tbl td{padding:6px 10px;border-bottom:1px solid #1a1e28}
.mkt-tbl tr:hover{background:#1c2535}
</style>"""

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown('#### 🇺🇸 美股指數 & 宏觀')
        rows = (
            _index_row('S&P500 (SPY)', 'SPY') +
            _index_row('納指 (QQQ)', 'QQQ') +
            _index_row('道指 (DIA)', 'DIA') +
            _index_row('美元指數 (DXY)', 'DXY', '{:.2f}') +
            _index_row('10年美債息 (%)', 'TNX', '{:.3f}') +
            _index_row('30年美債息 (%)', 'TYX', '{:.3f}')
        )
        st.markdown(tbl_style + f"<table class='mkt-tbl'><thead><tr><th>指標</th><th>現價</th><th>日變動</th></tr></thead><tbody>{rows}</tbody></table>", unsafe_allow_html=True)

        # Fed Funds
        ff = m.get('FEDFUNDS')
        if ff:
            st.caption(f"🏛️ 聯儲息率: **{ff.get('value', 'N/A'):.2f}%** ({ff.get('date', '')[:7]})")

    with col_r:
        st.markdown('#### 🛢️ 商品 & 加密 & 亞洲')
        rows2 = (
            _index_row('WTI 原油 (USD)', 'OIL', '${:.2f}') +
            _index_row('黃金 (USD/oz)', 'GOLD', '${:.2f}') +
            _index_row('Bitcoin (USD)', 'BTC', '${:,.0f}') +
            _index_row('Ethereum (USD)', 'ETH', '${:,.0f}') +
            _index_row('日經 225', 'NIKKEI', '{:,.0f}') +
            _index_row('恒生指數', 'HSI', '{:,.0f}') +
            _index_row('上證指數', 'SHCOMP', '{:,.0f}')
        )
        st.markdown(tbl_style + f"<table class='mkt-tbl'><thead><tr><th>指標</th><th>現價</th><th>日變動</th></tr></thead><tbody>{rows2}</tbody></table>", unsafe_allow_html=True)

    ts = datetime.datetime.now().strftime('%H:%M:%S')
    st.caption(f"⏱ 頁面渲染時間: {ts} | 數據快取: 5 分鐘 | 來源: yfinance + CNN F&G")

    # ── System Architecture Guide ──────────────────────────────────────
    st.markdown('---')
    with st.expander('📖 系統架構說明 — Fast Mode / Scanner / 數據庫建議', expanded=False):
        st.markdown(_architecture_guide())


def _architecture_guide() -> str:
    """Return architecture guidance markdown text."""
    return """
### ⚡ Fast Mode 架構說明

#### 當前推薦架構（免費方案）
```
scanner.py（每日盤後跑）
    ↓ yfinance 批量下載（100-250 隻/批）
    ↓ 計算 RS/MA/Volume/Setup 訊號
    ↓ 儲存 latest_scan.parquet + latest_scan.csv
        ↓
test-2.py（Streamlit 前端）
    ↓ 讀取 Parquet（毫秒級）
    ↓ 篩選 / 排序 / 顯示
    ↓ 按需新聞情緒（單隻股票）
```

#### MongoDB 唔係必須
- **MongoDB 唔係必須**，當前用法（Parquet/CSV）係最簡單嘅免費方案。
- Parquet 係列式儲存，讀取速度遠快於 SQLite/CSV，適合 Pandas 篩選。
- 如果股票池 < 2,000 隻，Parquet + Pandas 已經夠快。

#### DuckDB vs MongoDB（進階選項）
| 方案 | 優點 | 適用場景 |
|------|------|----------|
| **Parquet + Pandas** | 最簡單，無依賴 | < 2,000 隻，每日掃描 |
| **DuckDB + Parquet** | SQL 查詢，極快篩選 | 2,000–50,000 隻 |
| **MongoDB** | 彈性 Schema | 需要即時更新、多用戶 |

#### 如何處理 8,000+ 隻股票
1. **夜間排程掃描**（推薦）：`cron` 或 GitHub Actions 每晚盤後跑 `scanner.py`
2. **分批下載**：yfinance 每批 200 隻，共需約 10–20 分鐘
3. **付費 API**（可選）：Polygon.io / Tiingo / EOD Historical Data 支援大批量快速下載
4. **DuckDB**：將 Parquet 用 DuckDB 查詢，毫秒級篩選 8,000 隻

#### 如何運行 Scanner
```bash
# 最小測試（3隻）
python scanner.py --tickers "AAPL,MSFT,SPY" --max-tickers 3

# 預設精選池（約80隻）
python scanner.py

# 自定義 Universe
python scanner.py --universe-file my_tickers.csv --benchmark SPY

# 大型掃描
python scanner.py --universe-file sp500.csv --max-tickers 500
```

#### Streamlit 保持快速的關鍵
- **不要**在 Streamlit render 時自動跑全市場 yfinance 下載
- **要**用 Parquet 快取 → Streamlit 只負責讀取 + 篩選 + 顯示
- **按需**：新聞情緒、AI 分析只在用戶主動點擊時才觸發
"""



def render_sidebar_market_panel():
    # ── Header row with refresh ──────────────────────────────────────
    h_col1, h_col2 = st.columns([3, 1])
    with h_col1:
        st.markdown("### 📡 市場實時雷達")
    with h_col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄", use_container_width=True, key="refresh_mkt_sb", help="刷新所有市場數據"):
            fetch_sidebar_market_data.clear()
            st.rerun()

    # Fast: render N/A cards immediately, data loads via cached function
    try:
        m = fetch_sidebar_market_data()
    except Exception:
        m = {}

    # ── 更新時間戳 ──
    st.markdown(
        f"<div style='font-size:0.62rem;color:#555;margin-bottom:4px'>⏱ 更新: "
        f"{datetime.datetime.now().strftime('%H:%M:%S')} &nbsp;｜&nbsp; 快取5分鐘</div>",
        unsafe_allow_html=True)

    # ── Fear & Greed + VIX 並列 ──
    fg = m.get('FEAR_GREED', {}) or {}
    vix = m.get('VIX', {}) or {}

    fc1, fc2 = st.columns(2)
    with fc1:
        if fg and fg.get('score') is not None:
            score = fg['score']
            rating = fg.get('rating', '').upper()
            if score >= 75:   fg_emoji, fg_c = '🤑', '#e74c3c'
            elif score >= 55: fg_emoji, fg_c = '😊', '#e67e22'
            elif score >= 45: fg_emoji, fg_c = '😐', '#f1c40f'
            elif score >= 25: fg_emoji, fg_c = '😨', '#3498db'
            else:             fg_emoji, fg_c = '😱', '#2980b9'
            st.markdown(
                f"<div style='background:{fg_c}22;border:1px solid {fg_c}44;border-radius:6px;"
                f"padding:5px 7px;text-align:center'>"
                f"<div style='font-size:0.64rem;color:{fg_c}'>CNN 恐貪</div>"
                f"<div style='font-size:1.05rem;font-weight:bold'>{fg_emoji} {score:.0f}</div>"
                f"<div style='font-size:0.60rem;color:{fg_c}'>{rating}</div>"
                f"</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div style='font-size:0.70rem;color:#555;text-align:center'>😐 F&G N/A</div>",
                        unsafe_allow_html=True)
    with fc2:
        if vix and vix.get('price') is not None:
            v, pct = vix['price'], vix['pct']
            if v >= 30:   vl, vc = '恐慌', '#e74c3c'
            elif v >= 20: vl, vc = '緊張', '#e67e22'
            else:         vl, vc = '平靜', '#2ecc71'
            arrow = '▲' if pct >= 0 else '▼'
            st.markdown(
                f"<div style='background:{vc}22;border:1px solid {vc}44;border-radius:6px;"
                f"padding:5px 7px;text-align:center'>"
                f"<div style='font-size:0.64rem;color:{vc}'>VIX</div>"
                f"<div style='font-size:1.05rem;font-weight:bold;color:{vc}'>{v:.1f}</div>"
                f"<div style='font-size:0.60rem;color:{vc}'>{vl} {arrow}{abs(pct):.1f}%</div>"
                f"</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div style='font-size:0.70rem;color:#555;text-align:center'>VIX N/A</div>",
                        unsafe_allow_html=True)

    _divider()

    # ── 美股三大指數 ──
    _section("🇺🇸 美股指數")
    _pm("S&P500 (SPY)", "📈", m.get('SPY'))
    _pm("納指 (QQQ)", "💻", m.get('QQQ'))
    _pm("道指 (DIA)", "🏛️", m.get('DIA'))
    _divider()

    # ── 宏觀 & 債市 ──
    _section("🏦 宏觀 & 債市")
    _pm("美元指數 (DXY)", "💵", m.get('DXY'))
    _pm("10年美債息 (%)", "📉", m.get('TNX'), fmt='{:.3f}')
    _pm("30年美債息 (%)", "📉", m.get('TYX'), fmt='{:.3f}')
    ff = m.get('FEDFUNDS')
    if ff:
        _fred_row('聯儲息率 Fed Funds', '🏛️', f"{ff['value']:.2f}%",
                  f"({ff['date'][:7]})", color='#f1c40f')
    _divider()

    # ── 商品 ──
    _section("🛢️ 商品")
    _pm("WTI 原油 (USD)", "🛢️", m.get('OIL'), fmt='${:.2f}')
    _pm("黃金 (USD/oz)", "🥇", m.get('GOLD'), fmt='${:.2f}')
    _pm("天然氣 (USD)", "🔥", m.get('NG'), fmt='${:.3f}')
    _divider()

    # ── 加密貨幣 ──
    _section("₿ 加密貨幣")
    _pm("Bitcoin (USD)", "₿", m.get('BTC'), fmt='${:,.0f}')
    _pm("Ethereum (USD)", "⟠", m.get('ETH'), fmt='${:,.0f}')
    _divider()

    # ── 亞洲市場 ──
    _section("🌏 亞洲市場")
    _pm("日經 225", "🗾", m.get('NIKKEI'), fmt='{:,.0f}')
    _pm("恒生指數", "🇭🇰", m.get('HSI'), fmt='{:,.0f}')
    _pm("上證指數", "🇨🇳", m.get('SHCOMP'), fmt='{:,.0f}')



# ==========================================
# 🔥 當炒 / 低風險機會雷達  (新增模組)
# ==========================================

# ── Setup type labels ──────────────────────────────────────────────
SETUP_LABELS = {
    'HOT_MOMENTUM':   '🔥 強勢延續',
    'NEAR_BREAKOUT':  '🚀 接近爆發',
    'LOW_RISK':       '🛡️ 低風險回踩',
    'EARLY_TURN':     '👀 早期轉強',
    'OVEREXTENDED':   '⚠️ 過度延伸/風險高',
    'LAGGING':        '📉 跑輸觀望',
}

SETUP_COLORS = {
    '🔥 強勢延續':      ('#0D2010', '#00C851'),
    '🚀 接近爆發':      ('#1A0E00', '#FF8C00'),
    '🛡️ 低風險回踩':    ('#0A1A2A', '#4FC3F7'),
    '👀 早期轉強':      ('#1A1A00', '#F9A825'),
    '⚠️ 過度延伸/風險高': ('#2A1A00', '#FF6D00'),
    '📉 跑輸觀望':      ('#1A1A1A', '#888888'),
}


def classify_setup_type(rs_cat, rel_5d, rel_1m, rel_3m,
                        dist_ma20_pct, dist_20d_high_pct,
                        sent_label, volatility_pct,
                        vol_signal=None):
    """
    Classify a stock into one of 5 setup types using purely categorical /
    threshold logic.  No numeric RS rating is used.

    Parameters
    ----------
    rs_cat          : str  – one of '🟢 跑贏指數','🟠 接近突破','🟡 剛轉強','🔴 跑輸指數'
    rel_5d          : float|None – 5D relative return vs benchmark (pct)
    rel_1m          : float|None – 1M relative return vs benchmark (pct)
    rel_3m          : float|None – 3M relative return vs benchmark (pct)
    dist_ma20_pct   : float|None – (price/MA20 - 1)*100; positive = above MA20
    dist_20d_high_pct: float|None – (price/20D_high - 1)*100; ≤0 = below high
    sent_label      : str  – '🟢 正面','⚪ 中性','🔴 負面'
    volatility_pct  : float|None – 20D annualised vol proxy (std of daily returns * sqrt(252))
    vol_signal      : str|None  – '放量突破' | '縮量回踩' | '量價背馳' | '正常量能' | None

    Returns
    -------
    str – one of the SETUP_LABELS values
    """
    # Fallback when data is completely missing
    if rs_cat is None:
        return SETUP_LABELS['LAGGING']

    positive_sent = sent_label == '🟢 正面'
    neutral_or_pos = sent_label in ('🟢 正面', '⚪ 中性')
    negative_sent  = sent_label == '🔴 負面'

    # ── ⚠️ 過度延伸/風險高 ─────────────────────────────────────────
    # Price more than 12% above MA20, or RS is outperformer but vol is very high
    # 量價背馳 also raises caution when price is extended
    overextended = (
        dist_ma20_pct is not None and dist_ma20_pct > 12
    ) or (
        rs_cat == '🟢 跑贏指數'
        and volatility_pct is not None and volatility_pct > 80
    ) or (
        vol_signal == '量價背馳'
        and rs_cat in ('🟢 跑贏指數', '🟠 接近突破')
        and (dist_ma20_pct is not None and dist_ma20_pct > 8)
    )
    if overextended:
        return SETUP_LABELS['OVEREXTENDED']

    # ── Pre-compute low-risk pullback conditions ──────────────────
    # Pulled back toward MA20 (-8% to +4%) AND below recent 20D high (< -3%)
    low_risk_rs   = rs_cat in ('🟠 接近突破', '🟡 剛轉強')
    near_ma20     = (dist_ma20_pct is not None and -8 <= dist_ma20_pct <= 4)
    pullback_flag = (dist_20d_high_pct is not None and dist_20d_high_pct < -3)

    # ── 🛡️ 低風險回踩 (evaluated BEFORE 接近爆發 for priority) ─────
    # Enhanced: 縮量回踩 volume signal = strongest low-risk signal
    # Strict: RS near-breakout/just-turned + pulled back to MA20 + not deeply neg sentiment
    if low_risk_rs and near_ma20 and pullback_flag and neutral_or_pos:
        return SETUP_LABELS['LOW_RISK']  # 縮量回踩 makes this ideal low-risk entry
    # Broad: near-breakout RS + positive sentiment + price not over-extended (≤6% above MA20)
    if rs_cat == '🟠 接近突破' and positive_sent:
        if dist_ma20_pct is None or dist_ma20_pct <= 6:
            return SETUP_LABELS['LOW_RISK']

    # ── 🔥 強勢延續 ────────────────────────────────────────────────
    # Confirmed outperformer + positive sentiment + not overextended
    # 放量突破 further confirms hot momentum
    if (rs_cat == '🟢 跑贏指數'
            and positive_sent
            and (rel_5d is None or rel_5d >= 0)):
        return SETUP_LABELS['HOT_MOMENTUM']

    # ── 🚀 接近爆發 ────────────────────────────────────────────────
    # Near-breakout or confirmed RS + positive/neutral sentiment + not a pullback setup
    # 放量突破 + 剛轉強 => upgrade to near_breakout
    if rs_cat in ('🟠 接近突破', '🟢 跑贏指數'):
        if neutral_or_pos and (rel_5d is None or rel_5d >= -1):
            return SETUP_LABELS['NEAR_BREAKOUT']
    if vol_signal == '放量突破' and rs_cat == '🟡 剛轉強' and neutral_or_pos:
        return SETUP_LABELS['NEAR_BREAKOUT']

    # ── 👀 早期轉強 ────────────────────────────────────────────────
    # 剛轉強 RS, not deeply negative sentiment
    if rs_cat == '🟡 剛轉強' and neutral_or_pos:
        return SETUP_LABELS['EARLY_TURN']

    # ── 📉 跑輸觀望 ────────────────────────────────────────────────
    return SETUP_LABELS['LAGGING']


# ==========================================
# 成交量訊號輔助函數
# ==========================================
def compute_volume_signal(latest_vol, avg20_vol, dist_ma20_pct, dist_20d_high_pct, rel_5d):
    """
    Compute categorical volume signal.

    Returns one of:
      '放量突破'   – volume breakout: price near/at 20D high, vol ratio >= 1.5
      '縮量回踩'   – low-vol pullback: price pulled back toward MA20, vol ratio <= 0.85
      '量價背馳'   – price-volume divergence: price rising but vol drying, or price falling + vol surging
      '正常量能'   – normal / N/A
    """
    if latest_vol is None or avg20_vol is None or avg20_vol == 0:
        return '正常量能'

    vol_ratio = latest_vol / avg20_vol

    # 放量突破: near/at 20D high (within -3%) AND vol ratio >= 1.5
    near_high = (dist_20d_high_pct is not None and dist_20d_high_pct >= -3)
    if near_high and vol_ratio >= 1.5:
        return '放量突破'

    # 縮量回踩: pulled back toward MA20 (dist_ma20 between -8% and +4%), vol ratio <= 0.85
    near_ma20 = (dist_ma20_pct is not None and -8 <= dist_ma20_pct <= 4)
    below_high = (dist_20d_high_pct is not None and dist_20d_high_pct < -3)
    if near_ma20 and below_high and vol_ratio <= 0.85:
        return '縮量回踩'

    # 量價背馳: price up (5D rel > +2%) but vol ratio < 0.7 → vol drying while price rises
    if rel_5d is not None and rel_5d > 2 and vol_ratio < 0.7:
        return '量價背馳'
    # 量價背馳: price down (5D rel < -2%) but vol surging (ratio > 1.8) → distribution warning
    if rel_5d is not None and rel_5d < -2 and vol_ratio > 1.8:
        return '量價背馳'

    return '正常量能'


def _vol_signal_badge(signal):
    """Return coloured HTML badge for volume signal."""
    colors = {
        '放量突破': ('#003820', '#00C851'),
        '縮量回踩': ('#0A1A2A', '#4FC3F7'),
        '量價背馳': ('#3A1A00', '#FF8C00'),
        '正常量能': ('#1A1A1A', '#888888'),
    }
    bg, fg = colors.get(signal, ('#1A1A1A', '#888888'))
    return (
        f"<span style='background:{bg};color:{fg};padding:1px 7px;"
        f"border-radius:8px;font-size:0.70rem;font-weight:bold'>{signal}</span>"
    )


@st.cache_data(ttl=600, show_spinner=False)
def fetch_ma_data(tickers_tuple):
    """
    For each ticker, compute:
      - MA20, MA50 (20/50-day simple moving averages of Close)
      - dist_ma20_pct = (price/MA20 - 1)*100
      - dist_ma50_pct = (price/MA50 - 1)*100
      - dist_20d_high_pct = (price/rolling_20d_high - 1)*100   (≤0)
      - volatility_pct   = annualised 20-day std of daily returns (%)
      - latest_volume, avg20_volume, vol_ratio (volume metrics from OHLCV)
      - vol_signal: '放量突破' | '縮量回踩' | '量價背馳' | '正常量能'

    Returns dict: {ticker: {price, MA20, MA50, dist_ma20_pct, dist_ma50_pct,
                             dist_20d_high_pct, volatility_pct,
                             latest_volume, avg20_volume, vol_ratio, vol_signal}}
    """
    tickers = list(tickers_tuple)
    result = {}
    if not tickers:
        return result
    try:
        raw = yf.download(
            tickers, period='3mo', interval='1d',
            progress=False, auto_adjust=True, group_by='column', threads=True
        )
        if raw.empty:
            return result
        if isinstance(raw.columns, pd.MultiIndex):
            closes = raw['Close']
            volumes = raw['Volume'] if 'Volume' in raw.columns.get_level_values(0) else None
        else:
            closes = raw[['Close']] if 'Close' in raw.columns else raw
            volumes = raw[['Volume']] if 'Volume' in raw.columns else None

        for ticker in tickers:
            try:
                col = (closes[ticker] if ticker in closes.columns
                       else pd.Series(dtype=float)).dropna()
                if len(col) < 21:
                    result[ticker] = {}
                    continue
                price  = float(col.iloc[-1])
                ma20   = float(col.rolling(20).mean().iloc[-1])
                ma50v  = col.rolling(50).mean().iloc[-1]
                ma50   = float(ma50v) if not pd.isna(ma50v) else None
                high20 = float(col.rolling(20).max().iloc[-1])

                dist_ma20 = round((price / ma20 - 1) * 100, 2) if ma20 else None
                dist_ma50 = round((price / ma50 - 1) * 100, 2) if ma50 else None
                dist_h20  = round((price / high20 - 1) * 100, 2) if high20 else None

                # 20-day annualised volatility
                daily_rets = col.pct_change().dropna().tail(20)
                vol = round(float(daily_rets.std() * (252 ** 0.5) * 100), 1) if len(daily_rets) >= 10 else None

                # ── Volume metrics ───────────────────────────────────
                latest_volume, avg20_volume, vol_ratio = None, None, None
                try:
                    if volumes is not None:
                        vcol = (volumes[ticker] if ticker in volumes.columns
                                else pd.Series(dtype=float)).dropna()
                        if len(vcol) >= 2:
                            latest_volume = int(vcol.iloc[-1])
                            avg20_volume  = int(vcol.tail(20).mean())
                            vol_ratio     = round(latest_volume / avg20_volume, 2) if avg20_volume > 0 else None
                except Exception:
                    pass

                # 5D price return (absolute, not relative) for vol signal
                _5d_ret = None
                try:
                    if len(col) >= 6:
                        _5d_ret = round((float(col.iloc[-1]) / float(col.iloc[-6]) - 1) * 100, 2)
                except Exception:
                    pass

                vol_signal = compute_volume_signal(
                    latest_volume, avg20_volume, dist_ma20, dist_h20, _5d_ret
                )

                result[ticker] = {
                    'price':           price,
                    'MA20':            round(ma20, 2),
                    'MA50':            round(ma50, 2) if ma50 else None,
                    'dist_ma20_pct':   dist_ma20,
                    'dist_ma50_pct':   dist_ma50,
                    'dist_20d_high_pct': dist_h20,
                    'volatility_pct':  vol,
                    'latest_volume':   latest_volume,
                    'avg20_volume':    avg20_volume,
                    'vol_ratio':       vol_ratio,
                    'vol_signal':      vol_signal,
                }
            except Exception:
                result[ticker] = {}
    except Exception:
        pass
    return result


def _setup_badge(setup_label):
    bg, fg = SETUP_COLORS.get(setup_label, ('#1A1A2E', '#aaa'))
    return (
        f"<span style='background:{bg};color:{fg};padding:2px 9px;"
        f"border-radius:10px;font-size:0.72rem;font-weight:bold'>{setup_label}</span>"
    )


def _ma_dist_cell(val):
    """Coloured cell for MA/high distance percentage."""
    if val is None:
        return "<span style='color:#555'>N/A</span>"
    if val > 12:
        color, label = '#FF6D00', f'+{val:.1f}%'
    elif val >= 0:
        color, label = '#00C851', f'+{val:.1f}%'
    elif val >= -8:
        color, label = '#F9A825', f'{val:.1f}%'
    else:
        color, label = '#FF4444', f'{val:.1f}%'
    return f"<span style='color:{color}'>{label}</span>"


def _vol_flag(vol):
    """Return risk flag text for volatility."""
    if vol is None:
        return "<span style='color:#555'>N/A</span>"
    if vol > 80:
        return f"<span style='color:#FF4444'>🔴 高 ({vol:.0f}%)</span>"
    elif vol > 50:
        return f"<span style='color:#FF8C00'>🟠 中 ({vol:.0f}%)</span>"
    else:
        return f"<span style='color:#00C851'>🟢 低 ({vol:.0f}%)</span>"


def build_radar_rows(selected_themes, perf_data, sentiment_data, ma_data, benchmark='SPY'):
    """
    Build a unified list of rows for the radar watchlist.
    Each row contains all columns needed for display + setup classification.
    """
    seen  = set()
    rows  = []
    bench = perf_data.get(benchmark, {})

    def _rel(ticker, period):
        tv = perf_data.get(ticker, {}).get(period)
        bv = bench.get(period)
        if tv is None or bv is None:
            return None
        return round(tv - bv, 2)

    for theme in selected_themes:
        tdata = CATALYST_THEME_MAP.get(theme, {})
        for ticker, company in tdata.get('tickers', {}).items():
            if ticker in seen:
                continue
            seen.add(ticker)

            p    = perf_data.get(ticker, {})
            sent = sentiment_data.get(ticker, {})
            ma   = ma_data.get(ticker, {})

            rs_cat    = compute_rs_category(ticker, perf_data, benchmark)
            sent_lbl  = sent.get('label', '⚪ 中性')
            pos_kw    = sent.get('pos_kw', [])
            cat_tags  = tdata.get('catalyst_tags', [])
            all_tags  = list(dict.fromkeys(cat_tags[:2] + pos_kw[:1]))[:3]
            tag_str   = ' | '.join(all_tags) if all_tags else '—'

            rel5d = _rel(ticker, '5d')
            rel1m = _rel(ticker, '1m')
            rel3m = _rel(ticker, '3m')

            dist_ma20  = ma.get('dist_ma20_pct')
            dist_ma50  = ma.get('dist_ma50_pct')
            dist_h20   = ma.get('dist_20d_high_pct')
            vol        = ma.get('volatility_pct')
            vol_signal = ma.get('vol_signal', '正常量能')
            vol_ratio  = ma.get('vol_ratio')
            latest_vol = ma.get('latest_volume')
            avg20_vol  = ma.get('avg20_volume')

            setup = classify_setup_type(
                rs_cat, rel5d, rel1m, rel3m,
                dist_ma20, dist_h20,
                sent_lbl, vol,
                vol_signal=vol_signal
            )

            # Generate comment (include volume signal)
            comment = ''
            if setup == SETUP_LABELS['HOT_MOMENTUM'] and sent_lbl == '🟢 正面':
                comment = '⭐ 技術+情緒雙強'
                if vol_signal == '放量突破':
                    comment = '⭐ 放量突破+情緒雙強'
            elif setup == SETUP_LABELS['NEAR_BREAKOUT'] and sent_lbl == '🟢 正面':
                comment = '🔥 接近突破+正面消息'
            elif setup == SETUP_LABELS['LOW_RISK']:
                if vol_signal == '縮量回踩':
                    comment = '🛡️ 縮量回踩低風險'
                else:
                    comment = '🛡️ 回踩低風險位'
            elif setup == SETUP_LABELS['EARLY_TURN']:
                comment = '📈 早期轉強觀察'

            rows.append({
                'theme':      theme.split(' ', 1)[-1][:18] if ' ' in theme else theme[:18],
                'ticker':     ticker,
                'company':    company,
                'setup':      setup,
                'rs_cat':     rs_cat,
                'sent_label': sent_lbl,
                'tags':       tag_str,
                'price':      p.get('price'),
                'rel_5d':     rel5d,
                'rel_1m':     rel1m,
                'rel_3m':     rel3m,
                'dist_ma20':  dist_ma20,
                'dist_ma50':  dist_ma50,
                'dist_h20':   dist_h20,
                'vol':        vol,
                'vol_signal': vol_signal,
                'vol_ratio':  vol_ratio,
                'latest_vol': latest_vol,
                'avg20_vol':  avg20_vol,
                'comment':    comment,
            })
    return rows


def _radar_row_html(r, benchmark):
    """Build one HTML <tr> for the radar watchlist table."""
    price_str = f"${r['price']:.2f}" if r['price'] else 'N/A'
    tag_html  = (
        ''.join(
            f"<span style='background:#1A2A3A;color:#7EC8E3;padding:1px 5px;"
            f"border-radius:4px;font-size:0.68rem;margin:1px;display:inline-block'>{t}</span>"
            for t in r['tags'].split(' | ') if t and t != '—'
        ) or '—'
    )
    comment_html = (
        f"<span style='background:#004020;color:#00C851;padding:1px 6px;"
        f"border-radius:8px;font-size:0.67rem;font-weight:bold'>{r['comment']}</span>"
        if r['comment'] else ''
    )

    # Row background from setup colour
    bg, _ = SETUP_COLORS.get(r['setup'], ('#0E1117', '#aaa'))

    # Volume ratio display
    vr = r.get('vol_ratio')
    if vr is not None:
        vr_color = '#00C851' if vr >= 1.5 else ('#4FC3F7' if vr <= 0.85 else ('#FF8C00' if vr >= 1.8 else '#888'))
        vol_ratio_html = f"<span class='vol-ratio' style='color:{vr_color}'>{vr:.2f}x</span>"
    else:
        vol_ratio_html = "<span style='color:#444;font-size:0.68rem'>N/A</span>"

    vol_sig = r.get('vol_signal', '正常量能') or '正常量能'

    return (
        f"<tr style='background:{bg}'>"
        f"<td style='color:#888;font-size:0.73rem'>{r['theme']}</td>"
        f"<td><span style='font-weight:bold;color:#4FC3F7'>{r['ticker']}</span></td>"
        f"<td style='color:#ccc;font-size:0.74rem'>{r['company']}</td>"
        f"<td>{_setup_badge(r['setup'])}</td>"
        f"<td>{_rs_status_badge(r['rs_cat'])}</td>"
        f"<td>{_sentiment_badge(r['sent_label'])}</td>"
        f"<td>{_vol_signal_badge(vol_sig)}</td>"
        f"<td>{vol_ratio_html}</td>"
        f"<td>{tag_html}</td>"
        f"<td style='color:#ccc'>{price_str}</td>"
        f"<td>{_ret_cell(r['rel_5d'])}</td>"
        f"<td>{_ret_cell(r['rel_1m'])}</td>"
        f"<td>{_ret_cell(r['rel_3m'])}</td>"
        f"<td>{_ma_dist_cell(r['dist_ma20'])}</td>"
        f"<td>{_ma_dist_cell(r['dist_ma50'])}</td>"
        f"<td>{_ma_dist_cell(r['dist_h20'])}</td>"
        f"<td>{_vol_flag(r['vol'])}</td>"
        f"<td>{comment_html}</td>"
        f"</tr>"
    )


RADAR_TABLE_HEADER = (
    "<style>"
    ".rdr-tbl{width:100%;border-collapse:collapse;font-family:sans-serif;font-size:0.79rem}"
    ".rdr-tbl th{background:#1A1A2E;color:#9aa;padding:7px 9px;text-align:left;"
    "border-bottom:2px solid #2A2A3E;white-space:nowrap;font-size:0.76rem}"
    ".rdr-tbl td{padding:6px 9px;vertical-align:middle;border-bottom:1px solid #1A1A2A}"
    ".rdr-tbl tr:nth-child(even){background:#0C0F16}"
    ".rdr-tbl tr:nth-child(odd){background:#10141C}"
    ".rdr-tbl tr:hover{background:#1F2937!important}"
    ".vol-ratio{font-size:0.68rem;color:#aaa}"
    "</style>"
    '<table class="rdr-tbl"><thead><tr>'
    "<th>板塊</th><th>Ticker</th><th>公司</th>"
    "<th>Setup 類型</th><th>RS狀態</th><th>新聞情緒</th>"
    "<th>成交量訊號</th><th>Vol比率</th>"
    "<th>催化劑</th>"
    "<th>現價</th>"
    "<th>5D相對</th><th>1M相對</th><th>3M相對</th>"
    "<th>MA20距離</th><th>MA50距離</th><th>距20日高位</th>"
    "<th>波動風險</th><th>備注</th>"
    "</tr></thead><tbody>"
)


def render_theme_ranking_table(selected_themes, perf_data, sentiment_data, benchmark):
    """Compact theme-level ranking for the radar section."""
    heat_results = []
    for theme in selected_themes:
        h = compute_theme_heat(theme, perf_data, sentiment_data, benchmark)
        heat_results.append((theme, h))

    heat_order = {'🔥 當炒主線': 0, '🚀 下一輪潛在': 1, '👀 觀察/未確認': 2}
    heat_results.sort(
        key=lambda x: (heat_order.get(x[1]['heat_label'], 9),
                       -(x[1]['etf_rel_1m'] or -99))
    )

    heat_colors_map = {
        '🔥 當炒主線':   ('#0D2010', '#00C851'),
        '🚀 下一輪潛在': ('#1A0E00', '#FF8C00'),
        '👀 觀察/未確認': ('#101010', '#888'),
    }

    tbl = (
        "<style>"
        ".theme-rank{width:100%;border-collapse:collapse;font-family:sans-serif;font-size:0.78rem}"
        ".theme-rank th{background:#1E1E2E;color:#aaa;padding:6px 8px;text-align:left;"
        "border-bottom:2px solid #333;white-space:nowrap}"
        ".theme-rank td{padding:5px 8px;vertical-align:middle;border-bottom:1px solid #1A1A2E}"
        ".theme-rank tr:hover{background:#1F2937}"
        "</style>"
        '<table class="theme-rank"><thead><tr>'
        f"<th>#</th><th>板塊</th><th>ETF</th>"
        f"<th>ETF 5D相對({benchmark})</th><th>ETF 1M相對({benchmark})</th>"
        "<th>廣度(跑贏+接近)</th><th>板塊情緒</th><th>🌡️ 熱力</th>"
        "</tr></thead><tbody>"
    )
    rows_html = []
    for rank, (theme, h) in enumerate(heat_results, 1):
        bg, fg = heat_colors_map.get(h['heat_label'], ('#101010', '#888'))
        heat_badge = (
            f"<span style='background:{bg};color:{fg};padding:2px 7px;"
            f"border-radius:8px;font-size:0.71rem;font-weight:bold'>{h['heat_label']}</span>"
        )
        icon = theme.split()[0]
        name = theme.split(' ', 1)[-1][:20]
        breadth = f"{h['n_win'] + h['n_near']}/{h['total']}"
        rows_html.append(
            f"<tr style='background:{bg}'>"
            f"<td style='color:#666'>{rank}</td>"
            f"<td style='color:#ddd'>{icon} {name}</td>"
            f"<td style='color:#4FC3F7;font-weight:bold'>{h['etf']}</td>"
            f"<td>{_ret_cell(h['etf_rel_5d'])}</td>"
            f"<td>{_ret_cell(h['etf_rel_1m'])}</td>"
            f"<td style='color:#00C851'>{breadth}</td>"
            f"<td>{_sentiment_badge(h['theme_sent'])}</td>"
            f"<td>{heat_badge}</td>"
            f"</tr>"
        )
    return tbl + '\n'.join(rows_html) + '\n</tbody></table>'



# ─────────────────────────────────────────────────────────────────────────────
# Radar helper: Universe info card
# ─────────────────────────────────────────────────────────────────────────────
def _universe_info_card():
    """Show an informational card explaining the radar's stock universe."""
    total_themes = len(CATALYST_THEME_MAP)
    total_tickers = sum(len(v.get('tickers', {})) for v in CATALYST_THEME_MAP.values())
    st.markdown(
        f"<div style='background:linear-gradient(135deg,#0d1a2d 0%,#1a2740 100%);"
        f"border:1px solid #2a4a6b;border-radius:10px;padding:12px 16px;margin-bottom:12px'>"
        f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:6px'>"
        f"<span style='font-size:1rem'>🌐</span>"
        f"<span style='font-size:0.9rem;font-weight:700;color:#4FC3F7'>雷達掃描範圍 — 主題精選池</span>"
        f"<span style='background:#1e3a5f;color:#7ec8e3;font-size:0.65rem;"
        f"padding:2px 7px;border-radius:10px;border:1px solid #2a5080'>"
        f"非全市場 8,000+ 股</span>"
        f"</div>"
        f"<div style='display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:8px'>"
        f"<div style='background:#0a1520;border-radius:6px;padding:8px;text-align:center'>"
        f"<div style='font-size:1.3rem;font-weight:800;color:#4FC3F7'>{total_themes}</div>"
        f"<div style='font-size:0.68rem;color:#888'>精選板塊主題</div>"
        f"</div>"
        f"<div style='background:#0a1520;border-radius:6px;padding:8px;text-align:center'>"
        f"<div style='font-size:1.3rem;font-weight:800;color:#00C851'>~{total_tickers}</div>"
        f"<div style='font-size:0.68rem;color:#888'>候選個股</div>"
        f"</div>"
        f"<div style='background:#0a1520;border-radius:6px;padding:8px;text-align:center'>"
        f"<div style='font-size:1.3rem;font-weight:800;color:#f1c40f'>⚡</div>"
        f"<div style='font-size:0.68rem;color:#888'>即時掃描</div>"
        f"</div>"
        f"</div>"
        f"<div style='font-size:0.73rem;color:#aaa;line-height:1.5'>"
        f"⚠️ 本雷達係掃描 <b style='color:#4FC3F7'>{total_themes} 個精選主題板塊</b> "
        f"內嘅約 <b style='color:#00C851'>{total_tickers}</b> 隻個股，"
        f"<b>並非</b>全部 8,000+ 隻美股。主題池涵蓋 AI、半導體、能源、醫療、消費等高關注度板塊，"
        f"設計以提升掃描速度同數據質素為主。如需擴闊範圍，可自行添加板塊至 CATALYST_THEME_MAP。"
        f"</div>"
        f"</div>",
        unsafe_allow_html=True
    )


# ─────────────────────────────────────────────────────────────────────────────
# Radar helper: Stock-level sentiment panel
# ─────────────────────────────────────────────────────────────────────────────
def _render_stock_sentiment_panel(candidate_tickers, sentiment_data):
    """Render a stock-level sentiment analysis expander inside the radar module."""
    with st.expander("🔍 四. 個股情緒深度分析 Stock Sentiment", expanded=False):
        st.markdown(
            "<div style='font-size:0.78rem;color:#aaa;margin-bottom:10px'>"
            "選擇一隻股票，查看近期新聞標題、情緒評分及關鍵字詞統計。"
            "</div>",
            unsafe_allow_html=True
        )

        if not candidate_tickers:
            st.info("暫無候選股票，請先在上方選擇板塊。")
            return

        # Ticker selector — sorted alphabetically
        sorted_tickers = sorted(candidate_tickers)
        selected_ticker = st.selectbox(
            "選擇股票 Ticker:",
            sorted_tickers,
            key="sentiment_panel_ticker_select",
        )

        if not selected_ticker:
            return

        col_sent, col_news = st.columns([1, 2])

        with col_sent:
            # Sentiment summary card
            sent = sentiment_data.get(selected_ticker, {})
            label = sent.get("label", "⚪ 中性")
            pos   = sent.get("pos", 0)
            neg   = sent.get("neg", 0)
            count = sent.get("count", 0)
            pos_kw = sent.get("pos_kw", [])
            neg_kw = sent.get("neg_kw", [])

            if label == "🟢 正面":
                card_bg, label_color, border_color = "#0D2010", "#00C851", "#00C851"
            elif label == "🔴 負面":
                card_bg, label_color, border_color = "#200808", "#FF4444", "#FF4444"
            else:
                card_bg, label_color, border_color = "#141414", "#aaa", "#444"

            st.markdown(
                f"<div style='background:{card_bg};border:1px solid {border_color};"
                f"border-radius:10px;padding:14px'>"
                f"<div style='font-size:1.1rem;font-weight:800;color:{label_color};"
                f"margin-bottom:8px'>{label}</div>"
                f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:8px'>"
                f"<div style='background:#0a1010;border-radius:6px;padding:6px;text-align:center'>"
                f"<div style='font-size:1.1rem;font-weight:700;color:#00C851'>{pos}</div>"
                f"<div style='font-size:0.65rem;color:#888'>正面訊號</div>"
                f"</div>"
                f"<div style='background:#0a1010;border-radius:6px;padding:6px;text-align:center'>"
                f"<div style='font-size:1.1rem;font-weight:700;color:#FF4444'>{neg}</div>"
                f"<div style='font-size:0.65rem;color:#888'>負面訊號</div>"
                f"</div>"
                f"</div>"
                f"<div style='font-size:0.68rem;color:#aaa'>新聞數量: {count} 條</div>"
                + (
                    f"<div style='margin-top:6px;font-size:0.68rem'>"
                    f"<span style='color:#00C851'>▲ 正面詞: </span>"
                    f"<span style='color:#ccc'>{', '.join(pos_kw) if pos_kw else '—'}</span></div>"
                    if pos_kw else ""
                )
                + (
                    f"<div style='font-size:0.68rem'>"
                    f"<span style='color:#FF4444'>▼ 負面詞: </span>"
                    f"<span style='color:#ccc'>{', '.join(neg_kw) if neg_kw else '—'}</span></div>"
                    if neg_kw else ""
                )
                + "</div>",
                unsafe_allow_html=True
            )

        with col_news:
            # Fetch and display raw headlines
            st.markdown(
                f"<div style='font-size:0.8rem;font-weight:700;color:#4FC3F7;"
                f"margin-bottom:6px'>📰 近期新聞標題 — {selected_ticker}</div>",
                unsafe_allow_html=True
            )
            try:
                raw_headlines = []
                tkr = yf.Ticker(selected_ticker)
                news_list = tkr.news if hasattr(tkr, "news") and isinstance(tkr.news, list) else []
                for item in news_list[:10]:
                    content_d = item.get("content", {}) if isinstance(item, dict) else {}
                    title = str(content_d.get("title", item.get("title", ""))).strip()
                    if title:
                        raw_headlines.append(title)
            except Exception:
                raw_headlines = []

            if not raw_headlines:
                # Fallback: use finviz
                try:
                    news_df = finvizfinance(selected_ticker).ticker_news()
                    if not news_df.empty:
                        for _, row in news_df.head(10).iterrows():
                            t = str(row.get("Title", "")).strip()
                            if t:
                                raw_headlines.append(t)
                except Exception:
                    pass

            if raw_headlines:
                _, pos_s, neg_s, pos_kw_s, neg_kw_s = classify_news_sentiment(raw_headlines)
                for i, hl in enumerate(raw_headlines[:8], 1):
                    hl_lower = hl.lower()
                    has_pos = any(k in hl_lower for k in pos_kw_s)
                    has_neg = any(k in hl_lower for k in neg_kw_s)
                    if has_pos:
                        row_color = "#1a3a1a"
                        dot = "🟢"
                    elif has_neg:
                        row_color = "#3a1a1a"
                        dot = "🔴"
                    else:
                        row_color = "#1a1a1a"
                        dot = "⚪"
                    st.markdown(
                        f"<div style='background:{row_color};border-radius:5px;"
                        f"padding:5px 8px;margin-bottom:3px;font-size:0.74rem;color:#ddd'>"
                        f"{dot} {hl[:120]}</div>",
                        unsafe_allow_html=True
                    )
            else:
                st.markdown(
                    "<div style='color:#666;font-size:0.76rem;padding:8px'>"
                    "⚠️ 暫時找不到近期新聞，請稍後再試。</div>",
                    unsafe_allow_html=True
                )



def _render_fast_mode_cache_panel():
    """
    Fast Mode: render cache-based radar panel.
    Returns (df, used_fast_mode: bool).
    If cache is found, renders the full panel and returns (df, True).
    Otherwise shows instructions and returns (empty_df, False).
    """
    df, ts, status_msg = load_latest_scan_cache(DEFAULT_SCAN_PATH)
    age_h = _cache_age_hours(DEFAULT_SCAN_PATH)

    # ── Performance / status card ──────────────────────────────────────
    if not df.empty:
        cache_color = '#00C851' if age_h < 24 else ('#FF8C00' if age_h < 48 else '#e74c3c')
        age_warn = '' if age_h < 24 else f' ⚠️ 已 {age_h:.0f}h 未更新'
        bench = df['benchmark'].iloc[0] if 'benchmark' in df.columns else 'SPY'
        active_name = st.session_state.get('active_universe_name', '快取池')
        st.markdown(
            f"<div style='background:linear-gradient(135deg,#071a0d,#0d2a18);border:1px solid {cache_color}44;"
            f"border-radius:10px;padding:12px 16px;margin-bottom:12px'>"
            f"<div style='display:flex;align-items:center;gap:16px;flex-wrap:wrap'>"
            f"<span style='font-size:0.88rem;font-weight:700;color:{cache_color}'>⚡ Fast Mode 已啟動</span>"
            f"<span style='color:#aaa;font-size:0.8rem'>Universe: <b style='color:#4FC3F7'>{active_name}</b></span>"
            f"<span style='color:#aaa;font-size:0.8rem'>強制对象: <b style='color:#4FC3F7'>{len(df)}</b> 隻</span>"
            f"<span style='color:#aaa;font-size:0.8rem'>基準: <b style='color:#4FC3F7'>{bench}</b></span>"
            f"<span style='color:{cache_color};font-size:0.78rem'>{status_msg}{age_warn}</span>"
            f"</div></div>",
            unsafe_allow_html=True
        )
    else:
        st.info(status_msg, icon='⚡')
        st.markdown("""
#### 如何始用 Fast Mode

1. **安裝相依套件**：`pip install yfinance pyarrow pandas`
2. **運行小型測試掃描**：
   ```bash
   python scanner.py --tickers "AAPL,MSFT,NVDA,SPY,QQQ" --output data/latest_scan.parquet
   ```
3. **運行預設粿選池**（約 80 隻）：
   ```bash
   python scanner.py
   ```
4. **自定義 Universe**：
   ```bash
   python scanner.py --universe-file my_tickers.csv --benchmark SPY
   ```
5. 掃描完成後，刷新頁面即可看到快取結果。
        """)

    return df


def _render_fast_mode_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render filter controls for the cache-based view. Returns filtered DataFrame."""
    if df.empty:
        return df

    all_setups = sorted(df['setup_type'].dropna().unique().tolist()) if 'setup_type' in df.columns else []
    all_rs = sorted(df['rs_status'].dropna().unique().tolist()) if 'rs_status' in df.columns else []

    fc1, fc2, fc3 = st.columns([3, 3, 2])
    with fc1:
        default_setups = [s for s in all_setups if s in ('強勢延續', '接近爆發', '低風险回踩', '早期轉強')]
        selected_setups = st.multiselect(
            '築選 Setup 類型:', all_setups,
            default=default_setups or all_setups[:4],
            key='fm_setup_filter'
        )
    with fc2:
        selected_rs = st.multiselect(
            '築選 RS 狀態:', all_rs,
            default=all_rs,
            key='fm_rs_filter'
        )
    with fc3:
        max_rows = st.number_input('最多顯示行數', min_value=5, max_value=200, value=50, step=5, key='fm_max_rows')
        prefer_low = st.checkbox('優先顯示低風险', value=False, key='fm_prefer_low')

    # Apply filters
    filtered = df.copy()
    if selected_setups and 'setup_type' in filtered.columns:
        filtered = filtered[filtered['setup_type'].isin(selected_setups)]
    if selected_rs and 'rs_status' in filtered.columns:
        filtered = filtered[filtered['rs_status'].isin(selected_rs)]

    # Sort
    setup_order_map = {'強勢延續': 0, '接近爆發': 1, '低風险回踩': 2, '早期轉強': 3, '過度延伸': 4, '跑輸觀望': 5}
    if prefer_low:
        setup_order_map = {'低風险回踩': 0, '早期轉強': 1, '強勢延續': 2, '接近爆發': 3, '過度延伸': 4, '跑輸觀望': 5}
    if 'setup_type' in filtered.columns:
        filtered['_sort'] = filtered['setup_type'].map(setup_order_map).fillna(9)
        sort_by = ['_sort']
        if 'rel_1m' in filtered.columns:
            sort_by.append('rel_1m')
        filtered = filtered.sort_values(sort_by, ascending=[True, False]).drop(columns=['_sort'])

    return filtered.head(int(max_rows))


def _render_fast_mode_table(df: pd.DataFrame) -> None:
    """Render the filtered cache results as a styled table."""
    if df.empty:
        st.warning('⚠️ 當前築選條件下無候選股票，請調整等級築選。')
        return

    # Colour helpers
    def pct_colour(v):
        try:
            v = float(v)
            if v >= 5: return '#00C851'
            if v >= 1: return '#26A69A'
            if v >= -1: return '#f1c40f'
            if v >= -5: return '#FF8C00'
            return '#e74c3c'
        except: return '#888'

    SETUP_COLORS_FM = {
        '強勢延續': ('#0D2010','#00C851'),
        '接近爆發': ('#1A0E00','#FF8C00'),
        '低風险回踩': ('#0A1A2A','#4FC3F7'),
        '早期轉強': ('#1A1A0A','#FFD700'),
        '過度延伸': ('#2A0A0A','#FF4444'),
        '跑輸觀望': ('#111','#666'),
    }
    RS_COLORS_FM = {'跑贏指數': '#00C851', '接近突破': '#FF8C00', '剛轉強': '#FFD700', '跑輸指數': '#888'}

    rows_html = []
    for _, row in df.iterrows():
        setup = str(row.get('setup_type', ''))
        rs = str(row.get('rs_status', ''))
        bg, fg = SETUP_COLORS_FM.get(setup, ('#111', '#aaa'))
        rs_c = RS_COLORS_FM.get(rs, '#aaa')

        def _fmt(col, fmt='{:.2f}%', na='N/A'):
            v = row.get(col)
            try:
                return fmt.format(float(v))
            except: return na

        price = _fmt('latest_price', '${:.2f}')
        ret5d = _fmt('ret_5d')
        rel1m = _fmt('rel_1m')
        dist20 = _fmt('distance_to_ma20_pct')
        vol_s = str(row.get('volume_signal', 'N/A'))
        vol_ratio = _fmt('volume_ratio', '{:.1f}x', 'N/A')
        vol_col = '#00C851' if '放量' in vol_s else ('#4FC3F7' if '縮量' in vol_s else '#888')

        r5_c = pct_colour(row.get('ret_5d'))
        r1m_c = pct_colour(row.get('rel_1m'))
        d20_c = pct_colour(row.get('distance_to_ma20_pct'))

        rows_html.append(
            f"<tr>"
            f"<td><b style='color:#4FC3F7'>{row.get('ticker','')}</b></td>"
            f"<td style='color:{r5_c}'>{ret5d}</td>"
            f"<td style='color:{r1m_c}'>{rel1m}</td>"
            f"<td style='color:{d20_c}'>{dist20}</td>"
            f"<td style='color:{vol_col}'>{vol_s} <span style='color:#555'>({vol_ratio})</span></td>"
            f"<td><span style='background:{bg};color:{fg};padding:2px 7px;border-radius:8px;font-size:0.75rem'>{setup}</span></td>"
            f"<td><span style='color:{rs_c};font-size:0.78rem'>{rs}</span></td>"
            f"<td>{price}</td>"
            f"</tr>"
        )

    header = """
<style>.fm-tbl{width:100%;border-collapse:collapse;font-family:'Inter',sans-serif;font-size:0.79rem}
.fm-tbl th{background:#141824;color:#8a9bb0;padding:7px 9px;text-align:left;border-bottom:2px solid #252d3d;white-space:nowrap;font-size:0.72rem}
.fm-tbl td{padding:6px 9px;border-bottom:1px solid #1a1e28;vertical-align:middle}
.fm-tbl tr:nth-child(even){background:#0c0f18}.fm-tbl tr:nth-child(odd){background:#101420}
.fm-tbl tr:hover{background:#1c2535!important}</style>
<table class='fm-tbl'>
<thead><tr>
<th>Ticker</th><th>5D回報</th><th>1M超額回報</th>
<th>MA20距離</th><th>成交量訊號</th><th>Setup 類型</th><th>RS 狀態</th><th>現價</th>
</tr></thead><tbody>"""
    st.markdown(header + ''.join(rows_html) + '</tbody></table>', unsafe_allow_html=True)


def render_radar_module():
    """Main render function for 🔥 當炒 / 低風險機會雷達."""
    st.title('🔥 當炒 / 低風險機會雷達')
    st.caption(
        '目標：快速識別 🔥 潛在當炒股票/板塊 及 🛡️ 低風險買入機會。'
        '  ⚠️ 本模組僅供技術篩選/觀察清單用途，所有數據不構成任何形式之投資建議。'
    )

    # ── Universe Info Card ─────────────────────────────────────────
    _universe_info_card()

    # ── Universe Selector (compact bar) ───────────────────────────
    _init_universe_session_state()
    _render_universe_selector_compact()

    # ─────────────────────────────────────────────────────────────────
    # ⚡ FAST MODE — render cache-based results first (no blocking scan)
    # ─────────────────────────────────────────────────────────────────
    st.markdown('---')
    st.markdown('### ⚡ Fast Mode — 盤後預掃描快取模式')
    cache_df = _render_fast_mode_cache_panel()
    if not cache_df.empty:
        st.markdown('#### 📂 篩選控制框')
        filtered_df = _render_fast_mode_filters(cache_df)
        mc1, mc2, mc3, mc4, mc5 = st.columns(5)
        mc1.metric('候選股票', len(cache_df))
        mc2.metric('🔥 強勢延續', int((cache_df.get('setup_type', pd.Series()) == '強勢延續').sum()))
        mc3.metric('🚀 接近爆發', int((cache_df.get('setup_type', pd.Series()) == '接近爆發').sum()))
        mc4.metric('🛡️ 低風險回踩', int((cache_df.get('setup_type', pd.Series()) == '低風險回踩').sum()))
        mc5.metric('👀 早期轉強', int((cache_df.get('setup_type', pd.Series()) == '早期轉強').sum()))
        st.markdown(
            f"<div style='color:#888;font-size:0.78rem;margin:4px 0'>顯示 {len(filtered_df)} / {len(cache_df)} 隻（已套用篩選）。</div>",
            unsafe_allow_html=True
        )
        _render_fast_mode_table(filtered_df)
        st.markdown('---')
        with st.expander('📰 個股新聞情緒分析（按需，不會自動全部掃描）', expanded=False):
            st.caption('選擇單一股票，按鈕才發起 AI 新聞情緒分析。')
            _t_list = sorted(cache_df['ticker'].tolist()) if 'ticker' in cache_df.columns else []
            sel_ticker = st.selectbox('選擇股票', _t_list, key='fm_sent_ticker')
            if st.button('🔍 按需分析新聞情緒', key='fm_sent_btn') and sel_ticker:
                with st.spinner(f'分析 {sel_ticker} 新聞...'):
                    try:
                        _news = fetch_single_stock_news(sel_ticker)
                        st.markdown(analyze_single_stock_sentiment(sel_ticker, _news))
                    except Exception as _e:
                        st.warning(f'分析失敗: {_e}')

    # ── Legacy live-scan (slow) — collapsed ──────────────────────────
    st.markdown('---')
    with st.expander('🐢 Legacy 小型即時掃描（慢，主題精選池專用）', expanded=False):
        st.warning(
            '⚠️ 這個路徑會實時發起 yfinance 掃描，小型主題精選池會較快，'
            '廣義 Universe 會很慢。建議少於 50 隻。'
        )

        # ── Controls (Legacy live scan) ──────────────────────────────
        col_ctrl1, col_ctrl2, col_ctrl3 = st.columns([3, 2, 1])
        with col_ctrl1:
            all_themes = list(CATALYST_THEME_MAP.keys())
            radar_themes = st.multiselect(
                '選擇板塊 (可多選):',
                all_themes,
                default=all_themes[:5],
                key='radar_theme_select',
            )
        with col_ctrl2:
            radar_bench = st.selectbox(
                'RS 比較基準:',
                ['SPY', 'QQQ', 'IWM'],
                key='radar_bench',
            )
            setup_filter = st.multiselect(
                '篩選 Setup 類型:',
                list(SETUP_LABELS.values()),
                default=[
                    SETUP_LABELS['HOT_MOMENTUM'],
                    SETUP_LABELS['NEAR_BREAKOUT'],
                    SETUP_LABELS['LOW_RISK'],
                    SETUP_LABELS['EARLY_TURN'],
                ],
                key='radar_setup_filter',
            )
        with col_ctrl3:
            st.markdown('<br>', unsafe_allow_html=True)
            prefer_low_risk = st.checkbox('優先顯示低風險', value=False, key='radar_prefer_lowrisk')
            max_rows = st.number_input('最多顯示行數', min_value=5, max_value=100, value=30, step=5, key='radar_max_rows')
            refresh_radar_btn = st.button('🔄 刷新', use_container_width=True, key='refresh_radar_main')

        if refresh_radar_btn:
            try:
                fetch_catalyst_rs_data.clear()
                fetch_ticker_news_sentiment.clear()
                fetch_ma_data.clear()
            except Exception:
                pass
            st.rerun()

        if not radar_themes:
            st.info('請至少選擇一個板塊。')
            return

        # For broad universes, ensure radar_themes stays as selected (for theme ranking table).
        # The candidate_tickers section below will override tickers from the active universe.

        # ── Collect candidate tickers ───────────────────────────────────
        _active_universe_name = st.session_state.get('active_universe_name', '主題精選池')
        _max_scan_limit = st.session_state.get('universe_max_scan_limit', 150)
        _news_enabled = st.session_state.get('universe_news_enabled', True)

        if _active_universe_name == '主題精選池':
            # Default: iterate selected themes from CATALYST_THEME_MAP
            candidate_tickers = []
            for theme in radar_themes:
                for t in CATALYST_THEME_MAP[theme].get('tickers', {}):
                    if t not in candidate_tickers:
                        candidate_tickers.append(t)
        else:
            # Broad universe: use active universe tickers (capped to max_scan_limit)
            _universe_pool = get_active_universe_tickers()
            candidate_tickers = list(_universe_pool[:_max_scan_limit])
            if len(_universe_pool) > _max_scan_limit:
                st.warning(
                    f'⚠️ 當前 Universe「{_active_universe_name}」共 {len(_universe_pool)} 隻，'
                    f'已限制掃描首 {_max_scan_limit} 隻。可於 Universe Manager 調整上限。'
                )

        # Include theme ETFs for heatmap
        theme_etfs = list(THEME_ETF_MAP.values())
        all_fetch = list(set(candidate_tickers + theme_etfs + [radar_bench]))

        # ── Fetch data ──────────────────────────────────────────────────
        st.markdown('---')
        st.markdown('### 📡 一. 板塊熱力排名')

        with st.spinner(f'正在抓取 {len(all_fetch)} 隻股票數據...'):
            try:
                perf_data = fetch_catalyst_rs_data(
                    tuple(sorted(all_fetch)), benchmark=radar_bench
                )
            except Exception:
                perf_data = {}

        with st.spinner('分析新聞情緒...'):
            try:
                _skip_news = (not _news_enabled) or (len(candidate_tickers) > 100 and _active_universe_name != '主題精選池')
                if _skip_news:
                    sentiment_data = {}
                    if len(candidate_tickers) > 100 and _news_enabled:
                        st.info(
                            f'💡 廣義 Universe（{len(candidate_tickers)} 隻）已自動停用新聞情緒分析以加快掃描速度。'
                            '如需啟用，請於 Universe Manager → ⚙️ 設定中開啟「新聞情緒分析」。'
                        )
                else:
                    sentiment_data = fetch_ticker_news_sentiment(tuple(candidate_tickers))
            except Exception:
                sentiment_data = {}

        with st.spinner('計算 MA20/MA50 及波動率...'):
            try:
                ma_data = fetch_ma_data(tuple(candidate_tickers))
            except Exception:
                ma_data = {}

        # ── A. Theme ranking table ──────────────────────────────────────
        st.markdown(render_theme_ranking_table(radar_themes, perf_data, sentiment_data, radar_bench),
                    unsafe_allow_html=True)
        st.caption(
            '🔥 當炒主線 = ETF 1M相對>0 + 強勢廣度≥40% + 正面情緒；'
            '🚀 下一輪潛在 = ETF 5D改善 + 廣度≥35%；👀 觀察/未確認 = 其他情況。'
        )

        # ── B. Build watchlist rows ────────────────────────────────────
        all_rows = build_radar_rows(
            radar_themes, perf_data, sentiment_data, ma_data, radar_bench
        )

        # ── Bucket summaries ───────────────────────────────────────────
        st.markdown('---')
        st.markdown('### 🗂️ 二. 觀察清單 Watchlist 分類')

        hot_rows  = [r for r in all_rows if r['setup'] in (SETUP_LABELS['HOT_MOMENTUM'], SETUP_LABELS['NEAR_BREAKOUT'])]
        low_rows  = [r for r in all_rows if r['setup'] in (SETUP_LABELS['LOW_RISK'], SETUP_LABELS['EARLY_TURN'])]
        other_rows = [r for r in all_rows if r['setup'] not in (
            SETUP_LABELS['HOT_MOMENTUM'], SETUP_LABELS['NEAR_BREAKOUT'],
            SETUP_LABELS['LOW_RISK'], SETUP_LABELS['EARLY_TURN']
        )]

        bc1, bc2 = st.columns(2)
        with bc1:
            st.markdown(
                "<div style='background:#0D2010;border:1px solid #00C851;border-radius:8px;padding:10px 12px'>"
                "<div style='font-size:0.9rem;font-weight:bold;color:#00C851'>🔥 潛在當炒 / Hot Momentum</div>"
                "<div style='font-size:0.72rem;color:#888;margin:3px 0 6px'>RS強勢+正面情緒+板塊熱度高</div>"
                + ''.join(
                    f"<div style='font-size:0.79rem;color:#ccc;padding:1px 0'>"
                    f"• <b style='color:#4FC3F7'>{r['ticker']}</b> {r['company'][:20]}  "
                    f"<span style='color:#00C851;font-size:0.72rem'>{r['setup']}</span></div>"
                    for r in hot_rows[:8]
                )
                + (f"<div style='color:#555;font-size:0.72rem'>⋯ 另 {len(hot_rows)-8} 隻</div>" if len(hot_rows) > 8 else '')
                + (f"<div style='color:#555;font-size:0.72rem'>（暫無符合條件股票）</div>" if not hot_rows else '')
                + "</div>",
                unsafe_allow_html=True
            )
        with bc2:
            st.markdown(
                "<div style='background:#0A1A2A;border:1px solid #4FC3F7;border-radius:8px;padding:10px 12px'>"
                "<div style='font-size:0.9rem;font-weight:bold;color:#4FC3F7'>🛡️ 低風險買入 / Lower-Risk Setup</div>"
                "<div style='font-size:0.72rem;color:#888;margin:3px 0 6px'>回踩至MA20附近+RS仍強+未過度延伸</div>"
                + ''.join(
                    f"<div style='font-size:0.79rem;color:#ccc;padding:1px 0'>"
                    f"• <b style='color:#4FC3F7'>{r['ticker']}</b> {r['company'][:20]}  "
                    f"<span style='color:#4FC3F7;font-size:0.72rem'>{r['setup']}</span></div>"
                    for r in low_rows[:8]
                )
                + (f"<div style='color:#555;font-size:0.72rem'>⋯ 另 {len(low_rows)-8} 隻</div>" if len(low_rows) > 8 else '')
                + (f"<div style='color:#555;font-size:0.72rem'>（暫無符合條件股票）</div>" if not low_rows else '')
                + "</div>",
                unsafe_allow_html=True
            )

        # ── C. Full filtered table ─────────────────────────────────────
        st.markdown('<br>', unsafe_allow_html=True)
        st.markdown('### 📋 三. 完整篩選名單')

        # Sort order
        setup_order = {
            SETUP_LABELS['HOT_MOMENTUM']:  0,
            SETUP_LABELS['NEAR_BREAKOUT']: 1,
            SETUP_LABELS['LOW_RISK']:      2,
            SETUP_LABELS['EARLY_TURN']:    3,
            SETUP_LABELS['OVEREXTENDED']:  4,
            SETUP_LABELS['LAGGING']:       5,
        }
        rows_display = [r for r in all_rows if r['setup'] in setup_filter]
        if prefer_low_risk:
            rows_display.sort(key=lambda r: (
                0 if r['setup'] == SETUP_LABELS['LOW_RISK'] else
                1 if r['setup'] == SETUP_LABELS['EARLY_TURN'] else
                setup_order.get(r['setup'], 9),
                -(r['rel_1m'] or -999)
            ))
        else:
            rows_display.sort(key=lambda r: (
                setup_order.get(r['setup'], 9),
                -(r['rel_1m'] or -999)
            ))
        rows_display = rows_display[:int(max_rows)]

        # Metric bar
        mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
        bench_1d = perf_data.get(radar_bench, {}).get('1d')
        mc1.metric('候選股票', len(all_rows))
        mc2.metric('🔥 強勢延續', sum(1 for r in all_rows if r['setup'] == SETUP_LABELS['HOT_MOMENTUM']))
        mc3.metric('🚀 接近爆發', sum(1 for r in all_rows if r['setup'] == SETUP_LABELS['NEAR_BREAKOUT']))
        mc4.metric('🛡️ 低風險', sum(1 for r in all_rows if r['setup'] == SETUP_LABELS['LOW_RISK']))
        mc5.metric('👀 早期轉強', sum(1 for r in all_rows if r['setup'] == SETUP_LABELS['EARLY_TURN']))
        mc6.metric(f'{radar_bench} 今日', f"{bench_1d:+.2f}%" if bench_1d is not None else 'N/A')

        if rows_display:
            st.markdown(
                f"<div style='color:#888;font-size:0.78rem;margin:4px 0'>"
                f"顯示 {len(rows_display)} / {len(all_rows)} 隻（已套用篩選）。</div>",
                unsafe_allow_html=True
            )
            body_html = '\n'.join(_radar_row_html(r, radar_bench) for r in rows_display)
            st.markdown(RADAR_TABLE_HEADER + body_html + '\n</tbody></table>',
                        unsafe_allow_html=True)
        else:
            st.warning('⚠️ 當前篩選條件下無候選股票，請調整 Setup 類型篩選。')

    # ── D. Stock-level sentiment panel ────────────────────────────
    _render_stock_sentiment_panel(candidate_tickers, sentiment_data)

        # ── D. 點樣用 usage guide ──────────────────────────────────────
    st.markdown('---')
    with st.expander('📖 點樣用 — 篩選邏輯與注意事項', expanded=False):
        st.markdown("""
### 🎯 兩個主要工作流程

**工作流程 A：搵潛在當炒股票/板塊**
1. 睇板塊熱力排名（第一節）→ 優先關注 🔥 當炒主線 板塊
2. 睇 🔥 潛在當炒 Bucket → 重點係 **🔥 強勢延續** 及 **🚀 接近爆發** 兩類
3. 新聞情緒係 🟢 正面、RS狀態係 🟢 跑贏指數 或 🟠 接近突破，資金流向最明確
4. 配合右邊「板塊熱力圖」確認整體板塊動力

**工作流程 B：搵低風險買入機會**
1. 開啟「優先顯示低風險」核取方塊
2. 重點係 **🛡️ 低風險回踩** 類別：股價回踩到 MA20 附近（MA20距離 -8% 至 +4%），但 RS 仍係 🟠接近突破 或 🟡剛轉強
3. 距20日高位 應係負數（即已回踩），波動風險最好係 🟢 低（年化波動率 <50%）
4. **👀 早期轉強** 類別亦值得留意：5D相對已轉正但1M仍負，屬於早期進場訊號

### 📊 各欄位說明

| 欄位 | 含義 |
|------|------|
| Setup 類型 | 綜合 RS狀態、MA距離、情緒分類的操作形態 |
| RS狀態 | 相對強度 vs 基準指數（4級分類） |
| MA20距離 | 現價距20日均線百分比；正數=在均線之上 |
| MA50距離 | 現價距50日均線百分比 |
| 距20日高位 | 現價距近20日最高位；負數=回踩中 |
| 波動風險 | 年化20日歷史波動率：🟢<50% 🟠50-80% 🔴>80% |

### 🛡️ Setup 類型邏輯

| Setup | 條件 |
|-------|------|
| 🔥 強勢延續 | RS=跑贏指數 + 正面情緒 + 未過度延伸 |
| 🚀 接近爆發 | RS=接近突破/跑贏 + 正面/中性情緒 + 5D相對≥-1% |
| 🛡️ 低風險回踩 | RS=接近突破/剛轉強 + 回踩至MA20附近(-8%至+4%) + 距20日高位<-3% + 正面/中性情緒 |
| 👀 早期轉強 | RS=剛轉強 + 正面/中性情緒 |
| ⚠️ 過度延伸 | 距MA20 >12% 或 高波動 |

### ⚠️ 風險免責聲明
> 本雷達係純技術篩選/觀察工具。所有 Setup 類別、RS狀態、情緒分析均係基於歷史價格數據同新聞標題關鍵字，並**非**前瞻性預測，亦**不構成任何投資建議**。買賣前請做獨立研究，並了解個人風險承受能力。
        """)



# ==========================================
# 🗂️ Universe Manager – 掃描範圍管理器
# ==========================================
# Session-state keys:
#   active_universe_name  : str  – e.g. '主題精選池'
#   active_universe_tickers: list[str]
#   universe_news_enabled : bool – whether to fetch per-ticker news in radar

# ── Static fallback lists ──────────────────────────────────────────────────
_SP500_STATIC_FALLBACK = [
    'AAPL','MSFT','AMZN','NVDA','GOOGL','META','TSLA','BRK-B','AVGO','JPM',
    'UNH','XOM','V','MA','HD','LLY','ABBV','MRK','PEP','COST','BAC','PG',
    'CVX','ORCL','ADBE','NFLX','CRM','TMO','AMD','WMT','ABT','ACN','QCOM',
    'DIS','INTC','GE','NEE','PM','T','MCD','UPS','LOW','HON','IBM','CAT',
    'INTU','MDT','GS','AMT','SPGI','AXP','ISRG','DE','AMAT','BKNG','GILD',
    'LRCX','PLD','NOW','ADI','KLAC','TXN','PANW','SYK','BSX','MMC','CB',
    'ETN','REGN','VRTX','CI','MO','BDX','ADP','ZTS','SO','TJX','C','CME',
    'EL','AON','ITW','FCX','CRWD','WM','D','ECL','APH','NSC','MMM','MRNA',
    'CEG','VST','ENPH','TSCO','MSCI','DHR','PCG','PSX','SNPS','CDNS','ORLY',
]

_NDX100_STATIC_FALLBACK = [
    'AAPL','MSFT','AMZN','NVDA','META','GOOGL','GOOG','TSLA','AVGO','COST',
    'NFLX','ASML','AMD','QCOM','TMUS','INTC','CSCO','INTU','TXN','AMAT',
    'MU','LRCX','KLAC','ADI','MRVL','PANW','CRWD','SNPS','CDNS','MELI',
    'ORLY','REGN','VRTX','PYPL','ISRG','ADP','MAR','CSX','CTAS','PCAR',
    'FISV','IDXX','CEG','DASH','BKNG','KDP','ADSK','ROST','FAST','EXC',
    'SIRI','GEHC','FANG','BKR','XEL','CTSH','VRSK','MNST','DXCM','WBA',
    'ON','CPRT','PAYX','BIIB','ILMN','GILD','CHTR','EA','ZS','ANSS','SBUX',
    'ABNB','ALGN','TEAM','DDOG','ZM','DOCU','SGEN','ODFL','FTNT','WBD',
]


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_sp500_tickers():
    """Fetch S&P 500 tickers from Wikipedia. Cached 24 hours. Falls back to static list."""
    try:
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        df = tables[0]
        col = next((c for c in df.columns if 'symbol' in c.lower() or 'ticker' in c.lower()), None)
        if col is None:
            col = df.columns[0]
        tickers = [str(t).strip().replace('.', '-') for t in df[col].tolist() if isinstance(t, str) and len(t) <= 5]
        if len(tickers) >= 400:
            return tickers, '🟢 Wikipedia S&P 500 (實時)'
    except Exception:
        pass
    return _SP500_STATIC_FALLBACK.copy(), '🟡 S&P 500 靜態備援清單 (100隻代表股)'


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_nasdaq100_tickers():
    """Fetch Nasdaq-100 tickers from Wikipedia. Cached 24 hours. Falls back to static list."""
    try:
        tables = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')
        for df in tables:
            cols_lower = [c.lower() for c in df.columns]
            if any('ticker' in c or 'symbol' in c for c in cols_lower):
                col = next(c for c in df.columns if 'ticker' in c.lower() or 'symbol' in c.lower())
                tickers = [str(t).strip().replace('.', '-') for t in df[col].tolist()
                           if isinstance(t, str) and 1 < len(t) <= 5]
                if len(tickers) >= 80:
                    return tickers, '🟢 Wikipedia Nasdaq-100 (實時)'
    except Exception:
        pass
    return _NDX100_STATIC_FALLBACK.copy(), '🟡 Nasdaq-100 靜態備援清單'


def parse_manual_tickers(text):
    """Parse comma/newline separated ticker list. Returns deduplicated, uppercased list."""
    if not text or not text.strip():
        return []
    import re
    raw = re.split(r'[,\n\r\t;]+', text)
    seen = set()
    result = []
    for t in raw:
        t = t.strip().upper()
        t = re.sub(r'[^A-Z0-9.\-]', '', t)
        if t and t not in seen:
            seen.add(t)
            result.append(t)
    return result


def get_theme_pool_tickers():
    """Return all unique tickers from the curated CATALYST_THEME_MAP."""
    seen = set()
    result = []
    for tdata in CATALYST_THEME_MAP.values():
        for ticker in tdata.get('tickers', {}).keys():
            if ticker not in seen:
                seen.add(ticker)
                result.append(ticker)
    return result


def get_active_universe_tickers():
    """Return the currently active universe tickers from session_state.
    Falls back to the curated theme pool if nothing is set."""
    return st.session_state.get('active_universe_tickers', get_theme_pool_tickers())


def build_active_universe_theme_map():
    """Build a CATALYST_THEME_MAP-compatible dict for the active universe.
    For curated pool, returns CATALYST_THEME_MAP directly.
    For other universes, groups all tickers under 'Custom Universe'."""
    name = st.session_state.get('active_universe_name', '主題精選池')
    if name == '主題精選池':
        return CATALYST_THEME_MAP
    tickers = get_active_universe_tickers()
    # Build a synthetic map grouped by custom universe
    return {
        '🌐 Custom Universe': {
            'keywords': [],
            'etf': 'SPY',
            'tickers': {t: t for t in tickers},
            'catalyst_tags': ['Custom Universe'],
        }
    }


def _init_universe_session_state():
    """Ensure universe session state keys are initialised."""
    if 'active_universe_name' not in st.session_state:
        st.session_state['active_universe_name'] = '主題精選池'
    if 'active_universe_tickers' not in st.session_state:
        st.session_state['active_universe_tickers'] = get_theme_pool_tickers()
    if 'universe_news_enabled' not in st.session_state:
        st.session_state['universe_news_enabled'] = True


# ── Bulk OHLCV fetch helper ────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_bulk_ohlcv(tickers_tuple, period='6mo'):
    """
    Batch-download OHLCV for a list of tickers in one yfinance call.
    Returns dict: {ticker: {'close': pd.Series, 'volume': pd.Series}}
    Uses a single batched API call for efficiency.
    """
    tickers = list(tickers_tuple)
    result = {}
    if not tickers:
        return result
    try:
        raw = yf.download(
            tickers, period=period, interval='1d',
            progress=False, auto_adjust=True,
            group_by='column', threads=True
        )
        if raw.empty:
            return result
        closes = raw['Close'] if ('Close' in raw.columns.get_level_values(0)
                                   if isinstance(raw.columns, pd.MultiIndex) else 'Close' in raw.columns) else raw
        volumes = None
        try:
            if isinstance(raw.columns, pd.MultiIndex) and 'Volume' in raw.columns.get_level_values(0):
                volumes = raw['Volume']
        except Exception:
            pass
        for ticker in tickers:
            try:
                c = (closes[ticker].dropna() if ticker in closes.columns
                     else pd.Series(dtype=float))
                v = (volumes[ticker].dropna() if volumes is not None and ticker in volumes.columns
                     else pd.Series(dtype=float))
                result[ticker] = {'close': c, 'volume': v}
            except Exception:
                result[ticker] = {'close': pd.Series(dtype=float), 'volume': pd.Series(dtype=float)}
    except Exception:
        pass
    return result


# ── Universe Manager renderer ──────────────────────────────────────────────
def render_universe_manager_module():
    """Main render function for 🗂️ Universe Manager."""
    _init_universe_session_state()

    st.title('🗂️ Universe Manager — 掃描範圍管理器')
    st.caption(
        '選擇或自定義雷達掃描嘅股票池。唔同範圍有唔同速度：主題精選池最快，S&P 500/Nasdaq 100 中速，'
        '全手動輸入最靈活。設定後雷達模組會自動採用所選池。'
    )

    # ── Current universe status bar ──────────────────────────────────────
    cur_name = st.session_state.get('active_universe_name', '主題精選池')
    cur_tickers = st.session_state.get('active_universe_tickers', [])
    news_enabled = st.session_state.get('universe_news_enabled', True)

    st.markdown(
        f"<div style='background:linear-gradient(135deg,#0a1a10,#1a3020);border:1px solid #2a8a50;"
        f"border-radius:10px;padding:12px 16px;margin-bottom:16px'>"
        f"<div style='display:flex;align-items:center;gap:12px;flex-wrap:wrap'>"
        f"<span style='font-size:0.9rem;font-weight:700;color:#00C851'>✅ 當前啟用範圍</span>"
        f"<span style='background:#003a18;color:#00C851;padding:3px 10px;border-radius:8px;"
        f"font-size:0.82rem;font-weight:bold'>{cur_name}</span>"
        f"<span style='color:#aaa;font-size:0.82rem'>共 <b style='color:#4FC3F7'>{len(cur_tickers)}</b> 隻 Ticker</span>"
        f"<span style='color:#aaa;font-size:0.8rem'>｜ 新聞情緒: "
        f"<b style='color:{'#00C851' if news_enabled else '#888'}'>{'✅ 開啟' if news_enabled else '❌ 關閉'}</b></span>"
        f"</div></div>",
        unsafe_allow_html=True
    )

    # ── Universe cards ───────────────────────────────────────────────────
    st.markdown('### 🌐 選擇掃描範圍')
    st.markdown(
        "<div style='font-size:0.76rem;color:#888;margin-bottom:10px'>"
        "每個選項顯示大概速度、適用場合及 Ticker 數量。點擊「套用」即時切換。"
        "</div>",
        unsafe_allow_html=True
    )

    # Card definitions
    UNIVERSE_CARDS = [
        {
            'id': '主題精選池',
            'icon': '🎯',
            'title': '主題精選池 (預設)',
            'desc': '涵蓋 AI、半導體、能源、醫療、消費等 11 個熱門主題板塊，精選約 70 隻最具代表性美股。',
            'speed': '⚡ Fast',
            'speed_color': '#00C851',
            'recommended': '日常快速掃描、主題輪動分析',
            'ticker_count': lambda: len(get_theme_pool_tickers()),
            'news_default': True,
        },
        {
            'id': 'S&P 500',
            'icon': '📊',
            'title': 'S&P 500',
            'desc': '美國最大 500 家上市公司。涵蓋面廣，可捕捉大盤輪動機會。首次載入需從 Wikipedia 抓取清單。',
            'speed': '🟡 Medium',
            'speed_color': '#F9A825',
            'recommended': '大盤廣泛掃描，建議限制至 100-200 隻',
            'ticker_count': lambda: 500,
            'news_default': False,
        },
        {
            'id': 'Nasdaq 100',
            'icon': '💻',
            'title': 'Nasdaq 100',
            'desc': '納斯達克 100 家大型非金融科技及增長型企業。科技/AI 集中度高。',
            'speed': '🟡 Medium',
            'speed_color': '#F9A825',
            'recommended': '科技/成長股輪動分析，建議新聞關閉',
            'ticker_count': lambda: 100,
            'news_default': False,
        },
        {
            'id': '手動輸入',
            'icon': '✏️',
            'title': '手動輸入 Ticker 清單',
            'desc': '自行輸入任意美股 Ticker，用逗號或換行分隔。最靈活，可針對特定觀察名單。',
            'speed': '⚡ Fast (depends on count)',
            'speed_color': '#4FC3F7',
            'recommended': '個人觀察清單、特定組合監控',
            'ticker_count': lambda: len(st.session_state.get('manual_tickers_parsed', [])),
            'news_default': True,
        },
        {
            'id': 'CSV 上載',
            'icon': '📁',
            'title': 'CSV 上載',
            'desc': '上載 CSV 檔案，自動識別 ticker/symbol 欄或使用第一欄。支援標準格式。',
            'speed': '⚡ Fast (depends on count)',
            'speed_color': '#4FC3F7',
            'recommended': '批量輸入大量 Ticker',
            'ticker_count': lambda: len(st.session_state.get('csv_tickers_parsed', [])),
            'news_default': False,
        },
    ]

    # Render cards in grid
    card_cols = st.columns(len(UNIVERSE_CARDS))
    selected_universe_id = st.session_state.get('_universe_card_selected', '主題精選池')

    for col, card in zip(card_cols, UNIVERSE_CARDS):
        is_active = cur_name == card['id']
        border_color = '#00C851' if is_active else '#2a3a4a'
        bg_color = '#0a1a10' if is_active else '#0d1117'
        count = card['ticker_count']()
        with col:
            st.markdown(
                f"<div style='background:{bg_color};border:2px solid {border_color};"
                f"border-radius:10px;padding:12px 10px;min-height:160px;position:relative'>"
                f"<div style='font-size:1.3rem'>{card['icon']}</div>"
                f"<div style='font-size:0.82rem;font-weight:700;color:#eee;margin:4px 0'>{card['title']}</div>"
                f"<div style='font-size:0.68rem;color:#888;line-height:1.4;margin-bottom:6px'>{card['desc']}</div>"
                f"<div style='display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px'>"
                f"<span style='color:{card['speed_color']};font-size:0.68rem;font-weight:bold'>{card['speed']}</span>"
                f"<span style='background:#1a2a3a;color:#7EC8E3;padding:1px 6px;border-radius:6px;font-size:0.65rem'>"
                f"~{count} tickers</span>"
                f"</div>"
                f"{'<div style=\"background:#003a18;color:#00C851;font-size:0.63rem;padding:1px 6px;border-radius:6px;display:inline\">✅ 已啟用</div>' if is_active else ''}"
                f"</div>",
                unsafe_allow_html=True
            )
            if st.button(f"套用 {card['icon']}", key=f"apply_universe_{card['id']}", use_container_width=True):
                st.session_state['_universe_card_selected'] = card['id']
                st.session_state['_universe_apply_pending'] = card['id']

    # ── Manual input section ──────────────────────────────────────────────
    st.markdown('---')
    st.markdown('### ✏️ 手動輸入 / CSV 設定')
    tab_manual, tab_csv = st.tabs(['✏️ 手動輸入 Ticker', '📁 CSV 上載'])

    with tab_manual:
        manual_text = st.text_area(
            '輸入 Ticker（用逗號或換行分隔）：',
            value=st.session_state.get('_manual_input_text', ''),
            height=120,
            placeholder='例如:\nNVDA, AAPL, TSLA, AMD\nMSFT\nAMZN',
            key='manual_ticker_textarea',
        )
        if manual_text:
            parsed = parse_manual_tickers(manual_text)
            st.session_state['manual_tickers_parsed'] = parsed
            st.session_state['_manual_input_text'] = manual_text
            if parsed:
                st.markdown(
                    f"<div style='font-size:0.75rem;color:#00C851;margin-bottom:4px'>"
                    f"✅ 解析到 <b>{len(parsed)}</b> 隻 Ticker</div>",
                    unsafe_allow_html=True
                )
                st.markdown(
                    ' '.join(
                        f"<span style='background:#1A2A3A;color:#4FC3F7;padding:1px 6px;"
                        f"border-radius:4px;font-size:0.7rem;margin:2px;display:inline-block'>{t}</span>"
                        for t in parsed[:30]
                    ) + (f"<span style='color:#666;font-size:0.7rem'> ... +{len(parsed)-30} 隻</span>" if len(parsed) > 30 else ''),
                    unsafe_allow_html=True
                )

        if st.button('✏️ 套用手動清單', key='apply_manual_tickers',
                     disabled=not st.session_state.get('manual_tickers_parsed')):
            st.session_state['_universe_apply_pending'] = '手動輸入'

    with tab_csv:
        uploaded_file = st.file_uploader(
            '上載 CSV 檔案（需包含 ticker 或 symbol 欄，或使用第一欄）',
            type=['csv'],
            key='universe_csv_uploader'
        )
        if uploaded_file is not None:
            try:
                import io
                csv_df = pd.read_csv(io.BytesIO(uploaded_file.read()))
                # Try to find ticker column
                col_map = {c.lower(): c for c in csv_df.columns}
                ticker_col = (
                    col_map.get('ticker') or col_map.get('symbol') or
                    col_map.get('tickers') or col_map.get('symbols') or
                    csv_df.columns[0]
                )
                raw_tickers = csv_df[ticker_col].astype(str).tolist()
                parsed_csv = parse_manual_tickers(','.join(raw_tickers))
                st.session_state['csv_tickers_parsed'] = parsed_csv
                st.success(f'✅ CSV 解析成功：{len(parsed_csv)} 隻 Ticker，來自欄位「{ticker_col}」')
                if parsed_csv:
                    st.markdown(
                        ' '.join(
                            f"<span style='background:#1A2A3A;color:#4FC3F7;padding:1px 6px;"
                            f"border-radius:4px;font-size:0.7rem;margin:2px;display:inline-block'>{t}</span>"
                            for t in parsed_csv[:20]
                        ) + (f"<span style='color:#666;font-size:0.7rem'> ... +{len(parsed_csv)-20} 隻</span>" if len(parsed_csv) > 20 else ''),
                        unsafe_allow_html=True
                    )
            except Exception as e:
                st.error(f'⚠️ CSV 解析失敗: {e}')

        if st.button('📁 套用 CSV 清單', key='apply_csv_tickers',
                     disabled=not st.session_state.get('csv_tickers_parsed')):
            st.session_state['_universe_apply_pending'] = 'CSV 上載'

    # ── Max ticker limit control ──────────────────────────────────────────
    st.markdown('---')
    st.markdown('### ⚙️ 掃描設定')
    col_set1, col_set2, col_set3 = st.columns(3)
    with col_set1:
        max_scan_limit = st.number_input(
            '最多掃描 Ticker 數量（避免 yfinance 速率限制）',
            min_value=10, max_value=500, value=150, step=10,
            key='universe_max_scan_limit',
            help='超過 100 隻建議關閉新聞情緒以加快速度'
        )
    with col_set2:
        news_toggle = st.checkbox(
            '開啟新聞情緒分析',
            value=st.session_state.get('universe_news_enabled', True),
            key='universe_news_toggle',
            help='廣泛掃描（>100 隻）建議關閉，否則會很慢'
        )
    with col_set3:
        st.markdown('<br>', unsafe_allow_html=True)
        if st.button('🔄 重設至主題精選池', key='reset_to_theme_pool', use_container_width=True):
            st.session_state['active_universe_name'] = '主題精選池'
            st.session_state['active_universe_tickers'] = get_theme_pool_tickers()
            st.session_state['universe_news_enabled'] = True
            st.success('✅ 已重設至主題精選池')
            st.rerun()

    # ── Apply pending universe ────────────────────────────────────────────
    pending = st.session_state.pop('_universe_apply_pending', None)
    if pending:
        _apply_universe(pending, max_scan_limit, news_toggle)
        st.rerun()

    # Also apply news toggle update
    if st.session_state.get('universe_news_toggle') != st.session_state.get('universe_news_enabled'):
        st.session_state['universe_news_enabled'] = news_toggle

    # ── Data health warning ──────────────────────────────────────────────
    st.markdown('---')
    _n = len(cur_tickers)
    if _n > 200:
        st.warning(
            f'⚠️ **數據健康警告**：當前池有 {_n} 隻股票。yfinance 免費批量下載在超過 200 隻時容易遭到速率限制（429 錯誤）。'
            '建議：① 限制最多掃描數量 ② 關閉新聞情緒 ③ 使用分段掃描 ④ 或考慮 Polygon.io / Tiingo 等專業 API。'
        )
    elif _n > 100:
        st.info(
            f'ℹ️ 當前池有 {_n} 隻股票。建議關閉新聞情緒分析，或設定最多掃描 ≤ 100 隻以保持速度。'
        )

    # ── 點樣掃得快啲 panel ────────────────────────────────────────────────
    st.markdown('---')
    st.markdown('### ⚡ 點樣掃得快啲 — 加速掃描指南')
    _render_faster_scanning_guide()


def _apply_universe(universe_id, max_scan_limit, news_enabled):
    """Apply the selected universe to session state."""
    _init_universe_session_state()

    if universe_id == '主題精選池':
        tickers = get_theme_pool_tickers()
        st.session_state['active_universe_name'] = '主題精選池'
        st.session_state['active_universe_tickers'] = tickers
        st.session_state['universe_news_enabled'] = True
        st.success(f'✅ 已切換至主題精選池（{len(tickers)} 隻精選股票）')

    elif universe_id == 'S&P 500':
        with st.spinner('📡 從 Wikipedia 抓取 S&P 500 清單...'):
            tickers, msg = fetch_sp500_tickers()
        tickers = tickers[:max_scan_limit]
        st.session_state['active_universe_name'] = 'S&P 500'
        st.session_state['active_universe_tickers'] = tickers
        st.session_state['universe_news_enabled'] = news_enabled
        st.success(f'✅ 已切換至 S&P 500（{msg}，採用 {len(tickers)} 隻）')

    elif universe_id == 'Nasdaq 100':
        with st.spinner('📡 從 Wikipedia 抓取 Nasdaq-100 清單...'):
            tickers, msg = fetch_nasdaq100_tickers()
        tickers = tickers[:max_scan_limit]
        st.session_state['active_universe_name'] = 'Nasdaq 100'
        st.session_state['active_universe_tickers'] = tickers
        st.session_state['universe_news_enabled'] = news_enabled
        st.success(f'✅ 已切換至 Nasdaq 100（{msg}，採用 {len(tickers)} 隻）')

    elif universe_id == '手動輸入':
        tickers = st.session_state.get('manual_tickers_parsed', [])
        tickers = tickers[:max_scan_limit]
        if not tickers:
            st.error('⚠️ 手動清單為空，請先輸入 Ticker。')
            return
        st.session_state['active_universe_name'] = '手動輸入'
        st.session_state['active_universe_tickers'] = tickers
        st.session_state['universe_news_enabled'] = news_enabled
        st.success(f'✅ 已套用手動清單（{len(tickers)} 隻）')

    elif universe_id == 'CSV 上載':
        tickers = st.session_state.get('csv_tickers_parsed', [])
        tickers = tickers[:max_scan_limit]
        if not tickers:
            st.error('⚠️ CSV 清單為空，請先上載 CSV 檔案。')
            return
        st.session_state['active_universe_name'] = 'CSV 上載'
        st.session_state['active_universe_tickers'] = tickers
        st.session_state['universe_news_enabled'] = news_enabled
        st.success(f'✅ 已套用 CSV 清單（{len(tickers)} 隻）')


def _render_faster_scanning_guide():
    """Render the faster scanning educational panel."""
    st.markdown(
        "<div style='background:#0a1020;border:1px solid #2a3a5a;border-radius:10px;padding:16px;margin-bottom:12px'>"
        "<div style='font-size:0.92rem;font-weight:700;color:#4FC3F7;margin-bottom:10px'>"
        "🚀 六大加速方法 — 點解依個 App 掃唔完全市場 8,000+ 美股？</div>"
        "<div style='font-size:0.78rem;color:#aaa;line-height:1.6'>",
        unsafe_allow_html=True
    )

    tips = [
        {
            'icon': '1️⃣',
            'title': '兩階段掃描 (Two-Stage Scan)',
            'color': '#00C851',
            'body': (
                '第一階段：用 yfinance 批量下載所有 Ticker 嘅價格/成交量，快速篩走 RS 差同無動能嘅股票，'
                '縮短至 20-50 隻候選。'
                '第二階段：只對候選股做新聞/情緒分析（最慢嘅部分）。'
                '→ 本 App 已採用此邏輯：「情緒分析」只係喺板塊候選池上做，唔係全市場。'
            ),
        },
        {
            'icon': '2️⃣',
            'title': '批量 yfinance 下載 (Batch Download)',
            'color': '#4FC3F7',
            'body': (
                '唔好逐隻 ticker 叫 yfinance.Ticker(t).history()，'
                '應該用 yf.download([list of tickers], period="6mo") 一次過下載。'
                '本 App 嘅 fetch_bulk_ohlcv() 同 fetch_catalyst_rs_data() 已實現批量下載。'
                '100 隻 ticker 批量 vs 逐隻可節省 5-10 倍時間。'
            ),
        },
        {
            'icon': '3️⃣',
            'title': 'Streamlit 快取 (Cache EOD Prices)',
            'color': '#F9A825',
            'body': (
                '本 App 用 @st.cache_data(ttl=3600) 快取所有 yfinance 數據（1小時），'
                '唔係每次互動都重新抓取。'
                '更佳做法：用 CSV/Parquet 本地快取每日收盤價，只係盤前更新一次，'
                '日內所有請求讀快取。'
            ),
        },
        {
            'icon': '4️⃣',
            'title': '預計算夜間批次 (Precompute Nightly)',
            'color': '#FF8C00',
            'body': (
                '終極方案：每晚 00:00 跑一次 Python 腳本，'
                '批量下載全市場/自選池嘅 OHLCV、計算 RS/MA/成交量訊號，'
                '結果存入 CSV 或 Parquet 檔案。'
                'Streamlit App 只係讀預計算結果，唔做實時計算。'
                '掃 500 隻股票只需數秒而非分鐘。'
            ),
        },
        {
            'icon': '5️⃣',
            'title': '專業 API / 本地數據庫',
            'color': '#CE93D8',
            'body': (
                '免費 yfinance 有速率限制，不適合全市場 8,000+ 掃描。'
                '可考慮以下方案（各有取捨）：\n'
                '• Polygon.io — 美股全量實時/歷史數據，免費層有限額\n'
                '• Tiingo — EOD 數據 API，免費層500 隻/日\n'
                '• Financial Modeling Prep (FMP) — 財報+價格，免費250次/日\n'
                '• Alpaca Markets — 免費股票數據 API（需帳戶）\n'
                '• Stooq / NasdaqTrader — 可下載歷史 CSV，本地儲存\n'
                '• Interactive Brokers API — 需帳戶，但數據完整'
            ),
        },
        {
            'icon': '6️⃣',
            'title': 'DuckDB / Polars 本地向量化掃描',
            'color': '#FF6B6B',
            'body': (
                '若數據已本地化（Parquet格式），用 DuckDB SQL 或 Polars（比 pandas 快 5-20x）'
                '做向量化篩選，可喺毫秒內過濾 8,000+ 股票。'
                '配合預計算夜間批次，係最快嘅全市場方案。'
                '參考：duckdb.org 同 pola.rs'
            ),
        },
    ]

    st.markdown('</div></div>', unsafe_allow_html=True)

    for tip in tips:
        with st.expander(f"{tip['icon']} {tip['title']}", expanded=False):
            st.markdown(
                f"<div style='font-size:0.79rem;color:#ccc;line-height:1.65;white-space:pre-line'>{tip['body']}</div>",
                unsafe_allow_html=True
            )

    # Performance comparison table
    st.markdown('#### 📊 掃描方案速度比較')
    st.markdown("""
| 方案 | 典型速度 | 適合場景 | 注意事項 |
|------|----------|----------|----------|
| 主題精選池（~70隻）| **5-15 秒** | 日常快速輪動分析 | 本 App 預設 |
| S&P 500（批量）| **30-90 秒** | 大盤廣泛掃描 | 關閉新聞情緒 |
| Nasdaq 100 | **15-45 秒** | 科技股分析 | 關閉新聞情緒 |
| 全市場 8,000+（yfinance）| **30-120 分鐘** | 不推薦實時用 | 容易 rate limit |
| 預計算 Parquet + DuckDB | **< 1 秒讀取** | 最佳全市場方案 | 需夜間批次腳本 |
| Polygon.io / Tiingo API | **5-20 秒** | 專業全市場掃描 | 需付費/帳戶 |

> ⚠️ **免責聲明**：所有速度估算為參考值，實際取決於網絡狀況、API 可用性及快取狀態。
    """)

    # ── Fast Mode scanner.py info card ────────────────────────────────────
    st.markdown('---')
    st.markdown('### ⚡ Fast Mode — scanner.py 每日盤後預掃描')
    st.markdown("""
`scanner.py` 係本系統附帶嘅獨立掃描腳本，每日盤後跑一次，將結果存入 Parquet，
Streamlit App 只需讀取快取，唔做任何即時 yfinance 廣義掃描。

**快速上手：**
```bash
# 最小測試（3隻）
python scanner.py --tickers "AAPL,MSFT,SPY" --max-tickers 3

# 預設精選池（約80隻）
python scanner.py

# 自定義 Universe
python scanner.py --universe-file my_tickers.csv --benchmark SPY

# 大型掃描（500隻）
python scanner.py --universe-file sp500.csv --max-tickers 500
```

**關鍵優點：**
- MongoDB **唔係必須**：Parquet + Pandas 係免費輕量方案
- DuckDB + Parquet 比 MongoDB 更適合股票篩選
- Streamlit 只讀快取，< 1 秒載入
- 真正 8,000+ 全市場：夜間排程 (cron) 跑 scanner.py
    """)


# ── Universe selector compact control (for use inside radar module) ────────
def _render_universe_selector_compact():
    """Render a compact universe selector bar for use at the top of radar module."""
    _init_universe_session_state()
    cur_name = st.session_state.get('active_universe_name', '主題精選池')
    cur_tickers = st.session_state.get('active_universe_tickers', [])
    news_enabled = st.session_state.get('universe_news_enabled', True)

    col_u1, col_u2, col_u3 = st.columns([3, 2, 1])
    with col_u1:
        st.markdown(
            f"<div style='background:#0a1520;border:1px solid #2a4a6b;border-radius:8px;"
            f"padding:8px 12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap'>"
            f"<span style='font-size:0.78rem;color:#aaa'>🌐 掃描池：</span>"
            f"<span style='background:#003a5a;color:#4FC3F7;padding:2px 9px;border-radius:6px;"
            f"font-size:0.8rem;font-weight:bold'>{cur_name}</span>"
            f"<span style='color:#888;font-size:0.78rem'>{len(cur_tickers)} 隻 Ticker</span>"
            f"<span style='color:{'#00C851' if news_enabled else '#888'};font-size:0.72rem'>"
            f"{'✅ 新聞開' if news_enabled else '❌ 新聞關'}</span>"
            f"</div>",
            unsafe_allow_html=True
        )
    with col_u2:
        st.markdown(
            "<div style='font-size:0.72rem;color:#666;padding:8px 0'>"
            "⚙️ 如需更換掃描池，請至側欄選擇「🗂️ Universe Manager」</div>",
            unsafe_allow_html=True
        )
    with col_u3:
        if st.button('🔄 重設池', key='radar_reset_universe', use_container_width=True,
                     help='重設至主題精選池'):
            st.session_state['active_universe_name'] = '主題精選池'
            st.session_state['active_universe_tickers'] = get_theme_pool_tickers()
            st.session_state['universe_news_enabled'] = True
            st.rerun()


# ==========================================
# Sidebar
# ==========================================
with st.sidebar:
    # ── App title ──
    st.markdown(
        "<div style='padding:8px 0 4px'>"
        "<span style='font-size:1.1rem;font-weight:800;letter-spacing:0.5px'>"
        "🚀 美股全方位量化平台</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    # ── Module navigation ──
    st.markdown(
        "<div style='font-size:0.7rem;font-weight:700;color:#888;margin:4px 0 2px'>"
        "🗂️ 功能模組</div>",
        unsafe_allow_html=True,
    )
    app_mode = st.radio(
        label='模組',
        options=[
            '🗂️ Universe Manager',
            '🔥 當炒/低風險機會雷達',
            '📡 市場實時雷達',
            '🔥 熱門板塊關係圖',
            '🎯 產業故事 Radar / Scorecard',
            '📡 新聞催化劑 / RS 方法選股',
            '🎯 RS x MACD 動能狙擊手',
            '📰 近月 AI 洞察 (廣東話版)',
            '🕵️ 另類數據雷達 (6大維度)',
        ],
        label_visibility='collapsed',
    )

    _divider()

    # ── Market Radar panel (lazy: opt-in to keep first paint fast) ──
    # Initial cold load on Streamlit Cloud was hanging because the sidebar
    # synchronously fetched ~9 FRED CSVs + a yfinance batch (~16 tickers)
    # *before* anything could render. We now gate those calls behind a button.
    if 'sidebar_market_loaded' not in st.session_state:
        st.session_state.sidebar_market_loaded = False

    if st.session_state.sidebar_market_loaded:
        render_sidebar_market_panel()
        _divider()
        try:
            _sb_m = fetch_sidebar_market_data()
            render_sidebar_employment_expander(_sb_m)
            render_sidebar_macro_expander(_sb_m)
            _divider()
            render_macro_signal_sidebar(_sb_m)
        except Exception as _e:
            st.caption(f'⚠️ 宏觀數據載入失敗: {_e}')
    else:
        st.markdown(
            "<div style='font-size:0.72rem;color:#888;padding:6px 0'>"
            "📡 市場實時雷達 / 宏觀面板已暫停載入以加快首次開啟。"
            "</div>",
            unsafe_allow_html=True,
        )
        if st.button('📡 載入市場 / 宏觀數據', use_container_width=True, key='load_sidebar_mkt'):
            st.session_state.sidebar_market_loaded = True
            st.rerun()

# ==========================================
# 模組渲染
# ==========================================
if app_mode == '🗂️ Universe Manager':
    render_universe_manager_module()

elif app_mode == '🔥 當炒/低風險機會雷達':
    render_radar_module()

elif app_mode == '📡 市場實時雷達':
    render_market_real_time_radar_page()

elif app_mode == '🎯 產業故事 Radar / Scorecard':
    render_market_radar_module()

elif app_mode == '📡 新聞催化劑 / RS 方法選股':
    render_catalyst_screener_module()

elif app_mode == '🎯 RS x MACD 動能狙擊手':
    st.title('🎯 美股 RS x MACD x 趨勢 狙擊手')

    # ========= 參數設定 =========
    with st.expander('⚙️ 展開設定篩選參數', expanded=True):
        col1, col2, col3 = st.columns(3)

        # 1) 基礎與趨勢
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

        # 2) RS 動能
        with col2:
            st.markdown('#### 2️⃣ RS 動能')
            enable_rs = st.checkbox('啟動 【RS】 過濾', value=True)
            selected_rs = st.multiselect(
                '顯示 RS 階段:',
                ['🚀 啱啱突破', '🔥 已經突破', '🎯 就快突破 (<5%)'],
                default=['🚀 啱啱突破']
            ) if enable_rs else []

        # 3) MACD 爆發點
        with col3:
            st.markdown('#### 3️⃣ MACD 爆發點')
            enable_macd = st.checkbox('啟動 【MACD】 過濾', value=True)
            selected_macd = st.multiselect(
                '顯示 MACD 階段:',
                ['🚀 啱啱突破', '🔥 已經突破', '🎯 就快突破 (<5%)'],
                default=['🚀 啱啱突破']
            ) if enable_macd else []

        start_scan = st.button('🚀 開始全市場精確掃描', use_container_width=True, type='primary')

    # ========= 掃描流程 =========
    if start_scan:
        status_text, progress_bar = st.empty(), st.progress(0)
        status_text.markdown('**階段 1/3**: 搵緊 Finviz 基礎股票名單...')

        raw_data = fetch_finviz_data()
        progress_bar.progress(100)

        if not raw_data.empty:
            # 1) 基礎過濾：市值
            df_p = raw_data.copy()
            df_p['Mcap_Numeric'] = df_p['Market Cap'].apply(convert_mcap_to_float)
            final_df = df_p[df_p['Mcap_Numeric'] >= min_mcap].copy()

            if enable_rs or enable_macd or enable_sma:
                # 2) 技術指標計算
                progress_bar.progress(0)
                indicators = calculate_all_indicators(
                    final_df['Ticker'].tolist(),
                    sma_short, sma_long, close_condition,
                    _progress_bar=progress_bar,
                    _status_text=status_text
                )

                final_df['RS_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('RS', '無'))
                final_df['MACD_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('MACD', '無'))
                final_df['SMA多頭'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('SMA_Trend', False))

                # 3) 根據 UI 選項過濾
                if enable_sma:
                    final_df = final_df[final_df['SMA多頭'] == True]
                if enable_rs:
                    final_df = final_df[final_df['RS_階段'].isin(selected_rs)]
                if enable_macd:
                    final_df = final_df[final_df['MACD_階段'].isin(selected_macd)]

                if len(final_df) > 0:
                    # 4) 抓取基本面 + Sales
                    progress_bar.progress(0)
                    fund_df = fetch_fundamentals(
                        final_df['Ticker'].tolist(),
                        _progress_bar=progress_bar,
                        _status_text=status_text
                    )
                    final_df = pd.merge(final_df, fund_df, on='Ticker', how='left')

                    # ========= Sales 欄位處理 =========
                    def _fmt_sales(val):
                        try:
                            if pd.isna(val):
                                return 'N/A'
                            s = str(val).upper().replace(' ', '')
                            # 假設從 Finviz 來的是類似 "23.4B"、"850M"
                            if s.endswith('B') or s.endswith('M'):
                                return f"${s}"
                            return s
                        except:
                            return str(val)

                    if 'Sales' in final_df.columns:
                        final_df['Sales'] = final_df['Sales'].apply(_fmt_sales)

                    status_text.markdown('✅ **全市場掃描搞掂！**')
                    progress_bar.progress(100)
                    st.success(f'成功搵到 {len(final_df)} 隻潛力股票。')

                    # 5) 顯示結果表（已加入 Sales 欄位）
                    cols = ['Ticker'] + [
                        c for c in [
                            'RS_階段',
                            'MACD_階段',
                            'Company',
                            'Sector',
                            'Market Cap',
                            'Sales',                 # 🆕 新增營收欄位
                            'EPS (近4季)',
                            'EPS Growth (QoQ)',
                            'Sales Growth (QoQ)',
                        ]
                        if c in final_df.columns
                    ]
                    st.dataframe(final_df[cols], use_container_width=True, hide_index=True)
                else:
                    status_text.markdown('✅ **全市場掃描搞掂！**')
                    progress_bar.progress(100)
                    st.warning('⚠️ 搵唔到完全滿足條件嘅股票。')
        else:
            st.warning("⚠️ 暫時攞唔到 Finviz 股票清單。")

elif app_mode == '📰 近月 AI 洞察 (廣東話版)':
    st.title('📰 近月 AI 新聞深度分析')
    if st.button('🚀 攞今日 AI 報告', type='primary', use_container_width=True):
        with st.spinner('⏳ 嘗試緊從多個渠道 (Finviz/Yahoo/RSS) 攞歷史財經頭條同摘要...'):
            news_list = fetch_top_news()
        st.caption(f"已抓取新聞數量: {len(news_list)}")
        if news_list:
            with st.expander("🔎 Debug: 查看原始新聞抓取資料"):
                st.write(news_list[:8])
            with st.spinner('🧠 AI 認真睇緊內文，為你撰寫市場焦點...'):
                report = final_text_sanitize(analyze_news_ai(news_list))
                st.markdown('### 🤖 華爾街 AI 深度洞察報告')
                with st.container(border=True):
                    st.markdown(report)
        else:
            st.warning("⚠️ 所有資料源 (包括 RSS 備援) 暫時失效，無法抓取新聞。")

elif app_mode == '🕵️ 另類數據雷達 (6大維度)':
    st.title('🕵️ 另類數據雷達 (6大維度)')
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('**1. Reddit WSB 討論熱度 (Top 10)**')
        r_df, r_msg = fetch_reddit_sentiment()
        st.caption(r_msg); st.dataframe(r_df.head(10), use_container_width=True, hide_index=True)
    with c2:
        st.markdown('**2. StockTwits 全美熱搜榜 (Top 10)**')
        t_df, t_msg = fetch_stocktwits_trending()
        st.caption(t_msg); st.dataframe(t_df.head(10), use_container_width=True, hide_index=True)
    c3, c4 = st.columns(2)
    with c3:
        st.markdown('**3. X / FinTwit 社交情緒熱度 (Top 10)**')
        x_df, x_msg = fetch_x_sentiment()
        st.caption(x_msg); st.dataframe(x_df.head(10), use_container_width=True, hide_index=True)
    with c4:
        st.markdown('**4. 高層 Insider 真金白銀買入 (全市場)**')
        with st.spinner('🔍 抓取全市場真實 Insider 買入數據...'):
            i_df, i_msg = fetch_insider_buying()
        st.caption(i_msg)
        st.dataframe(i_df.head(10), use_container_width=True, hide_index=True)
    c5, c6 = st.columns(2)
    with c5:
        st.markdown('**5. 國會議員交易 (QuiverQuant 真實數據)**')
        with st.spinner('🔍 從 QuiverQuant 抓取真實國會交易數據...'):
            c_df, c_msg = fetch_congress_trades()
        st.caption(c_msg); st.dataframe(c_df.head(10), use_container_width=True, hide_index=True)
    with c6:
        st.markdown('**6. 日本 2ch/5ch 海外散戶情緒**')
        jp_df, jp_msg = fetch_5ch_sentiment()
        st.caption(jp_msg); st.dataframe(jp_df.head(10), use_container_width=True, hide_index=True)
    if st.button('🚀 啟動 AI 六維交叉博弈分析', type='primary', use_container_width=True):
        with st.spinner('🧠 AI 正在進行多維度深度分析...'):
            res = final_text_sanitize(analyze_alt_data_ai(r_df, t_df, x_df, i_df, c_df))
            st.markdown('### 🤖 另類數據 AI 偵測深度報告')
            with st.container(border=True):
                st.markdown(res)

elif app_mode == '🔥 熱門板塊關係圖':
    render_hot_sectors_module()

