
import os, re, json, time, random, datetime, requests
import pandas as pd
import streamlit as st
import yfinance as yf
import concurrent.futures
from datetime import timedelta
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance

st.set_page_config(page_title='🚀 美股全方位量化與 AI 平台', page_icon='📈', layout='wide')

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

def call_pollinations(messages, model='openai-fast', timeout=60):
    try:
        r = requests.post('https://text.pollinations.ai/', json={'messages': messages, 'model': model}, timeout=timeout)
        return final_text_sanitize(r.text)
    except Exception as e:
        return f"⚠️ AI 發生錯誤: {e}"

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

@st.cache_data(ttl=3600, show_spinner=False)
def _robust_json_parse(raw):
    """強力 JSON 解析，容錯多種格式"""
    import re, json
    if not raw or not isinstance(raw, str):
        return None
    # 過濾 HTML 錯誤頁
    if raw.strip().startswith('<!DOCTYPE') or raw.strip().startswith('<html'):
        return None
    # 嘗試多種 pattern
    patterns = [
        r'```json\s*(.*?)\s*```',
        r'```\s*([\[{].*?[}\]])\s*```',
        r'([\[{][\s\S]*[}\]])',
    ]
    for p in patterns:
        m = re.search(p, raw, re.DOTALL)
        if m:
            chunk = m.group(1).strip()
            # 修復常見 JSON 錯誤：末尾多餘逗號
            chunk = re.sub(r',\s*([}\]])', r'\1', chunk)
            try:
                return json.loads(chunk)
            except:
                pass
    # 直接 parse 全文
    text = raw.strip()
    text = re.sub(r',\s*([}\]])', r'\1', text)
    try:
        return json.loads(text)
    except:
        pass
    return None

@st.cache_data(ttl=3600, show_spinner=False)
def ai_generate_sectors_only(headlines: str):
    """呼叫 1: 只生成板塊清單（簡單JSON，成功率高）"""
    sys_p = """你係美股板塊專家。根據新聞，返回今日最熱門的5-6個板塊。
只返回 JSON array，格式：
[{"name":"板塊名","emoji":"emoji","desc":"描述","stocks":{"TICKER":"公司名"}}]
每個板塊包含5-6隻代表股票。只返回JSON，不要其他文字。"""
    raw = call_pollinations([
        {'role': 'system', 'content': sys_p},
        {'role': 'user',   'content': f"新聞：\n{headlines}"}
    ], timeout=50)
    data = _robust_json_parse(raw)
    if isinstance(data, list) and len(data) >= 3:
        return data, True
    return None, False

@st.cache_data(ttl=3600, show_spinner=False)
def ai_generate_relations_only(headlines: str, ticker_list: str):
    """呼叫 2: 只生成公司關係（獨立，唔依賴板塊結果）"""
    sys_p = """你係美股分析師。根據新聞及以下股票清單，生成公司間關係。
只返回 JSON array，格式：
[{"company_a":"TICKER","company_b":"TICKER","type":"合作","desc":"描述20字內","strength":"強"}]
type 只可以係：合作、供應商、客戶、競爭、投資
生成15-20條關係，涵蓋 AI晶片、數據中心、能源、安全等熱門領域。只返回JSON。"""
    raw = call_pollinations([
        {'role': 'system', 'content': sys_p},
        {'role': 'user',   'content': f"新聞：\n{headlines}\n\n相關股票：{ticker_list}"}
    ], timeout=60)
    data = _robust_json_parse(raw)
    if isinstance(data, list) and len(data) >= 5:
        return data, True
    return None, False

def ai_generate_company_relations(headlines: str):
    """
    拆成兩次獨立 AI 呼叫，提高成功率：
    呼叫1: 板塊清單
    呼叫2: 公司關係
    """
    # 固定常用 ticker 列表給關係生成參考
    default_tickers = "NVDA,AMD,MSFT,GOOGL,AMZN,META,AAPL,PLTR,VRT,SMCI,MU,AVGO,ARM,CRWD,PANW,CEG,VST,CCJ,RKLB,ASTS,TSLA,SNOW,DDOG,CRM,NET"

    sectors, sec_ok   = ai_generate_sectors_only(headlines)
    relations, rel_ok = ai_generate_relations_only(headlines, default_tickers)

    result = {}
    if sec_ok:
        result['sectors'] = sectors
    if rel_ok:
        result['relations'] = relations

    if sec_ok or rel_ok:
        return result, True
    return None, False

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
            ai_generate_sectors_only.clear()
            ai_generate_relations_only.clear()
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

    raw_sectors   = ai_data.get('sectors',   []) if ai_data else []
    raw_relations = ai_data.get('relations', []) if ai_data else []

    sector_stocks = build_sector_stocks_from_ai(raw_sectors) if raw_sectors else build_sector_stocks_fallback()
    relations     = raw_relations if raw_relations else FALLBACK_RELATIONS

    sec_src = f"AI識別 {len(sector_stocks)} 個板塊" if raw_sectors else "預設板塊"
    rel_src = f"AI生成 {len(relations)} 條關係" if raw_relations else "預設關係數據"
    st.info(f"📊 {sec_src}　｜　🔗 {rel_src}")

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

    # FRED: Unemployment Rate
    try:
        r = requests.get('https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE', headers=get_headers(), timeout=8)
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            last = lines[-1].split(',')
            data['UNRATE'] = {'date': last[0], 'value': float(last[1])}
    except:
        data['UNRATE'] = None

    # FRED: CPI YoY
    try:
        r = requests.get('https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL', headers=get_headers(), timeout=8)
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            vals = []
            for l in lines[-14:]:
                parts = l.split(',')
                if len(parts) == 2:
                    try: vals.append((parts[0], float(parts[1])))
                    except: pass
            if len(vals) >= 13:
                yoy = (vals[-1][1] - vals[-13][1]) / vals[-13][1] * 100
                data['CPI_YOY'] = {'date': vals[-1][0], 'value': round(yoy, 2)}
    except:
        data['CPI_YOY'] = None

    # FRED: Fed Funds Rate
    try:
        r = requests.get('https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDFUNDS', headers=get_headers(), timeout=8)
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            last = lines[-1].split(',')
            data['FEDFUNDS'] = {'date': last[0], 'value': float(last[1])}
    except:
        data['FEDFUNDS'] = None

    # FRED: Initial Jobless Claims
    try:
        r = requests.get('https://fred.stlouisfed.org/graph/fredgraph.csv?id=ICSA', headers=get_headers(), timeout=8)
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            last = lines[-1].split(',')
            data['JOBLESS'] = {'date': last[0], 'value': int(float(last[1]))}
    except:
        data['JOBLESS'] = None

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

def render_sidebar_market_panel():
    st.markdown("### 📡 市場實時雷達")
    with st.spinner("載入..."):
        m = fetch_sidebar_market_data()

    # ── Fear & Greed ──
    fg = m.get('FEAR_GREED', {})
    if fg and fg.get('score') is not None:
        score = fg['score']
        rating = fg.get('rating', 'N/A').upper()
        if score >= 75:   fg_emoji, fg_c = '🤑', '#e74c3c'
        elif score >= 55: fg_emoji, fg_c = '😊', '#e67e22'
        elif score >= 45: fg_emoji, fg_c = '😐', '#f1c40f'
        elif score >= 25: fg_emoji, fg_c = '😨', '#3498db'
        else:             fg_emoji, fg_c = '😱', '#2980b9'
        st.markdown(
            f"<div style='background:{fg_c}22;border-left:3px solid {fg_c};border-radius:5px;padding:5px 8px;margin-bottom:5px'>"
            f"<span style='font-size:0.72rem;color:{fg_c}'><b>CNN 恐貪指數</b></span><br>"
            f"<span style='font-size:1.15rem'><b>{fg_emoji} {score:.0f}</b></span> "
            f"<span style='font-size:0.68rem;color:{fg_c}'>{rating}</span></div>", unsafe_allow_html=True)
    else:
        st.caption("😐 CNN 恐貪指數: 暫無數據")

    # ── VIX ──
    vix = m.get('VIX', {})
    if vix and vix.get('price') is not None:
        v, pct = vix['price'], vix['pct']
        if v >= 30:   vl, vc = '極度恐慌', '#e74c3c'
        elif v >= 20: vl, vc = '市場緊張', '#e67e22'
        else:         vl, vc = '市場平靜', '#2ecc71'
        arrow = '▲' if pct >= 0 else '▼'
        st.markdown(
            f"<div style='background:{vc}22;border-left:3px solid {vc};border-radius:5px;padding:5px 8px;margin-bottom:5px'>"
            f"<span style='font-size:0.72rem;color:{vc}'><b>VIX 恐慌指數</b></span><br>"
            f"<span style='font-size:1.1rem'><b>{v:.2f}</b></span> "
            f"<span style='font-size:0.68rem;color:{vc}'>{vl} {arrow}{abs(pct):.1f}%</span></div>", unsafe_allow_html=True)
    else:
        st.caption("VIX: N/A")

    _divider()

    # ── 美股三大指數 ──
    _section("🇺🇸 美股三大指數")
    _pm("S&P500 (SPY)", "📈", m.get('SPY'))
    _pm("納指 (QQQ)", "💻", m.get('QQQ'))
    _pm("道指 (DIA)", "🏛️", m.get('DIA'))
    _divider()

    # ── 商品 ──
    _section("🛢️ 商品市場")
    _pm("WTI 原油 (USD)", "🛢️", m.get('OIL'), fmt='${:.2f}')
    _pm("黃金 (USD/oz)", "🥇", m.get('GOLD'), fmt='${:.2f}')
    _pm("白銀 (USD/oz)", "🥈", m.get('SILVER'), fmt='${:.2f}')
    _pm("天然氣 (USD)", "🔥", m.get('NG'), fmt='${:.3f}')
    _divider()

    # ── 宏觀/債息 ──
    _section("🏦 宏觀 & 債市")
    _pm("美元指數 (DXY)", "💵", m.get('DXY'))
    _pm("10年美債息 (%)", "📉", m.get('TNX'), fmt='{:.3f}')
    _pm("30年美債息 (%)", "📉", m.get('TYX'), fmt='{:.3f}')

    # Fed Funds Rate
    ff = m.get('FEDFUNDS')
    if ff:
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;padding:2px 0'>"
            f"<span style='font-size:0.78rem'>🏛️ <b>聯儲息率</b></span>"
            f"<span style='font-size:0.78rem;color:#f1c40f'><b>{ff['value']:.2f}%</b>"
            f"<span style='font-size:0.65rem;color:#666'> ({ff['date'][:7]})</span></span></div>", unsafe_allow_html=True)

    _divider()

    # ── 就業數據 ──
    _section("👷 就業數據")
    unemp = m.get('UNRATE')
    if unemp:
        uc = '#e74c3c' if unemp['value'] > 5 else ('#e67e22' if unemp['value'] > 4 else '#2ecc71')
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;padding:2px 0'>"
            f"<span style='font-size:0.78rem'>📊 <b>失業率</b></span>"
            f"<span style='font-size:0.78rem;color:{uc}'><b>{unemp['value']:.1f}%</b>"
            f"<span style='font-size:0.65rem;color:#666'> ({unemp['date'][:7]})</span></span></div>", unsafe_allow_html=True)

    jl = m.get('JOBLESS')
    if jl:
        jc = '#e74c3c' if jl['value'] > 250000 else ('#e67e22' if jl['value'] > 220000 else '#2ecc71')
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;padding:2px 0'>"
            f"<span style='font-size:0.78rem'>📋 <b>首次申領失業</b></span>"
            f"<span style='font-size:0.78rem;color:{jc}'><b>{jl['value']:,}</b>"
            f"<span style='font-size:0.65rem;color:#666'> ({jl['date']})</span></span></div>", unsafe_allow_html=True)

    # CPI
    cpi = m.get('CPI_YOY')
    if cpi:
        cc = '#e74c3c' if cpi['value'] > 3.5 else ('#e67e22' if cpi['value'] > 2.5 else '#2ecc71')
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;padding:2px 0'>"
            f"<span style='font-size:0.78rem'>📦 <b>CPI 通脹 (YoY)</b></span>"
            f"<span style='font-size:0.78rem;color:{cc}'><b>{cpi['value']:.1f}%</b>"
            f"<span style='font-size:0.65rem;color:#666'> ({cpi['date'][:7]})</span></span></div>", unsafe_allow_html=True)

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

    _divider()
    st.caption(f"⏱ {datetime.datetime.now().strftime('%H:%M:%S')} 更新")
    if st.button("🔄 刷新數據", use_container_width=True, key="refresh_mkt_sb"):
        fetch_sidebar_market_data.clear()
        st.rerun()

# ==========================================
# Sidebar
# ==========================================
with st.sidebar:
    st.title('🧰 投資雙引擎')

    render_sidebar_market_panel()

    st.markdown('---')
    st.markdown("### 🗂️ 功能模組")
    app_mode = st.radio('選擇模組', [
        '🔥 熱門板塊關係圖',
        '🎯 RS x MACD 動能狙擊手',
        '📰 近月 AI 洞察 (廣東話版)',
        '🕵️ 另類數據雷達 (6大維度)',
        '🔍 個股驗證模式 (Bottom-Up)',
        '⚔️ 終極雙劍合璧 (Full Integration)'
    ])

# ==========================================
# 模組渲染
# ==========================================
if app_mode == '🎯 RS x MACD 動能狙擊手':
    st.title('🎯 美股 RS x MACD x 趨勢 狙擊手')
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
                close_condition = st.selectbox('額外 Close 條件', ['唔揀', 'Close > 短期 SMA', 'Close > 長期 SMA', 'Close > 短期及長期 SMA'], index=1)
            else:
                sma_short, sma_long, close_condition = 25, 125, '唔揀'
        with col2:
            st.markdown('#### 2️⃣ RS 動能')
            enable_rs = st.checkbox('啟動 【RS】 過濾', value=True)
            selected_rs = st.multiselect('顯示 RS 階段:', ['🚀 啱啱突破', '🔥 已經突破', '🎯 就快突破 (<5%)'], default=['🚀 啱啱突破']) if enable_rs else []
        with col3:
            st.markdown('#### 3️⃣ MACD 爆發點')
            enable_macd = st.checkbox('啟動 【MACD】 過濾', value=True)
            selected_macd = st.multiselect('顯示 MACD 階段:', ['🚀 啱啱突破', '🔥 已經突破', '🎯 就快突破 (<5%)'], default=['🚀 啱啱突破']) if enable_macd else []
        start_scan = st.button('🚀 開始全市場精確掃描', use_container_width=True, type='primary')
    if start_scan:
        status_text, progress_bar = st.empty(), st.progress(0)
        status_text.markdown('**階段 1/3**: 搵緊 Finviz 基礎股票名單...')
        raw_data = fetch_finviz_data()
        progress_bar.progress(100)
        if not raw_data.empty:
            df_p = raw_data.copy()
            df_p['Mcap_Numeric'] = df_p['Market Cap'].apply(convert_mcap_to_float)
            final_df = df_p[df_p['Mcap_Numeric'] >= min_mcap].copy()
            if enable_rs or enable_macd or enable_sma:
                progress_bar.progress(0)
                indicators = calculate_all_indicators(final_df['Ticker'].tolist(), sma_short, sma_long, close_condition, _progress_bar=progress_bar, _status_text=status_text)
                final_df['RS_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('RS', '無'))
                final_df['MACD_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('MACD', '無'))
                final_df['SMA多頭'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('SMA_Trend', False))
                if enable_sma: final_df = final_df[final_df['SMA多頭'] == True]
                if enable_rs: final_df = final_df[final_df['RS_階段'].isin(selected_rs)]
                if enable_macd: final_df = final_df[final_df['MACD_階段'].isin(selected_macd)]
                if len(final_df) > 0:
                    progress_bar.progress(0)
                    fund_df = fetch_fundamentals(final_df['Ticker'].tolist(), _progress_bar=progress_bar, _status_text=status_text)
                    final_df = pd.merge(final_df, fund_df, on='Ticker', how='left')
                    status_text.markdown('✅ **全市場掃描搞掂！**'); progress_bar.progress(100)
                    st.success(f'成功搵到 {len(final_df)} 隻潛力股票。')
                    cols = ['Ticker'] + [c for c in ['RS_階段', 'MACD_階段', 'Company', 'Sector', 'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales Growth (QoQ)'] if c in final_df.columns]
                    st.dataframe(final_df[cols], use_container_width=True, hide_index=True)
                else:
                    status_text.markdown('✅ **全市場掃描搞掂！**'); progress_bar.progress(100)
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

elif app_mode == '🔍 個股驗證模式 (Bottom-Up)':
    st.title('🔍 個股驗證模式 (Bottom-Up)')
    target_ticker = st.text_input("輸入美股代號 (例如 TSLA, NVDA):").upper().strip()
    if st.button('🧠 立即驗證', type='primary') and target_ticker:
        with st.spinner(f'抓取緊 {target_ticker} 嘅最新新聞並交由 AI 分析...'):
            news = fetch_single_stock_news(target_ticker)
        if news:
            res = final_text_sanitize(analyze_single_stock_sentiment(target_ticker, news))
            st.subheader(f"📊 {target_ticker} 驗證結果")
            lines = [x.strip() for x in res.split('\n') if x.strip()]
            if lines:
                st.markdown(f"### {lines[0]}")
                with st.container(border=True):
                    st.markdown(final_text_sanitize('\n\n'.join(lines[1:]) if len(lines) > 1 else '暫無補充。'))
            else:
                with st.container(border=True): st.markdown(res)
            with st.expander("📄 查看 AI 參考嘅原始新聞"):
                for n in news: st.caption(n)
        else:
            st.warning(f"⚠️ 搵唔到 {target_ticker} 嘅近期新聞。")

elif app_mode == '🔥 熱門板塊關係圖':
    render_hot_sectors_module()

elif app_mode == '⚔️ 終極雙劍合璧 (Full Integration)':
    st.title('⚔️ 終極雙劍合璧 (Full Integration)')
    st.info("💡 呢個功能會自動掃描全市場再入 AI 驗證，需時約 2-3 分鐘。")
    if st.button('🚀 啟動終極掃描', type='primary', use_container_width=True):
        status_text, progress_bar = st.empty(), st.progress(0)
        status_text.markdown('**階段 1/2**: 執行全市場 RS x MACD 掃描 (強制市值 > 20億)...')
        try:
            f_sc = Overview()
            f_sc.set_filter(filters_dict={'Market Cap.': '+Mid (over $2bln)'})
            raw_data = f_sc.screener_view()
        except: raw_data = pd.DataFrame()
        if not raw_data.empty:
            df_p = raw_data.copy()
            indicators = calculate_all_indicators(df_p['Ticker'].tolist(), 25, 125, 'Close > 短期及長期 SMA', _progress_bar=progress_bar, _status_text=status_text)
            df_p['RS_階段'] = df_p['Ticker'].map(lambda x: indicators.get(x, {}).get('RS', '無'))
            df_p['MACD_階段'] = df_p['Ticker'].map(lambda x: indicators.get(x, {}).get('MACD', '無'))
            df_p['SMA多頭'] = df_p['Ticker'].map(lambda x: indicators.get(x, {}).get('SMA_Trend', False))
            tech_df = df_p[(df_p['SMA多頭'] == True) & (df_p['RS_階段'].isin(['🚀 啱啱突破', '🔥 已經突破'])) & (df_p['MACD_階段'].isin(['🚀 啱啱突破', '🔥 已經突破']))].copy()
            if not tech_df.empty:
                st.success(f"✅ 搵到 {len(tech_df)} 隻技術突破股。準備交由 AI 驗證基本面...")
                golden_df = run_full_integration(tech_df, progress_bar, status_text)
                status_text.markdown('✅ **終極掃描完成！**'); progress_bar.progress(100)
                if not golden_df.empty:
                    st.balloons()
                    st.subheader(f"🏆 終極黃金共振名單 (共 {len(golden_df)} 隻)")
                    ec = [c for c in ['Ticker', 'Company', 'Sector', 'RS_階段', 'MACD_階段', 'AI 消息情緒'] if c in golden_df.columns]
                    st.dataframe(golden_df[ec], use_container_width=True, hide_index=True)
                    st.markdown("### 🧠 AI 深度分析逐隻睇")
                    for _, row in golden_df.iterrows():
                        with st.expander(f"{row.get('Ticker', 'N/A')} | {row.get('AI 消息情緒', 'N/A')}"):
                            st.markdown(final_text_sanitize(row.get('AI 深度分析', '無分析內容。')))
                else:
                    st.warning('⚠️ AI 驗證後未見有足夠強烈好消息支持，本次無黃金名單輸出。')
            else:
                status_text.markdown('✅ 掃描完成。'); st.warning("無股票同時符合嚴格雙突破條件。")
        else:
            status_text.markdown('⚠️ 暫時攞唔到 Finviz 股票清單。')




