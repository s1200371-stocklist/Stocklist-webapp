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
# Sidebar 市場數據函數
# ==========================================
# ==========================================
# Sidebar 市場數據函數
# ==========================================
@st.cache_data(ttl=300, show_spinner=False)
def fetch_sidebar_market_data():
    """抓取 Sidebar 重要市場數據，每5分鐘更新一次"""
    data = {}
    tickers = {
        'VIX':   '^VIX',
        'SPY':   'SPY',
        'QQQ':   'QQQ',
        'DXY':   'DX-Y.NYB',
        'GOLD':  'GC=F',
        'OIL':   'CL=F',
        'BTC':   'BTC-USD',
        'TNX':   '^TNX',   # 10年美債息
        'NIKKEI':'^N225',
        'HSI':   '^HSI',
    }
    try:
        raw = yf.download(list(tickers.values()), period='2d', interval='1d', progress=False, auto_adjust=True)
        closes = raw['Close'] if 'Close' in raw else raw
        for label, sym in tickers.items():
            try:
                col = sym
                prices = closes[col].dropna()
                if len(prices) >= 2:
                    prev, curr = float(prices.iloc[-2]), float(prices.iloc[-1])
                    pct = (curr - prev) / prev * 100
                    data[label] = {'price': curr, 'pct': pct, 'sym': sym}
                elif len(prices) == 1:
                    data[label] = {'price': float(prices.iloc[-1]), 'pct': 0.0, 'sym': sym}
            except:
                data[label] = {'price': None, 'pct': 0.0, 'sym': sym}
    except Exception as e:
        pass

    # CNN Fear & Greed Index (via alternative.me-style scrape or fallback)
    try:
        r = requests.get('https://production.dataviz.cnn.io/index/fearandgreed/graphdata', headers=get_headers(), timeout=8)
        if r.status_code == 200:
            j = r.json()
            fg = j.get('fear_and_greed', {})
            score = fg.get('score', None)
            rating = fg.get('rating', 'N/A')
            data['FEAR_GREED'] = {'score': round(score, 1) if score else None, 'rating': rating}
    except:
        data['FEAR_GREED'] = {'score': None, 'rating': 'N/A'}

    # US Unemployment Rate (FRED - latest cached value)
    try:
        r = requests.get('https://fred.stlouisfed.org/graph/fredgraph.csv?id=UNRATE', headers=get_headers(), timeout=8)
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            last = lines[-1].split(',')
            data['UNRATE'] = {'date': last[0], 'value': float(last[1])}
    except:
        data['UNRATE'] = None

    # US CPI YoY (FRED)
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

    return data

def render_price_metric(label, emoji, d, fmt='{:.2f}'):
    if d and d.get('price') is not None:
        price = d['price']
        pct = d['pct']
        color = '🟢' if pct >= 0 else '🔴'
        arrow = '▲' if pct >= 0 else '▼'
        pct_str = f"{arrow}{abs(pct):.2f}%"
        price_str = fmt.format(price)
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;align-items:center;padding:3px 0'>"
            f"<span style='font-size:0.8rem'>{emoji} <b>{label}</b></span>"
            f"<span style='font-size:0.8rem'>{price_str} <span style='color:{'green' if pct>=0 else 'red'}'>{pct_str}</span></span>"
            f"</div>",
            unsafe_allow_html=True
        )
    else:
        st.markdown(f"<div style='font-size:0.75rem;color:gray'>{emoji} {label}: N/A</div>", unsafe_allow_html=True)

def render_sidebar_market_panel():
    st.markdown("### 📡 市場實時雷達")
    with st.spinner("載入市場數據..."):
        mdata = fetch_sidebar_market_data()

    # --- Fear & Greed ---
    fg = mdata.get('FEAR_GREED', {})
    if fg and fg.get('score') is not None:
        score = fg['score']
        rating = fg.get('rating', 'N/A').upper()
        if score >= 75:
            fg_emoji, fg_color = '🤑', '#e74c3c'
        elif score >= 55:
            fg_emoji, fg_color = '😊', '#e67e22'
        elif score >= 45:
            fg_emoji, fg_color = '😐', '#f1c40f'
        elif score >= 25:
            fg_emoji, fg_color = '😨', '#3498db'
        else:
            fg_emoji, fg_color = '😱', '#2980b9'
        st.markdown(
            f"<div style='background:{fg_color}22;border-left:3px solid {fg_color};border-radius:6px;padding:6px 10px;margin-bottom:6px'>"
            f"<span style='font-size:0.75rem;color:{fg_color}'><b>CNN 恐貪指數</b></span><br>"
            f"<span style='font-size:1.2rem'><b>{fg_emoji} {score:.0f}</b></span>"
            f"<span style='font-size:0.7rem;color:{fg_color};margin-left:6px'>{rating}</span>"
            f"</div>",
            unsafe_allow_html=True
        )
    else:
        st.caption("😐 CNN 恐貪指數: 暫無數據")

    # --- VIX ---
    vix = mdata.get('VIX', {})
    if vix and vix.get('price') is not None:
        v = vix['price']
        pct = vix['pct']
        if v >= 30:
            vix_label, vix_c = '極度恐慌', '#e74c3c'
        elif v >= 20:
            vix_label, vix_c = '市場緊張', '#e67e22'
        else:
            vix_label, vix_c = '市場平靜', '#2ecc71'
        arrow = '▲' if pct >= 0 else '▼'
        st.markdown(
            f"<div style='background:{vix_c}22;border-left:3px solid {vix_c};border-radius:6px;padding:6px 10px;margin-bottom:6px'>"
            f"<span style='font-size:0.75rem;color:{vix_c}'><b>VIX 恐慌指數</b></span><br>"
            f"<span style='font-size:1.1rem'><b>{v:.2f}</b></span>"
            f"<span style='font-size:0.7rem;color:{vix_c};margin-left:6px'>{vix_label} {arrow}{abs(pct):.1f}%</span>"
            f"</div>",
            unsafe_allow_html=True
        )
    else:
        st.caption("📊 VIX: 暫無數據")

    st.markdown("<hr style='margin:6px 0;opacity:0.3'>", unsafe_allow_html=True)

    # --- 美股指數 ---
    st.markdown("<span style='font-size:0.78rem;font-weight:600;color:#aaa'>🇺🇸 美股指數</span>", unsafe_allow_html=True)
    render_price_metric("SPY (S&P500)", "📈", mdata.get('SPY'), fmt='{:.2f}')
    render_price_metric("QQQ (納指)", "💻", mdata.get('QQQ'), fmt='{:.2f}')

    st.markdown("<hr style='margin:6px 0;opacity:0.3'>", unsafe_allow_html=True)

    # --- 商品 ---
    st.markdown("<span style='font-size:0.78rem;font-weight:600;color:#aaa'>🛢️ 商品市場</span>", unsafe_allow_html=True)
    render_price_metric("WTI 原油 (USD)", "🛢️", mdata.get('OIL'), fmt='${:.2f}')
    render_price_metric("黃金 (USD/oz)", "🥇", mdata.get('GOLD'), fmt='${:.2f}')

    st.markdown("<hr style='margin:6px 0;opacity:0.3'>", unsafe_allow_html=True)

    # --- 宏觀 ---
    st.markdown("<span style='font-size:0.78rem;font-weight:600;color:#aaa'>🏦 宏觀數據</span>", unsafe_allow_html=True)
    render_price_metric("美元指數 (DXY)", "💵", mdata.get('DXY'), fmt='{:.2f}')
    render_price_metric("10年美債息 (%)", "📉", mdata.get('TNX'), fmt='{:.3f}')

    # 失業率
    unemp = mdata.get('UNRATE')
    if unemp:
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;padding:3px 0'>"
            f"<span style='font-size:0.8rem'>👷 <b>失業率</b></span>"
            f"<span style='font-size:0.8rem'>{unemp['value']:.1f}%"
            f"<span style='font-size:0.65rem;color:gray'> ({unemp['date'][:7]})</span></span>"
            f"</div>",
            unsafe_allow_html=True
        )

    # CPI
    cpi = mdata.get('CPI_YOY')
    if cpi:
        cpi_color = '#e74c3c' if cpi['value'] > 3.5 else ('#e67e22' if cpi['value'] > 2.5 else '#2ecc71')
        st.markdown(
            f"<div style='display:flex;justify-content:space-between;padding:3px 0'>"
            f"<span style='font-size:0.8rem'>📦 <b>CPI (YoY)</b></span>"
            f"<span style='font-size:0.8rem;color:{cpi_color}'><b>{cpi['value']:.1f}%</b>"
            f"<span style='font-size:0.65rem;color:gray'> ({cpi['date'][:7]})</span></span>"
            f"</div>",
            unsafe_allow_html=True
        )

    st.markdown("<hr style='margin:6px 0;opacity:0.3'>", unsafe_allow_html=True)

    # --- 加密貨幣 ---
    st.markdown("<span style='font-size:0.78rem;font-weight:600;color:#aaa'>₿ 加密貨幣</span>", unsafe_allow_html=True)
    render_price_metric("Bitcoin (USD)", "₿", mdata.get('BTC'), fmt='${:,.0f}')

    st.markdown("<hr style='margin:6px 0;opacity:0.3'>", unsafe_allow_html=True)

    # --- 亞洲市場 ---
    st.markdown("<span style='font-size:0.78rem;font-weight:600;color:#aaa'>🌏 亞洲市場</span>", unsafe_allow_html=True)
    render_price_metric("日經 225", "🗾", mdata.get('NIKKEI'), fmt='{:,.0f}')
    render_price_metric("恒生指數", "🇭🇰", mdata.get('HSI'), fmt='{:,.0f}')

    st.markdown("<hr style='margin:4px 0;opacity:0.2'>", unsafe_allow_html=True)
    st.caption(f"⏱ 更新時間: {datetime.datetime.now().strftime('%H:%M:%S')}")
    if st.button("🔄 刷新市場數據", use_container_width=True, key="refresh_mkt"):
        fetch_sidebar_market_data.clear()
        st.rerun()



# ==========================================
# Sidebar
# ==========================================
with st.sidebar:
    st.title('🧰 投資雙引擎')

    # 市場實時雷達面板
    render_sidebar_market_panel()

    st.markdown('---')
    st.markdown("### 🗂️ 功能模組")
    app_mode = st.radio('可用模組', [
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
            selected_macd = st.multselect('顯示 MACD 階段:', ['🚀 啱啱突破', '🔥 已經突破', '🎯 就快突破 (<5%)'], default=['🚀 啱啱突破']) if enable_macd else []
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
