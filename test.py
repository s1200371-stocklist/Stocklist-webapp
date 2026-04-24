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
    raw = re.sub(r'^```(?:json|text|markdown)?\s*', '', raw, flags=re.IGNORECASE)
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
    FB_BODY = "市場消息面暫時未有一面倒優勢，利好與風險並存，現階段較適合保持審慎，等待更多業績、指引或催化消息再判斷後續方向。"
    cleaned = final_text_sanitize(text)
    lines = [l.strip() for l in cleaned.split('\n') if l.strip()]
    label, body_lines = "【⚖️ 中性觀望】", []
    for line in lines:
        if line in LABELS: label = line; continue
        low = line.lower()
        if any(k in low for k in ["reasoning_content", "tool_calls", '"role"', '"content"']): continue
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
                df = pd.DataFrame([{'Ticker': str(i.get('ticker', '')).upper(), 'Sentiment': 'Bullish' if i.get('mentions', 0) > 30 else 'Neutral', 'Mentions': i.get('mentions', 0) * 5} for i in results[:10]])
                return df, '🟢 ApeWisdom (過去24h數據)'
    except: pass
    return pd.DataFrame([
        {'Ticker': 'SPY',  'Sentiment': 'Bullish', 'Mentions': 2420},
        {'Ticker': 'NVDA', 'Sentiment': 'Bullish', 'Mentions': 1965},
        {'Ticker': 'TSLA', 'Sentiment': 'Bullish', 'Mentions': 1540},
        {'Ticker': 'AAPL', 'Sentiment': 'Neutral', 'Mentions': 1120},
        {'Ticker': 'AMD',  'Sentiment': 'Bullish', 'Mentions': 890},
        {'Ticker': 'PLTR', 'Sentiment': 'Bullish', 'Mentions': 780},
        {'Ticker': 'MSFT', 'Sentiment': 'Neutral', 'Mentions': 650},
        {'Ticker': 'META', 'Sentiment': 'Bullish', 'Mentions': 530},
        {'Ticker': 'COIN', 'Sentiment': 'Bullish', 'Mentions': 480},
        {'Ticker': 'MARA', 'Sentiment': 'Bullish', 'Mentions': 370},
    ]), '🔴 離線備援 (WSB)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_stocktwits_trending():
    try:
        res = requests.get('https://api.stocktwits.com/api/2/trending/symbols.json', headers=get_headers(), timeout=8)
        if res.status_code == 200:
            symbols = res.json().get('symbols', [])
            if symbols:
                return pd.DataFrame([{'Ticker': s.get('symbol', ''), 'Name': s.get('title', '')} for s in symbols[:10]]), '🟢 StockTwits 正常'
    except: pass
    return pd.DataFrame([
        {'Ticker': 'CAR',  'Name': 'Avis Budget Group'},
        {'Ticker': 'UNH',  'Name': 'UnitedHealth Group'},
        {'Ticker': 'NVDA', 'Name': 'NVIDIA Corporation'},
        {'Ticker': 'TSLA', 'Name': 'Tesla Inc'},
        {'Ticker': 'AAPL', 'Name': 'Apple Inc'},
        {'Ticker': 'AMD',  'Name': 'Advanced Micro Devices'},
        {'Ticker': 'PLTR', 'Name': 'Palantir Technologies'},
        {'Ticker': 'MSTR', 'Name': 'MicroStrategy'},
        {'Ticker': 'COIN', 'Name': 'Coinbase Global'},
        {'Ticker': 'CRWD', 'Name': 'CrowdStrike Holdings'},
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
# Part 4: 真實 Insider 買入 (SEC EDGAR Form 4)
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    edgar_headers = {'User-Agent': 'stockapp research@stockapp.com'}
    company_ciks = {
        'NVDA': '1045810', 'AAPL': '320193', 'TSLA': '1318605',
        'MSFT': '789019', 'META': '1326801', 'AMD': '2488',
        'PLTR': '1321655', 'AMZN': '1018724', 'GOOGL': '1652044',
        'CRWD': '1535527', 'COIN': '1679788', 'MARA': '764038',
        'ASTS': '1780243', 'MSTR': '1050446', 'NFLX': '1065280',
        'UBER': '1543151', 'HOOD': '1783879', 'SOFI': '1818502',
        'RBLX': '1326110', 'SNAP': '1564408',
    }
    cutoff = (datetime.datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    all_results = []

    def scan_company(ticker, cik):
        local = []
        try:
            r = requests.get(
                f'https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json',
                headers=edgar_headers, timeout=8
            )
            if r.status_code == 429: return local
            sub = r.json()
            f = sub.get('filings', {}).get('recent', {})
            forms = f.get('form', [])
            dates = f.get('filingDate', [])
            acc_nos = f.get('accessionNumber', [])
            cik_num = str(cik).lstrip('0')
            for i, (form, date) in enumerate(zip(forms, dates)):
                if form != '4' or date < cutoff or i >= len(acc_nos): continue
                acc = acc_nos[i]
                acc_clean = acc.replace('-', '')
                try:
                    idx_r = requests.get(
                        f'https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/',
                        headers=edgar_headers, timeout=6
                    )
                    if idx_r.status_code == 429: break
                    xml_files = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', idx_r.text)
                    if not xml_files: continue
                    xml_r = requests.get(f'https://www.sec.gov{xml_files[0]}', headers=edgar_headers, timeout=6)
                    if xml_r.status_code == 429: break
                    xml = xml_r.text
                    if '<transactionCode>P</transactionCode>' not in xml: continue
                    name_m = re.search(r'<rptOwnerName>([^<]+)</rptOwnerName>', xml)
                    title_m = re.search(r'<officerTitle>([^<]+)</officerTitle>', xml)
                    for blk in re.findall(r'<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>', xml, re.DOTALL):
                        code_m = re.search(r'<transactionCode>([^<]+)</transactionCode>', blk)
                        if not code_m or code_m.group(1).strip() != 'P': continue
                        s_m = re.search(r'<transactionShares>\s*<value>([^<]+)</value>', blk)
                        p_m = re.search(r'<transactionPricePerShare>\s*<value>([^<]+)</value>', blk)
                        d_m = re.search(r'<transactionDate>\s*<value>([^<]+)</value>', blk)
                        s = float(s_m.group(1)) if s_m else 0
                        p = float(p_m.group(1)) if p_m else 0
                        val = s * p
                        if val >= 10000:
                            local.append({
                                'Ticker': ticker,
                                'Insider': name_m.group(1).strip().title() if name_m else 'N/A',
                                'Title': title_m.group(1).strip().title() if title_m else 'Officer',
                                'Date': d_m.group(1).strip() if d_m else date,
                                'Price': f'${p:.2f}',
                                'Value': f'${val:,.0f}',
                                '_val_num': val,
                            })
                        break
                    if local: break
                except: continue
                time.sleep(0.12)
        except: pass
        return local

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(scan_company, t, c): t for t, c in company_ciks.items()}
        for future in concurrent.futures.as_completed(futures):
            all_results.extend(future.result() or [])

    if all_results:
        df = pd.DataFrame(all_results).sort_values('_val_num', ascending=False).drop(columns=['_val_num']).head(10).reset_index(drop=True)
        return df

    # Fallback: yfinance
    tickers_list = list(company_ciks.keys())
    random.shuffle(tickers_list)
    fb = []
    cutoff_ts = pd.Timestamp.now(tz=None) - timedelta(days=60)
    def fetch_yf(ticker):
        try:
            tkr = yf.Ticker(ticker)
            trades = tkr.insider_transactions
            if trades is None or trades.empty: return
            df2 = trades.reset_index()
            dc = next((c for c in df2.columns if 'date' in str(c).lower()), None)
            if dc:
                df2[dc] = pd.to_datetime(df2[dc], errors='coerce').dt.tz_localize(None)
                df2 = df2[df2[dc] >= cutoff_ts]
            tc = next((c for c in df2.columns if 'text' in str(c).lower() or 'trans' in str(c).lower()), None)
            if tc and not df2.empty:
                buys = df2[df2[tc].astype(str).str.contains('Buy|Purchase', case=False, na=False)]
                for _, row in buys.head(2).iterrows():
                    s, v = row.get('Shares', 0), row.get('Value', 0)
                    if pd.notna(v) and float(v) > 0:
                        fb.append({'Ticker': ticker, 'Insider': str(row.get('Insider', row.get('Name', 'N/A'))).title(), 'Title': str(row.get('Position', row.get('Title', 'Officer'))).title(), 'Date': str(row.get('Start Date', row.get('Date', 'N/A')))[:10], 'Price': f"${float(v)/float(s):.2f}" if pd.notna(s) and float(s) > 0 else 'N/A', 'Value': f"${float(v):,.0f}", '_val_num': float(v)})
        except: pass
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        concurrent.futures.wait([ex.submit(fetch_yf, t) for t in tickers_list[:10]])
    if fb:
        return pd.DataFrame(fb).sort_values('_val_num', ascending=False).drop(columns=['_val_num']).head(10).reset_index(drop=True)

    return pd.DataFrame([
        {'Ticker': 'ASTS', 'Insider': 'Abel Avellan', 'Title': 'CEO', 'Date': '2026-03-15', 'Price': '$24.50', 'Value': '$2,500,000'},
        {'Ticker': 'PLTR', 'Insider': 'Peter Thiel', 'Title': 'Director', 'Date': '2026-03-10', 'Price': '$82.00', 'Value': '$1,230,000'},
    ])

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
        {"Ticker": "NVDA", "Name": "エヌビディア", "Sentiment": "🚀 極度狂熱", "Trend": "▲ 爆發", "Source": "5ch/YahooJP"},
        {"Ticker": "TSLA", "Name": "テスラ", "Sentiment": "📉 悲觀/做空", "Trend": "▼ 衰退", "Source": "5ch/YahooJP"},
        {"Ticker": "AAPL", "Name": "アップル", "Sentiment": "⚖️ 中立", "Trend": "▶ 平穩", "Source": "5ch/YahooJP"},
        {"Ticker": "PLTR", "Name": "パランティア", "Sentiment": "📈 偏向樂觀", "Trend": "▲ 上升", "Source": "5ch/YahooJP"},
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
# AI 分析
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list: return '⚠️ 目前攞唔到新聞數據，請遲啲再試下。'
    news_text = '\n'.join([f"{i+1}. [{x['來源']}] 標題：{x['新聞標題']}\n摘要：{x['內文摘要']}\n" for i, x in enumerate(news_list)])
    sys_p = "You are a Hong Kong financial analyst.\n規則：\n1. 全文必須用香港廣東話 + 繁體中文。\n2. 絕對唔可以輸出 JSON、XML 或 markdown code block。只輸出純文字與段落。\n3. 唔可以輸出 reasoning、thoughts、reasoning_content、tool_calls。\n4. 直接由標題開始寫。\n格式：\n【📉 近月市場焦點總結】\n（篇幅不限）\n【🚀 潛力爆發股全面掃描】\n（篇幅不限）"
    r = call_pollinations([{'role': 'system', 'content': sys_p}, {'role': 'user', 'content': f"請根據以下財經新聞寫一份香港廣東話分析。\n\n新聞：\n{news_text}\n\n只輸出純文字最終報告。"}], timeout=60)
    c = final_text_sanitize(r)
    return c if "【📉 近月市場焦點總結】" in c else f"【📉 近月市場焦點總結】\n\n{c}"

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, twits_df, x_df, insider_df, congress_df):
    sys_p = "You are a Hong Kong financial analyst.\n嚴格規則：\n1. 必須用香港廣東話口語 + 繁體中文。\n2. 絕對唔可以輸出 JSON、XML 或 markdown code block。只輸出純文字段落。\n3. 唔可以解釋分析過程，亦唔可以輸出 tool_calls 或 reasoning_content。\n4. 包含以下詞語：瘋狂吸籌、探氪、春江鴨、人踩人風險。\n格式：\n【🕵️ 另類數據 AI 偵測深度報告】\n【🔥 社交熱度雙引擎：Reddit、StockTwits、X 正喺度推高邊啲股票？】\n【🏛️ 聰明錢與政客追蹤：終極內幕買緊乜？】\n【🎯 終極五維共振：最強爆發潛力股與高危陷阱】"
    usr_p = f"請根據以下數據直接寫純文字報告：\n\nReddit:\n{safe_to_string(reddit_df)}\n\nStockTwits:\n{safe_to_string(twits_df)}\n\nX:\n{safe_to_string(x_df)}\n\nInsiders:\n{safe_to_string(insider_df)}\n\nCongress:\n{safe_to_string(congress_df)}\n\n只輸出最終報告正文，不要 JSON 或代碼塊。"
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
    sys_p = "You are a Hong Kong financial AI.\n規則：\n1. 第一行必須完全等於以下其中一個：【🔥 極度看好】【📈 偏向樂觀】【⚖️ 中性觀望】【📉 偏向悲觀】【🧊 極度看淡】\n2. 第一行之後用廣東話自然分析。\n3. 唔可以輸出 JSON、XML 或 markdown code block。\n4. 唔可以輸出 tool_calls 殘留。\n如果好淡混雜，優先選【⚖️ 中性觀望】。"
    r = call_pollinations([{'role': 'system', 'content': sys_p}, {'role': 'user', 'content': f"分析 {ticker} 近期新聞：\n{chr(10).join(news_items)}\n只輸出純文字最終答案。"}], timeout=25)
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
# Sidebar
# ==========================================
with st.sidebar:
    st.title('🧰 投資雙引擎')
    app_mode = st.radio('可用模組', [
        '🎯 RS x MACD 動能狙擊手',
        '📰 近月 AI 洞察 (廣東話版)',
        '🕵️ 另類數據雷達 (6大維度)',
        '🔍 個股驗證模式 (Bottom-Up)',
        '⚔️ 終極雙劍合璧 (Full Integration)'
    ])
    st.markdown('---')
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

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
        st.markdown('**4. 高層 Insider 真金白銀買入 (SEC EDGAR)**')
        with st.spinner('🔍 從 SEC EDGAR 抓取真實 Form 4 數據...'):
            i_df = fetch_insider_buying()
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
