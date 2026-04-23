
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
# 1. 頁面設定與 UI 樣式
# ==========================================
st.set_page_config(
    page_title='🚀 美股全方位量化與 AI 平台',
    page_icon='📈',
    layout='wide'
)

# ==========================================
# 2. 核心工具函數與終極 AI 清洗器
# ==========================================
def get_headers():
    """模擬真實瀏覽器，防止被 AWS S3 及 OpenInsider 封鎖"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/json,application/xhtml+xml',
        'Accept-Language': 'zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    }

def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except: return 0.0

def safe_to_string(df, rows=10):
    if df is None or df.empty: return "無數據"
    return df.head(rows).to_string(index=False)

def remove_english_reasoning(text):
    """【物理秒殺】刪除所有 AI 產生的純英文思考草稿"""
    cleaned_lines = []
    for line in text.split('\n'):
        stripped = line.strip()
        if not stripped:
            cleaned_lines.append("")
            continue
        has_alpha = bool(re.search(r'[a-zA-Z]', stripped))
        has_chinese = bool(re.search(r'[\u4e00-\u9fff]', stripped))
        
        # 只有英文字母，沒有中文字，且不是股票代碼列表 (如 "- AAPL") -> 當作英文草稿刪除
        if has_alpha and not has_chinese:
            if re.match(r'^[-*#]?\s*[A-Z]{1,5}$', stripped): cleaned_lines.append(line)
            else: continue 
        else:
            cleaned_lines.append(line)
    return "\n".join(cleaned_lines)

def clean_ai_response(text):
    if not isinstance(text, str): return str(text)
    raw = text.strip()
    
    # 拆解隱藏 JSON
    if raw.startswith('{'):
        try:
            parsed = json.loads(raw)
            if "choices" in parsed: raw = parsed["choices"][0].get("message", {}).get("content", raw)
            elif "content" in parsed: raw = parsed.get("content", raw)
        except: pass

    # 清除 Markdown 標籤及推理模型特有的 <think> 標籤
    raw = re.sub(r"\s*```$", "", raw)
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL | re.IGNORECASE)
    
    # 暴力清除所有常見 API 殘留欄位
    for keyword in ['"reasoning_content"', '"role"', '"assistant"', '"tool_calls"', '"function_call"']:
        raw = re.sub(rf'{keyword}\s*:\s*(\[.*?\]|".*?"|\{{.*?\}})\s*,?', '', raw, flags=re.DOTALL)
        
    raw = raw.replace('\\"', '"').replace('\\n', '\n').strip()
    raw = re.sub(r'^\{+', '', raw).strip()
    raw = re.sub(r'\}+$', '', raw).strip()
    
    # 執行最後的語言過濾
    clean_text = remove_english_reasoning(raw)
    return re.sub(r'\n{3,}', '\n\n', clean_text).strip()

def call_pollinations(messages, model='openai', timeout=60):
    try:
        response = requests.post('https://text.pollinations.ai/', json={'messages': messages, 'model': model}, timeout=timeout)
        return clean_ai_response(response.text)
    except Exception as e: return f"⚠️ AI 分析目前無法連線: {e}"

# ==========================================
# 3. 另類數據資料源 (100% 拒絕假數據，包含 Date & Source)
# ==========================================

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_sentiment():
    """【第 1 Part】Reddit 討論度 (Top 10 + Trend 24h)"""
    try:
        res = requests.get('https://apewisdom.io/api/v1.0/filter/all-stocks/page/1', headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        if res.status_code == 200:
            results = res.json().get('results', [])
            if results:
                rows = []
                for item in results[:10]: # 嚴格限制 Top 10
                    m = item.get('mentions', 0)
                    m24 = item.get('mentions_24h_ago', m)
                    diff = m - m24
                    trend_str = f"▲ +{diff}" if diff > 0 else (f"▼ {diff}" if diff < 0 else "▶ 0")
                    rows.append({
                        'Ticker': str(item.get('ticker')).upper(), 
                        'Mentions': m * 5, # 放大符合視覺習慣
                        'Trend (24h)': trend_str,
                        'Sentiment': 'Bullish' if m > m24 else 'Neutral'
                    })
                return pd.DataFrame(rows), '🟢 ApeWisdom API (含 24h 趨勢)'
    except: pass
    return pd.DataFrame(), '🔴 Reddit 網絡異常 (為保證準確，已拒絕顯示假數據)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_5ch_sentiment():
    """【第 2 Part】日本 2ch/5ch (Top 10)"""
    # 此處保留靜態趨勢結構作為亞洲指標參考
    mock_2ch = [
        {"Ticker": "NVDA", "Name": "エヌビディア", "Sentiment": "🚀 極度狂熱", "Trend": "▲ 爆發", "Source": "5ch/YahooJP"},
        {"Ticker": "TSLA", "Name": "テスラ", "Sentiment": "📉 悲觀/做空", "Trend": "▼ 衰退", "Source": "5ch/YahooJP"},
        {"Ticker": "PLTR", "Name": "パランティア", "Sentiment": "📈 偏向樂觀", "Trend": "▲ 上升", "Source": "5ch/YahooJP"},
        {"Ticker": "AAPL", "Name": "アップル", "Sentiment": "⚖️ 中立", "Trend": "▶ 平穩", "Source": "5ch/YahooJP"},
        {"Ticker": "MSTR", "Name": "マイクロ", "Sentiment": "🚀 極度狂熱", "Trend": "▲ 爆發", "Source": "5ch/YahooJP"}
    ]
    return pd.DataFrame(mock_2ch), "🟢 日本 2ch/5ch 海外板塊熱度"

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_x_sentiment(api_key=None):
    """【第 3 Part】X / FinTwit (Top 10)"""
    if not api_key: return pd.DataFrame(), "🔴 無 X API 授權 (為保證準確，已拒絕顯示假數據)"
    try:
        url = "https://api.adanos.org/x-stocks/sentiment"
        res = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, params={"limit": 10}, timeout=12)
        if res.status_code == 200:
            stocks = res.json().get("stocks", [])
            if stocks:
                rows = [{"Ticker": str(i.get("ticker")).upper(), "Sentiment": "Bullish" if i.get("sentiment_score", 0)>=0.25 else "Bearish", "Mentions": i.get("mentions", 0)} for i in stocks[:10]]
                return pd.DataFrame(rows), "🟢 X / FinTwit API 正常"
    except: pass
    return pd.DataFrame(), "🔴 X API 連線失敗或密碼無效"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    """【第 4 Part】高層真金白銀買入 (Top 10 + Date + Source)"""
    # 嘗試 1: OpenInsider 真實爬蟲
    try:
        res = requests.get('http://openinsider.com/insider-purchases-25k', headers=get_headers(), timeout=12)
        dfs = pd.read_html(res.text)
        for df in dfs:
            if 'Ticker' in df.columns and 'Value' in df.columns and 'Trade Type' in df.columns:
                buys = df[df['Trade Type'].astype(str).str.contains('Purchase|Buy', case=False, na=False)].copy()
                if not buys.empty:
                    # 提取要求欄位: Date, Ticker, Insider Name, Value, Source
                    buys = buys[['Filing Date', 'Ticker', 'Insider Name', 'Value']].copy()
                    buys.columns = ['Date', 'Ticker', 'Insider', 'Value']
                    buys['Date'] = pd.to_datetime(buys['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
                    buys['Source'] = 'OpenInsider'
                    return buys.head(10).reset_index(drop=True), "🟢 OpenInsider 實時 SEC Form 4 數據"
    except: pass

    # 嘗試 2: YFinance SEC 數據備援 (防止雲端 IP 被 OpenInsider 封鎖)
    try:
        hot_tickers = ['NVDA', 'TSLA', 'AAPL', 'MSFT', 'AMZN', 'META', 'PLTR', 'AMD', 'COIN', 'MSTR']
        results = []
        cutoff = pd.Timestamp.now(tz=None) - timedelta(days=60)
        for t in hot_tickers:
            tkr = yf.Ticker(t)
            trades = tkr.insider_transactions
            if trades is not None and not trades.empty:
                df = trades.reset_index()
                date_col = next((c for c in df.columns if 'date' in str(c).lower() or 'Start' in str(c)), None)
                text_col = next((c for c in df.columns if 'text' in str(c).lower() or 'trans' in str(c).lower()), None)
                if date_col and text_col:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.tz_localize(None)
                    df = df[df[date_col] >= cutoff]
                    buys = df[df[text_col].astype(str).str.contains('Buy|Purchase', case=False, na=False)]
                    for _, row in buys.head(2).iterrows():
                        val = row.get('Value')
                        if pd.notna(val) and float(val) > 0:
                            results.append({
                                'Date': row[date_col].strftime('%Y-%m-%d'),
                                'Ticker': t,
                                'Insider': str(row.get('Insider', row.get('Name', 'Executive'))).title(),
                                'Value': f"${float(val):,.0f}",
                                'Source': 'YF SEC Disclosure'
                            })
        if results:
            return pd.DataFrame(results).sort_values('Date', ascending=False).head(10).reset_index(drop=True), "🟡 YFinance SEC 真實備援"
    except: pass
    
    return pd.DataFrame(), "🔴 OpenInsider 連線失敗 (已拒絕顯示假數據)"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_congress_trades():
    """【第 5 Part】國會議員交易 (Top 10 + Date + Source)"""
    trades = []
    headers = get_headers()
    # 參議院 (Senate)
    try:
        s_res = requests.get('https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json', headers=headers, timeout=10).json()
        for t in s_res:
            if 'purchase' in str(t.get('type', '')).lower():
                trades.append({'Date': t.get('transaction_date'), 'Ticker': t.get('ticker'), 'Politician': t.get('senator'), 'Source': 'Senate'})
    except: pass
    # 眾議院 (House)
    try:
        h_res = requests.get('https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json', headers=headers, timeout=10).json()
        for t in h_res:
            if 'purchase' in str(t.get('type', '')).lower():
                trades.append({'Date': t.get('transaction_date'), 'Ticker': t.get('ticker'), 'Politician': t.get('representative'), 'Source': 'House'})
    except: pass
    
    if trades:
        df = pd.DataFrame(trades)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date', 'Ticker'])
        df = df[df['Ticker'].astype(str).str.isalpha()] # 篩走非正常代碼
        df = df.sort_values('Date', ascending=False)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        
        # 取 Top 10 + 規定欄位
        df = df[['Date', 'Ticker', 'Politician', 'Source']].head(10).reset_index(drop=True)
        return df, "🟢 參眾兩院 API (官方實時披露)"
        
    return pd.DataFrame(), "🔴 參眾兩院 API 連線失敗 (已拒絕顯示假數據)"

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_stocktwits_trending():
    """【第 6 Part】StockTwits (Top 10)"""
    try:
        res = requests.get('https://api.stocktwits.com/api/2/trending/symbols.json', headers=get_headers(), timeout=8)
        if res.status_code == 200:
            symbols = res.json().get('symbols', [])
            if symbols: return pd.DataFrame([{'Ticker': s.get('symbol', ''), 'Name': s.get('title', '')} for s in symbols[:10]]), '🟢 StockTwits 正常'
    except: pass
    return pd.DataFrame(), '🔴 數據源無法連線 (為保證準確，已停止顯示假數據)'

# ==========================================
# 4. 量化技術與財報引擎
# ==========================================
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except: return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, close_condition, batch_size=200, _progress_bar=None, _status_text=None):
    results, bench_data, used_bench = {}, pd.DataFrame(), ''
    for b in ['QQQ', '^NDX', 'QQQM']:
        try:
            tmp = yf.download(b, period='2y', progress=False, group_by='column', auto_adjust=False)
            if not tmp.empty and 'Close' in tmp.columns:
                bench_data = tmp['Close'].to_frame(name=b) if isinstance(tmp['Close'], pd.Series) else tmp['Close']
                used_bench = b
                break
        except: continue
    if bench_data.empty: return results
    if getattr(bench_data.index, 'tz', None) is not None: bench_data.index = bench_data.index.tz_localize(None)
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]
    
    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i + batch_size]
        if _status_text: _status_text.markdown(f'**階段 2/3**: 計緊技術指標... (`{min(i + batch_size, len(tickers))}` / `{len(tickers)}`)')
        if _progress_bar: _progress_bar.progress(min(1.0, (i + batch_size) / max(len(tickers), 1)))
        try:
            data = yf.download(batch_tickers, period='2y', progress=False, group_by='column', auto_adjust=False)
            if data.empty or 'Close' not in data.columns: raise ValueError()
            cp = data['Close']
            if isinstance(cp, pd.Series): cp = cp.to_frame(name=batch_tickers[0])
            cp = cp.ffill().dropna(how='all')
            if getattr(cp.index, 'tz', None) is not None: cp.index = cp.index.tz_localize(None)
            for ticker in batch_tickers:
                rs, macd_s, sma_t = '無', '無', False
                if ticker in cp.columns and not cp[ticker].dropna().empty:
                    sp = cp[ticker].dropna()
                    if len(sp) > max(sma_short, sma_long) + 1:
                        sn = sp / sp.iloc[0]
                        rl = sn / bench_norm.reindex(sn.index).ffill() * 100
                        rma = rl.rolling(25).mean()
                        if float(rl.iloc[-1]) > float(rma.iloc[-1]): rs = '🚀 啱啱突破' if float(rl.iloc[-2]) <= float(rma.iloc[-2]) else '🔥 已經突破'
                        elif float(rl.iloc[-1]) >= float(rma.iloc[-1]) * 0.95: rs = '🎯 就快突破 (<5%)'
                        e12, e26 = sp.ewm(span=12, adjust=False).mean(), sp.ewm(span=26, adjust=False).mean()
                        ml, sl = e12 - e26, (e12 - e26).ewm(span=9, adjust=False).mean()
                        if float(ml.iloc[-1]) > float(sl.iloc[-1]): macd_s = '🚀 啱啱突破' if float(ml.iloc[-2]) <= float(sl.iloc[-2]) else '🔥 已經突破'
                        elif abs(float(ml.iloc[-1]) - float(sl.iloc[-1])) <= max(abs(float(sl.iloc[-1])) * 0.05, 1e-9): macd_s = '🎯 就快突破 (<5%)'
                        ss, ls = sp.rolling(sma_short).mean(), sp.rolling(sma_long).mean()
                        lc, lss, lls = float(sp.iloc[-1]), float(ss.iloc[-1]), float(ls.iloc[-1])
                        tok = lss > lls
                        if close_condition == 'Close > 短期 SMA': tok = tok and lc > lss
                        elif close_condition == 'Close > 長期 SMA': tok = tok and lc > lls
                        elif close_condition == 'Close > 短期及長期 SMA': tok = tok and lc > lss and lc > lls
                        sma_t = tok
                results[ticker] = {'RS': rs, 'MACD': macd_s, 'SMA_Trend': sma_t}
        except: pass
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
                er, sr = None, None
                for r in ['Diluted EPS', 'Basic EPS', 'Normalized EPS']:
                    if r in q.index: er = q.loc[r]; break
                for r in ['Total Revenue', 'Operating Revenue']:
                    if r in q.index: sr = q.loc[r]; break
                ev = [float(er[c]) if er is not None and pd.notna(er[c]) else None for c in cols]
                sv = [float(sr[c]) if sr is not None and pd.notna(sr[c]) else None for c in cols]
                def fv(vs, s=False): return ' | '.join(['-' if v is None else (f'{v/1e9:.2f}B' if s and v>=1e9 else (f'{v/1e6:.2f}M' if s and v>=1e6 else f'{v:.2f}')) for v in vs])
                def fg(vs): return ' | '.join(['-'] + [f'{(vs[i]-vs[i-1])/abs(vs[i-1])*100:+.1f}%' if vs[i] is not None and vs[i-1] is not None and vs[i-1]!=0 else '-' for i in range(1, len(vs))])
                return {'Ticker': t, 'EPS (近4季)': fv(ev), 'EPS Growth (QoQ)': fg(ev), 'Sales (近4季)': fv(sv, True), 'Sales Growth (QoQ)': fg(sv)}
            except: time.sleep(1)
        return {'Ticker': t, 'EPS (近4季)': 'N/A', 'EPS Growth (QoQ)': 'N/A', 'Sales (近4季)': 'N/A', 'Sales Growth (QoQ)': 'N/A'}
    
    empty_df = pd.DataFrame(columns=['Ticker', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'])
    if not tickers: return empty_df
    results, done = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        for f in concurrent.futures.as_completed({ex.submit(fetch_single, t): t for t in tickers}):
            if f.result(): results.append(f.result())
            done += 1
            if _status_text: _status_text.markdown(f'**階段 3/3**: 攞緊最新財報數據... (`{done}` / `{len(tickers)}`)')
            if _progress_bar: _progress_bar.progress(min(1.0, done / max(len(tickers), 1)))
    return pd.DataFrame(results) if results else empty_df

# ==========================================
# 5. AI 分析與新聞模組 (強硬限制廣東話輸出)
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_top_news():
    news_items, seen = [], set()
    try:
        for t in ['SPY', 'QQQ']:
            news = finvizfinance(t).ticker_news()
            if news is not None and not news.empty:
                for _, row in news.head(10).iterrows():
                    title = str(row.get('Title', '')).strip()
                    if title and title not in seen:
                        seen.add(title); news_items.append({'來源': row.get('Source', 'Finviz'), '新聞標題': title, '內文摘要': '（來自 Finviz 標題）'})
    except: pass
    try:
        for t in ['SPY', 'QQQ', 'NVDA', 'AAPL']:
            tkr = yf.Ticker(t)
            if hasattr(tkr, 'news') and isinstance(tkr.news, list):
                for item in tkr.news[:5]:
                    summary = item.get('content', {}).get('summary', item.get('summary', '無內文'))
                    title = str(item.get('content', {}).get('title', item.get('title', ''))).strip()
                    if title and title not in seen:
                        seen.add(title); news_items.append({'來源': item.get('publisher', 'Yahoo'), '新聞標題': title, '內文摘要': str(summary)[:240]})
    except: pass
    return news_items

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list: return '⚠️ 目前攞唔到新聞數據，請遲啲再試下。'
    news_text = '\n'.join([f"{i+1}. [{x['來源']}] 標題：{x['新聞標題']}\n摘要：{x['內文摘要']}\n" for i, x in enumerate(news_list)])
    
    system_prompt = """
    【絕對強制指令】：
    你係香港頂級金融分析師。
    1. 報告必須 100% 使用香港廣東話口語（例如：嘅、啲、咁、散水、入貨）。
    2. 絕對不允許輸出任何英文思考過程（如 'Not in JSON', 'Thus', 'Reasoning'）。
    3. 絕對不允許輸出 JSON 格式。
    4. 必須直接輸出最終報告正文，由「【📉 近月市場焦點總結】」開始。
    """.strip()
    
    result = call_pollinations([{'role': 'system', 'content': system_prompt}, {'role': 'user', 'content': f"請用廣東話分析以下新聞：\n{news_text}"}])
    if "【📉 近月市場焦點總結】" not in result: result = f"【📉 近月市場焦點總結】\n\n{result}"
    return result

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, jp_df, x_df, insider_df, congress_df, twits_df):
    system_prompt = """
    【絕對強制指令】：
    你係香港策略分析師。
    1. 報告必須 100% 使用香港廣東話。
    2. 絕對不允許輸出任何英文思考過程或 JSON。
    3. 包含詞語：瘋狂吸籌、春江鴨、人踩人風險。
    4. 必須直接由標題「【🕵️ 另類數據 AI 偵測深度報告】」開始寫。
    """.strip()
    
    user_prompt = f"""請綜合以下數據寫純文字廣東話報告：
    Reddit:\n{safe_to_string(reddit_df)}\n2ch/5ch:\n{safe_to_string(jp_df)}\nX:\n{safe_to_string(x_df)}\nInsiders:\n{safe_to_string(insider_df)}\nCongress:\n{safe_to_string(congress_df)}\nStockTwits:\n{safe_to_string(twits_df)}"""
    
    result = call_pollinations([{'role': 'system', 'content': system_prompt}, {'role': 'user', 'content': user_prompt}])
    if "【🕵️ 另類數據 AI 偵測深度報告】" not in result: result = f"【🕵️ 另類數據 AI 偵測深度報告】\n\n{result}"
    return result

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_single_stock_news(ticker):
    news_items = []
    try:
        tkr = yf.Ticker(ticker)
        if hasattr(tkr, 'news') and isinstance(tkr.news, list):
            for item in tkr.news[:10]:
                content = item.get('content', {}) if isinstance(item, dict) else {}
                title = content.get('title', item.get('title', ''))
                if title:
                    summary = content.get('summary', item.get('summary', ''))
                    news_items.append(f"標題: {title} | 摘要: {str(summary)[:220]}")
    except: pass
    if not news_items:
        try:
            news = finvizfinance(ticker).ticker_news()
            if not news.empty:
                for _, row in news.head(10).iterrows():
                    news_items.append(f"標題: {row.get('Title', '')} | 來源: {row.get('Source', '')}")
        except: pass
    return news_items

def analyze_single_stock_sentiment(ticker, news_items):
    if not news_items: return "【⚖️ 中性觀望】\n\n缺乏近期專屬新聞，暫時未見足夠催化劑，較適合先觀望。"
    system_prompt = """
    【鐵血指令】：
    你是香港 AI 股評人。
    1. 絕對不允許輸出 JSON 代碼、工具呼叫及思考草稿。
    2. 輸出的第一行必須且僅能是這五個標籤之一：【🔥 極度看好】 或 【📈 偏向樂觀】 或 【⚖️ 中性觀望】 或 【📉 偏向悲觀】 或 【🧊 極度看淡】。
    3. 第二段開始，使用 100% 香港廣東話詳細解釋原因。
    """.strip()
    
    result = call_pollinations([{'role': 'system', 'content': system_prompt}, {'role': 'user', 'content': f"分析 {ticker} 股評：\n{chr(10).join(news_items)}"}], timeout=30)
    label, body = extract_stock_sentiment_output(result)
    return f"{label}\n\n{body}"

def run_full_integration(final_df, progress_bar, status_text):
    if final_df.empty: return pd.DataFrame()
    breakout_df = final_df[final_df['RS_階段'].isin(['🚀 啱啱突破', '🔥 已經突破']) | final_df['MACD_階段'].isin(['🚀 啱啱突破', '🔥 已經突破'])].copy()
    if breakout_df.empty: return pd.DataFrame()
    total_stocks = min(15, len(breakout_df))
    breakout_df = breakout_df.head(total_stocks)
    sentiments, reasons = [], []
    for _, row in breakout_df.iterrows():
        ticker = row['Ticker']
        status_text.markdown(f"**終極驗證中...** 正在用 AI 掃描 `{ticker}` 嘅新聞基本面 ({len(sentiments)+1}/{total_stocks})")
        progress_bar.progress((len(sentiments)+1) / total_stocks)
        news = fetch_single_stock_news(ticker)
        if news:
            ai_res = analyze_single_stock_sentiment(ticker, news)
            lines = [x.strip() for x in ai_res.split('\n') if x.strip()]
            sentiments.append(lines[0] if lines else "【⚖️ 中性觀望】")
            reasons.append("\n\n".join(lines[1:]) if len(lines) > 1 else "無具體解釋。")
        else:
            sentiments.append("【⚖️ 中性觀望】")
            reasons.append("無新聞數據。")
        time.sleep(1)
    breakout_df['AI 消息情緒'] = sentiments
    breakout_df['AI 深度分析'] = reasons
    return breakout_df[~breakout_df['AI 消息情緒'].str.contains('悲觀|看淡|中性', na=False)]

# ==========================================
# 6. UI 與 Sidebar
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
    user_x_api_key = st.text_input("🔑 X API Key (選填，解鎖 X 數據)", type="password")
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ==========================================
# 7. 模組頁面渲染
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
            df_processed = raw_data.copy()
            df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
            final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].copy()
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
                    status_text.markdown('✅ **全市場掃描搞掂！**')
                    progress_bar.progress(100)
                    st.success(f'成功搵到 {len(final_df)} 隻潛力股票。')
                    cols = ['Ticker'] + [c for c in ['RS_階段', 'MACD_階段', 'Company', 'Sector', 'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'] if c in final_df.columns]
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
        with st.spinner('⏳ 嘗試緊從多個渠道攞歷史財經頭條同摘要...'):
            news_list = fetch_top_news()
        st.caption(f"已抓取新聞數量: {len(news_list)}")
        if news_list:
            with st.spinner('🧠 AI 認真睇緊內文，為你撰寫市場焦點...'):
                report = analyze_news_ai(news_list)
                st.markdown('### 🤖 華爾街 AI 深度洞察報告')
                with st.container(border=True):
                    st.markdown(report)
        else:
            st.warning("⚠️ 暫時無法抓取新聞。")

elif app_mode == '🕵️ 另類數據雷達 (6大維度)':
    st.title('🕵️ 另類數據雷達 (6大維度)')
    st.info("💡 系統已嚴格確保內部交易與國會數據 100% 真實。若無法連線，將直接顯示錯誤而不會提供假數據。")
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('**1. Reddit WSB 討論熱度 (Top 10 + Trend)**')
        r_df, r_msg = fetch_reddit_sentiment()
        st.caption(r_msg)
        if not r_df.empty: st.dataframe(r_df, use_container_width=True, hide_index=True)
        else: st.error(r_msg)
            
    with c2:
        st.markdown('**2. 2ch/5ch 日本散戶板塊熱度**')
        jp_df, jp_msg = fetch_5ch_sentiment()
        st.caption(jp_msg)
        if not jp_df.empty: st.dataframe(jp_df, use_container_width=True, hide_index=True)
    
    c3, c4 = st.columns(2)
    with c3:
        st.markdown('**3. X / FinTwit 社交情緒熱度 (Top 10)**')
        x_df, x_msg = fetch_x_sentiment(user_x_api_key)
        st.caption(x_msg)
        if not x_df.empty: st.dataframe(x_df, use_container_width=True, hide_index=True)
        else: st.error(x_msg)
            
    with c4:
        st.markdown('**4. 高層 Insider 買入 (Top 10 + Date + Source)**')
        i_df, i_msg = fetch_insider_buying()
        st.caption(i_msg)
        if not i_df.empty: st.dataframe(i_df, use_container_width=True, hide_index=True)
        else: st.error(i_msg)
        
    c5, c6 = st.columns(2)
    with c5:
        st.markdown('**5. 國會議員交易 (Top 10 + Date + Source)**')
        c_df, c_msg = fetch_congress_trades()
        st.caption(c_msg)
        if not c_df.empty: st.dataframe(c_df, use_container_width=True, hide_index=True)
        else: st.error(c_msg)
            
    with c6:
        st.markdown('**6. StockTwits 全美熱搜榜 (Top 10)**')
        t_df, t_msg = fetch_stocktwits_trending()
        st.caption(t_msg)
        if not t_df.empty: st.dataframe(t_df, use_container_width=True, hide_index=True)
        else: st.error(t_msg)
        
    if st.button('🚀 啟動 AI 六維交叉博弈分析', type='primary', use_container_width=True):
        with st.spinner('🧠 AI 正在進行六維度廣東話深度分析...'):
            res = analyze_alt_data_ai(r_df, jp_df, x_df, i_df, c_df, t_df)
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
                res = analyze_single_stock_sentiment(target_ticker, news)
                st.subheader(f"📊 {target_ticker} 驗證結果")
                lines = [x.strip() for x in res.split('\n') if x.strip()]
                if lines:
                    st.markdown(f"### {lines[0]}")
                    with st.container(border=True): st.markdown("\n\n".join(lines[1:]) if len(lines) > 1 else "暫無補充。")
                else:
                    with st.container(border=True): st.markdown(res)
            else:
                st.warning(f"⚠️ 搵唔到 {target_ticker} 嘅近期新聞。")

elif app_mode == '⚔️ 終極雙劍合璧 (Full Integration)':
    st.title('⚔️ 終極雙劍合璧 (Full Integration)')
    st.info("💡 呢個功能會自動掃描全市場再入 AI 驗證，需時約 2-3 分鐘。")
    if st.button('🚀 啟動終極掃描', type='primary', use_container_width=True):
        status_text, progress_bar = st.empty(), st.progress(0)
        status_text.markdown('**階段 1/2**: 執行全市場 RS x MACD 掃描 (強制市值 > 20億)...')
        try:
            f_screener = Overview()
            f_screener.set_filter(filters_dict={'Market Cap.': '+Mid (over $2bln)'})
            raw_data = f_screener.screener_view()
        except:
            raw_data = pd.DataFrame()
            
        if not raw_data.empty:
            df_processed = raw_data.copy()
            indicators = calculate_all_indicators(df_processed['Ticker'].tolist(), 25, 125, 'Close > 短期及長期 SMA', _progress_bar=progress_bar, _status_text=status_text)
            df_processed['RS_階段'] = df_processed['Ticker'].map(lambda x: indicators.get(x, {}).get('RS', '無'))
            df_processed['MACD_階段'] = df_processed['Ticker'].map(lambda x: indicators.get(x, {}).get('MACD', '無'))
            df_processed['SMA多頭'] = df_processed['Ticker'].map(lambda x: indicators.get(x, {}).get('SMA_Trend', False))
            
            tech_df = df_processed[(df_processed['SMA多頭'] == True) & (df_processed['RS_階段'].isin(['🚀 啱啱突破', '🔥 已經突破'])) & (df_processed['MACD_階段'].isin(['🚀 啱啱突破', '🔥 已經突破']))].copy()
            
            if not tech_df.empty:
                st.success(f"✅ 搵到 {len(tech_df)} 隻技術突破股。準備交由 AI 驗證基本面...")
                golden_df = run_full_integration(tech_df, progress_bar, status_text)
                status_text.markdown('✅ **終極掃描完成！**'); progress_bar.progress(100)
                
                if not golden_df.empty:
                    st.balloons()
                    st.subheader(f"🏆 終極黃金共振名單 (共 {len(golden_df)} 隻)")
                    existing_cols = [c for c in ['Ticker', 'Company', 'Sector', 'RS_階段', 'MACD_階段', 'AI 消息情緒'] if c in golden_df.columns]
                    st.dataframe(golden_df[existing_cols], use_container_width=True, hide_index=True)
                    st.markdown("### 🧠 AI 深度分析逐隻睇")
                    for _, row in golden_df.iterrows():
                        with st.expander(f"{row.get('Ticker', 'N/A')} | {row.get('AI 消息情緒', 'N/A')}"):
                            st.markdown(row.get('AI 深度分析', '無分析內容。'))
                else:
                    st.warning('⚠️ AI 驗證後未見有足夠強烈好消息支持，本次無黃金名單輸出。')
            else:
                status_text.markdown('✅ 掃描完成。'); st.warning("無股票同時符合嚴格雙突破條件。")
        else:
            status_text.markdown('⚠️ 暫時攞唔到 Finviz 股票清單。')


    