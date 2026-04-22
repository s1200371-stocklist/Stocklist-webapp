
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
# 2. 基本工具函數與字串清洗
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
        'Accept-Language': 'zh-HK,en-US;q=0.9,en;q=0.8',
    }

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
        if df is None or df.empty: return "無數據"
        return df.head(rows).to_string(index=False)
    except: return "無數據"

def remove_english_reasoning(text):
    """【終極過濾器】: 刪除所有「純英文/無中文字」的思考過程草稿"""
    cleaned_lines = []
    for line in text.split('\n'):
        stripped = line.strip()
        if not stripped:
            cleaned_lines.append("")
            continue
            
        # 檢查該行是否包含英文字母，且【完全不包含】中文字符
        has_alpha = bool(re.search(r'[a-zA-Z]', stripped))
        has_chinese = bool(re.search(r'[\u4e00-\u9fff]', stripped))
        
        if has_alpha and not has_chinese:
            # 例外：保留純粹係 Ticker 嘅列表行 (例如 "- AAPL" 或 "### TSLA")
            if re.match(r'^[-*#]\s*[A-Z]{1,5}$', stripped):
                cleaned_lines.append(line)
            else:
                continue # 命中！呢行係 AI 嘅英文廢話 (例如 "Not in JSON", "Thus our answer")，直接刪除！
        else:
            cleaned_lines.append(line)
            
    res = "\n".join(cleaned_lines)
    return re.sub(r'\n{3,}', '\n\n', res).strip()

def clean_ai_response(text):
    if not isinstance(text, str): return str(text)
    raw = text.strip()
    
    # 拆解 JSON
    if raw.startswith('{'):
        try:
            parsed = json.loads(raw)
            if "choices" in parsed: raw = parsed["choices"][0].get("message", {}).get("content", raw)
            elif "content" in parsed: raw = parsed.get("content", raw)
        except: pass
        
    # 清除 markdown 標籤與 <think>
    ```(?
    raw = re.sub(r"\s*```$", "", raw)
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL | re.IGNORECASE)
    
    # 清除殘留系統字段
    for keyword in ['"reasoning_content"', '"role"', '"assistant"', '"tool_calls"', '"function_call"']:
        raw = re.sub(rf'{keyword}\s*:\s*(\[.*?\]|".*?"|\{{.*?\}})\s*,?', '', raw, flags=re.DOTALL)
        
    raw = raw.replace('\\"', '"').replace('\\n', '\n').strip()
    raw = re.sub(r'^\{+', '', raw).strip()
    raw = re.sub(r'\}+$', '', raw).strip()
    
    # 過濾英文思考過程
    raw = remove_english_reasoning(raw)
    return raw

def final_text_sanitize(text):
    t = clean_ai_response(text)
    return re.sub(r'\n{3,}', '\n\n', t).strip()

def call_pollinations(messages, model='openai', timeout=60):
    try:
        response = requests.post(
            'https://text.pollinations.ai/',
            json={'messages': messages, 'model': model},
            timeout=timeout
        )
        return final_text_sanitize(response.text)
    except Exception as e:
        return f"⚠️ AI 發生錯誤: {e}"

def extract_cantonese_report(text):
    cleaned = final_text_sanitize(text)
    anchor = "【🕵️ 另類數據 AI 偵測深度報告】"
    idx = cleaned.find(anchor)
    if idx != -1: cleaned = cleaned[idx:].strip()
    return cleaned if cleaned else "⚠️ AI 回傳格式異常，建議重新生成一次。"

def extract_stock_sentiment_output(text):
    """【精準提取】即使 AI 亂塞格式，依然強制抽取出唯一的情緒標籤"""
    cleaned = final_text_sanitize(text)
    
    # 允許的正統標籤 (包含無空格版)
    label_map = {
        "極度看好": "【🔥 極度看好】",
        "偏向樂觀": "【📈 偏向樂觀】",
        "中性觀望": "【⚖️ 中性觀望】",
        "偏向悲觀": "【📉 偏向悲觀】",
        "極度看淡": "【🧊 極度看淡】"
    }
    
    found_label = "【⚖️ 中性觀望】" # 預設值
    
    # 從內文中尋找標籤特徵
    for key, formatted_label in label_map.items():
        if key in cleaned:
            found_label = formatted_label
            break

    # 清除正文中的標籤與無用符號
    body = cleaned
    for word in list(label_map.keys()) + ["🔥", "📈", "⚖️", "📉", "🧊", "【", "】"]:
        body = body.replace(word, "")
        
    body = re.sub(r'^[:：\s]+', '', body.strip())
    
    if not body:
        body = "市場消息面暫時未有一面倒優勢，利好與風險並存，建議等待更多催化消息再作判斷。"
        
    return found_label, body

# ==========================================
# 3. 新聞資料源 (RSS 解析與備援)
# ==========================================
def parse_rss_items(xml_text, source_name, limit=10):
    items = []
    try:
        blocks = re.findall(r'<item>(.*?)</item>', xml_text, flags=re.DOTALL | re.IGNORECASE)
        for block in blocks[:limit]:
            title_match = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>|<title>(.*?)</title>', block, flags=re.DOTALL | re.IGNORECASE)
            desc_match = re.search(r'<description><!\[CDATA\[(.*?)\]\]></description>|<description>(.*?)</description>', block, flags=re.DOTALL | re.IGNORECASE)
            title = title_match.group(1) or title_match.group(2) or "" if title_match else ""
            desc = desc_match.group(1) or desc_match.group(2) or "" if desc_match else ""
            title = re.sub(r'<.*?>', '', title).strip()
            desc = re.sub(r'<.*?>', '', desc).strip()
            if title: items.append({'來源': source_name, '新聞標題': title, '內文摘要': desc[:240] if desc else '（RSS 摘要）'})
    except Exception: pass
    return items

def fetch_rss_market_news():
    rss_sources = [
        ('CNBC', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?profile=120000000&id=10000664'),
        ('MarketWatch', 'https://feeds.content.dowjones.io/public/rss/mw_topstories'),
        ('Investing', 'https://www.investing.com/rss/news_25.rss')
    ]
    all_items, seen = [], set()
    for source_name, url in rss_sources:
        try:
            res = requests.get(url, headers=get_headers(), timeout=10)
            if res.status_code == 200 and res.text:
                for item in parse_rss_items(res.text, source_name, limit=8):
                    if item['新聞標題'] not in seen:
                        seen.add(item['新聞標題'])
                        all_items.append(item)
        except: continue
    return all_items

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_top_news():
    news_items, seen = [], set()
    try:
        for t in ['SPY', 'QQQ']:
            news = finvizfinance(t).ticker_news()
            if news is not None and not news.empty:
                for _, row in news.head(15).iterrows():
                    title = str(row.get('Title', '')).strip()
                    if title and title not in seen:
                        seen.add(title); news_items.append({'來源': row.get('Source', 'Finviz'), '新聞標題': title, '內文摘要': '（來自 Finviz 標題）'})
    except: pass
    try:
        for t in ['SPY', 'QQQ', 'NVDA', 'AAPL']:
            tkr = yf.Ticker(t)
            if hasattr(tkr, 'news') and isinstance(tkr.news, list):
                for item in tkr.news[:5]:
                    content = item.get('content', {}) if isinstance(item, dict) else {}
                    title = content.get('title', item.get('title', ''))
                    summary = content.get('summary', item.get('summary', '無內文'))
                    publisher = item.get('publisher', 'Yahoo Finance')
                    title = str(title).strip()
                    if title and title not in seen:
                        seen.add(title); news_items.append({'來源': publisher, '新聞標題': title, '內文摘要': str(summary)[:240]})
    except: pass
    if len(news_items) < 8:
        for item in fetch_rss_market_news():
            if item['新聞標題'] not in seen:
                seen.add(item['新聞標題']); news_items.append(item)
    if not news_items:
        news_items = [
            {'來源': 'System Mock', '新聞標題': '大型科技股進入財報前觀望期，市場聚焦 AI 資本開支與指引', '內文摘要': '投資者正觀望雲端、晶片與廣告平台巨頭對 AI 投資回報的最新說法。'},
            {'來源': 'System Mock', '新聞標題': '聯儲局政策預期反覆，成長股波動加劇', '內文摘要': '市場對減息時間表仍有分歧，高估值板塊短線走勢受壓。'}
        ]
    return news_items

# ==========================================
# 4. 另類數據資料源
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_sentiment():
    try:
        url = 'https://apewisdom.io/api/v1.0/filter/all-stocks/page/1'
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                df = pd.DataFrame([{'Ticker': str(item.get('ticker', '')).upper(), 'Sentiment': 'Bullish' if item.get('mentions', 0) > 30 else 'Neutral', 'Mentions': item.get('mentions', 0) * 5} for item in results[:10]])
                return df, '🟢 ApeWisdom (過去24h數據)'
    except: pass
    mock = [{'Ticker': 'SPY', 'Sentiment': 'Bullish', 'Mentions': 2420}, {'Ticker': 'NVDA', 'Sentiment': 'Bullish', 'Mentions': 765}]
    return pd.DataFrame(mock), '🔴 離線備援 (WSB)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_stocktwits_trending():
    try:
        url = 'https://api.stocktwits.com/api/2/trending/symbols.json'
        res = requests.get(url, headers=get_headers(), timeout=8)
        if res.status_code == 200:
            symbols = res.json().get('symbols', [])
            if symbols:
                df = pd.DataFrame([{'Ticker': s.get('symbol', ''), 'Name': s.get('title', '')} for s in symbols[:10]])
                return df, '🟢 StockTwits 正常 (即時數據)'
    except: pass
    return pd.DataFrame([{'Ticker': 'AAPL', 'Name': 'Apple Inc'}]), '🔴 離線備援 (StockTwits)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_x_sentiment():
    return pd.DataFrame([
        {"Ticker": "TSLA", "Sentiment": "Bullish", "Mentions": 4820, "Bullish %": 68, "Trend": "Rising"},
        {"Ticker": "NVDA", "Sentiment": "Bullish", "Mentions": 3910, "Bullish %": 72, "Trend": "Rising"},
        {"Ticker": "PLTR", "Sentiment": "Bullish", "Mentions": 2440, "Bullish %": 66, "Trend": "Stable"},
    ]), "🔴 離線備援 (X / FinTwit)"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    target_tickers = ['NVDA', 'AAPL', 'MSFT', 'AMZN', 'META', 'GOOGL', 'TSLA', 'AMD', 'PLTR', 'CRWD']
    random.shuffle(target_tickers)
    results = []
    cutoff_date = pd.Timestamp.now(tz=None) - timedelta(days=30)
    def fetch_yf_insider(ticker):
        try:
            tkr = yf.Ticker(ticker)
            trades = tkr.insider_transactions
            if trades is None or trades.empty: return
            df = trades.reset_index()
            date_col = next((c for c in df.columns if 'date' in str(c).lower()), None)
            if date_col:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.tz_localize(None)
                df = df[df[date_col] >= cutoff_date]
            text_col = next((c for c in df.columns if 'text' in str(c).lower() or 'trans' in str(c).lower()), None)
            if text_col and not df.empty:
                buys = df[df[text_col].astype(str).str.contains('Buy|Purchase', case=False, na=False)].copy()
                for _, row in buys.head(2).iterrows():
                    shares, value = row.get('Shares', 0), row.get('Value', 0)
                    if pd.notna(value) and float(value) > 0:
                        results.append({
                            'Ticker': ticker, 'Owner': str(row.get('Insider', row.get('Name', 'N/A'))).title(),
                            'Relationship': str(row.get('Position', row.get('Title', 'Executive'))).title(),
                            'Cost': f"${float(value)/float(shares):.2f}" if pd.notna(shares) and float(shares) > 0 else 'N/A', 'Value': f"${float(value):,.0f}"
                        })
        except: pass
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        concurrent.futures.wait([executor.submit(fetch_yf_insider, t) for t in target_tickers[:8]])
    if results:
        df_final = pd.DataFrame(results)
        df_final['SortValue'] = df_final['Value'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)
        return df_final.sort_values('SortValue', ascending=False).drop(columns=['SortValue']).head(10).reset_index(drop=True)
    return pd.DataFrame([{'Ticker': 'ASTS', 'Owner': 'Abel Avellan', 'Relationship': 'CEO', 'Cost': '$24.50', 'Value': '$2,500,000'}])

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_congress_trades():
    return pd.DataFrame([{'Date': '2024-04-15', 'Politician': 'Nancy Pelosi', 'Ticker': 'PANW', 'Amount': '$1M - $5M'}]), '🔴 離線備援 (Congress)'

# ==========================================
# 5. 量化技術與財報引擎
# ==========================================
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, close_condition, batch_size=200, _progress_bar=None, _status_text=None):
    results = {}
    bench_data, used_bench = pd.DataFrame(), ''
    for b in ['QQQ', '^NDX', 'QQQM']:
        try:
            tmp = yf.download(b, period='2y', progress=False)
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
            data = yf.download(batch_tickers, period='2y', progress=False)
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
# 6. AI 分析模組
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list: return '⚠️ 目前攞唔到新聞數據，請遲啲再試下。'
    news_text = '\n'.join([f"{i+1}. [{x['來源']}] 標題：{x['新聞標題']}\n摘要：{x['內文摘要']}\n" for i, x in enumerate(news_list)])
    system_prompt = """
    你是香港頂級金融分析師。
    【強制指令】：
    1. 報告必須 100% 使用香港廣東話（例如：嘅、啲、咁、大戶、散水）。
    2. 絕對不允許輸出英文（除了股票代號）、JSON 或思考過程。
    3. 直接以「【📉 近月市場焦點總結】」作為開頭第一句。
    """.strip()
    user_prompt = f"請用廣東話分析以下新聞：\n{news_text}"
    result = call_pollinations([{'role': 'system', 'content': system_prompt}, {'role': 'user', 'content': user_prompt}], timeout=60)
    cleaned = final_text_sanitize(result)
    if "【📉 近月市場焦點總結】" not in cleaned: cleaned = f"【📉 近月市場焦點總結】\n\n{cleaned}"
    return cleaned

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, twits_df, x_df, insider_df, congress_df):
    system_prompt = """
    你是香港頂級金融分析師。
    【強制指令】：
    1. 報告必須 100% 使用香港廣東話。
    2. 絕對不允許輸出英文思考過程（如 'Not in JSON', 'Thus'）。
    3. 必須直接輸出標題：「【🕵️ 另類數據 AI 偵測深度報告】」。
    """.strip()
    user_prompt = f"""請綜合數據寫純文字廣東話報告：
    Reddit:\n{safe_to_string(reddit_df)}\nStockTwits:\n{safe_to_string(twits_df)}\nX:\n{safe_to_string(x_df)}\nInsiders:\n{safe_to_string(insider_df)}\nCongress:\n{safe_to_string(congress_df)}"""
    result = call_pollinations([{'role': 'system', 'content': system_prompt}, {'role': 'user', 'content': user_prompt}], timeout=80)
    return extract_cantonese_report(result)

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
    你是香港 AI 股評人。
    【鐵血指令】：
    1. 只允許輸出兩段文字。
    2. 第一段必須且僅能是這五個標籤之一：【🔥 極度看好】 或 【📈 偏向樂觀】 或 【⚖️ 中性觀望】 或 【📉 偏向悲觀】 或 【🧊 極度看淡】。
    3. 第二段開始，使用 100% 香港廣東話詳細解釋原因。
    4. 絕對嚴禁任何英文單字（除 Ticker 外）、JSON 代碼及思考草稿。
    """.strip()
    user_prompt = f"分析 {ticker} 股評：\n{chr(10).join(news_items)}"
    result = call_pollinations([{'role': 'system', 'content': system_prompt}, {'role': 'user', 'content': user_prompt}], timeout=30)
    label, body = extract_stock_sentiment_output(result)
    return f"{label}\n\n{body}"

# ==========================================
# 7. 終極雙劍合璧整合模組
# ==========================================
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
            ai_res = final_text_sanitize(analyze_single_stock_sentiment(ticker, news))
            lines = [x.strip() for x in ai_res.split('\n') if x.strip()]
            sentiments.append(lines[0] if lines else "【⚖️ 中性觀望】")
            reasons.append(final_text_sanitize("\n\n".join(lines[1:]) if len(lines) > 1 else "無具體解釋。"))
        else:
            sentiments.append("【⚖️ 中性觀望】")
            reasons.append("無新聞數據。")
        time.sleep(1)
    breakout_df['AI 消息情緒'] = sentiments
    breakout_df['AI 深度分析'] = reasons
    return breakout_df[~breakout_df['AI 消息情緒'].str.contains('悲觀|看淡|中性', na=False)]

# ==========================================
# 8. UI 與 Sidebar
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
# 9. 模組頁面渲染
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
                    cols = ['Ticker'] + [c for c in ['RS_階段', 'MACD_階段', 'Company', 'Sector', 'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales Growth (QoQ)'] if c in final_df.columns]
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

elif app_mode == '🕵️ 另類數據雷達 (5大維度)':
    st.title('🕵️ 另類數據雷達 (5大維度)')
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('**1. Reddit WSB 討論熱度**')
        r_df, r_msg = fetch_reddit_sentiment()
        st.caption(r_msg); st.dataframe(r_df.head(8), use_container_width=True, hide_index=True)
    with c2:
        st.markdown('**2. StockTwits 全美熱搜榜**')
        t_df, t_msg = fetch_stocktwits_trending()
        st.caption(t_msg); st.dataframe(t_df.head(8), use_container_width=True, hide_index=True)
    c3, c4 = st.columns(2)
    with c3:
        st.markdown('**3. X / FinTwit 社交情緒熱度**')
        x_df, x_msg = fetch_x_sentiment()
        st.caption(x_msg); st.dataframe(x_df.head(8), use_container_width=True, hide_index=True)
    with c4:
        st.markdown('**4. 高層 Insider 真金白銀買入**')
        i_df = fetch_insider_buying()
        st.dataframe(i_df.head(8), use_container_width=True, hide_index=True)
    st.markdown('**5. 國會議員交易 (過去45日申報)**')
    c_df, c_msg = fetch_congress_trades()
    st.caption(c_msg); st.dataframe(c_df.head(8), use_container_width=True, hide_index=True)
    if st.button('🚀 啟動 AI 五維交叉博弈分析', type='primary', use_container_width=True):
        with st.spinner('🧠 AI 正在進行五維度深度分析...'):
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
                    with st.container(border=True): st.markdown(final_text_sanitize("\n\n".join(lines[1:]) if len(lines) > 1 else "暫無補充。"))
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
            f_screener = Overview()
            f_screener.set_filter(filters_dict={'Market Cap.': '+Mid (over $2bln)'})
            raw_data = f_screener.screener_view()
        except Exception:
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
                            st.markdown(final_text_sanitize(row.get('AI 深度分析', '無分析內容。')))
                else:
                    st.warning('⚠️ AI 驗證後未見有足夠強烈好消息支持，本次無黃金名單輸出。')
            else:
                status_text.markdown('✅ 掃描完成。'); st.warning("無股票同時符合嚴格雙突破條件。")
        else:
            status_text.markdown('⚠️ 暫時攞唔到 Finviz 股票清單。')


