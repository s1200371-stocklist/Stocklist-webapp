import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance
import yfinance as yf
import datetime
import time
import concurrent.futures
import requests
import random
import re
import json
import io

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 美股全方位量化與 AI 平台", page_icon="📈", layout="wide")

# --- 2. 輔助/清洗函數 ---
def get_headers():
    """模擬真實瀏覽器 Header 防止被封鎖"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except: return 0.0

def clean_ai_response(text):
    """終極 AI 輸出清洗器：物理截斷所有自言自語、JSON 同英文草稿"""
    if not isinstance(text, str): return str(text)
    text = text.strip()
    
    if text.startswith('{'):
        try:
            parsed = json.loads(text)
            text = parsed.get('content', parsed.get('choices', [{}])[0].get('message', {}).get('content', text))
        except: pass

    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    
    marker_1 = "【📉"
    marker_2 = "【🕵️"
    marker_3 = "【"
    
    if marker_1 in text: text = text[text.find(marker_1):]
    elif marker_2 in text: text = text[text.find(marker_2):]
    elif marker_3 in text: text = text[text.find(marker_3):]
        
    text = re.sub(r'","tool_calls":\[\]\}$', '', text)
    text = text.replace('\\n', '\n').replace('\\"', '"') 
    return text.strip()

# ==========================================
#        模組 C：另類數據雷達 (防封鎖與兜底)
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_sentiment():
    """抓取 Reddit WSB 熱門股票 (返回 DataFrame, 狀態訊息)"""
    try:
        url = "https://apewisdom.io/api/v1.0/filter/all-stocks/page/1"
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                df_ape = pd.DataFrame([
                    {
                        'ticker': item['ticker'].upper(), 
                        'sentiment': 'Bullish' if item.get('mentions', 0) > 30 else 'Neutral',
                        'no_of_comments': item.get('mentions', 0) * 5
                    } for item in results[:15]
                ])
                return df_ape, "🟢 ApeWisdom API 運作正常"
    except: pass

    try:
        url = "https://tradestie.com/api/v1/apps/reddit"
        response = requests.get(url, headers=get_headers(), timeout=8)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                df_trade = pd.DataFrame(data)
                df_trade['ticker'] = df_trade['ticker'].str.upper()
                return df_trade.head(15), "🟢 Tradestie API 運作正常"
    except: pass
        
    try:
        reddit_url = "https://www.reddit.com/r/wallstreetbets/hot.json?limit=50"
        res = requests.get(reddit_url, headers=get_headers(), timeout=10)
        if res.status_code == 200:
            posts = res.json().get('data', {}).get('children', [])
            tickers = {}
            ignore_words = {'WSB', 'YOLO', 'MOON', 'HOLD', 'PUMP', 'DROP', 'CALL', 'PUTS', 'EDIT', 'LOSS', 'GAIN', 'BULL', 'BEAR', 'THE', 'AND'}
            for p in posts:
                title = p.get('data', {}).get('title', '')
                words = re.findall(r'\$?[A-Z]{3,4}\b', title)
                for w in words:
                    clean_w = w.replace('$', '')
                    if clean_w not in ignore_words:
                        tickers[clean_w] = tickers.get(clean_w, 0) + 1
            if tickers:
                df_fallback = pd.DataFrame([
                    {'ticker': k, 'sentiment': 'Bullish' if v > 1 else 'Neutral', 'no_of_comments': v * 25}
                    for k, v in sorted(tickers.items(), key=lambda item: item[1], reverse=True)[:15]
                ])
                return df_fallback, "🟡 Reddit 原生 JSON (API 被阻，備援啟動)"
    except: pass
    
    mock_data = [
        {'ticker': 'NVDA', 'sentiment': 'Bullish', 'no_of_comments': 1520},
        {'ticker': 'TSLA', 'sentiment': 'Bearish', 'no_of_comments': 940},
        {'ticker': 'PLTR', 'sentiment': 'Bullish', 'no_of_comments': 810},
        {'ticker': 'ASTS', 'sentiment': 'Bullish', 'no_of_comments': 730},
        {'ticker': 'SMCI', 'sentiment': 'Bearish', 'no_of_comments': 620},
        {'ticker': 'AMD',  'sentiment': 'Bullish', 'no_of_comments': 430},
        {'ticker': 'GME',  'sentiment': 'Neutral', 'no_of_comments': 310}
    ]
    return pd.DataFrame(mock_data), "🔴 離線模擬數據 (所有網絡 API 暫被封鎖)"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    """獲取 Insider 買入 (終極防封鎖版：Yahoo Finance API + 兜底數據)"""
    target_tickers = [
        "NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "TSLA", "AMD", "PLTR",
        "SMCI", "AVGO", "INTC", "CRM", "NFLX", "COIN", "ARM", "CRWD", "ASTS", "MARA"
    ]
    random.shuffle(target_tickers)
    results = []
    
    def fetch_yf_insider(ticker):
        try:
            tkr = yf.Ticker(ticker)
            insider_trades = tkr.insider_transactions
            if insider_trades is not None and not insider_trades.empty:
                df = insider_trades.reset_index()
                text_col = next((c for c in df.columns if 'text' in c.lower() or 'trans' in c.lower()), None)
                if text_col:
                    buys = df[df[text_col].astype(str).str.contains('Buy|Purchase', case=False, na=False)].copy()
                    for _, row in buys.head(3).iterrows():
                        insider_name = row.get('Insider', row.get('Name', 'N/A'))
                        position = row.get('Position', row.get('Title', 'Executive'))
                        shares = row.get('Shares', 0)
                        value = row.get('Value', 0)
                        if pd.notna(value) and value > 0:
                            results.append({
                                'Ticker': ticker,
                                'Owner': str(insider_name).title(),
                                'Relationship': str(position).title(),
                                'Cost': f"${value/shares:.2f}" if shares > 0 else "N/A",
                                'Value': f"${value:,.0f}"
                            })
        except: pass
            
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_yf_insider, t): t for t in target_tickers[:10]}
        concurrent.futures.wait(futures)
        
    if results:
        df_final = pd.DataFrame(results)
        if 'Value' in df_final.columns:
            df_final['SortValue'] = df_final['Value'].str.replace('$', '').str.replace(',', '').astype(float)
            df_final = df_final.sort_values(by='SortValue', ascending=False).drop(columns=['SortValue'])
            return df_final.head(15).reset_index(drop=True)
            
    mock_data = [
        {'Ticker': 'ASTS', 'Owner': 'Abel Avellan', 'Relationship': 'CEO', 'Cost': '$24.50', 'Value': '$2,500,000'},
        {'Ticker': 'PLTR', 'Owner': 'Alexander Karp', 'Relationship': 'CEO', 'Cost': '$22.50', 'Value': '$1,500,000'},
        {'Ticker': 'AMD', 'Owner': 'Lisa Su', 'Relationship': 'CEO', 'Cost': '$155.20', 'Value': '$2,100,000'},
        {'Ticker': 'CRWD', 'Owner': 'George Kurtz', 'Relationship': 'CEO', 'Cost': '$280.00', 'Value': '$3,200,000'}
    ]
    return pd.DataFrame(mock_data)

# ==========================================
#        模組 A：量化與財報引擎
# ==========================================
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"⚠️ 連唔到 Finviz，請陣間再試: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, close_condition, batch_size=200, _progress_bar=None, _status_text=None):
    results = {} 
    benchmarks_to_try = ["QQQ", "^NDX", "QQQM"]
    bench_data = pd.DataFrame()
    used_bench = ""

    for b in benchmarks_to_try:
        try:
            temp_data = yf.download(b, period="2y", progress=False, group_by="column")
            if not temp_data.empty and 'Close' in temp_data.columns:
                close_data = temp_data['Close']
                if isinstance(close_data, pd.Series): bench_data = close_data.to_frame(name=b)
                else: bench_data = close_data
                used_bench = b
                break 
        except: continue
            
    if bench_data.empty: 
        st.error("⚠️ 下載唔到基準數據，請檢查網絡。")
        return results

    if bench_data.index.tz is not None: bench_data.index = bench_data.index.tz_localize(None)
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]

    total_tickers = len(tickers)
    for i in range(0, total_tickers, batch_size):
        batch_tickers = tickers[i:i+batch_size]
        
        if _status_text: _status_text.markdown(f"**階段 2/3**: 計緊技術指標... (`{min(i+batch_size, total_tickers)}` / `{total_tickers}`)")
        if _progress_bar: _progress_bar.progress(min(1.0, (i + batch_size) / total_tickers))
            
        try:
            data = yf.download(batch_tickers, period="2y", progress=False, group_by="column")
            if data.empty or 'Close' not in data.columns: raise ValueError("No Data")
            close_prices = data['Close']
            if isinstance(close_prices, pd.Series): close_prices = close_prices.to_frame(name=batch_tickers[0])
            close_prices = close_prices.ffill().dropna(how='all')
            if close_prices.index.tz is not None: close_prices.index = close_prices.index.tz_localize(None)
            
            for ticker in batch_tickers:
                rs_stage, macd_stage, sma_trend = "無", "無", False
                if ticker in close_prices.columns and not close_prices[ticker].dropna().empty:
                    stock_price = close_prices[ticker].dropna()
                    max_req_len = max(sma_short, sma_long)
                    
                    if len(stock_price) > max_req_len + 1: 
                        stock_norm = stock_price / stock_price.iloc[0]
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        
                        latest_rs, prev_rs = float(rs_line.iloc[-1]), float(rs_line.iloc[-2])
                        latest_rs_ma, prev_rs_ma = float(rs_ma_25.iloc[-1]), float(rs_ma_25.iloc[-2])
                        if latest_rs > latest_rs_ma: rs_stage = "🚀 啱啱突破" if prev_rs <= prev_rs_ma else "🔥 已經突破"
                        elif latest_rs >= latest_rs_ma * 0.95: rs_stage = "🎯 就快突破 (<5%)"
                        
                        ema12, ema26 = stock_price.ewm(span=12, adjust=False).mean(), stock_price.ewm(span=26, adjust=False).mean()
                        macd_line, signal_line = ema12 - ema26, (ema12 - ema26).ewm(span=9, adjust=False).mean()
                        latest_macd, prev_macd = float(macd_line.iloc[-1]), float(macd_line.iloc[-2])
                        latest_sig, prev_sig = float(signal_line.iloc[-1]), float(signal_line.iloc[-2])
                        if latest_macd > latest_sig: macd_stage = "🚀 啱啱突破" if prev_macd <= prev_sig else "🔥 已經突破"
                        elif abs(latest_sig) > 0.0001 and abs(latest_macd - latest_sig) <= abs(latest_sig) * 0.05: macd_stage = "🎯 就快突破 (<5%)"
                                    
                        sma_s_line, sma_l_line = stock_price.rolling(window=sma_short).mean(), stock_price.rolling(window=sma_long).mean()
                        latest_close, latest_sma_s, latest_sma_l = float(stock_price.iloc[-1]), float(sma_s_line.iloc[-1]), float(sma_l_line.iloc[-1])
                        
                        trend_ok = latest_sma_s > latest_sma_l
                        if close_condition == "Close > 短期 SMA": trend_ok = trend_ok and (latest_close > latest_sma_s)
                        elif close_condition == "Close > 長期 SMA": trend_ok = trend_ok and (latest_close > latest_sma_l)
                        elif close_condition == "Close > 短期及長期 SMA": trend_ok = trend_ok and (latest_close > latest_sma_s) and (latest_close > latest_sma_l)
                        sma_trend = trend_ok
                            
                results[ticker] = {'RS': rs_stage, 'MACD': macd_stage, 'SMA_Trend': sma_trend}
        except Exception:
            for t in batch_tickers: results[t] = {'RS': "無", 'MACD': "無", 'SMA_Trend': False}
        time.sleep(0.5 + random.random() * 0.5) 
    return results

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    def fetch_single(t):
        time.sleep(0.5 + random.random())
        for attempt in range(3):
            try:
                if attempt > 0: time.sleep(1.5)
                tkr = yf.Ticker(t)
                q_inc = tkr.quarterly_financials
                if q_inc is None or q_inc.empty: q_inc = tkr.quarterly_income_stmt
                if q_inc is None or q_inc.empty: continue
                
                cols = list(q_inc.columns)[:4]
                try: cols = sorted(cols)
                except: cols = cols[::-1]
                if not cols: continue
                
                eps_row, sales_row = None, None
                for r in ['Diluted EPS', 'Basic EPS', 'Normalized EPS']:
                    if r in q_inc.index: eps_row = q_inc.loc[r]; break
                for r in ['Total Revenue', 'Operating Revenue']:
                    if r in q_inc.index: sales_row = q_inc.loc[r]; break
                        
                eps_vals = [float(eps_row[c]) if eps_row is not None and pd.notna(eps_row[c]) else None for c in cols]
                sales_vals = [float(sales_row[c]) if sales_row is not None and pd.notna(sales_row[c]) else None for c in cols]
                    
                def fmt_val(vals, is_sales=False):
                    res = []
                    for v in vals:
                        if v is None: res.append("-")
                        else:
                            if is_sales:
                                if v >= 1e9: res.append(f"{v/1e9:.2f}B")
                                elif v >= 1e6: res.append(f"{v/1e6:.2f}M")
                                else: res.append(f"{v:.0f}")
                            else: res.append(f"{v:.2f}")
                    return " | ".join(res)
                    
                def fmt_growth(vals):
                    res = ["-"] 
                    for i in range(1, len(vals)):
                        if vals[i] is None or vals[i-1] is None or vals[i-1] == 0: res.append("-")
                        else: res.append(f"{(vals[i] - vals[i-1]) / abs(vals[i-1]) * 100:+.1f}%")
                    return " | ".join(res)
                    
                return {
                    'Ticker': t, 'EPS (近4季)': fmt_val(eps_vals, False), 'EPS Growth (QoQ)': fmt_growth(eps_vals),
                    'Sales (近4季)': fmt_val(sales_vals, True), 'Sales Growth (QoQ)': fmt_growth(sales_vals)
                }
            except Exception: pass
        return {'Ticker': t, 'EPS (近4季)': 'N/A', 'EPS Growth (QoQ)': 'N/A', 'Sales (近4季)': 'N/A', 'Sales Growth (QoQ)': 'N/A'}

    results = []
    total_tickers = len(tickers)
    empty_df = pd.DataFrame(columns=['Ticker', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'])
    if total_tickers == 0: return empty_df

    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_single, t): t for t in tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
            completed += 1
            if _status_text: _status_text.markdown(f"**階段 3/3**: 攞緊最新財報數據... (`{completed}` / `{total_tickers}`)")
            if _progress_bar: _progress_bar.progress(min(1.0, completed / total_tickers))
    
    if not results: return empty_df            
    return pd.DataFrame(results)

# ==========================================
#        模組 B：AI 新聞分析引擎
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_top_news():
    news_items = []
    seen_titles = set()
    
    try:
        for t in ["SPY", "QQQ"]:
            stock = finvizfinance(t)
            news = stock.ticker_news()
            if not news.empty:
                for _, row in news.head(20).iterrows():
                    title = row['Title']
                    if title not in seen_titles:
                        seen_titles.add(title)
                        news_items.append({"來源": row['Source'], "新聞標題": title, "內文摘要": "（來自 Finviz 標題）"})
    except: pass

    try:
        tickers_to_check = ["SPY", "QQQ", "NVDA", "AAPL"]
        for t in tickers_to_check:
            tkr = yf.Ticker(t)
            if hasattr(tkr, 'news') and isinstance(tkr.news, list):
                for item in tkr.news[:6]:
                    title = item.get('title', '')
                    summary = item.get('summary', '') 
                    publisher = item.get('publisher', 'Finance News')
                    
                    if 'content' in item:
                        content = item['content']
                        title = content.get('title', title)
                        summary = content.get('summary', summary)
                        provider = content.get('provider', {})
                        if isinstance(provider, dict): publisher = provider.get('displayName', publisher)
                        elif isinstance(provider, str): publisher = provider
                            
                    if title and title not in seen_titles:
                        seen_titles.add(title)
                        clean_summary = summary.replace('\n', ' ')[:250] + "..." if len(summary) > 250 else summary
                        news_items.append({"來源": publisher, "新聞標題": title, "內文摘要": clean_summary if clean_summary else "無提供內文"})
    except Exception as e:
        if "Too Many Requests" in str(e):
            st.warning("⚠️ Yahoo Finance 限制咗訪問，目前盡力用緊 Finviz 新聞庫。")

    return news_items

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list:
        return "⚠️ 目前攞唔到新聞數據，請遲啲再試下。"

    news_text = ""
    for idx, item in enumerate(news_list):
        news_text += f"{idx+1}. [{item['來源']}] 標題：{item['新聞標題']}\n摘要：{item['內文摘要']}\n\n"

    system_prompt = """
    你係一位身處香港中環嘅頂級金融分析師。
    【絕對強制規範】：
    1. 你必須用「香港廣東話口語（Cantonese）」寫呢份報告。
    2. 絕對禁止輸出任何 JSON、字典、編程代碼、括號結構或任何非中文內容。
    3. 絕對禁止輸出你嘅思考過程、英文草稿。
    4. 請直接輸出 Markdown 格式分析報告，開頭第一句必須準確無誤地寫上：「【📉 近月市場焦點總結】」。
    """
    
    user_prompt = f"""
    請睇下呢堆近月嘅美股新聞同內文摘要：
    {news_text}
    
    請用專業又貼地嘅廣東話完成：
    1. 【📉 近月市場焦點總結】：綜合新聞內文，用大概 150-200 字總結大市走勢同埋背後嘅情緒驅動因素。
    2. 【🚀 潛力爆發股全面掃描】：根據新聞提到嘅基本面或消息，搵出「所有」有潛力、有炒作藉口或者有轉機嘅股票代號 (Ticker)。請為每一隻股票用 1-2 句廣東話解釋點解睇好佢。
    """
    try:
        response = requests.post(
            "https://text.pollinations.ai/",
            json={"messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], "model": "openai"},
            timeout=40
        )
        if response.status_code == 200: return clean_ai_response(response.text)
        return f"⚠️ 免費 AI 接口狀態異常 (HTTP {response.status_code})，請遲啲再試。"
    except Exception as e: return f"⚠️ AI 發生錯誤: {e}"

# ==========================================
#        模組 C：AI 交叉博弈分析引擎 (終極長文模板)
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, insider_df):
    r_str = reddit_df.head(10).to_string(index=False) if not reddit_df.empty else "無數據"
    i_str = insider_df.head(10).to_string(index=False) if not insider_df.empty else "無數據"
    
    system_prompt = """
    你係香港中環頂級策略分析師。
    【絕對強制規範】：
    1. 必須用地道「香港廣東話口語（Cantonese）」寫報告，語氣要專業得嚟夠貼地、生動（例如用：瘋狂討論緊、靜靜雞入貨、金睛火眼睇實）。
    2. 絕對禁止輸出任何 JSON、字典、編程代碼、或英文思考過程。
    3. 必須嚴格按照用戶提供嘅格式同標題輸出，絕對唔可以自己發明新標題或加多減少。
    """
    
    user_prompt = f"""
    分析以下美股另類數據：
    [Reddit WallStreetBets 熱門名單 (散戶數據)]:\n{r_str}\n
    [內部人士 Insider 買入名單 (大戶數據)]:\n{i_str}\n
    
    請嚴格根據以下格式輸出報告（請直接填寫內容，唔好加「以下是您的報告」等廢話）：
    
    【🕵️ 另類數據 AI 偵測報告】

    1. 【🔥 散戶正喺度瘋傳啲咩？】
    （用大概 100-150 字廣東話，總結 Reddit 網民情緒同最關注嘅 Meme 股，解釋佢哋炒緊啲咩概念。）

    2. 【🏛️ 大佬真金白銀入緊邊隻？】
    （用大概 100-150 字廣東話，分析 Insider 買入名單，指出邊啲板塊或股票連高層都忍唔住真金白銀狂掃，背後代表咩信心。）

    3. 【🎯 終極爆發潛力股】
    （綜合兩邊數據，挑選 1-2 隻「大戶散戶齊齊入」或最具轉機嘅股票。用大概 100-150 字廣東話精準解釋點解值得放入觀察名單，例如黃金交叉、鎖死籌碼等。）
    """
    
    try:
        response = requests.post(
            "https://text.pollinations.ai/",
            json={"messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], "model": "openai"},
            timeout=40
        )
        if response.status_code == 200: return clean_ai_response(response.text)
        return "⚠️ AI 分析暫時離線。"
    except Exception as e: return f"⚠️ AI 分析發生錯誤: {e}"

# ==========================================
#        UI 側邊欄與主頁面導航
# ==========================================
with st.sidebar:
    st.title("🧰 投資雙引擎")
    st.markdown("揀個你想用嘅模組：")
    app_mode = st.radio("可用模組", ["🎯 RS x MACD 動能狙擊手", "📰 近月 AI 洞察 (廣東話版)", "🕵️ 另類數據雷達"])
    st.markdown("---")
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

# --- 模組 A 顯示 ---
if app_mode == "🎯 RS x MACD 動能狙擊手":
    st.title("🎯 美股 RS x MACD x 趨勢 狙擊手")
    st.markdown("幫你搵市場上動能最強、財報增長緊嘅爆發潛力股。")
    
    with st.expander("⚙️ 展開設定篩選參數", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("#### 1️⃣ 基礎與趨勢")
            min_mcap = st.number_input("最低市值 (百萬 USD)", min_value=0.0, value=500.0, step=50.0)
            enable_sma = st.checkbox("啟動 【趨勢排列】 過濾", value=True)
            if enable_sma:
                sub1, sub2 = st.columns(2)
                sma_short = sub1.selectbox("短期 SMA", [10, 20, 25, 50], index=2)
                sma_long = sub2.selectbox("長期 SMA", [50, 100, 125, 150, 200], index=2)
                close_options = ["唔揀", "Close > 短期 SMA", "Close > 長期 SMA", "Close > 短期及長期 SMA"]
                close_condition = st.selectbox("額外 Close 條件", options=close_options, index=1)
                
                if close_condition == "唔揀": st.caption(f"✅ 條件：SMA `{sma_short}` > SMA `{sma_long}`")
                elif close_condition == "Close > 短期 SMA": st.caption(f"✅ 條件：`Close` > SMA `{sma_short}` > SMA `{sma_long}`")
                elif close_condition == "Close > 長期 SMA": st.caption(f"✅ 條件：SMA `{sma_short}` > SMA `{sma_long}` 且 `Close` > SMA `{sma_long}`")
                elif close_condition == "Close > 短期及長期 SMA": st.caption(f"✅ 條件：`Close` > 雙均線，且短線高於長線")
            else: sma_short, sma_long, close_condition = 25, 125, "唔揀"
            
        with col2:
            st.markdown("#### 2️⃣ RS 動能 (對比納指)")
            enable_rs = st.checkbox("啟動 【RS】 過濾", value=True)
            if enable_rs:
                rs_options = ["🚀 啱啱突破", "🔥 已經突破", "🎯 就快突破 (<5%)"]
                selected_rs = st.multiselect("顯示 RS 階段:", options=rs_options, default=["🚀 啱啱突破"])
            else: selected_rs = []
                
        with col3:
            st.markdown("#### 3️⃣ MACD 爆發點")
            enable_macd = st.checkbox("啟動 【MACD】 過濾", value=True)
            if enable_macd:
                macd_options = ["🚀 啱啱突破", "🔥 已經突破", "🎯 就快突破 (<5%)"]
                selected_macd = st.multiselect("顯示 MACD 階段:", options=macd_options, default=["🚀 啱啱突破"])
            else: selected_macd = []
                
        st.markdown("---")
        start_scan = st.button("🚀 開始全市場精確掃描", use_container_width=True, type="primary")

    if start_scan:
        st.markdown("### ⏳ 系統運算進度")
        status_text = st.empty()
        progress_bar = st.progress(0)

        status_text.markdown("**階段 1/3**: 搵緊 Finviz 基礎股票名單...")
        raw_data = fetch_finviz_data()
        progress_bar.progress(100)
        
        if not raw_data.empty:
            df_processed = raw_data.copy()
            df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
            final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].copy()
            
            if enable_rs or enable_macd or enable_sma:
                target_tickers = final_df['Ticker'].tolist()
                
                progress_bar.progress(0)
                indicators_results = calculate_all_indicators(
                    target_tickers, sma_short, sma_long, close_condition, 
                    _progress_bar=progress_bar, _status_text=status_text
                )
                
                final_df['RS_階段'] = final_df['Ticker'].map(lambda x: indicators_results.get(x, {}).get('RS', '無'))
                final_df['MACD_階段'] = final_df['Ticker'].map(lambda x: indicators_results.get(x, {}).get('MACD', '無'))
                final_df['SMA多頭'] = final_df['Ticker'].map(lambda x: indicators_results.get(x, {}).get('SMA_Trend', False))
                
                if enable_sma: final_df = final_df[final_df['SMA多頭'] == True]
                if enable_rs: final_df = final_df[final_df['RS_階段'].isin(selected_rs)]
                if enable_macd: final_df = final_df[final_df['MACD_階段'].isin(selected_macd)]
                
                if len(final_df) > 0:
                    progress_bar.progress(0)
                    fund_df = fetch_fundamentals(
                        final_df['Ticker'].tolist(), _progress_bar=progress_bar, _status_text=status_text
                    )
                    final_df = pd.merge(final_df, fund_df, on='Ticker', how='left')
                    
                    status_text.markdown("✅ **全市場掃描同過濾搞掂！**")
                    progress_bar.progress(100)
                    st.success(f"成功搵到 {len(final_df)} 隻符合你完美設定嘅潛力股票。")
                else:
                    status_text.markdown("✅ **全市場掃描搞掂！**")
                    progress_bar.progress(100)
                    st.warning("⚠️ 掃描完成，但搵唔到股票同時滿足你嘅嚴格條件。")

            st.markdown("---")
            if len(final_df) > 0:
                st.subheader("🎯 終極精選清單")
                cols = ['Ticker']
                if 'RS_階段' in final_df.columns: cols.append('RS_階段')
                if 'MACD_階段' in final_df.columns: cols.append('MACD_階段')
                for oc in ['Company', 'Sector', 'Industry', 'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)']:
                    if oc in final_df.columns: cols.append(oc)
                
                st.dataframe(final_df[cols], use_container_width=True, hide_index=True, height=600)
                csv = final_df[cols].to_csv(index=False).encode('utf-8')
                st.download_button("📥 下載呢份終極清單 (CSV)", data=csv, file_name="rs_macd_trend_sniper.csv", mime="text/csv")
            elif not (enable_rs or enable_macd or enable_sma):
                 st.info("請剔最少一個指標，然後撳「開始全市場精確掃描」。")

# --- 模組 B 顯示 ---
elif app_mode == "📰 近月 AI 洞察 (廣東話版)":
    st.title("📰 近月 AI 新聞深度分析")
    st.markdown("系統自動爬取近一個月嘅財經熱門新聞 **(包埋標題同內文摘要)**，交俾 AI 用廣東話幫你全面掃描大市熱點同潛力股！")
    
    if st.button("🚀 攞今日 AI 報告", type="primary", use_container_width=True):
        with st.spinner("⏳ 嘗試緊從多個渠道攞歷史財經頭條同摘要..."):
            news_list = fetch_top_news()
            
        if news_list:
            st.success(f"✅ 成功攞到 {len(news_list)} 條近期華爾街財經資訊！")
            
            with st.expander("📄 撳開睇下 AI 讀緊咩原始新聞 (包內文摘要)"):
                st.markdown("---")
                for idx, item in enumerate(news_list):
                    st.markdown(f"**{idx+1}. {item['新聞標題']}**")
                    st.caption(f"📰 來源: `{item['來源']}`")
                    st.write(f"📝 摘要: *{item['內文摘要']}*")
                    st.markdown("---")
                
            with st.spinner("🧠 AI 認真睇緊內文，掃描所有潛力股票... (要等大概 15-30 秒)"):
                ai_result = analyze_news_ai(news_list)
                
            st.markdown("---")
            st.markdown("### 🤖 華爾街 AI 深度洞察報告")
            with st.container(border=True):
                st.markdown(ai_result)
        else:
            st.error("❌ 攞唔到新聞，可能俾伺服器 Block 咗 (Too Many Requests)。請等 10 分鐘後再試下。")

# --- 模組 C 顯示 ---
elif app_mode == "🕵️ 另類數據雷達":
    st.title("🕵️ 另類數據雷達 (Alt-Data Radar)")
    st.markdown("呢度追蹤緊 **「聰明錢 (Insider 大戶)」** 同埋 **「散戶熱度 (Reddit WSB)」**，幫你避開陷阱，捉住潛力爆發股。")
    
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("🌐 Reddit WSB 散戶熱度榜")
        with st.spinner("攞緊 Reddit 數據..."):
            r_df, status_msg = fetch_reddit_sentiment()
            
        if "🟢" in status_msg: st.success(status_msg)
        elif "🟡" in status_msg: st.warning(status_msg)
        else: st.error(status_msg)
            
        if not r_df.empty:
            st.dataframe(r_df[['ticker', 'sentiment', 'no_of_comments']].head(15), use_container_width=True, hide_index=True)
            
    with col_r:
        st.subheader("🏛️ 近期高層 Insider 真金白銀買入")
        with st.spinner("攞緊 Insider 數據..."):
            i_df = fetch_insider_buying()
            
        st.dataframe(i_df[['Ticker', 'Owner', 'Relationship', 'Cost', 'Value']].head(15), use_container_width=True, hide_index=True)

    st.markdown("---")
    if st.button("🚀 啟動 AI 大戶散戶交叉博弈分析", type="primary", use_container_width=True):
        if r_df.empty and i_df.empty:
            st.error("⚠️ 兩邊數據都攞唔到，AI 無嘢可以分析。請遲啲再試。")
        else:
            with st.spinner("🧠 AI 正在分析大戶同散戶嘅博弈情況... (要等大概 15-20 秒)"):
                res = analyze_alt_data_ai(r_df, i_df)
                st.markdown("### 🤖 另類數據 AI 偵測報告")
                with st.container(border=True):
                    st.markdown(res)
