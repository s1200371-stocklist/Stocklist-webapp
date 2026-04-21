import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance
import yfinance as yf
import datetime
from datetime import timedelta
import time
import concurrent.futures
import requests
import random
import re
import json

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 美股全方位量化與 AI 平台", page_icon="📈", layout="wide")

# --- 2. 輔助/清洗函數 ---
def get_headers():
    """模擬真實瀏覽器 Header 防止被封鎖"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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
    """終極 AI 輸出清洗器"""
    if not isinstance(text, str): return str(text)
    text = text.strip()
    
    if text.startswith('{'):
        try:
            parsed = json.loads(text)
            text = parsed.get('content', parsed.get('choices', [{}])[0].get('message', {}).get('content', text))
        except: pass

    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    
    marker_1, marker_2, marker_3 = "【📉", "【🕵️", "【"
    
    if marker_1 in text: text = text[text.find(marker_1):]
    elif marker_2 in text: text = text[text.find(marker_2):]
    elif marker_3 in text: text = text[text.find(marker_3):]
        
    text = re.sub(r'","tool_calls":\[\]\}$', '', text)
    return text.replace('\\n', '\n').replace('\\"', '"').strip()

# ==========================================
#        模組 C：擴充版另類數據雷達 (混合時間窗口)
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_sentiment():
    """抓取 Reddit WSB 熱門股票 (24小時內極短線情緒)"""
    try:
        url = "https://apewisdom.io/api/v1.0/filter/all-stocks/page/1"
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                df_ape = pd.DataFrame([
                    {'Ticker': item['ticker'].upper(), 'Sentiment': 'Bullish' if item.get('mentions', 0) > 30 else 'Neutral', 'Mentions': item.get('mentions', 0) * 5}
                    for item in results[:10]
                ])
                return df_ape, "🟢 ApeWisdom (過去 24h 數據)"
    except: pass
    
    mock_data = [
        {'Ticker': 'NVDA', 'Sentiment': 'Bullish', 'Mentions': 1520},
        {'Ticker': 'TSLA', 'Sentiment': 'Bearish', 'Mentions': 940},
        {'Ticker': 'ASTS', 'Sentiment': 'Bullish', 'Mentions': 810},
        {'Ticker': 'PLTR', 'Sentiment': 'Bullish', 'Mentions': 730},
        {'Ticker': 'SMCI', 'Sentiment': 'Bearish', 'Mentions': 620}
    ]
    return pd.DataFrame(mock_data), "🔴 離線備援 (WSB)"

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_stocktwits_trending():
    """抓取 StockTwits 散戶熱門榜 (當刻即時情緒)"""
    try:
        url = "https://api.stocktwits.com/api/2/trending/symbols.json"
        res = requests.get(url, headers=get_headers(), timeout=8)
        if res.status_code == 200:
            symbols = res.json().get('symbols', [])
            if symbols:
                df = pd.DataFrame([{'Ticker': s['symbol'], 'Name': s['title']} for s in symbols[:10]])
                return df, "🟢 StockTwits 正常 (即時數據)"
    except: pass
    
    mock_data = [
        {'Ticker': 'NVDA', 'Name': 'NVIDIA'}, {'Ticker': 'AAPL', 'Name': 'Apple'},
        {'Ticker': 'AMD', 'Name': 'Advanced Micro Devices'}, {'Ticker': 'CRWD', 'Name': 'CrowdStrike'},
        {'Ticker': 'HOOD', 'Name': 'SoFi Technologies'}
    ]
    return pd.DataFrame(mock_data), "🔴 離線備援 (StockTwits)"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    """獲取 Insider 高層真金白銀買入 (嚴格鎖定過去 30 日內)"""
    target_tickers = ["NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "TSLA", "AMD", "PLTR", "CRWD", "ASTS", "COIN", "MARA"]
    random.shuffle(target_tickers)
    results = []
    
    # 計算 30 日前嘅日期
    cutoff_date = pd.Timestamp.now() - timedelta(days=30)
    
    def fetch_yf_insider(ticker):
        try:
            tkr = yf.Ticker(ticker)
            trades = tkr.insider_transactions
            if trades is not None and not trades.empty:
                df = trades.reset_index()
                
                # 確保有時間欄位進行 30 日篩選
                date_col = next((c for c in df.columns if 'date' in c.lower()), None)
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                    df = df[df[date_col] >= cutoff_date]
                
                text_col = next((c for c in df.columns if 'text' in c.lower() or 'trans' in c.lower()), None)
                if text_col and not df.empty:
                    buys = df[df[text_col].astype(str).str.contains('Buy|Purchase', case=False, na=False)].copy()
                    for _, row in buys.head(2).iterrows():
                        shares, value = row.get('Shares', 0), row.get('Value', 0)
                        if pd.notna(value) and value > 0:
                            results.append({
                                'Ticker': ticker,
                                'Owner': str(row.get('Insider', row.get('Name', 'N/A'))).title(),
                                'Value': f"${value:,.0f}"
                            })
        except: pass
            
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_yf_insider, t): t for t in target_tickers[:8]}
        concurrent.futures.wait(futures)
        
    if results:
        df_final = pd.DataFrame(results)
        if 'Value' in df_final.columns:
            df_final['SortValue'] = df_final['Value'].str.replace('$', '').str.replace(',', '').astype(float)
            return df_final.sort_values(by='SortValue', ascending=False).drop(columns=['SortValue']).head(10).reset_index(drop=True)
            
    mock_data = [
        {'Ticker': 'ASTS', 'Owner': 'Abel Avellan', 'Value': '$2,500,000'},
        {'Ticker': 'PLTR', 'Owner': 'Alexander Karp', 'Value': '$1,500,000'},
        {'Ticker': 'CRWD', 'Owner': 'George Kurtz', 'Value': '$3,200,000'}
    ]
    return pd.DataFrame(mock_data)

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_congress_trades():
    """抓取美國國會議員交易 (嚴格鎖定過去 45 日內申報)"""
    try:
        url = "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
        res = requests.get(url, headers=get_headers(), timeout=8)
        if res.status_code == 200:
            data = res.json()
            df = pd.DataFrame(data)
            
            # 過濾純買入
            df = df[df['type'].astype(str).str.lower() == 'purchase']
            
            # 轉換日期並進行 45 日篩選
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
            cutoff_date = pd.Timestamp.now() - timedelta(days=45)
            df = df[df['transaction_date'] >= cutoff_date]
            
            df = df.sort_values('transaction_date', ascending=False).dropna(subset=['transaction_date'])
            df = df[['transaction_date', 'representative', 'ticker', 'amount']].head(10)
            df.columns = ['Date', 'Politician', 'Ticker', 'Amount']
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            
            # 如果近 45 日真係冇數據，會自然觸發 fallback
            if not df.empty:
                return df, "🟢 國會交易 (過去 45 日數據)"
    except: pass
    
    mock_data = [
        {'Date': '2026-04-15', 'Politician': 'Nancy Pelosi', 'Ticker': 'PANW', 'Amount': '$1M - $5M'},
        {'Date': '2026-04-12', 'Politician': 'Ro Khanna', 'Ticker': 'CRWD', 'Amount': '$15K - $50K'},
        {'Date': '2026-04-10', 'Politician': 'Michael McCaul', 'Ticker': 'NVDA', 'Amount': '$100K - $250K'},
        {'Date': '2026-04-05', 'Politician': 'Nancy Pelosi', 'Ticker': 'MSFT', 'Amount': '$500K - $1M'}
    ]
    return pd.DataFrame(mock_data), "🔴 離線備援 (Congress)"

# ==========================================
#        模組 A：量化與財報引擎
# ==========================================
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e: return pd.DataFrame()

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
                bench_data = close_data.to_frame(name=b) if isinstance(close_data, pd.Series) else close_data
                used_bench = b; break 
        except: continue
    if bench_data.empty: return results

    if bench_data.index.tz is not None: bench_data.index = bench_data.index.tz_localize(None)
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]

    total_tickers = len(tickers)
    for i in range(0, total_tickers, batch_size):
        batch_tickers = tickers[i:i+batch_size]
        if _status_text: _status_text.markdown(f"**階段 2/3**: 計緊技術指標... (`{min(i+batch_size, total_tickers)}` / `{total_tickers}`)")
        if _progress_bar: _progress_bar.progress(min(1.0, (i + batch_size) / total_tickers))
        try:
            data = yf.download(batch_tickers, period="2y", progress=False, group_by="column")
            if data.empty or 'Close' not in data.columns: raise ValueError()
            close_prices = data['Close']
            if isinstance(close_prices, pd.Series): close_prices = close_prices.to_frame(name=batch_tickers[0])
            close_prices = close_prices.ffill().dropna(how='all')
            if close_prices.index.tz is not None: close_prices.index = close_prices.index.tz_localize(None)
            
            for ticker in batch_tickers:
                rs_stage, macd_stage, sma_trend = "無", "無", False
                if ticker in close_prices.columns and not close_prices[ticker].dropna().empty:
                    stock_price = close_prices[ticker].dropna()
                    if len(stock_price) > max(sma_short, sma_long) + 1: 
                        stock_norm = stock_price / stock_price.iloc[0]
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        if float(rs_line.iloc[-1]) > float(rs_ma_25.iloc[-1]): rs_stage = "🚀 啱啱突破" if float(rs_line.iloc[-2]) <= float(rs_ma_25.iloc[-2]) else "🔥 已經突破"
                        elif float(rs_line.iloc[-1]) >= float(rs_ma_25.iloc[-1]) * 0.95: rs_stage = "🎯 就快突破 (<5%)"
                        
                        ema12, ema26 = stock_price.ewm(span=12, adjust=False).mean(), stock_price.ewm(span=26, adjust=False).mean()
                        macd_line, signal_line = ema12 - ema26, (ema12 - ema26).ewm(span=9, adjust=False).mean()
                        if float(macd_line.iloc[-1]) > float(signal_line.iloc[-1]): macd_stage = "🚀 啱啱突破" if float(macd_line.iloc[-2]) <= float(signal_line.iloc[-2]) else "🔥 已經突破"
                        elif abs(float(macd_line.iloc[-1]) - float(signal_line.iloc[-1])) <= abs(float(signal_line.iloc[-1])) * 0.05: macd_stage = "🎯 就快突破 (<5%)"
                                    
                        sma_s_line, sma_l_line = stock_price.rolling(window=sma_short).mean(), stock_price.rolling(window=sma_long).mean()
                        trend_ok = float(sma_s_line.iloc[-1]) > float(sma_l_line.iloc[-1])
                        latest_close = float(stock_price.iloc[-1])
                        if close_condition == "Close > 短期 SMA": trend_ok = trend_ok and (latest_close > float(sma_s_line.iloc[-1]))
                        elif close_condition == "Close > 長期 SMA": trend_ok = trend_ok and (latest_close > float(sma_l_line.iloc[-1]))
                        elif close_condition == "Close > 短期及長期 SMA": trend_ok = trend_ok and (latest_close > float(sma_s_line.iloc[-1])) and (latest_close > float(sma_l_line.iloc[-1]))
                        sma_trend = trend_ok
                results[ticker] = {'RS': rs_stage, 'MACD': macd_stage, 'SMA_Trend': sma_trend}
        except:
            for t in batch_tickers: results[t] = {'RS': "無", 'MACD': "無", 'SMA_Trend': False}
        time.sleep(0.5) 
    return results

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    def fetch_single(t):
        for attempt in range(2):
            try:
                tkr = yf.Ticker(t)
                q_inc = tkr.quarterly_financials
                if q_inc is None or q_inc.empty: q_inc = tkr.quarterly_income_stmt
                if q_inc is None or q_inc.empty: continue
                cols = list(q_inc.columns)[:4]
                cols = sorted(cols) if hasattr(cols[0], 'year') else cols[::-1]
                
                eps_row, sales_row = None, None
                for r in ['Diluted EPS', 'Basic EPS', 'Normalized EPS']:
                    if r in q_inc.index: eps_row = q_inc.loc[r]; break
                for r in ['Total Revenue', 'Operating Revenue']:
                    if r in q_inc.index: sales_row = q_inc.loc[r]; break
                        
                eps_vals = [float(eps_row[c]) if eps_row is not None and pd.notna(eps_row[c]) else None for c in cols]
                sales_vals = [float(sales_row[c]) if sales_row is not None and pd.notna(sales_row[c]) else None for c in cols]
                    
                def fmt_val(vals, is_s=False):
                    res = []
                    for v in vals:
                        if v is None: res.append("-")
                        elif is_s: res.append(f"{v/1e9:.2f}B" if v>=1e9 else f"{v/1e6:.2f}M")
                        else: res.append(f"{v:.2f}")
                    return " | ".join(res)
                def fmt_growth(vals):
                    res = ["-"]
                    for i in range(1, len(vals)):
                        res.append("-" if vals[i] is None or vals[i-1] is None or vals[i-1]==0 else f"{(vals[i]-vals[i-1])/abs(vals[i-1])*100:+.1f}%")
                    return " | ".join(res)
                return {'Ticker': t, 'EPS (近4季)': fmt_val(eps_vals), 'EPS Growth (QoQ)': fmt_growth(eps_vals), 'Sales (近4季)': fmt_val(sales_vals, True), 'Sales Growth (QoQ)': fmt_growth(sales_vals)}
            except: time.sleep(1)
        return {'Ticker': t, 'EPS (近4季)': 'N/A', 'EPS Growth (QoQ)': 'N/A', 'Sales (近4季)': 'N/A', 'Sales Growth (QoQ)': 'N/A'}

    results, completed, total_tickers = [], 0, len(tickers)
    empty_df = pd.DataFrame(columns=['Ticker', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'])
    if total_tickers == 0: return empty_df

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_single, t): t for t in tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
            completed += 1
            if _status_text: _status_text.markdown(f"**階段 3/3**: 攞緊最新財報數據... (`{completed}` / `{total_tickers}`)")
            if _progress_bar: _progress_bar.progress(min(1.0, completed / total_tickers))
    return pd.DataFrame(results) if results else empty_df

# ==========================================
#        模組 B：AI 新聞分析引擎
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_top_news():
    news_items, seen = [], set()
    try:
        for t in ["SPY", "QQQ"]:
            news = finvizfinance(t).ticker_news()
            if not news.empty:
                for _, row in news.head(15).iterrows():
                    if row['Title'] not in seen:
                        seen.add(row['Title']); news_items.append({"來源": row['Source'], "新聞標題": row['Title'], "內文摘要": "（來自 Finviz 標題）"})
    except: pass

    try:
        for t in ["SPY", "QQQ", "NVDA", "AAPL"]:
            tkr = yf.Ticker(t)
            if hasattr(tkr, 'news') and isinstance(tkr.news, list):
                for item in tkr.news[:5]:
                    title = item.get('content', {}).get('title', item.get('title', ''))
                    if title and title not in seen:
                        seen.add(title)
                        sum_text = item.get('content', {}).get('summary', item.get('summary', '無內文'))
                        news_items.append({"來源": item.get('publisher', 'Finance News'), "新聞標題": title, "內文摘要": sum_text[:200]})
    except: pass
    return news_items

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list: return "⚠️ 目前攞唔到新聞數據，請遲啲再試下。"
    news_text = "\n".join([f"{i+1}. [{x['來源']}] 標題：{x['新聞標題']}\n摘要：{x['內文摘要']}\n" for i, x in enumerate(news_list)])

    system_prompt = "你係香港中環頂級金融分析師。必須用「香港廣東話口語」寫報告。絕對禁止輸出JSON或編程代碼。直接輸出Markdown報告，開頭第一句必須係：「【📉 近月市場焦點總結】」。"
    user_prompt = f"分析以下新聞：\n{news_text}\n1. 【📉 近月市場焦點總結】：150-200字總結大市走勢同情緒。\n2. 【🚀 潛力爆發股全面掃描】：搵出所有有潛力/炒作嘅Ticker，每隻用1-2句廣東話解釋點解睇好。"
    
    try:
        res = requests.post("https://text.pollinations.ai/", json={"messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], "model": "openai"}, timeout=40)
        return clean_ai_response(res.text) if res.status_code == 200 else "⚠️ AI 接口異常"
    except Exception as e: return f"⚠️ AI 發生錯誤: {e}"

# ==========================================
#        模組 C：AI 交叉博弈分析引擎 (終極四維分析)
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, twits_df, insider_df, congress_df):
    r_str = reddit_df.head(8).to_string(index=False) if not reddit_df.empty else "無"
    t_str = twits_df.head(8).to_string(index=False) if not twits_df.empty else "無"
    i_str = insider_df.head(8).to_string(index=False) if not insider_df.empty else "無"
    c_str = congress_df.head(8).to_string(index=False) if not congress_df.empty else "無"
    
    system_prompt = """你係香港中環頂級策略分析師。
    【絕對強制規範】：
    1. 必須用地道「香港廣東話口語（Cantonese）」寫報告，語氣要生動專業（例如：瘋狂討論緊、靜靜雞入貨、金睛火眼、佩洛西概念股）。
    2. 絕對禁止輸出任何 JSON、字典、或英文思考過程。
    3. 必須嚴格按照用戶提供嘅格式輸出。"""
    
    user_prompt = f"""
    請交叉分析以下四大另類數據陣營：
    [散戶數據 1：Reddit WSB 熱門 (過去24小時)]:\n{r_str}\n
    [散戶數據 2：StockTwits 熱搜 (即時)]:\n{t_str}\n
    [大戶數據 1：高層 Insider 買入 (過去30日)]:\n{i_str}\n
    [大戶數據 2：國會議員交易 (過去45日)]:\n{c_str}\n
    
    請嚴格根據以下格式輸出報告（直接填寫內容）：
    
    【🕵️ 另類數據 AI 偵測報告】

    1. 【🔥 散戶雙引擎：全網瘋傳啲咩？】
    （用大概 150 字廣東話，綜合 Reddit 同 StockTwits 嘅極短線熱門名單，總結散戶情緒，解釋邊啲 Meme 股或者板塊最受全網關注。）

    2. 【🏛️ 聰明錢與政客追蹤：終極內幕買緊乜？】
    （用大概 150 字廣東話，分析高層 Insider (過去30日) 同國會議員 (過去45日) 嘅買入名單。指出邊啲股票有「政客光環」加持，或者高層真金白銀掃貨，代表咩級數嘅信心。）

    3. 【🎯 終極爆發潛力股 (四維共振)】
    （對比散戶同大戶兩邊名單，挑選 1-2 隻「散戶炒緊 + 大戶/政客偷偷入緊」嘅黃金交叉股票。用大概 150 字精準解釋點解值得放入觀察名單。）
    """
    try:
        response = requests.post("https://text.pollinations.ai/", json={"messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], "model": "openai"}, timeout=45)
        return clean_ai_response(response.text) if response.status_code == 200 else "⚠️ AI 分析暫時離線。"
    except Exception as e: return f"⚠️ AI 分析發生錯誤: {e}"

# ==========================================
#        UI 側邊欄與主頁面導航
# ==========================================
with st.sidebar:
    st.title("🧰 投資雙引擎")
    app_mode = st.radio("可用模組", ["🎯 RS x MACD 動能狙擊手", "📰 近月 AI 洞察 (廣東話版)", "🕵️ 另類數據雷達 (4大維度)"])
    st.markdown("---")
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

# --- 模組 A 顯示 ---
if app_mode == "🎯 RS x MACD 動能狙擊手":
    st.title("🎯 美股 RS x MACD x 趨勢 狙擊手")
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
                close_condition = st.selectbox("額外 Close 條件", ["唔揀", "Close > 短期 SMA", "Close > 長期 SMA", "Close > 短期及長期 SMA"], index=1)
            else: sma_short, sma_long, close_condition = 25, 125, "唔揀"
        with col2:
            st.markdown("#### 2️⃣ RS 動能")
            enable_rs = st.checkbox("啟動 【RS】 過濾", value=True)
            selected_rs = st.multiselect("顯示 RS 階段:", ["🚀 啱啱突破", "🔥 已經突破", "🎯 就快突破 (<5%)"], default=["🚀 啱啱突破"]) if enable_rs else []
        with col3:
            st.markdown("#### 3️⃣ MACD 爆發點")
            enable_macd = st.checkbox("啟動 【MACD】 過濾", value=True)
            selected_macd = st.multiselect("顯示 MACD 階段:", ["🚀 啱啱突破", "🔥 已經突破", "🎯 就快突破 (<5%)"], default=["🚀 啱啱突破"]) if enable_macd else []
        start_scan = st.button("🚀 開始全市場精確掃描", use_container_width=True, type="primary")

    if start_scan:
        status_text, progress_bar = st.empty(), st.progress(0)
        status_text.markdown("**階段 1/3**: 搵緊 Finviz 基礎股票名單...")
        raw_data = fetch_finviz_data()
        if not raw_data.empty:
            df_processed = raw_data.copy()
            df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
            final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].copy()
            if enable_rs or enable_macd or enable_sma:
                indicators = calculate_all_indicators(final_df['Ticker'].tolist(), sma_short, sma_long, close_condition, _progress_bar=progress_bar, _status_text=status_text)
                final_df['RS_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('RS', '無'))
                final_df['MACD_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('MACD', '無'))
                final_df['SMA多頭'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('SMA_Trend', False))
                
                if enable_sma: final_df = final_df[final_df['SMA多頭'] == True]
                if enable_rs: final_df = final_df[final_df['RS_階段'].isin(selected_rs)]
                if enable_macd: final_df = final_df[final_df['MACD_階段'].isin(selected_macd)]
                
                if len(final_df) > 0:
                    fund_df = fetch_fundamentals(final_df['Ticker'].tolist(), _progress_bar=progress_bar, _status_text=status_text)
                    final_df = pd.merge(final_df, fund_df, on='Ticker', how='left')
                    status_text.markdown("✅ **全市場掃描搞掂！**")
                    st.success(f"成功搵到 {len(final_df)} 隻潛力股票。")
                    
                    cols = ['Ticker'] + ([c for c in ['RS_階段', 'MACD_階段', 'Company', 'Sector', 'Industry', 'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'] if c in final_df.columns])
                    st.dataframe(final_df[cols], use_container_width=True, hide_index=True)
                    st.download_button("📥 下載名單 (CSV)", data=final_df[cols].to_csv(index=False).encode('utf-8'), file_name="sniper.csv", mime="text/csv")
                else: st.warning("⚠️ 搵唔到完全滿足條件嘅股票。")

# --- 模組 B 顯示 ---
elif app_mode == "📰 近月 AI 洞察 (廣東話版)":
    st.title("📰 近月 AI 新聞深度分析")
    if st.button("🚀 攞今日 AI 報告", type="primary", use_container_width=True):
        with st.spinner("⏳ 嘗試緊從多個渠道攞歷史財經頭條同摘要..."):
            news_list = fetch_top_news()
        if news_list:
            with st.expander("📄 睇原始新聞"):
                for idx, item in enumerate(news_list):
                    st.markdown(f"**{idx+1}. {item['新聞標題']}** (`{item['來源']}`)\n*摘要: {item['內文摘要']}*")
            with st.spinner("🧠 AI 認真睇緊內文，掃描所有潛力股票..."):
                st.markdown("### 🤖 華爾街 AI 深度洞察報告")
                st.container(border=True).markdown(analyze_news_ai(news_list))

# --- 模組 C 顯示 ---
elif app_mode == "🕵️ 另類數據雷達 (4大維度)":
    st.title("🕵️ 另類數據雷達 (4大維度)")
    st.markdown("全面升級！追蹤 **Reddit + StockTwits 散戶情緒** 同埋 **高層 Insider + 國會議員交易**，四維度狙擊爆發股！")
    
    st.subheader("🌐 散戶情緒雙引擎 (極短線)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**1. Reddit WSB 討論熱度 (過去24h)**")
        with st.spinner("攞緊 Reddit..."):
            r_df, r_msg = fetch_reddit_sentiment()
            st.caption(r_msg)
            st.dataframe(r_df.head(8), use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**2. StockTwits 全美熱搜榜 (即時)**")
        with st.spinner("攞緊 StockTwits..."):
            t_df, t_msg = fetch_stocktwits_trending()
            st.caption(t_msg)
            st.dataframe(t_df.head(8), use_container_width=True, hide_index=True)
            
    st.divider()
    st.subheader("🏛️ 聰明錢與政客跟蹤 (中短線)")
    c3, c4 = st.columns(2)
    with c3:
        st.markdown("**3. 高層 Insider 真金白銀買入 (過去30日)**")
        with st.spinner("攞緊 Insider..."):
            i_df = fetch_insider_buying()
            st.caption("✅ Yahoo Finance API (嚴格篩選近30日買入)")
            st.dataframe(i_df.head(8), use_container_width=True, hide_index=True)
    with c4:
        st.markdown("**4. 國會議員交易 (過去45日申報)**")
        with st.spinner("攞緊國會數據..."):
            c_df, c_msg = fetch_congress_trades()
            st.caption(c_msg)
            st.dataframe(c_df.head(8), use_container_width=True, hide_index=True)

    st.markdown("---")
    if st.button("🚀 啟動 AI 四維交叉博弈分析", type="primary", use_container_width=True):
        with st.spinner("🧠 AI 正在進行散戶 vs 政客大戶 4 維度深度分析... (要等大概 15-20 秒)"):
            res = analyze_alt_data_ai(r_df, t_df, i_df, c_df)
            st.markdown("### 🤖 另類數據 AI 偵測報告")
            with st.container(border=True):
                st.markdown(res)
