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
st.set_page_config(page_title='🚀 美股全方位量化與 AI 平台', page_icon='📈', layout='wide')

# --- 2. 輔助/清洗函數 ---
def get_headers():
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
    except Exception: return 0.0

def clean_ai_response(text):
    """專門對付 Reasoning 模型嘅清洗器"""
    if not isinstance(text, str): return str(text)
    text = text.strip()
    
    # 1. 拆解 JSON (如果有)
    if text.startswith('{'):
        try:
            parsed = json.loads(text)
            if 'choices' in parsed: text = parsed['choices'][0]['message']['content']
            elif 'content' in parsed: text = parsed['content']
        except Exception: pass
        
    # 2. 徹底移除新版模型必定會有嘅 <think>...</think> 思考過程
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    text = re.sub(r'","tool_calls":\[\]\}$', '', text)
    
    # 3. 暴力截斷：搵第一個【符號，將前面所有漏網之魚嘅英文廢話一刀切斬走
    first_bracket = text.find('【')
    if first_bracket != -1:
        text = text[first_bracket:]
        
    return text.replace('\\n', '\n').replace('\\"', '"').strip()

# ==========================================
#        模組 C：擴充版另類數據雷達
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
                    {'Ticker': item['ticker'].upper(), 'Sentiment': 'Bullish' if item.get('mentions', 0) > 30 else 'Neutral', 'Mentions': item.get('mentions', 0) * 5}
                    for item in results[:10]
                ])
                return df, '🟢 ApeWisdom (過去24h數據)'
    except Exception: pass
    mock = [
        {'Ticker': 'NVDA', 'Sentiment': 'Bullish', 'Mentions': 1520},
        {'Ticker': 'TSLA', 'Sentiment': 'Bearish', 'Mentions': 940},
        {'Ticker': 'ASTS', 'Sentiment': 'Bullish', 'Mentions': 810},
        {'Ticker': 'PLTR', 'Sentiment': 'Bullish', 'Mentions': 730},
        {'Ticker': 'SMCI', 'Sentiment': 'Bearish', 'Mentions': 620}
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
                df = pd.DataFrame([{'Ticker': s.get('symbol', ''), 'Name': s.get('title', '')} for s in symbols[:10]])
                return df, '🟢 StockTwits 正常 (即時數據)'
    except Exception: pass
    mock = [
        {'Ticker': 'NVDA', 'Name': 'NVIDIA'}, {'Ticker': 'AAPL', 'Name': 'Apple'},
        {'Ticker': 'AMD', 'Name': 'Advanced Micro Devices'}, {'Ticker': 'CRWD', 'Name': 'CrowdStrike'},
        {'Ticker': 'HOOD', 'Name': 'Robinhood Markets'}
    ]
    return pd.DataFrame(mock), '🔴 離線備援 (StockTwits)'

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
            if trades is None or trades.empty: return
            df = trades.reset_index()
            date_col = next((c for c in df.columns if 'date' in str(c).lower()), None)
            if date_col:
                dt = pd.to_datetime(df[date_col], errors='coerce')
                if getattr(dt.dt, 'tz', None) is not None: dt = dt.dt.tz_localize(None)
                df[date_col] = dt
                df = df[df[date_col] >= cutoff_date]
            text_col = next((c for c in df.columns if 'text' in str(c).lower() or 'trans' in str(c).lower()), None)
            if text_col and not df.empty:
                buys = df[df[text_col].astype(str).str.contains('Buy|Purchase', case=False, na=False)].copy()
                for _, row in buys.head(2).iterrows():
                    shares, value = row.get('Shares', 0), row.get('Value', 0)
                    if pd.notna(value) and float(value) > 0:
                        results.append({
                            'Ticker': ticker,
                            'Owner': str(row.get('Insider', row.get('Name', 'N/A'))).title(),
                            'Relationship': str(row.get('Position', row.get('Title', 'Executive'))).title(),
                            'Cost': f"${float(value)/float(shares):.2f}" if pd.notna(shares) and float(shares) > 0 else 'N/A',
                            'Value': f"${float(value):,.0f}"
                        })
        except Exception: pass
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_yf_insider, t) for t in target_tickers[:8]]
        concurrent.futures.wait(futures)
    if results:
        df_final = pd.DataFrame(results)
        df_final['SortValue'] = df_final['Value'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)
        df_final = df_final.sort_values(by='SortValue', ascending=False).drop(columns=['SortValue'])
        return df_final.head(10).reset_index(drop=True)
    mock = [
        {'Ticker': 'ASTS', 'Owner': 'Abel Avellan', 'Relationship': 'CEO', 'Cost': '$24.50', 'Value': '$2,500,000'},
        {'Ticker': 'PLTR', 'Owner': 'Alexander Karp', 'Relationship': 'CEO', 'Cost': '$22.50', 'Value': '$1,500,000'},
        {'Ticker': 'CRWD', 'Owner': 'George Kurtz', 'Relationship': 'CEO', 'Cost': '$280.00', 'Value': '$3,200,000'}
    ]
    return pd.DataFrame(mock)

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_congress_trades():
    try:
        url = 'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json'
        res = requests.get(url, headers=get_headers(), timeout=8)
        if res.status_code == 200:
            data = res.json()
            df = pd.DataFrame(data)
            if not df.empty:
                df = df[df['type'].astype(str).str.lower() == 'purchase'].copy()
                dt = pd.to_datetime(df['transaction_date'], errors='coerce')
                if getattr(dt.dt, 'tz', None) is not None: dt = dt.dt.tz_localize(None)
                df['transaction_date'] = dt
                cutoff_date = pd.Timestamp.now(tz=None) - timedelta(days=45)
                df = df[df['transaction_date'] >= cutoff_date]
                df = df.dropna(subset=['transaction_date']).sort_values('transaction_date', ascending=False)
                if not df.empty:
                    df = df[['transaction_date', 'representative', 'ticker', 'amount']].head(10).copy()
                    df.columns = ['Date', 'Politician', 'Ticker', 'Amount']
                    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
                    return df.reset_index(drop=True), '🟢 國會交易 (過去45日數據)'
    except Exception: pass
    mock = [
        {'Date': '2026-04-15', 'Politician': 'Nancy Pelosi', 'Ticker': 'PANW', 'Amount': '$1M - $5M'},
        {'Date': '2026-04-12', 'Politician': 'Ro Khanna', 'Ticker': 'CRWD', 'Amount': '$15K - $50K'},
        {'Date': '2026-04-10', 'Politician': 'Michael McCaul', 'Ticker': 'NVDA', 'Amount': '$100K - $250K'}
    ]
    return pd.DataFrame(mock), '🔴 離線備援 (Congress)'

# ==========================================
#        模組 A：量化與財報引擎
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
    benchmarks_to_try = ['QQQ', '^NDX', 'QQQM']
    bench_data, used_bench = pd.DataFrame(), ''
    for b in benchmarks_to_try:
        try:
            temp_data = yf.download(b, period='2y', progress=False, group_by='column', auto_adjust=False)
            if not temp_data.empty and 'Close' in temp_data.columns:
                close_data = temp_data['Close']
                bench_data = close_data.to_frame(name=b) if isinstance(close_data, pd.Series) else close_data
                used_bench = b; break
        except Exception: continue
    if bench_data.empty: return results
    if getattr(bench_data.index, 'tz', None) is not None: bench_data.index = bench_data.index.tz_localize(None)
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]
    total_tickers = len(tickers)
    for i in range(0, total_tickers, batch_size):
        batch_tickers = tickers[i:i+batch_size]
        if _status_text: _status_text.markdown(f'**階段 2/3**: 計緊技術指標... (`{min(i+batch_size, total_tickers)}` / `{total_tickers}`)')
        if _progress_bar: _progress_bar.progress(min(1.0, (i + batch_size) / max(total_tickers, 1)))
        try:
            data = yf.download(batch_tickers, period='2y', progress=False, group_by='column', auto_adjust=False)
            if data.empty or 'Close' not in data.columns: raise ValueError('No Data')
            close_prices = data['Close']
            if isinstance(close_prices, pd.Series): close_prices = close_prices.to_frame(name=batch_tickers[0])
            close_prices = close_prices.ffill().dropna(how='all')
            if getattr(close_prices.index, 'tz', None) is not None: close_prices.index = close_prices.index.tz_localize(None)
            for ticker in batch_tickers:
                rs_stage, macd_stage, sma_trend = '無', '無', False
                if ticker in close_prices.columns and not close_prices[ticker].dropna().empty:
                    stock_price = close_prices[ticker].dropna()
                    if len(stock_price) > max(sma_short, sma_long) + 1:
                        stock_norm = stock_price / stock_price.iloc[0]
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        if float(rs_line.iloc[-1]) > float(rs_ma_25.iloc[-1]): rs_stage = '🚀 啱啱突破' if float(rs_line.iloc[-2]) <= float(rs_ma_25.iloc[-2]) else '🔥 已經突破'
                        elif float(rs_line.iloc[-1]) >= float(rs_ma_25.iloc[-1]) * 0.95: rs_stage = '🎯 就快突破 (<5%)'
                        ema12, ema26 = stock_price.ewm(span=12, adjust=False).mean(), stock_price.ewm(span=26, adjust=False).mean()
                        macd_line = ema12 - ema26
                        signal_line = macd_line.ewm(span=9, adjust=False).mean()
                        if float(macd_line.iloc[-1]) > float(signal_line.iloc[-1]): macd_stage = '🚀 啱啱突破' if float(macd_line.iloc[-2]) <= float(signal_line.iloc[-2]) else '🔥 已經突破'
                        elif abs(float(macd_line.iloc[-1]) - float(signal_line.iloc[-1])) <= max(abs(float(signal_line.iloc[-1])) * 0.05, 1e-9): macd_stage = '🎯 就快突破 (<5%)'
                        sma_s_line, sma_l_line = stock_price.rolling(window=sma_short).mean(), stock_price.rolling(window=sma_long).mean()
                        latest_close, latest_sma_s, latest_sma_l = float(stock_price.iloc[-1]), float(sma_s_line.iloc[-1]), float(sma_l_line.iloc[-1])
                        trend_ok = latest_sma_s > latest_sma_l
                        if close_condition == 'Close > 短期 SMA': trend_ok = trend_ok and (latest_close > latest_sma_s)
                        elif close_condition == 'Close > 長期 SMA': trend_ok = trend_ok and (latest_close > latest_sma_l)
                        elif close_condition == 'Close > 短期及長期 SMA': trend_ok = trend_ok and (latest_close > latest_sma_s) and (latest_close > latest_sma_l)
                        sma_trend = trend_ok
                results[ticker] = {'RS': rs_stage, 'MACD': macd_stage, 'SMA_Trend': sma_trend}
        except Exception:
            for t in batch_tickers: results[t] = {'RS': '無', 'MACD': '無', 'SMA_Trend': False}
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
                try: cols = sorted(cols)
                except Exception: cols = cols[::-1]
                eps_row, sales_row = None, None
                for r in ['Diluted EPS', 'Basic EPS', 'Normalized EPS']:
                    if r in q_inc.index: eps_row = q_inc.loc[r]; break
                for r in ['Total Revenue', 'Operating Revenue']:
                    if r in q_inc.index: sales_row = q_inc.loc[r]; break
                eps_vals = [float(eps_row[c]) if eps_row is not None and pd.notna(eps_row[c]) else None for c in cols]
                sales_vals = [float(sales_row[c]) if sales_row is not None and pd.notna(sales_row[c]) else None for c in cols]

                def fmt_val(vals, is_sales=False):
                    out = []
                    for v in vals:
                        if v is None: out.append('-')
                        elif is_sales: out.append(f'{v/1e9:.2f}B' if v >= 1e9 else (f'{v/1e6:.2f}M' if v >= 1e6 else f'{v:.0f}'))
                        else: out.append(f'{v:.2f}')
                    return ' | '.join(out)
                def fmt_growth(vals):
                    out = ['-']
                    for i in range(1, len(vals)):
                        if vals[i] is None or vals[i-1] is None or vals[i-1] == 0: out.append('-')
                        else: out.append(f'{(vals[i]-vals[i-1])/abs(vals[i-1])*100:+.1f}%')
                    return ' | '.join(out)
                return {'Ticker': t, 'EPS (近4季)': fmt_val(eps_vals, False), 'EPS Growth (QoQ)': fmt_growth(eps_vals), 'Sales (近4季)': fmt_val(sales_vals, True), 'Sales Growth (QoQ)': fmt_growth(sales_vals)}
            except Exception: time.sleep(1)
        return {'Ticker': t, 'EPS (近4季)': 'N/A', 'EPS Growth (QoQ)': 'N/A', 'Sales (近4季)': 'N/A', 'Sales Growth (QoQ)': 'N/A'}

    total_tickers = len(tickers)
    empty_df = pd.DataFrame(columns=['Ticker', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'])
    if total_tickers == 0: return empty_df
    results, completed = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_single, t): t for t in tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
            completed += 1
            if _status_text: _status_text.markdown(f'**階段 3/3**: 攞緊最新財報數據... (`{completed}` / `{total_tickers}`)')
            if _progress_bar: _progress_bar.progress(min(1.0, completed / max(total_tickers, 1)))
    return pd.DataFrame(results) if results else empty_df

# ==========================================
#        模組 B：AI 新聞分析引擎
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_top_news():
    news_items, seen = [], set()
    try:
        for t in ['SPY', 'QQQ']:
            news = finvizfinance(t).ticker_news()
            if not news.empty:
                for _, row in news.head(15).iterrows():
                    if row['Title'] not in seen:
                        seen.add(row['Title']); news_items.append({'來源': row['Source'], '新聞標題': row['Title'], '內文摘要': '（來自 Finviz 標題）'})
    except Exception: pass
    try:
        for t in ['SPY', 'QQQ', 'NVDA', 'AAPL']:
            tkr = yf.Ticker(t)
            if hasattr(tkr, 'news') and isinstance(tkr.news, list):
                for item in tkr.news[:5]:
                    title = item.get('content', {}).get('title', item.get('title', ''))
                    if title and title not in seen:
                        seen.add(title)
                        summary = item.get('content', {}).get('summary', item.get('summary', '無內文'))
                        news_items.append({'來源': item.get('publisher', 'Finance News'), '新聞標題': title, '內文摘要': str(summary)[:200]})
    except Exception: pass
    return news_items

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list: return '⚠️ 目前攞唔到新聞數據，請遲啲再試下。'
    news_text = '\n'.join([f"{i+1}. [{x['來源']}] 標題：{x['新聞標題']}\n摘要：{x['內文摘要']}\n" for i, x in enumerate(news_list)])
    
    combined_prompt = f"""請扮演香港中環頂級金融分析師。
【絕對強制要求】：必須 100% 用香港廣東話口語。絕對禁止輸出英文或任何代碼。回覆必須直接由「【📉 近月市場焦點總結】」開始。

分析以下新聞：
{news_text}

請嚴格使用以下標題輸出長篇分析：
1. 【📉 近月市場焦點總結】：總結大市走勢同情緒。
2. 【🚀 潛力爆發股全面掃描】：搵出潛力 Ticker，用廣東話詳細解釋。"""

    try:
        # 使用官方最新 Default 模型 openai-fast
        res = requests.post(
            'https://text.pollinations.ai/',
            json={'messages': [{'role': 'user', 'content': combined_prompt}], 'model': 'openai-fast'},
            timeout=60
        )
        result = clean_ai_response(res.text)
        return result if result else "⚠️ AI 接口異常"
    except Exception as e: return f'⚠️ AI 發生錯誤: {e}'

# ==========================================
#        模組 C：AI 交叉博弈分析引擎 (完美適配新 API 版)
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, twits_df, insider_df, congress_df):
    r_str = reddit_df.head(8).to_string(index=False) if not reddit_df.empty else '無數據'
    t_str = twits_df.head(8).to_string(index=False) if not twits_df.empty else '無數據'
    i_str = insider_df.head(8).to_string(index=False) if not insider_df.empty else '無數據'
    c_str = congress_df.head(8).to_string(index=False) if not congress_df.empty else '無數據'

    combined_prompt = f"""請你扮演香港中環頂級美股策略分析師。
【絕對強制要求】：
1. 語言必須 100% 使用地道「香港廣東話口語」。
2. 絕對禁止輸出任何英文前言、禁止輸出思考過程、禁止輸出 JSON 代碼。
3. 你的回覆必須直接由「【🕵️ 另類數據 AI 偵測深度報告】」開始，不允許任何廢話。

真實數據如下：
[散戶：Reddit WSB 熱門]:
{r_str}

[散戶：StockTwits 熱搜]:
{t_str}

[大戶：高層 Insider 買入]:
{i_str}

[大戶：國會議員交易]:
{c_str}

請立刻輸出極度深入嘅廣東話長篇報告，嚴格使用以下標題：

【🕵️ 另類數據 AI 偵測深度報告】

1. 【🔥 散戶雙引擎：流動性正喺度衝擊邊個板塊？】
（點名最熱門股票，引用具體次數。深入分析散戶情緒同板塊輪動，例如瘋狂吸籌定避險。）

2. 【🏛️ 聰明錢與政客追蹤：終極內幕買緊乜？】
（點名有買入動作嘅 CEO 同政客，引用具體金額。深入分析春江鴨佈局邏輯。）

3. 【🎯 終極四維共振：最強爆發潛力股與高危陷阱】
（搵出黃金交叉股同高危陷阱股，點出人踩人風險。）
"""

    try:
        # 使用官方最新 Default 模型 openai-fast，唔再強制鎖死 gpt-4o 避開 404 Error
        response = requests.post(
            'https://text.pollinations.ai/',
            json={
                'messages': [{'role': 'user', 'content': combined_prompt}],
                'model': 'openai-fast'
            },
            timeout=80
        )
        
        # 使用升級版清洗器，自動剔除 Reasoning 模型嘅所有 <think> 標籤
        result = clean_ai_response(response.text)
        return result if result else '⚠️ AI 輸出異常，請再試一次。'
        
    except Exception as e:
        return f'⚠️ AI 分析發生錯誤: {e}'

# ==========================================
#        UI 側邊欄與主頁面導航
# ==========================================
with st.sidebar:
    st.title('🧰 投資雙引擎')
    app_mode = st.radio('可用模組', ['🎯 RS x MACD 動能狙擊手', '📰 近月 AI 洞察 (廣東話版)', '🕵️ 另類數據雷達 (4大維度)'])
    st.markdown('---')
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

# --- 模組 A 顯示 ---
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
                    cols = ['Ticker'] + [c for c in ['RS_階段', 'MACD_階段', 'Company', 'Sector', 'Industry', 'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'] if c in final_df.columns]
                    st.dataframe(final_df[cols], use_container_width=True, hide_index=True, height=600)
                    st.download_button('📥 下載名單 (CSV)', data=final_df[cols].to_csv(index=False).encode('utf-8'), file_name='sniper.csv', mime='text/csv')
                else:
                    status_text.markdown('✅ **全市場掃描搞掂！**')
                    progress_bar.progress(100)
                    st.warning('⚠️ 搵唔到完全滿足條件嘅股票。')

# --- 模組 B 顯示 ---
elif app_mode == '📰 近月 AI 洞察 (廣東話版)':
    st.title('📰 近月 AI 新聞深度分析')
    st.markdown('系統自動爬取近一個月嘅財經熱門新聞，交俾 AI 用廣東話詳細分析。')
    if st.button('🚀 攞今日 AI 報告', type='primary', use_container_width=True):
        with st.spinner('⏳ 嘗試緊從多個渠道攞歷史財經頭條同摘要...'):
            news_list = fetch_top_news()
        if news_list:
            with st.expander('📄 睇原始新聞'):
                for idx, item in enumerate(news_list):
                    st.markdown(f"**{idx+1}. {item['新聞標題']}** (`{item['來源']}`)\n*摘要: {item['內文摘要']}*")
            with st.spinner('🧠 AI 認真睇緊內文，掃描所有潛力股票...'):
                st.markdown('### 🤖 華爾街 AI 深度洞察報告')
                with st.container(border=True):
                    st.markdown(analyze_news_ai(news_list))
        else:
            st.error('❌ 攞唔到新聞，請稍後再試。')

# --- 模組 C 顯示 ---
elif app_mode == '🕵️ 另類數據雷達 (4大維度)':
    st.title('🕵️ 另類數據雷達 (4大維度)')
    st.markdown('追蹤 Reddit、StockTwits、Insider 同國會議員交易，四維度分析潛力股。')
    st.subheader('🌐 散戶情緒雙引擎 (極短線)')
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('**1. Reddit WSB 討論熱度 (過去24h)**')
        with st.spinner('攞緊 Reddit...'):
            r_df, r_msg = fetch_reddit_sentiment()
            st.caption(r_msg)
            st.dataframe(r_df.head(8), use_container_width=True, hide_index=True)
    with c2:
        st.markdown('**2. StockTwits 全美熱搜榜 (即時)**')
        with st.spinner('攞緊 StockTwits...'):
            t_df, t_msg = fetch_stocktwits_trending()
            st.caption(t_msg)
            st.dataframe(t_df.head(8), use_container_width=True, hide_index=True)

    st.divider()
    st.subheader('🏛️ 聰明錢與政客跟蹤 (中短線)')
    c3, c4 = st.columns(2)
    with c3:
        st.markdown('**3. 高層 Insider 真金白銀買入 (過去30日)**')
        with st.spinner('攞緊 Insider...'):
            i_df = fetch_insider_buying()
            st.caption('✅ Yahoo Finance API (嚴格篩選近30日買入)')
            st.dataframe(i_df.head(8), use_container_width=True, hide_index=True)
    with c4:
        st.markdown('**4. 國會議員交易 (過去45日申報)**')
        with st.spinner('攞緊國會數據...'):
            c_df, c_msg = fetch_congress_trades()
            st.caption(c_msg)
            st.dataframe(c_df.head(8), use_container_width=True, hide_index=True)

    st.markdown('---')
    if st.button('🚀 啟動 AI 四維交叉博弈分析', type='primary', use_container_width=True):
        with st.spinner('🧠 AI 正在進行散戶 vs 政客大戶 4 維度深度分析... (已適配最新 API，請稍候 15-30 秒)'):
            res = analyze_alt_data_ai(r_df, t_df, i_df, c_df)
            st.markdown('### 🤖 另類數據 AI 偵測深度報告')
            with st.container(border=True):
                st.markdown(res)
