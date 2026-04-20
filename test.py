```python
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

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 美股量化與 AI 分析平台", page_icon="📈", layout="wide")

# --- 2. 數據清洗函數 ---
def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except:
        return 0.0

# --- 3. 量化引擎：Finviz 基礎數據獲取 ---
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"連線至 Finviz 失敗，請稍後再試: {e}")
        return pd.DataFrame()

# --- 4. 量化引擎：批量計算 (RS, MACD, SMA, Close) ---
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, close_condition, batch_size=200, _progress_bar=None, _status_text=None):
    results = {} 
    benchmarks_to_try = ["QQQ", "^NDX", "QQQM"]
    bench_data = pd.DataFrame()
    used_bench = ""

    for b in benchmarks_to_try:
        try:
            temp_data = yf.download(b, period="2y", progress=False)
            if not temp_data.empty and 'Close' in temp_data.columns:
                close_data = temp_data['Close']
                if isinstance(close_data, pd.Series): bench_data = close_data.to_frame(name=b)
                else: bench_data = close_data
                used_bench = b
                break 
        except Exception: continue
            
    if bench_data.empty: 
        st.error("⚠️ 無法下載基準數據。")
        return results

    if bench_data.index.tz is not None:
        bench_data.index = bench_data.index.tz_localize(None)
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]

    total_tickers = len(tickers)
    for i in range(0, total_tickers, batch_size):
        batch_tickers = tickers[i:i+batch_size]
        if _status_text: _status_text.markdown(f"**階段 2/3**: 運算中... (`{min(i+batch_size, total_tickers)}` / `{total_tickers}`)")
        if _progress_bar: _progress_bar.progress(min(1.0, (i + batch_size) / total_tickers))
            
        try:
            data = yf.download(batch_tickers, period="2y", progress=False)
            if data.empty or 'Close' not in data.columns: raise ValueError("No Data")
            close_prices = data['Close']
            if isinstance(close_prices, pd.Series): close_prices = close_prices.to_frame(name=batch_tickers[0])
            close_prices = close_prices.ffill().dropna(how='all')
            if close_prices.index.tz is not None: close_prices.index = close_prices.index.tz_localize(None)
            
            for ticker in batch_tickers:
                rs_stage, macd_stage, sma_trend = "無", "無", False
                if ticker in close_prices.columns and not close_prices[ticker].dropna().empty:
                    stock_price = close_prices[ticker].dropna()
                    if len(stock_price) > max(sma_short, sma_long, 30): 
                        stock_norm = stock_price / stock_price.iloc[0]
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        
                        latest_rs, prev_rs = float(rs_line.iloc[-1]), float(rs_line.iloc[-2])
                        latest_rs_ma, prev_rs_ma = float(rs_ma_25.iloc[-1]), float(rs_ma_25.iloc[-2])
                        if latest_rs > latest_rs_ma: rs_stage = "🚀 剛剛突破" if prev_rs <= prev_rs_ma else "🔥 已經突破"
                        elif latest_rs >= latest_rs_ma * 0.95: rs_stage = "🎯 即將突破 (<5%)"
                        
                        ema12, ema26 = stock_price.ewm(span=12, adjust=False).mean(), stock_price.ewm(span=26, adjust=False).mean()
                        macd_line, signal_line = ema12 - ema26, (ema12 - ema26).ewm(span=9, adjust=False).mean()
                        latest_macd, prev_macd = float(macd_line.iloc[-1]), float(macd_line.iloc[-2])
                        latest_sig, prev_sig = float(signal_line.iloc[-1]), float(signal_line.iloc[-2])
                        if latest_macd > latest_sig: macd_stage = "🚀 剛剛突破" if prev_macd <= prev_sig else "🔥 已經突破"
                        elif abs(latest_sig) > 0.0001 and abs(latest_macd - latest_sig) <= abs(latest_sig) * 0.05: macd_stage = "🎯 即將突破 (<5%)"
                                    
                        sma_s, sma_l = stock_price.rolling(window=sma_short).mean().iloc[-1], stock_price.rolling(window=sma_long).mean().iloc[-1]
                        latest_close = float(stock_price.iloc[-1])
                        trend_ok = sma_s > sma_l
                        if close_condition == "Close > 短期 SMA": trend_ok &= (latest_close > sma_s)
                        elif close_condition == "Close > 長期 SMA": trend_ok &= (latest_close > sma_l)
                        elif close_condition == "Close > 短期及長期 SMA": trend_ok &= (latest_close > sma_s and latest_close > sma_l)
                        sma_trend = trend_ok
                results[ticker] = {'RS': rs_stage, 'MACD': macd_stage, 'SMA_Trend': sma_trend}
        except Exception:
            for t in batch_tickers: results[t] = {'RS': "無", 'MACD': "無", 'SMA_Trend': False}
        time.sleep(0.5 + random.random() * 0.5) 
    return results

# --- 5. 量化引擎：財報數據抓取 ---
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    def fetch_single(t):
        time.sleep(0.3 + random.random() * 0.5)
        try:
            tkr = yf.Ticker(t)
            q_inc = tkr.quarterly_financials
            if q_inc is None or q_inc.empty: q_inc = tkr.quarterly_income_stmt
            if q_inc is None or q_inc.empty: return None
            cols = sorted(list(q_inc.columns))[-4:]
            if not cols: return None
            eps_row = next((q_inc.loc[r] for r in ['Diluted EPS', 'Basic EPS'] if r in q_inc.index), None)
            sales_row = next((q_inc.loc[r] for r in ['Total Revenue', 'Operating Revenue'] if r in q_inc.index), None)
            eps_vals = [float(eps_row[c]) if eps_row is not None and pd.notna(eps_row[c]) else None for c in cols]
            sales_vals = [float(sales_row[c]) if sales_row is not None and pd.notna(sales_row[c]) else None for c in cols]
            
            def fmt_val(vals, is_sales=False):
                res = []
                for v in vals:
                    if v is None: res.append("-")
                    else:
                        if is_sales:
                            if v >= 1e9: res.append(f"{v/1e9:.1f}B")
                            elif v >= 1e6: res.append(f"{v/1e6:.1f}M")
                            else: res.append(f"{v:.0f}")
                        else: res.append(f"{v:.2f}")
                return " | ".join(res)
            
            def fmt_growth(vals):
                res = ["-"] 
                for i in range(1, len(vals)):
                    if vals[i] is None or vals[i-1] is None or vals[i-1] == 0: res.append("-")
                    else: res.append(f"{(vals[i]-vals[i-1])/abs(vals[i-1])*100:+.1f}%")
                return " | ".join(res)
                
            return {
                'Ticker': t, 'EPS (近4季)': fmt_val(eps_vals), 'EPS Growth (QoQ)': fmt_growth(eps_vals),
                'Sales (近4季)': fmt_val(sales_vals, True), 'Sales Growth (QoQ)': fmt_growth(sales_vals)
            }
        except: return None

    results = []
    total = len(tickers)
    empty_df = pd.DataFrame(columns=['Ticker', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'])
    if total == 0: return empty_df

    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_single, t): t for t in tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: results.append(res)
            completed += 1
            if _status_text: _status_text.markdown(f"**階段 3/3**: 財報獲取中... (`{completed}` / `{total}`)")
            if _progress_bar: _progress_bar.progress(completed / total)
    return pd.DataFrame(results) if results else empty_df

# --- 6. AI 引擎：新聞掃描與洞察 ---
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_top_news():
    news_items, seen = [], set()
    try:
        for t in ["SPY", "QQQ"]:
            news = finvizfinance(t).ticker_news()
            if not news.empty:
                for _, row in news.head(35).iterrows():
                    if row['Title'] not in seen:
                        seen.add(row['Title'])
                        news_items.append(f"- [{row['Source']}] {row['Title']}")
    except: pass
    return "\n".join(news_items) if news_items else None

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_with_ai(news_text):
    if not news_text: return "⚠️ 目前無法獲取新聞數據，請稍後再試。"
    
    # 【修復】強制 AI 的行為準則，禁止輸出英文和思考過程
    system_prompt = """
    你是一位專業的華爾街金融分析師。
    【絕對要求】：
    1. 你只能輸出繁體中文 (Traditional Chinese)。
    2. 絕對禁止輸出任何英文思考過程 (例如 'Let's compile...', 'Ok do answer' 等)。
    3. 絕對禁止輸出任何程式碼或 JSON 格式。
    4. 直接輸出最終的 Markdown 排版分析報告。
    """
    
    user_prompt = f"""
    請閱讀以下近一個月的美股新聞：
    {news_text}
    
    請以專業口吻完成：
    1. 【📉 近月市場焦點總結】：150-200字精煉總結大盤走勢與核心情緒。
    2. 【🚀 潛在機會股票掃描】：列出新聞中所有具備潛力或轉機的股票代號 (Ticker)，不限數量。並用1句話解釋看好理由。
    """
    
    try:
        # 強制指定 model="openai" 確保調用高智商模型
        response = requests.post(
            "https://text.pollinations.ai/",
            json={
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "model": "openai" 
            },
            timeout=40
        )
        if response.status_code == 200:
            return response.text
        else: return "⚠️ AI 接口暫時異常，請稍後再試。"
    except: return "⚠️ AI 分析連線錯誤。"

# --- 7. UI 控制與功能切換 ---
with st.sidebar:
    st.title("🧰 投資雙引擎")
    app_mode = st.radio("選擇模組", ["🎯 動能狙擊手", "📰 近月 AI 洞察"])
    st.markdown("---")
    st.caption(f"最後更新: {datetime.datetime.now().strftime('%H:%M')}")

if app_mode == "🎯 動能狙擊手":
    st.title("🎯 RS x MACD x 趨勢排列 狙擊手")
    with st.expander("⚙️ 篩選設定", expanded=True):
        col1, col2, col3 = st.columns(3)
        min_mcap = col1.number_input("最低市值 (M USD)", value=500.0)
        
        enable_sma = col1.checkbox("啟動 【趨勢排列】 過濾", value=True)
        if enable_sma:
            sub1, sub2 = col1.columns(2)
            sma_short = sub1.selectbox("短期 SMA", [10, 20, 25, 50], index=2)
            sma_long = sub2.selectbox("長期 SMA", [50, 100, 125, 150, 200], index=2)
            close_condition = col1.selectbox("Close 條件", ["不選擇", "Close > 短期 SMA", "Close > 長期 SMA", "Close > 短期及長期 SMA"], index=1)
        else: sma_short, sma_long, close_condition = 25, 125, "不選擇"
            
        enable_rs = col2.checkbox("啟動 【RS】 過濾", value=True)
        selected_rs = col2.multiselect("RS 階段", ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"], default=["🚀 剛剛突破"]) if enable_rs else []
        
        enable_macd = col3.checkbox("啟動 【MACD】 過濾", value=True)
        selected_macd = col3.multiselect("MACD 階段", ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"], default=["🚀 剛剛突破"]) if enable_macd else []
        
        start = st.button("🚀 開始全市場掃描", use_container_width=True, type="primary")

    if start:
        progress = st.progress(0)
        status = st.empty()
        # 步驟 1: Finviz
        df = fetch_finviz_data()
        if not df.empty:
            df['Mcap_Num'] = df['Market Cap'].apply(convert_mcap_to_float)
            df = df[df['Mcap_Num'] >= min_mcap]
            # 步驟 2: 指標
            t_list = df['Ticker'].tolist()
            ind_res = calculate_all_indicators(t_list, sma_short, sma_long, close_condition, _progress_bar=progress, _status_text=status)
            df['RS_階段'] = df['Ticker'].map(lambda x: ind_res.get(x, {}).get('RS', '無'))
            df['MACD_階段'] = df['Ticker'].map(lambda x: ind_res.get(x, {}).get('MACD', '無'))
            df['SMA多頭'] = df['Ticker'].map(lambda x: ind_res.get(x, {}).get('SMA_Trend', False))
            # 過濾
            if enable_sma: df = df[df['SMA多頭']==True]
            if enable_rs: df = df[df['RS_階段'].isin(selected_rs)]
            if enable_macd: df = df[df['MACD_階段'].isin(selected_macd)]
            
            if not df.empty:
                # 步驟 3: 財報
                fund = fetch_fundamentals(df['Ticker'].tolist(), _progress_bar=progress, _status_text=status)
                df = pd.merge(df, fund, on='Ticker', how='left')
                status.markdown("✅ **掃描完成！**")
                progress.progress(100)
                st.success(f"找到 {len(df)} 隻標的")
                
                cols = ['Ticker']
                if 'RS_階段' in df.columns: cols.append('RS_階段')
                if 'MACD_階段' in df.columns: cols.append('MACD_階段')
                for oc in ['Company', 'Sector', 'Industry', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)']:
                    if oc in df.columns: cols.append(oc)
                
                st.dataframe(df[cols], use_container_width=True)
            else: 
                status.markdown("✅ **掃描完成！**")
                progress.progress(100)
                st.warning("未找到符合條件的股票。")

elif app_mode == "📰 近月 AI 洞察":
    st.title("📰 近月市場趨勢與全面潛力掃描")
    st.markdown("自動抓取涵蓋近一個月的熱門財經新聞，交由 AI 全面掃描市場熱點與所有具備潛在爆發機會的股票！")
    
    if st.button("🚀 執行 AI 深度分析", type="primary", use_container_width=True):
        with st.spinner("獲取新聞與分析中... (此過程需要約 20 秒，請耐心等待)"):
            news = fetch_top_news()
            if news:
                with st.expander("📄 查看被分析的原始新聞標題"):
                    st.text(news)
                ai_res = analyze_with_ai(news)
                st.markdown("### 🤖 華爾街 AI 全面洞察報告")
                with st.container(border=True):
                    st.markdown(ai_res)
            else: st.error("目前無法獲取新聞來源。")


```
