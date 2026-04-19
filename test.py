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
# 設定網頁標題、圖標及寬屏佈局
st.set_page_config(page_title="🚀 美股量化與 AI 分析平台", page_icon="📈", layout="wide")

# --- 2. 數據清洗函數 ---
def convert_mcap_to_float(val):
    """將 Finviz 的市值字串 (如 10.5B, 500M) 轉換為浮點數數字"""
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
    """獲取 Finviz 全市場基礎股票名單"""
    try:
        f_screener = Overview()
        # 初步篩選市值大於 300M 的公司以過濾仙股
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"連線至 Finviz 失敗，請稍後再試: {e}")
        return pd.DataFrame()

# --- 4. 量化引擎：批量計算指標 (RS, MACD, SMA) ---
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, close_condition, batch_size=200, _progress_bar=None, _status_text=None):
    """批量下載股價並計算技術指標，包含 RS 相對強度與 MACD 突破點"""
    results = {} 
    
    # 設置基準指數 (納斯達克 100) 進行相對強度對比
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
        st.error("⚠️ 無法下載基準數據，請檢查網路連線。")
        return results

    if bench_data.index.tz is not None:
        bench_data.index = bench_data.index.tz_localize(None)
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]

    total_tickers = len(tickers)
    for i in range(0, total_tickers, batch_size):
        batch_tickers = tickers[i:i+batch_size]
        
        if _status_text:
            _status_text.markdown(f"**階段 2/3**: 運算技術指標中... (`{min(i+batch_size, total_tickers)}` / `{total_tickers}`)")
        if _progress_bar:
            _progress_bar.progress(min(1.0, (i + batch_size) / total_tickers))
            
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
                        # --- RS 相對強度分析 ---
                        stock_norm = stock_price / stock_price.iloc[0]
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        
                        latest_rs, prev_rs = float(rs_line.iloc[-1]), float(rs_line.iloc[-2])
                        latest_rs_ma, prev_rs_ma = float(rs_ma_25.iloc[-1]), float(rs_ma_25.iloc[-2])
                        
                        if latest_rs > latest_rs_ma:
                            rs_stage = "🚀 剛剛突破" if prev_rs <= prev_rs_ma else "🔥 已經突破"
                        elif latest_rs >= latest_rs_ma * 0.95:
                            rs_stage = "🎯 即將突破 (<5%)"
                        
                        # --- MACD 突破分析 ---
                        ema12 = stock_price.ewm(span=12, adjust=False).mean()
                        ema26 = stock_price.ewm(span=26, adjust=False).mean()
                        macd_line = ema12 - ema26
                        signal_line = macd_line.ewm(span=9, adjust=False).mean()
                        
                        latest_macd, prev_macd = float(macd_line.iloc[-1]), float(macd_line.iloc[-2])
                        latest_sig, prev_sig = float(signal_line.iloc[-1]), float(signal_line.iloc[-2])
                        
                        if latest_macd > latest_sig:
                            macd_stage = "🚀 剛剛突破" if prev_macd <= prev_sig else "🔥 已經突破"
                        elif abs(latest_sig) > 0.0001 and abs(latest_macd - latest_sig) <= abs(latest_sig) * 0.05:
                            macd_stage = "🎯 即將突破 (<5%)"
                                    
                        # --- SMA 趨勢排列分析 ---
                        sma_s = stock_price.rolling(window=sma_short).mean().iloc[-1]
                        sma_l = stock_price.rolling(window=sma_long).mean().iloc[-1]
                        latest_close = float(stock_price.iloc[-1])
                        
                        trend_ok = sma_s > sma_l
                        if close_condition == "Close > 短期 SMA": trend_ok &= (latest_close > sma_s)
                        elif close_condition == "Close > 長期 SMA": trend_ok &= (latest_close > sma_l)
                        elif close_condition == "Close > 短期及長期 SMA": trend_ok &= (latest_close > sma_s and latest_close > sma_l)
                        sma_trend = trend_ok
                            
                results[ticker] = {'RS': rs_stage, 'MACD': macd_stage, 'SMA_Trend': sma_trend}
                
        except Exception:
            for t in batch_tickers: results[t] = {'RS': "無", 'MACD': "無", 'SMA_Trend': False}
        time.sleep(0.5 + random.random() * 0.5) # 防封鎖延遲
        
    return results

# --- 5. 量化引擎：財報數據抓取 (EPS & Sales) ---
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    """獲取最近 4 季的財報數據，用於驗證增長性"""
    def fetch_single(t):
        time.sleep(0.3 + random.random() * 0.5)
        try:
            tkr = yf.Ticker(t)
            q_inc = tkr.quarterly_financials
            if q_inc is None or q_inc.empty: q_inc = tkr.quarterly_income_stmt
            if q_inc is None or q_inc.empty: return None
            
            # 排序日期並取最近 4 季
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
            if _status_text: _status_text.markdown(f"**階段 3/3**: 財報數據獲取中... (`{completed}` / `{total}`)")
            if _progress_bar: _progress_bar.progress(completed / total)
    
    return pd.DataFrame(results) if results else empty_df

# --- 6. AI 引擎：新聞掃描與洞察 ---
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_top_news():
    """獲取過去一個月的市場關鍵新聞"""
    news_items, seen = [], set()
    # 從 Finviz 獲取較多歷史新聞
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
    """利用 AI 分析近月新聞並給出不限數量的潛力股建議"""
    if not news_text: return "⚠️ 目前無法獲取新聞數據，請稍後再試。"
    prompt = f"你是頂級分析師。請讀完這些近一個月新聞：\n{news_text}\n\n請用繁體中文回覆：\n1. 【近月市場總結】：150字總結情緒與趨勢。\n2. 【潛在機會股票全面掃描】：列出所有提到的潛力股 Ticker 並解釋理由（不限數量）。"
    try:
        r = requests.post("https://text.pollinations.ai/", json={"messages": [{"role": "user", "content": prompt}], "model": "openai"}, timeout=30)
        return r.text if r.status_code == 200 else "AI 分析暫時不可用。"
    except: return "AI 分析連線錯誤。"

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
        selected_rs = col2.multiselect("RS 階段", ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"], default=["🚀 剛剛突破"])
        selected_macd = col3.multiselect("MACD 階段", ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"], default=["🚀 剛剛突破"])
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
            ind_res = calculate_all_indicators(t_list, 25, 125, "Close > 短期 SMA", _progress_bar=progress, _status_text=status)
            df['RS_階段'] = df['Ticker'].map(lambda x: ind_res.get(x, {}).get('RS', '無'))
            df['MACD_階段'] = df['Ticker'].map(lambda x: ind_res.get(x, {}).get('MACD', '無'))
            df['SMA多頭'] = df['Ticker'].map(lambda x: ind_res.get(x, {}).get('SMA_Trend', False))
            # 過濾
            df = df[(df['SMA多頭']==True) & (df['RS_階段'].isin(selected_rs)) & (df['MACD_階段'].isin(selected_macd))]
            if not df.empty:
                # 步驟 3: 財報
                fund = fetch_fundamentals(df['Ticker'].tolist(), _progress_bar=progress, _status_text=status)
                df = pd.merge(df, fund, on='Ticker', how='left')
                st.success(f"找到 {len(df)} 隻標的")
                st.dataframe(df[['Ticker', 'Company', 'RS_階段', 'MACD_階段', 'EPS Growth (QoQ)', 'Sales Growth (QoQ)']], use_container_width=True)
            else: st.warning("未找到符合條件的股票。")

elif app_mode == "📰 近月 AI 洞察":
    st.title("📰 近月市場趨勢與全面潛力掃描")
    if st.button("🚀 執行 AI 深度分析", type="primary", use_container_width=True):
        with st.spinner("獲取新聞與分析中..."):
            news = fetch_top_news()
            if news:
                st.info("成功讀取近月關鍵新聞，AI 正在分析...")
                st.markdown(analyze_with_ai(news))
            else: st.error("目前無法獲取新聞來源。")
