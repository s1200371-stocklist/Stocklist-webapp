import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime
import time
import concurrent.futures
from duckduckgo_search import DDGS

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
        st.error("⚠️ 無法下載納指基準，請稍後再試。")
        return results

    if bench_data.index.tz is not None:
        bench_data.index = bench_data.index.tz_localize(None)
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]

    total_tickers = len(tickers)
    for i in range(0, total_tickers, batch_size):
        batch_tickers = tickers[i:i+batch_size]
        
        if _status_text:
            _status_text.markdown(f"**階段 2/3**: 正在下載並運算技術指標... (`{min(i+batch_size, total_tickers)}` / `{total_tickers}`)")
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
                    max_req_len = max(sma_short, sma_long)
                    
                    if len(stock_price) > max_req_len + 1: 
                        # RS 動能
                        stock_norm = stock_price / stock_price.iloc[0]
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        
                        latest_rs, prev_rs = float(rs_line.iloc[-1]), float(rs_line.iloc[-2])
                        latest_rs_ma, prev_rs_ma = float(rs_ma_25.iloc[-1]), float(rs_ma_25.iloc[-2])
                        
                        if latest_rs > latest_rs_ma:
                            if prev_rs <= prev_rs_ma: rs_stage = "🚀 剛剛突破"
                            else: rs_stage = "🔥 已經突破"
                        elif latest_rs >= latest_rs_ma * 0.95: rs_stage = "🎯 即將突破 (<5%)"
                        
                        # MACD
                        ema12 = stock_price.ewm(span=12, adjust=False).mean()
                        ema26 = stock_price.ewm(span=26, adjust=False).mean()
                        macd_line = ema12 - ema26
                        signal_line = macd_line.ewm(span=9, adjust=False).mean()
                        
                        latest_macd, prev_macd = float(macd_line.iloc[-1]), float(macd_line.iloc[-2])
                        latest_sig, prev_sig = float(signal_line.iloc[-1]), float(signal_line.iloc[-2])
                        
                        if latest_macd > latest_sig:
                            if prev_macd <= prev_sig: macd_stage = "🚀 剛剛突破"
                            else: macd_stage = "🔥 已經突破"
                        else:
                            if abs(latest_sig) > 0.0001:
                                if abs(latest_macd - latest_sig) <= abs(latest_sig) * 0.05:
                                    macd_stage = "🎯 即將突破 (<5%)"
                                    
                        # 動態 SMA 趨勢與 Close 判斷
                        sma_s_line = stock_price.rolling(window=sma_short).mean()
                        sma_l_line = stock_price.rolling(window=sma_long).mean()
                        
                        latest_close = float(stock_price.iloc[-1])
                        latest_sma_s = float(sma_s_line.iloc[-1])
                        latest_sma_l = float(sma_l_line.iloc[-1])
                        
                        trend_ok = latest_sma_s > latest_sma_l
                        if close_condition == "Close > 短期 SMA":
                            trend_ok = trend_ok and (latest_close > latest_sma_s)
                        elif close_condition == "Close > 長期 SMA":
                            trend_ok = trend_ok and (latest_close > latest_sma_l)
                        elif close_condition == "Close > 短期及長期 SMA":
                            trend_ok = trend_ok and (latest_close > latest_sma_s) and (latest_close > latest_sma_l)
                            
                        sma_trend = trend_ok
                            
                results[ticker] = {'RS': rs_stage, 'MACD': macd_stage, 'SMA_Trend': sma_trend}
                
        except Exception as e:
            for t in batch_tickers: results[t] = {'RS': "無", 'MACD': "無", 'SMA_Trend': False}
        time.sleep(0.5) 
        
    return results

# --- 5. 量化引擎：多執行緒獲取 4 季財報序列數據 ---
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    def fetch_single(t):
        time.sleep(0.5)
        for attempt in range(3):
            try:
                if attempt > 0: time.sleep(1.5)
                tkr = yf.Ticker(t)
                q_inc = tkr.quarterly_financials
                if q_inc is None or q_inc.empty:
                    q_inc = tkr.quarterly_income_stmt
                if q_inc is None or q_inc.empty: continue
                    
                cols = sorted([c for c in q_inc.columns if isinstance(c, pd.Timestamp)])[-4:]
                if not cols: continue
                
                eps_vals, sales_vals = [], []
                eps_row, sales_row = None, None
                
                for r in ['Diluted EPS', 'Basic EPS', 'Normalized EPS']:
                    if r in q_inc.index: eps_row = q_inc.loc[r]; break
                for r in ['Total Revenue', 'Operating Revenue']:
                    if r in q_inc.index: sales_row = q_inc.loc[r]; break
                        
                for c in cols:
                    eps_vals.append(float(eps_row[c]) if eps_row is not None and pd.notna(eps_row[c]) else None)
                    sales_vals.append(float(sales_row[c]) if sales_row is not None and pd.notna(sales_row[c]) else None)
                    
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
                        curr, prev = vals[i], vals[i-1]
                        if curr is None or prev is None or prev == 0: res.append("-")
                        else:
                            g = (curr - prev) / abs(prev) * 100
                            res.append(f"{g:+.1f}%")
                    return " | ".join(res)
                    
                return {
                    'Ticker': t,
                    'EPS (近4季)': fmt_val(eps_vals, False),
                    'EPS Growth (QoQ)': fmt_growth(eps_vals),
                    'Sales (近4季)': fmt_val(sales_vals, True),
                    'Sales Growth (QoQ)': fmt_growth(sales_vals)
                }
            except Exception: pass
        return {
            'Ticker': t, 'EPS (近4季)': 'N/A', 'EPS Growth (QoQ)': 'N/A', 
            'Sales (近4季)': 'N/A', 'Sales Growth (QoQ)': 'N/A'
        }

    results = []
    total_tickers = len(tickers)
    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_single, t): t for t in tickers}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            completed += 1
            if _status_text: _status_text.markdown(f"**階段 3/3**: 正在獲取最新財報數據... (`{completed}` / `{total_tickers}`)")
            if _progress_bar: _progress_bar.progress(min(1.0, completed / total_tickers))
                
    return pd.DataFrame(results)

# --- 6. AI 引擎：新聞獲取與免費 LLM 分析 ---
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_top_news():
    """從 yfinance 抓取大盤 (SPY, QQQ) 的最新熱門新聞"""
    try:
        spy, qqq = yf.Ticker("SPY"), yf.Ticker("QQQ")
        news_items = []
        if spy.news: news_items.extend(spy.news[:6])
        if qqq.news: news_items.extend(qqq.news[:6])
            
        seen_links = set()
        formatted_news = ""
        for item in news_items:
            link = item.get('link', '')
            if link not in seen_links:
                seen_links.add(link)
                title = item.get('title', '無標題')
                publisher = item.get('publisher', '未知來源')
                formatted_news += f"- [{publisher}] {title}\n"
        return formatted_news
    except Exception as e:
        return ""

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_with_free_ai(news_text):
    """調用 DuckDuckGo Search 的免費 LLM 接口進行語意分析與繁體中文總結"""
    prompt = f"""
    你是華爾街的頂級分析師。請閱讀以下今日美股新聞標題：
    
    {news_text}
    
    請嚴格以「繁體中文」輸出以下內容：
    1. 【市場焦點總結】：用 100-150 字精煉總結今日大盤走勢與核心情緒驅動因素。
    2. 【🚀 潛力股觀察】：列出 3 隻從新聞中發現的最具潛力股票代號 (Ticker)，並用一句話解釋看好理由。若無明確個股，請推斷板塊龍頭。
    """
    try:
        with DDGS() as ddgs:
            return ddgs.chat(prompt, model='gpt-4o-mini')
    except Exception as e:
        return f"⚠️ AI 分析暫時無法使用，請稍後再試。錯誤資訊: {e}"


# --- 7. UI 側邊欄與導航 ---
with st.sidebar:
    st.title("🧰 量化選股與 AI 系統")
    st.markdown("請選擇你要使用的功能：")
    
    app_mode = st.radio(
        "可用模組", 
        ["🎯 RS x MACD 動能狙擊手", "📰 每日 AI 新聞潛力分析", "🚧 價值投資掃描器 (開發中)"]
    )
    st.markdown("---")
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")


# --- 8. 主頁面：功能切換邏輯 ---

# 【功能 A：量化篩選器】
if app_mode == "🎯 RS x MACD 動能狙擊手":
    st.title("🎯 美股 RS x MACD x 趨勢 狙擊手")
    st.markdown("尋找市場上動能最強、財報正在加速增長的潛力爆發股。")
    
    with st.expander("⚙️ 展開設定篩選參數", expanded=True):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### 1️⃣ 基礎與趨勢")
            min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
            
            enable_sma = st.checkbox("啟動 【趨勢排列】 過濾", value=True)
            if enable_sma:
                sub1, sub2 = st.columns(2)
                sma_short = sub1.selectbox("短期 SMA", [10, 20, 25, 50], index=2)
                sma_long = sub2.selectbox("長期 SMA", [50, 100, 125, 150, 200], index=2)
                
                close_options = ["不選擇", "Close > 短期 SMA", "Close > 長期 SMA", "Close > 短期及長期 SMA"]
                close_condition = st.selectbox("額外 Close 條件", options=close_options, index=1)
                
                if close_condition == "不選擇": st.caption(f"✅ 條件：SMA `{sma_short}` > SMA `{sma_long}`")
                elif close_condition == "Close > 短期 SMA": st.caption(f"✅ 條件：`Close` > SMA `{sma_short}` > SMA `{sma_long}`")
                elif close_condition == "Close > 長期 SMA": st.caption(f"✅ 條件：SMA `{sma_short}` > SMA `{sma_long}` 且 `Close` > SMA `{sma_long}`")
                elif close_condition == "Close > 短期及長期 SMA": st.caption(f"✅ 條件：`Close` > 雙均線，且短線高於長線")
            else:
                sma_short, sma_long, close_condition = 25, 125, "不選擇"
            
        with col2:
            st.markdown("#### 2️⃣ RS 動能 (對比納指)")
            enable_rs = st.checkbox("啟動 【RS】 過濾", value=True)
            if enable_rs:
                rs_options = ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"]
                selected_rs = st.multiselect("顯示 RS 階段:", options=rs_options, default=["🚀 剛剛突破"])
            else: selected_rs = []
                
        with col3:
            st.markdown("#### 3️⃣ MACD 爆發點")
            enable_macd = st.checkbox("啟動 【MACD】 過濾", value=True)
            if enable_macd:
                macd_options = ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"]
                selected_macd = st.multiselect("顯示 MACD 階段:", options=macd_options, default=["🚀 剛剛突破"])
            else: selected_macd = []
                
        st.markdown("---")
        start_scan = st.button("🚀 執行全市場精確掃描", use_container_width=True, type="primary")

    if start_scan:
        st.markdown("### ⏳ 系統運算進度")
        status_text = st.empty()
        progress_bar = st.progress(0)

        status_text.markdown("**階段 1/3**: 正在連接 Finviz 獲取基礎股票名單...")
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
                    
                    status_text.markdown("✅ **全市場掃描與過濾完成！**")
                    progress_bar.progress(100)
                    st.success(f"成功尋找到 {len(final_df)} 隻符合你完美設定的潛力股票。")
                else:
                    status_text.markdown("✅ **全市場掃描完成！**")
                    progress_bar.progress(100)
                    st.warning("⚠️ 掃描完成，但沒有股票能同時滿足你設定的嚴格條件。")

            st.markdown("---")
            if len(final_df) > 0:
                st.subheader("🎯 終極精選清單")
                cols = ['Ticker']
                if 'RS_階段' in final_df.columns: cols.append('RS_階段')
                if 'MACD_階段' in final_df.columns: cols.append('MACD_階段')
                
                other_cols = ['Company', 'Sector', 'Industry', 'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)']
                for oc in other_cols:
                    if oc in final_df.columns: cols.append(oc)
                
                st.dataframe(final_df[cols], use_container_width=True, height=600)
                csv = final_df[cols].to_csv(index=False).encode('utf-8')
                st.download_button("📥 下載此終極清單 (CSV)", data=csv, file_name="rs_macd_trend_sniper.csv", mime="text/csv")
            elif not (enable_rs or enable_macd or enable_sma):
                 st.info("請勾選至少一個指標，並點擊「執行全市場精確掃描」。")

# 【功能 B：AI 新聞分析】
elif app_mode == "📰 每日 AI 新聞潛力分析":
    st.title("📰 每日 AI 新聞潛力分析")
    st.markdown("利用零成本的 AI 模型，自動為你閱讀並解讀今日華爾街最熱門的財經新聞，提煉核心市場敘事，並為你發掘 **Top 3 潛力爆發股**！")
    st.info("💡 運作邏輯：系統會實時抓取 SPY (標普) 與 QQQ (納指) 的最新動態，交由 AI 進行全繁體中文的深層語意分析。")
    
    if st.button("🚀 獲取今日 AI 洞察", type="primary", use_container_width=True):
        with st.spinner("⏳ 正在從 Yahoo Finance 抓取全球最新金融頭條..."):
            news_data = fetch_top_news()
            
        if news_data:
            st.success("✅ 成功獲取最新華爾街財經資訊！")
            with st.expander("📄 點擊查看 AI 正在閱讀的英文原生新聞標題"):
                st.markdown(news_data)
                
            with st.spinner("🧠 AI 正在進行深層語意分析與繁體中文總結... (預計需時 10-20 秒)"):
                ai_result = analyze_with_free_ai(news_data)
                
            st.markdown("---")
            st.markdown("### 🤖 華爾街 AI 洞察報告")
            with st.container(border=True):
                st.markdown(ai_result)
            st.caption("聲明：以上分析由免費 AI 模型生成，僅供觀察市場熱點參考，不構成任何投資建議。")
        else:
            st.error("❌ 未能獲取最新新聞數據，請稍後再試。")

# 【功能 C：開發中】
else:
    st.title(app_mode)
    st.info("這個強大的量化選股功能正在開發中，請先使用左側導航欄的「🎯 RS x MACD 動能狙擊手」或「📰 每日 AI 新聞潛力分析」！")
