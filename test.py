import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime
import time
import concurrent.futures

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 美股量化篩選平台", page_icon="📈", layout="wide")

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

# --- 3. Finviz 基礎數據獲取 (快取 1 小時) ---
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"連線至 Finviz 失敗，請稍後再試: {e}")
        return pd.DataFrame()

# --- 4. yfinance 批量計算引擎 (包含 RS, MACD 及自訂 SMA 趨勢) ---
# 注意：加入底線 _ 開頭的變數，不會被 Streamlit cache 追蹤，容許我們傳入 UI 元件
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, batch_size=200, _progress_bar=None, _status_text=None):
    results = {} 
    
    benchmarks_to_try = ["QQQ", "^NDX", "QQQM"]
    bench_data = pd.DataFrame()
    used_bench = ""

    for b in benchmarks_to_try:
        try:
            # 延長至 2y 確保可以準確計算高達 SMA 200 的指標
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
        
        # --- UI 進度更新 ---
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
                    
                    # 判斷所需的最低數據天數
                    required_len_short = 1 if sma_short == "Close" else sma_short
                    required_len_long = 1 if sma_long == "Close" else sma_long
                    max_req_len = max(required_len_short, required_len_long)
                    
                    # 確保有足夠長度的數據來計算使用者選擇的指標
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
                                    
                        # 動態 SMA 趨勢判斷 (支援 Close)
                        if sma_short == "Close":
                            sma_s_line = stock_price
                        else:
                            sma_s_line = stock_price.rolling(window=sma_short).mean()
                            
                        if sma_long == "Close":
                            sma_l_line = stock_price
                        else:
                            sma_l_line = stock_price.rolling(window=sma_long).mean()
                            
                        sma_trend = float(sma_s_line.iloc[-1]) > float(sma_l_line.iloc[-1])
                            
                results[ticker] = {'RS': rs_stage, 'MACD': macd_stage, 'SMA_Trend': sma_trend}
                
        except Exception as e:
            for t in batch_tickers: results[t] = {'RS': "無", 'MACD': "無", 'SMA_Trend': False}
        time.sleep(0.5) 
        
    return results

# --- 5. 多執行緒獲取 4 季財報序列數據 (穩定版 API) ---
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    def fetch_single(t):
        time.sleep(0.5) # 保護機制
        for attempt in range(3):
            try:
                if attempt > 0: time.sleep(1.5)
                
                tkr = yf.Ticker(t)
                q_inc = tkr.quarterly_financials
                if q_inc is None or q_inc.empty:
                    q_inc = tkr.quarterly_income_stmt # 後備屬性
                
                if q_inc is None or q_inc.empty:
                    continue # 觸發重試
                    
                cols = sorted([c for c in q_inc.columns if isinstance(c, pd.Timestamp)])
                cols = cols[-4:]
                if not cols: continue
                
                eps_vals, sales_vals = [], []
                
                eps_row = None
                for r in ['Diluted EPS', 'Basic EPS', 'Normalized EPS']:
                    if r in q_inc.index:
                        eps_row = q_inc.loc[r]
                        break
                        
                sales_row = None
                for r in ['Total Revenue', 'Operating Revenue']:
                    if r in q_inc.index:
                        sales_row = q_inc.loc[r]
                        break
                        
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
            except Exception:
                pass
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
            
            # --- UI 進度更新 ---
            if _status_text:
                _status_text.markdown(f"**階段 3/3**: 正在獲取最新財報數據... (`{completed}` / `{total_tickers}`)")
            if _progress_bar:
                _progress_bar.progress(min(1.0, completed / total_tickers))
                
    return pd.DataFrame(results)

# --- 6. 全新 UI 佈局 (側邊欄為導航，主頁為篩選器) ---

# 側邊欄：功能導航
with st.sidebar:
    st.title("🧰 量化選股系統")
    st.markdown("請選擇你要使用的篩選器：")
    
    app_mode = st.radio(
        "可用模組", 
        ["🎯 RS x MACD 動能狙擊手", "🚧 價值投資掃描器 (開發中)", "🚧 高息股探測器 (開發中)"]
    )
    
    st.markdown("---")
    st.info("💡 提示：所有篩選條件已經移至主頁面板，操作更直觀！")
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

# 主頁：根據選擇的模式顯示對應的內容
if app_mode == "🎯 RS x MACD 動能狙擊手":
    
    st.title("🎯 美股 RS x MACD x 趨勢 狙擊手")
    st.markdown("尋找市場上動能最強、財報正在加速增長的潛力爆發股。")
    
    with st.expander("⚙️ 展開設定篩選參數", expanded=True):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### 1️⃣ 基礎與趨勢")
            min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
            
            enable_sma = st.checkbox("啟動 【短期 > 長期】 趨勢過濾", value=True)
            if enable_sma:
                # 使用 sub-columns 讓參數選擇更緊湊，並加入 Close
                sub1, sub2 = st.columns(2)
                sma_short = sub1.selectbox("短期指標", ["Close", 10, 20, 25, 50], index=3) # 預設 25
                sma_long = sub2.selectbox("長期指標", ["Close", 50, 100, 125, 150, 200], index=3) # 預設 125
                st.caption(f"✅ 條件：`{sma_short}` 必須高於 `{sma_long}`")
            else:
                sma_short, sma_long = 25, 125 # 預設值以防出錯
            
        with col2:
            st.markdown("#### 2️⃣ RS 動能 (對比納指)")
            enable_rs = st.checkbox("啟動 【RS】 過濾", value=True)
            if enable_rs:
                rs_options = ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"]
                selected_rs = st.multiselect("顯示 RS 階段:", options=rs_options, default=["🚀 剛剛突破"])
            else:
                selected_rs = []
                
        with col3:
            st.markdown("#### 3️⃣ MACD 爆發點")
            enable_macd = st.checkbox("啟動 【MACD】 過濾", value=True)
            if enable_macd:
                macd_options = ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"]
                selected_macd = st.multiselect("顯示 MACD 階段:", options=macd_options, default=["🚀 剛剛突破"])
            else:
                selected_macd = []
                
        st.markdown("---")
        start_scan = st.button("🚀 執行全市場精確掃描", use_container_width=True, type="primary")

    # --- 7. 主程式邏輯與進度條 ---
    if start_scan:
        st.markdown("### ⏳ 系統運算進度")
        
        # 建立 UI 空白容器，用於動態顯示進度和文字
        status_text = st.empty()
        progress_bar = st.progress(0)

        status_text.markdown("**階段 1/3**: 正在連接 Finviz 獲取基礎股票名單...")
        raw_data = fetch_finviz_data()
        progress_bar.progress(100) # 完成階段 1
        
        if not raw_data.empty:
            df_processed = raw_data.copy()
            df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
            final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].copy()
            
            if enable_rs or enable_macd or enable_sma:
                target_tickers = final_df['Ticker'].tolist()
                
                # 重置進度條進入階段 2
                progress_bar.progress(0)
                indicators_results = calculate_all_indicators(
                    target_tickers, 
                    sma_short, 
                    sma_long, 
                    _progress_bar=progress_bar, 
                    _status_text=status_text
                )
                
                final_df['RS_階段'] = final_df['Ticker'].map(lambda x: indicators_results.get(x, {}).get('RS', '無'))
                final_df['MACD_階段'] = final_df['Ticker'].map(lambda x: indicators_results.get(x, {}).get('MACD', '無'))
                final_df['SMA多頭'] = final_df['Ticker'].map(lambda x: indicators_results.get(x, {}).get('SMA_Trend', False))
                
                # 過濾邏輯
                if enable_sma: final_df = final_df[final_df['SMA多頭'] == True]
                if enable_rs: final_df = final_df[final_df['RS_階段'].isin(selected_rs)]
                if enable_macd: final_df = final_df[final_df['MACD_階段'].isin(selected_macd)]
                
                if len(final_df) > 0:
                    # 重置進度條進入階段 3
                    progress_bar.progress(0)
                    fund_df = fetch_fundamentals(
                        final_df['Ticker'].tolist(), 
                        _progress_bar=progress_bar, 
                        _status_text=status_text
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
            
            # --- 8. 結果展示與匯出 ---
            if len(final_df) > 0:
                st.subheader("🎯 終極精選清單")
                
                cols = ['Ticker']
                if 'RS_階段' in final_df.columns: cols.append('RS_階段')
                if 'MACD_階段' in final_df.columns: cols.append('MACD_階段')
                
                # 【Market Cap 與 最近 4 季數據】
                other_cols = ['Company', 'Sector', 'Industry', 'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)']
                for oc in other_cols:
                    if oc in final_df.columns: cols.append(oc)
                
                st.dataframe(final_df[cols], use_container_width=True, height=600)
                
                csv = final_df[cols].to_csv(index=False).encode('utf-8')
                st.download_button("📥 下載此終極清單 (CSV)", data=csv, file_name="rs_macd_trend_sniper.csv", mime="text/csv")
            elif not (enable_rs or enable_macd or enable_sma):
                 st.info("請勾選至少一個指標，並點擊「執行全市場精確掃描」。")

else:
    # 處理未來其他模組的畫面
    st.title(app_mode)
    st.info("這個強大的量化選股功能正在開發中，請先使用左側導航欄的「🎯 RS x MACD 動能狙擊手」！")
