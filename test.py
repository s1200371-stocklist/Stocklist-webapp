import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime
import time

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 美股全階段動能狙擊手", page_icon="🎯", layout="wide")

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

# --- 4. yfinance 批量計算引擎 ---
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_rs(tickers, batch_size=200):
    rs_signals = {} 
    
    benchmarks_to_try = ["QQQ", "^NDX", "QQQM"]
    bench_data = pd.DataFrame()
    used_bench = ""

    for b in benchmarks_to_try:
        try:
            temp_data = yf.download(b, period="3mo", progress=False)
            if not temp_data.empty and 'Close' in temp_data.columns:
                close_data = temp_data['Close']
                if isinstance(close_data, pd.Series):
                    bench_data = close_data.to_frame(name=b)
                else:
                    bench_data = close_data
                used_bench = b
                break 
        except Exception: continue
            
    if bench_data.empty: 
        st.error("⚠️ 無法下載納指基準，請稍後再試。")
        return rs_signals

    if bench_data.index.tz is not None:
        bench_data.index = bench_data.index.tz_localize(None)
        
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]

    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i+batch_size]
        try:
            data = yf.download(batch_tickers, period="3mo", progress=False)
            if data.empty or 'Close' not in data.columns: raise ValueError("No Data")
            close_prices = data['Close']
            if isinstance(close_prices, pd.Series): close_prices = close_prices.to_frame(name=batch_tickers[0])
            close_prices = close_prices.ffill().dropna(how='all')
            if close_prices.index.tz is not None: close_prices.index = close_prices.index.tz_localize(None)
            
            for ticker in batch_tickers:
                if ticker in close_prices.columns and not close_prices[ticker].dropna().empty:
                    stock_price = close_prices[ticker].dropna()
                    if len(stock_price) > 30: 
                        stock_norm = stock_price / stock_price.iloc[0]
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        
                        latest_rs = float(rs_line.iloc[-1])
                        latest_ma = float(rs_ma_25.iloc[-1])
                        prev_rs = float(rs_line.iloc[-2])
                        prev_ma = float(rs_ma_25.iloc[-2])
                        
                        if latest_rs > latest_ma:
                            if prev_rs <= prev_ma: rs_signals[ticker] = "🚀 剛剛突破"
                            else: rs_signals[ticker] = "🔥 已經突破"
                        elif latest_rs >= latest_ma * 0.95: rs_signals[ticker] = "🎯 即將突破 (<5%)"
                        else: rs_signals[ticker] = "無"
                    else: rs_signals[ticker] = "無"
                else: rs_signals[ticker] = "無"
        except:
            for t in batch_tickers: rs_signals[t] = "無"
        time.sleep(0.5) 
    return rs_signals

# --- 5. UI 側邊欄 ---
st.title("🎯 美股納指動能過濾器")
st.caption(f"最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with st.sidebar:
    st.header("⚙️ 篩選與顯示控制")
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    
    st.markdown("---")
    # 【新增功能】：選擇顯示的突破類型
    st.subheader("📊 顯示過濾")
    stage_options = ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"]
    selected_stages = st.multiselect("選擇要顯示的階段:", options=stage_options, default=stage_options)
    
    enable_rs = st.checkbox("📈 執行全市場動能掃描", value=False)
    if enable_rs:
        st.warning("⏳ 掃描中，請勿重新整理網頁。")

# --- 6. 主程式邏輯 ---
with st.spinner("獲取 Finviz 數據..."):
    raw_data = fetch_finviz_data()

if not raw_data.empty:
    df_processed = raw_data.copy()
    df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
    final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].copy()
    
    if enable_rs:
        target_tickers = final_df['Ticker'].tolist()
        with st.spinner(f"正在分析 {len(target_tickers)} 隻股票..."):
            rs_results = calculate_all_rs(target_tickers)
            final_df['RS_階段'] = final_df['Ticker'].map(rs_results).fillna("無")
            
            # 【核心過濾代碼】：根據 UI 選擇過濾 DataFrame
            final_df = final_df[final_df['RS_階段'].isin(selected_stages)]
            
            if len(final_df) > 0:
                st.success(f"✅ 篩選完成：找到 {len(final_df)} 隻符合條件的股票。")
            else:
                st.warning("⚠️ 沒有股票符合所選的突破階段。")

    st.markdown("---")
    
    # --- 7. 結果展示與匯出 ---
    if len(final_df) > 0:
        st.subheader("🎯 掃描清單")
        
        # 智能排版欄位
        cols = ['Ticker']
        if 'RS_階段' in final_df.columns:
            cols.append('RS_階段')
        
        # 加入其他重要欄位
        other_cols = ['Company', 'Sector', 'Industry', 'Price', 'Change', 'Volume']
        for oc in other_cols:
            if oc in final_df.columns: cols.append(oc)
        
        st.dataframe(final_df[cols], use_container_width=True, height=600)
        
        csv = final_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 下載此清單 (CSV)", data=csv, file_name="momentum_scan.csv", mime="text/csv")
    else:
        st.info("目前沒有符合篩選條件的股票。請調整左側「最低市值」或確保已勾選「執行掃描」。")
