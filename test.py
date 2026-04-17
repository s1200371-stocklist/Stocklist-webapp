import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime
import time

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 美股 RS x MACD 動能狙擊手", page_icon="🎯", layout="wide")

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

# --- 4. yfinance 批量計算引擎 (包含 RS, MACD 及 SMA 趨勢) ---
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, batch_size=200):
    """一次下載，同時計算 RS、MACD 及 SMA125 趨勢，確保極速且防封鎖"""
    results = {} 
    
    # 【基準下載 (用於 RS)】
    benchmarks_to_try = ["QQQ", "^NDX", "QQQM"]
    bench_data = pd.DataFrame()
    used_bench = ""

    for b in benchmarks_to_try:
        try:
            # 【升級】延長至 1y (一年)，確保有足夠日數計算 SMA125
            temp_data = yf.download(b, period="1y", progress=False)
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

    # 【分批下載與指標計算】
    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i+batch_size]
        try:
            # 同樣延長至 1y
            data = yf.download(batch_tickers, period="1y", progress=False)
            if data.empty or 'Close' not in data.columns: raise ValueError("No Data")
            close_prices = data['Close']
            if isinstance(close_prices, pd.Series): close_prices = close_prices.to_frame(name=batch_tickers[0])
            close_prices = close_prices.ffill().dropna(how='all')
            if close_prices.index.tz is not None: close_prices.index = close_prices.index.tz_localize(None)
            
            for ticker in batch_tickers:
                # 預設狀態為無 / 不符合
                rs_stage = "無"
                macd_stage = "無"
                sma_trend = False
                
                if ticker in close_prices.columns and not close_prices[ticker].dropna().empty:
                    stock_price = close_prices[ticker].dropna()
                    
                    # 確保有足夠數據 (需要最少 126 日來計算 SMA125 及比較)
                    if len(stock_price) > 126: 
                        
                        # --- 運算 1：RS 動能 ---
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
                        
                        # --- 運算 2：MACD ---
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
                                    
                        # --- 運算 3：SMA 趨勢 (SMA25 > SMA125) ---
                        sma25 = stock_price.rolling(window=25).mean()
                        sma125 = stock_price.rolling(window=125).mean()
                        
                        latest_sma25 = float(sma25.iloc[-1])
                        latest_sma125 = float(sma125.iloc[-1])
                        
                        sma_trend = latest_sma25 > latest_sma125
                            
                results[ticker] = {'RS': rs_stage, 'MACD': macd_stage, 'SMA_Trend': sma_trend}
                
        except Exception as e:
            for t in batch_tickers: results[t] = {'RS': "無", 'MACD': "無", 'SMA_Trend': False}
        time.sleep(0.5) 
        
    return results

# --- 5. UI 側邊欄設計 ---
st.title("🎯 美股 RS x MACD x 趨勢 狙擊手")
st.caption(f"最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with st.sidebar:
    st.header("⚙️ 基礎篩選")
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    
    st.markdown("---")
    st.header("📈 指標過濾控制")
    
    # 趨勢開關
    enable_sma = st.checkbox("啟動 【SMA25 > SMA125】 長線多頭過濾", value=True)
    if enable_sma:
        st.info("✅ 已啟用：只顯示中線趨勢強於長線趨勢的股票。")
        
    st.markdown("---")
    
    # RS 開關與多選
    enable_rs = st.checkbox("啟動 【RS 對比納指】 過濾", value=True)
    if enable_rs:
        rs_options = ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"]
        selected_rs = st.multiselect("顯示 RS 階段:", options=rs_options, default=["🚀 剛剛突破"])
    
    st.markdown("---")
    
    # MACD 開關與多選
    enable_macd = st.checkbox("啟動 【MACD】 過濾", value=True)
    if enable_macd:
        macd_options = ["🚀 剛剛突破", "🔥 已經突破", "🎯 即將突破 (<5%)"]
        selected_macd = st.multiselect("顯示 MACD 階段:", options=macd_options, default=["🚀 剛剛突破"])

    st.markdown("---")
    start_scan = st.button("🚀 執行全市場精確掃描", use_container_width=True, type="primary")

# --- 6. 主程式邏輯 ---
if start_scan:
    with st.spinner("獲取 Finviz 基礎數據..."):
        raw_data = fetch_finviz_data()

    if not raw_data.empty:
        df_processed = raw_data.copy()
        df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
        final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].copy()
        
        if enable_rs or enable_macd or enable_sma:
            target_tickers = final_df['Ticker'].tolist()
            with st.spinner(f"正在全速下載並運算 {len(target_tickers)} 隻股票的各項指標 (資料量增至 1 年)... 預計需時數分鐘 ☕"):
                indicators_results = calculate_all_indicators(target_tickers)
                
                # 將結果 Map 回 DataFrame
                final_df['RS_階段'] = final_df['Ticker'].map(lambda x: indicators_results.get(x, {}).get('RS', '無'))
                final_df['MACD_階段'] = final_df['Ticker'].map(lambda x: indicators_results.get(x, {}).get('MACD', '無'))
                final_df['SMA多頭'] = final_df['Ticker'].map(lambda x: indicators_results.get(x, {}).get('SMA_Trend', False))
                
                # 根據用戶 UI 選擇進行嚴格過濾
                if enable_sma:
                    final_df = final_df[final_df['SMA多頭'] == True]
                if enable_rs:
                    final_df = final_df[final_df['RS_階段'].isin(selected_rs)]
                if enable_macd:
                    final_df = final_df[final_df['MACD_階段'].isin(selected_macd)]
                
                if len(final_df) > 0:
                    st.success(f"✅ 篩選完成！成功尋找到 {len(final_df)} 隻符合你完美設定的股票。")
                else:
                    st.warning("⚠️ 掃描完成，但沒有股票能同時滿足你設定的嚴格條件。")

        st.markdown("---")
        
        # --- 7. 結果展示與匯出 ---
        if len(final_df) > 0:
            st.subheader("🎯 終極精選清單")
            
            # 智能排版：將 Ticker, RS, MACD 放在最顯眼的最左側
            cols = ['Ticker']
            if 'RS_階段' in final_df.columns: cols.append('RS_階段')
            if 'MACD_階段' in final_df.columns: cols.append('MACD_階段')
            
            # 加入基本面欄位，過濾掉輔助數值 (包括純布林值的 SMA多頭，因為已過濾)
            other_cols = ['Company', 'Sector', 'Industry', 'Price', 'Change', 'Volume']
            for oc in other_cols:
                if oc in final_df.columns: cols.append(oc)
            
            st.dataframe(final_df[cols], use_container_width=True, height=600)
            
            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 下載此終極清單 (CSV)", data=csv, file_name="rs_macd_trend_sniper.csv", mime="text/csv")
        elif not (enable_rs or enable_macd or enable_sma):
             st.info("請勾選至少一個指標，並點擊「執行全市場精確掃描」。")
