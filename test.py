import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime
import time

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="美股全量動能掃描器", page_icon="📈", layout="wide")

# --- 2. 數據轉換函數 ---
def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except:
        return 0.0

# --- 3. Finviz 數據獲取 ---
@st.cache_data(ttl=1800)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"連線至 Finviz 失敗: {e}")
        return pd.DataFrame()

# --- 4. yfinance 全量批量計算引擎 (加入分批與防禦機制) ---
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_rs(tickers, benchmark="^GSPC", batch_size=500):
    """將龐大名單分批下載，避免 Yahoo Finance 封鎖 IP"""
    rs_signals = {}
    
    # 步驟一：先獨立下載基準指數 (如 S&P 500)
    bench_data = yf.download(benchmark, period="3mo", progress=False)['Close']
    if bench_data.empty: return rs_signals
    bench_norm = bench_data / bench_data.iloc[0] # 基準標準化

    # 步驟二：分批 (Chunking) 下載股票數據
    total_batches = (len(tickers) // batch_size) + 1
    
    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i+batch_size]
        
        # 批量下載 (關閉 progress bar 避免 UI 混亂)
        data = yf.download(batch_tickers, period="3mo", progress=False)['Close']
        
        # 處理如果 batch 只有 1 隻股票時，yfinance 回傳 Series 而非 DataFrame 嘅情況
        if isinstance(data, pd.Series):
            data = data.to_frame(name=batch_tickers[0])
            
        data = data.ffill().dropna(how='all')
        
        # 運算 RS
        for ticker in batch_tickers:
            if ticker in data.columns and not data[ticker].dropna().empty:
                stock_price = data[ticker].dropna()
                if len(stock_price) > 25:
                    stock_norm = stock_price / stock_price.iloc[0]
                    # 對齊交易日
                    aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                    
                    rs_line = stock_norm / aligned_bench * 100
                    rs_ma_25 = rs_line.rolling(window=25).mean()
                    
                    latest_rs = rs_line.iloc[-1]
                    latest_ma = rs_ma_25.iloc[-1]
                    rs_signals[ticker] = latest_rs > latest_ma
                else:
                    rs_signals[ticker] = False
            else:
                rs_signals[ticker] = False
        
        # 【關鍵防禦】：每下載完 500 隻，強迫系統休息 1 秒，保護 IP 唔被封鎖
        time.sleep(1)
        
    return rs_signals

# --- 5. UI 介面 ---
st.title("🚀 美股全量掃描器 (Finviz + RS 強勢濾網)")
st.caption(f"數據最後更新時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with st.sidebar:
    st.header("⚙️ 第一層：基本面篩選")
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    
    st.markdown("---")
    st.header("📈 第二層：技術面篩選 (RS 動能)")
    enable_rs = st.checkbox("啟用 RS > 25D MA 全市場掃描", value=False)
    
    if enable_rs:
        st.warning("⏳ **注意：全市場掃描模式已開啟。** 系統將運算超過數千隻股票，第一次執行可能需要 **2 至 5 分鐘**。請耐心等候，切勿重新整理網頁。")

# --- 6. 執行主邏輯 ---
with st.spinner("正在從 Finviz 獲取基礎數據..."):
    raw_data = fetch_finviz_data()

if not raw_data.empty:
    df_processed = raw_data.copy()
    df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
    
    # 第一層過濾 (市值)
    final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].sort_values(by='Mcap_Numeric', ascending=False)
    
    # 第二層過濾 (全量 RS 均線)
    if enable_rs:
        target_tickers = final_df['Ticker'].tolist() # 攞取全部股票代號
        
        with st.spinner(f"正在全速運算 {len(target_tickers)} 隻股票嘅 RS 動能... 預計需時數分鐘，請稍候 ☕"):
            rs_results = calculate_all_rs(target_tickers)
            
            # 將結果 Map 回 DataFrame
            final_df['RS_Strong'] = final_df['Ticker'].map(rs_results)
            
            # 過濾並剔除空值
            final_df = final_df[final_df['RS_Strong'] == True]
            st.success(f"✅ 全市場 RS 技術動能篩選完成！成功過濾出強勢股。")

    # 顯示結果
    st.metric(label="符合所有條件的強勢股數量", value=len(final_df))
    
    display_cols = [c for c in final_df.columns if c not in ['Mcap_Numeric', 'RS_Strong']]
    st.dataframe(final_df[display_cols], use_container_width=True, height=600)
    
    csv = final_df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 匯出強勢股清單 (CSV)", data=csv, file_name="all_market_strong_stocks.csv", mime="text/csv")
    
else:
    st.error("未能獲取初始數據。")
