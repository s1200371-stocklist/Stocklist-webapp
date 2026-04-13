import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="美股 CAN SLIM 極速掃描器", page_icon="⚡", layout="wide")

# --- 2. 工具函數 ---
def convert_mcap_to_float(val):
    """清洗市值數據"""
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except:
        return 0.0

@st.cache_data(ttl=1800)
def fetch_finviz_data():
    """第一階段：獲取基礎股票池 (>300M)"""
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"連線至 Finviz 失敗: {e}")
        return pd.DataFrame()

# --- 3. 核心升級：極速 RS 批量計算引擎 ---
@st.cache_data(ttl=3600)
def check_rs_signal_bulk(ticker_list, benchmark="^GSPC", period="6mo"):
    """
    極速版 RS 掃描器：使用批量下載與 Pandas 矩陣運算
    """
    try:
        # A. 處理 Ticker 格式差異 (Finviz '.' -> Yahoo '-')
        yf_tickers = [t.replace('.', '-') for t in ticker_list]
        all_tickers = yf_tickers + [benchmark]
        
        # B. 批量下載 (開啟多線程 threads=True)
        data = yf.download(all_tickers, period=period, threads=True, progress=False)['Close']
        
        if data.empty or benchmark not in data.columns:
            return []
            
        # C. 分離大盤與個股數據
        bench_data = data[benchmark].ffill().dropna()
        stock_data = data.drop(columns=[benchmark]).ffill().dropna(axis=1, how='all')
        
        # 將欄位名稱由 Yahoo 格式轉換返 Finviz 格式 (方便後續 Mapping)
        stock_data.columns = [c.replace('-', '.') for c in stock_data.columns]
        
        # D. 基準化 (第一日設為 100)
        bench_norm = (bench_data / bench_data.iloc[0]) * 100
        stock_norm = (stock_data / stock_data.iloc[0].values) * 100
        
        # E. 矩陣運算：一次過計算所有股票的 RS Line 及 25D MA
        rs_lines = stock_norm.div(bench_norm, axis=0)
        rs_ma_25 = rs_lines.rolling(window=25).mean()
        
        # F. 生成訊號：最新一日 RS Line > 25D MA
        latest_rs = rs_lines.iloc[-1]
        latest_ma = rs_ma_25.iloc[-1]
        
        strong_condition = latest_rs > latest_ma
        passed_tickers = strong_condition[strong_condition].index.tolist()
        
        return passed_tickers

    except Exception as e:
        st.error(f"批量運算發生錯誤: {e}")
        return []

# --- 4. 主畫面 UI ---
st.title("⚡ 美股 CAN SLIM 雙重極速掃描器")
st.caption(f"數據最後更新時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# 側邊欄設定
with st.sidebar:
    st.header("⚙️ 第一階段：基本面 (Finviz)")
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    
    st.header("🎯 第二階段：技術面 (RS 動能)")
    # 注意：上限大幅提升至 1000 隻
    scan_limit = st.slider("選擇要進行 RS 分析的股票數量", min_value=50, max_value=1000, value=500, step=50)
    st.success("✅ 已啟用向量化引擎，支援 1000 隻股票極速掃描！")

# --- 5. 執行邏輯 ---
with st.spinner("正在獲取全市場基礎數據..."):
    raw_data = fetch_finviz_data()

if not raw_data.empty:
    df_processed = raw_data.copy()
    df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
    
    # 第一關：過濾市值並按市值排序
    base_filtered_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].sort_values(by='Mcap_Numeric', ascending=False)
    
    st.subheader(f"📊 第一關：市值大於 {min_mcap}M 的股票 (總數: {len(base_filtered_df)})")
    st.dataframe(base_filtered_df.drop(columns=['Mcap_Numeric']).head(100), height=300) 
    st.caption("👆 上表僅預覽前 100 隻股票。")
    
    st.divider()
    
    # 第二關：RS 動能掃描區塊
    st.subheader(f"🔥 第二關：RS 強度掃描 (針對前 {scan_limit} 大股票)")
    
    target_tickers = base_filtered_df['Ticker'].head(scan_limit).tolist()
    
    if st.button(f"🚀 立即執行極速 RS 掃描", type="primary"):
        start_time = datetime.datetime.now()
        
        with st.spinner(f"正在從 Yahoo Finance 批量下載 {scan_limit} 隻股票數據並進行矩陣運算..."):
            # 執行極速版 RS 計算
            strong_tickers = check_rs_signal_bulk(target_tickers)
            
            end_time = datetime.datetime.now()
            time_taken = (end_time - start_time).total_seconds()
            
            if strong_tickers:
                st.success(f"⚡ 掃描完成！耗時 {time_taken:.1f} 秒。在 {scan_limit} 隻股票中，有 {len(strong_tickers)} 隻突破 25 天 RS 均線。")
                
                # 提取強勢股完整數據
                final_rs_df = base_filtered_df[base_filtered_df['Ticker'].isin(strong_tickers)]
                
                st.dataframe(final_rs_df.drop(columns=['Mcap_Numeric']), use_container_width=True, height=500)
                
                # 下載按鈕
                csv = final_rs_df.to_csv(index=False).encode('utf-8')
                st.download_button("📥 匯出強勢股清單 (CSV)", csv, "can_slim_strong_stocks.csv", "text/csv")
            else:
                st.warning("目前沒有符合條件的股票，可能大盤處於極度弱勢。")
else:
    st.error("未能從 Finviz 獲取基礎數據。")
