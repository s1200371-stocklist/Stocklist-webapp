import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import datetime

# --- 專業配置 ---
st.set_page_config(page_title="美股 500M+ 市值掃描器", layout="wide")

# 1. 強健的市值轉換函數 (Double Checked)
def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-':
            return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val:
            return float(val.replace('B', '')) * 1000  # 轉為 Million
        if 'M' in val:
            return float(val.replace('M', ''))
        return float(val)
    except:
        return 0.0

# 2. 數據獲取 (加入快取與錯誤處理)
@st.cache_data(ttl=1800)
def fetch_stock_data():
    try:
        f_screener = Overview()
        # 為了覆蓋 500M 以上，我們需要抓取 Small, Mid, Large 等區間
        # 這裡示範抓取 Small ($300mln to $2bln)，如需更多可疊加或不設限
        filters_dict = {'Market Cap.': 'Small ($300mln to $2bln)'}
        f_screener.set_filter(filters_dict=filters_dict)
        df = f_screener.screener_view()
        return df
    except Exception as e:
        st.error(f"連線至 Finviz 失敗: {e}")
        return pd.DataFrame()

# --- UI 介面 ---
st.title("🚀 專業股票掃描器 (Finviz 引擎)")
st.caption(f"最後更新時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with st.sidebar:
    st.header("篩選參數")
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    st.info("註：目前設定掃描範圍為 Finviz 'Small Cap' 區別 (300M - 2B)。")

# 執行邏輯
raw_data = fetch_stock_data()

if not raw_data.empty:
    # 複製一份進行處理
    df_processed = raw_data.copy()
    
    # 轉換市值欄位供過濾使用
    df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
    
    # 執行過濾
    final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].sort_values(by='Mcap_Numeric', ascending=False)
    
    # 顯示結果
    st.metric("符合條件股票數", len(final_df))
    
    # 移除輔助欄位後顯示
    display_cols = [c for c in final_df.columns if c != 'Mcap_Numeric']
    st.dataframe(final_df[display_cols], use_container_width=True)
    
    # 下載功能
    csv = final_df.to_csv(index=False).encode('utf-8')
    st.download_button("匯出數據 (CSV)", csv, "scanner_results.csv", "text/csv")
else:
    st.warning("未能獲取數據，請檢查 Finviz 是否封鎖了當前 IP。")

