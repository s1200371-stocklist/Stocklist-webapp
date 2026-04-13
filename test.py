import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import datetime

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="美股 500M+ 市值掃描器", page_icon="📈", layout="wide")

# --- 2. 強健的市值轉換函數 (Double Checked) ---
def convert_mcap_to_float(val):
    """
    將 Finviz 嘅字串市值 (例如 '3.04B', '500.50M') 轉換為浮點數 (單位：Million)
    加入咗 try-except 確保遇到缺失值 '-' 時唔會報錯。
    """
    try:
        if pd.isna(val) or val == '-':
            return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val:
            return float(val.replace('B', '')) * 1000  # Billion 轉為 Million
        if 'M' in val:
            return float(val.replace('M', ''))
        return float(val)
    except:
        return 0.0

# --- 3. 數據獲取引擎 (加入快取與錯誤處理) ---
@st.cache_data(ttl=1800) # 快取 30 分鐘，保護 IP 不被 Finviz 封鎖
def fetch_stock_data():
    try:
        f_screener = Overview()
        # 【關鍵設定】：改用 '+Small (over $300mln)'
        # 確保抓取所有市值大於 3 億美金嘅股票，包含 NVDA, TSLA 等巨型股 (Mega Cap)
        filters_dict = {'Market Cap.': '+Small (over $300mln)'}
        f_screener.set_filter(filters_dict=filters_dict)
        
        # 獲取篩選結果 (DataFrame 格式)
        df = f_screener.screener_view()
        return df
    except Exception as e:
        st.error(f"連線至 Finviz 失敗，可能遭遇 IP 限制。錯誤訊息: {e}")
        return pd.DataFrame()

# --- 4. 主畫面 UI 介面 ---
st.title("🚀 專業美股掃描器 (Finviz 引擎)")
st.caption(f"數據最後更新時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# --- 5. 側邊欄設定 ---
with st.sidebar:
    st.header("⚙️ 篩選參數")
    # 用戶可自定義最低市值，預設為 500M
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    
    st.markdown("---")
    st.info("""
    **📊 掃描器邏輯說明：**\n
    後台引擎會先從 Finviz 抓取所有 **>300M** 的股票名單 (包含中大盤及巨頭股)，然後再根據你在上方設定的數值進行精確過濾。
    """)

# --- 6. 執行邏輯與數據呈現 ---
with st.spinner("正在從 Finviz 獲取並清洗數據，請稍候..."):
    raw_data = fetch_stock_data()

if not raw_data.empty:
    # 複製一份數據進行處理，避免影響快取原始資料
    df_processed = raw_data.copy()
    
    # 新增一個數值欄位供 Python 內部過濾排序使用
    df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
    
    # 執行用戶設定的過濾條件，並按市值由大至小排序
    final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].sort_values(by='Mcap_Numeric', ascending=False)
    
    # 顯示過濾後的股票總數
    st.metric(label=f"符合條件的股票數量 (市值 >= {min_mcap}M)", value=len(final_df))
    
    # 整理要顯示的欄位 (隱藏輔助用的 Mcap_Numeric 欄位)
    display_cols = [c for c in final_df.columns if c != 'Mcap_Numeric']
    
    # 顯示美化後的資料表
    st.dataframe(final_df[display_cols], use_container_width=True, height=600)
    
    # 提供 CSV 下載按鈕
    csv = final_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 匯出清單 (CSV)", 
        data=csv, 
        file_name=f"scanner_mcap_{int(min_mcap)}M_plus.csv", 
        mime="text/csv"
    )
else:
    st.warning("⚠️ 目前未能獲取數據，請檢查網絡連線，或稍後再試。")

