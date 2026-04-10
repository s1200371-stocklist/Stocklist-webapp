import streamlit as st
import pandas as pd
from yahooquery import Ticker
import time
import os

# --- 1. 網頁配置 ---
st.set_page_config(page_title="US Stock Screener Pro", layout="wide")

st.title("📈 全美股市值篩選器 (修復版)")

import re # 加入正則表達式庫

# --- 優化版：符號清洗邏輯 ---
@st.cache_data
def load_symbols():
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    all_symbols = []
    
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f)
                cols = [c for c in df.columns if c.lower() in ['symbol', 'ticker']]
                if cols:
                    symbols = df[cols[0]].dropna().astype(str).str.strip().tolist()
                    all_symbols.extend(symbols)
            except Exception:
                pass
    
    clean_symbols = []
    for s in list(set(all_symbols)):
        # 1. 將所有 CSV 的點 (.) 或斜線 (/) 換成 Yahoo 認可的橫線 (-)
        # 例如 BRK.B -> BRK-B
        formatted_s = re.sub(r'[\./]', '-', s.upper())
        
        # 2. 放寬限制：允許字母同橫線，長度限制放寬到 8
        if re.match(r'^[A-Z-]+$', formatted_s) and len(formatted_s) < 8:
            clean_symbols.append(formatted_s)
            
    return sorted(list(set(clean_symbols)))

# --- 4. UI 介面 ---
st.sidebar.header("篩選條件")
target_cap = st.sidebar.number_input("最低市值 (USD)", value=500000000, step=100000000, format="%d")

if st.button("🔍 開始全市場掃描"):
    try:
        symbols = load_symbols()
        
        if not symbols:
            st.error("❌ 找不到股票代號。請確保 CSV 檔案與 app.py 放在同一個資料夾。")
        else:
            with st.spinner(f"正在從 Yahoo 抓取 {len(symbols)} 隻股票數據..."):
                df_result = fetch_stock_data(symbols, target_cap)
                
                if not df_result.empty:
                    st.balloons()
                    st.success(f"找到 {len(df_result)} 隻市值 > {target_cap/1e6:.0f}M 的美股")
                    
                    # 專業表格顯示 (修復了 AttributeError 可能的來源)
                    st.dataframe(
                        df_result.sort_values("MarketCap", ascending=False),
                        column_config={
                            "Symbol": "代號",
                            "Name": "公司名",
                            "MarketCap": st.column_config.NumberColumn("市值 ($)", format="$%.2e"),
                            "Price": st.column_config.NumberColumn("股價", format="$%.2f"),
                            "Sector": "板塊"
                        },
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.warning("符合條件的股票數量為 0。")
                    
    except Exception as global_e:
        st.error("🚨 程式運行出現非預期錯誤：")
        st.exception(global_e) # 這裡會顯示具體的錯誤行數，方便我們 debug
else:
    st.info("請點擊左側或上方的按鈕開始掃描。")
