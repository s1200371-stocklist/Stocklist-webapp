import streamlit as st
import pandas as pd
from yahooquery import Ticker
import time
import os

# --- 1. 網頁配置 ---
st.set_page_config(page_title="US Stock Screener Pro", layout="wide")

st.title("📈 全美股市值篩選器 (修復版)")

# --- 2. 符號獲取邏輯 ---
@st.cache_data
def load_symbols():
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    all_symbols = []
    
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f)
                # 自動搜尋可能的欄位名稱 (不分大小寫)
                cols = [c for c in df.columns if c.lower() in ['symbol', 'ticker']]
                if cols:
                    symbols = df[cols[0]].dropna().astype(str).str.strip().unique().tolist()
                    all_symbols.extend(symbols)
            except Exception as e:
                st.warning(f"讀取 {f} 時發生小錯誤: {e}")
    
    # 移除重複並過濾掉無效符號 (例如包含 $ 的權證或過長的名稱)
    clean_symbols = [s for s in list(set(all_symbols)) if s.isalpha() and len(s) < 6]
    return sorted(clean_symbols)

# --- 3. 數據抓取邏輯 ---
@st.cache_data(ttl=3600)
def fetch_stock_data(symbols, min_cap):
    results = []
    batch_size = 200 # 縮小批次以提高穩定性
    
    progress = st.progress(0)
    status = st.empty()
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status.text(f"🚀 正在分析: {i} / {len(symbols)} 隻股票...")
        progress.progress(min(i / len(symbols), 1.0))
        
        try:
            # 使用 yahooquery 抓取數據
            t = Ticker(batch, asynchronous=True)
            data = t.summary_detail
            
            # 確保 data 是字典格式
            if not isinstance(data, dict):
                continue
                
            for s in batch:
                s_info = data.get(s)
                if isinstance(s_info, dict):
                    mkt_cap = s_info.get('marketCap')
                    # 嚴格數字檢查
                    if isinstance(mkt_cap, (int, float)) and mkt_cap >= min_cap:
                        results.append({
                            "Symbol": s,
                            "Name": s_info.get('shortName', 'N/A'),
                            "MarketCap": mkt_cap,
                            "Price": s_info.get('previousClose', 0),
                            "Sector": s_info.get('sector', 'N/A')
                        })
        except Exception:
            continue # 忽略單一記錄錯誤
            
    progress.empty()
    status.empty()
    return pd.DataFrame(results)

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
