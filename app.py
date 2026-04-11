import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re

# --- 1. 網頁基本配置 ---
st.set_page_config(page_title="極速市值測試器", layout="wide")

st.title("🚀 極速市值測試器 (數量優先)")
st.markdown("呢個版本只會抓取「市值」數據，目標係測試能否搵返所有達標股票。")

# --- 2. 符號讀取與清洗 (保留最強容錯邏輯) ---
@st.cache_data
def load_symbols():
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    raw_symbols = []
    
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f)
                cols = [c for c in df.columns if c.lower() in ['symbol', 'ticker']]
                if cols:
                    symbols = df[cols[0]].dropna().astype(str).str.strip().tolist()
                    raw_symbols.extend(symbols)
            except Exception as e:
                st.warning(f"讀取 {f} 錯誤: {e}")
                
    # 確保格式統一 (例如 BRK.B -> BRK-B)
    clean_symbols = []
    for s in list(set(raw_symbols)):
        formatted_s = re.sub(r'[\./]', '-', s.upper())
        if re.match(r'^[A-Z-]+$', formatted_s) and len(formatted_s) < 8:
            clean_symbols.append(formatted_s)
            
    return sorted(list(set(clean_symbols)))

# --- 3. 數據抓取：極速版 (只攞市值) ---
@st.cache_data(ttl=3600)
def fetch_only_market_cap(symbols, min_cap):
    results = []
    batch_size = 100 # 只攞市值，Batch 可以大返啲
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_text.text(f"🚀 正在極速掃描: {i} / {len(symbols)}...")
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            # 重點：只用 summary_detail，唔好 get_modules
            t = Ticker(batch, asynchronous=True)
            data = t.summary_detail 
            
            if not isinstance(data, dict):
                continue
                
            for s in batch:
                s_info = data.get(s)
                if isinstance(s_info, dict):
                    mkt_cap = s_info.get('marketCap')
                    # 只要市值過關，就記錄
                    if isinstance(mkt_cap, (int, float)) and mkt_cap >= min_cap:
                        results.append({
                            "Symbol": s,
                            "Name": s_info.get('shortName', s),
                            "MarketCap": mkt_cap,
                            "Price": s_info.get('previousClose', 0)
                        })
        except Exception:
            continue
            
    progress_bar.empty()
    status_text.empty()
    return pd.DataFrame(results)

# --- 4. 介面邏輯 ---
st.sidebar.header("測試設定")
target_cap = st.sidebar.number_input("最低市值 (USD)", value=500000000, step=100000000)

if st.button("🔍 開始極速掃描", use_container_width=True):
    all_symbols = load_symbols()
    
    if not all_symbols:
        st.error("❌ 搵唔到股票代號，請檢查 CSV 檔案。")
    else:
        st.info(f"名單共有 {len(all_symbols)} 隻股票，開始向 Yahoo 請求市值...")
        
        with st.spinner("執行中..."):
            df_result = fetch_only_market_cap(all_symbols, target_cap)
            
            if not df_result.empty:
                st.success(f"✅ 成功搵返 {len(df_result)} 隻符合條件嘅股票！")
                
                # 顯示結果
                st.dataframe(
                    df_result.sort_values("MarketCap", ascending=False),
                    column_config={
                        "MarketCap": st.column_config.NumberColumn("市值 ($)", format="$%.2e"),
                        "Price": st.column_config.NumberColumn("股價", format="$%.2f")
                    },
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.warning("⚠️ 依然搵唔到股票。可能係 CSV 代號格式問題或者 API 暫時被限流。")

else:
    st.info("請點擊按鈕開始測試。")
