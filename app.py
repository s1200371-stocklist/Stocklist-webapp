import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re

# --- 1. 網頁配置 ---
st.set_page_config(page_title="市值優先篩選器", layout="wide")

st.title("📈 市值優先篩選器 (保證數量版)")
st.markdown("只要市值達標就會顯示，行業/板塊資料抓不到時顯示 N/A。")

# --- 2. 符號清洗 ---
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
            except: pass
    
    clean = []
    for s in list(set(all_symbols)):
        # 轉換為 Yahoo 支援格式
        formatted = re.sub(r'[\./]', '-', s.upper())
        if re.match(r'^[A-Z-]+$', formatted) and len(formatted) < 8:
            clean.append(formatted)
    return sorted(list(set(clean)))

# --- 3. 數據抓取 (重點修復邏輯) ---
@st.cache_data(ttl=3600)
def fetch_robust_data(symbols, min_cap):
    results = []
    batch_size = 50 # 縮小批次，極大提高穩定性
    
    progress_bar = st.progress(0)
    status = st.empty()
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status.text(f"正在掃描: {i} / {len(symbols)}...")
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            t = Ticker(batch, asynchronous=True)
            # 同時拿這兩個模組
            all_data = t.get_modules(['summaryDetail', 'assetProfile'])
            
            for s in batch:
                s_data = all_data.get(s)
                if not isinstance(s_data, dict): continue
                
                # 拿數據
                summary = s_data.get('summaryDetail', {})
                profile = s_data.get('assetProfile', {})
                
                # 市值是唯一「硬指標」
                mkt_cap = summary.get('marketCap')
                
                if isinstance(mkt_cap, (int, float)) and mkt_cap >= min_cap:
                    # 只要市值夠，就收進來！行業抓不到就給 N/A
                    results.append({
                        "Symbol": s,
                        "Name": summary.get('shortName', s),
                        "MarketCap": mkt_cap,
                        "Price": summary.get('previousClose', 0),
                        "Sector": profile.get('sector', 'N/A') if isinstance(profile, dict) else 'N/A',
                        "Industry": profile.get('industry', 'N/A') if isinstance(profile, dict) else 'N/A'
                    })
        except:
            continue
            
    progress_bar.empty()
    status.empty()
    return pd.DataFrame(results)

# --- 4. UI 邏輯 ---
st.sidebar.header("篩選參數")
target_cap = st.sidebar.number_input("最低市值 (USD)", value=500_000_000, step=100_000_000)

if st.button("🔍 開始全市場掃描", use_container_width=True):
    symbols = load_symbols()
    if not symbols:
        st.error("找不到代碼。")
    else:
        with st.spinner("正在全力抓取數據..."):
            df = fetch_robust_data(symbols, target_cap)
            if not df.empty:
                st.success(f"找到 {len(df)} 隻市值達標股票！")
                st.dataframe(
                    df.sort_values("MarketCap", ascending=False),
                    column_config={
                        "MarketCap": st.column_config.NumberColumn("市值", format="$%.2e"),
                        "Price": st.column_config.NumberColumn("股價", format="$%.2f")
                    },
                    use_container_width=True, hide_index=True
                )
            else:
                st.warning("無符合股票。")
