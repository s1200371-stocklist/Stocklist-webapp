import streamlit as st
from yahooquery import Ticker
import pandas as pd
import time

st.set_page_config(page_title="Ultra Stock Scanner", layout="wide")
st.title("🦅 全美股掃描器 (KeyError 徹底修復版)")

# --- 側邊欄：預留給你的「其他篩選」 ---
with st.sidebar:
    st.header("⚙️ 篩選條件設定")
    min_cap = st.number_input("最低市值 (百萬 USD)", value=500)
    # 你提到的「其他篩選」可以加在這裡，例如：
    min_rs = st.slider("最低 RS 分數門檻", 0, 100, 0)
    target_sector = st.multiselect("特定行業 (留空則選全部)", 
                                  ["Technology", "Healthcare", "Financial", "Energy", "Consumer Cyclical"])

@st.cache_data(ttl=86400)
def get_symbols():
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        df = pd.read_csv(url, header=None)
        return [s.strip().upper() for s in df[0].dropna().astype(str).tolist() if s.isalpha() and len(s) <= 5]
    except:
        return ["AAPL", "NVDA", "TSLA", "MSFT"]

def run_safe_scan():
    all_symbols = get_symbols()
    qualified_list = []
    
    with st.status("🚀 正在掃描市場...", expanded=True) as status:
        # --- 階段 1: 市值篩選 (解決 KeyError: 'marketCap') ---
        status.write("🔍 正在過濾市值...")
        batch_size = 500
        for i in range(0, len(all_symbols), batch_size):
            batch = all_symbols[i:i+batch_size]
            try:
                t = Ticker(batch)
                p_data = t.price
                for s in batch:
                    # 關鍵修復：先檢查是否為字典，再檢查鍵值是否存在
                    if isinstance(p_data, dict) and s in p_data and isinstance(p_data[s], dict):
                        m_cap = p_data[s].get('marketCap', 0)
                        if isinstance(m_cap, (int, float)) and m_cap >= (min_cap * 1_000_000):
                            qualified_list.append(s)
            except: continue
        
        if not qualified_list:
            st.error("找不到符合條件的股票。")
            return None

        # --- 階段 2: 動能計算 (解決 KeyError: 'close') ---
        status.write(f"📈 正在分析 {len(qualified_list)} 隻股票的動能...")
        all_h = []
        for i in range(0, len(qualified_list), 200):
            try:
                h = Ticker(qualified_list[i:i+200], asynchronous=True).history(period="1y", interval="1d")
                # 關鍵修復：確保有回傳資料且包含 'close' 欄位
                if not h.empty and 'close' in h.columns:
                    all_h.append(h)
            except: continue
            
        if not all_h: return None
        
        full_df = pd.concat(all_h)
        # 處理多層索引並確保數據完整
        prices = full_df['close'].unstack(level=0).ffill()
        
        # 計算 RS (相對強度)
        ret_now = (prices.iloc[-1] / prices.iloc[0]) - 1
        ret_3d = (prices.iloc[-4] / prices.iloc[0]) - 1
        
        final_df = pd.DataFrame({
            'Price': prices.iloc[-1],
            'RS_Now': (ret_now.rank(pct=True) * 98 + 1).fillna(0).astype(int),
            'RS_3D': (ret_3d.rank(pct=True) * 98 + 1).fillna(0).astype(int)
        })
        final_df['RS_Change'] = final_df['RS_Now'] - final_df['RS_3D']

        # --- 階段 3: 行業同步 (限制抓取量避免封鎖) ---
        status.write("🏢 正在抓取行業分類 (前 1500 隻)...")
        # 為了效能，我們只對 RS 較高的股票抓行業
        top_for_sector = final_df.sort_values('RS_Now', ascending=False).head(1500).index.tolist()
        s_map = {}
        for i in range(0, len(top_for_sector), 50):
            batch = top_for_sector[i:i+50]
            try:
                prof = Ticker(batch).asset_profile
                for s in batch:
                    if isinstance(prof, dict) and s in prof and isinstance(prof[s], dict):
                        s_map[s] = prof[s].get('sector', 'Unknown')
            except: pass
            time.sleep(0.5)
            
        final_df['Sector'] = final_df.index.map(s_map).fillna("Other/Weak")

        # --- 階段 4: 你要的「其他篩選」可以在這裡執行 ---
        # 例如：篩選 RS 分數
        final_df = final_df[final_df['RS_Now'] >= min_rs]
        
        # 例如：篩選特定行業
        if target_sector:
            final_df = final_df[final_df['Sector'].isin(target_sector)]

        status.update(label="✅ 掃描完成！", state="complete", expanded=False)
        return final_df

if st.button("🚀 開始全量掃描"):
    df_result = run_safe_scan()
    
    if df_result is not None:
        st.divider()
        st.subheader(f"📊 全部符合條件的股票 (共 {len(df_result)} 隻)")
        
        # 整理表格順序
        output = df_result.reset_index().rename(columns={'index': 'Symbol'})
        output = output.sort_values('RS_Now', ascending=False)
        
        # 顯示全部結果
        st.dataframe(output[['Symbol', 'Sector', 'RS_Now', 'RS_Change', 'Price']], 
                     use_container_width=True, height=800)
        
        # 提供下載
        csv = output.to_csv(index=False).encode('utf-8')
        st.download_button("📥 下載完整名單 (CSV)", csv, "stocks.csv", "text/csv")
