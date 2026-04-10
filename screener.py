import streamlit as st
from yahooquery import Ticker
import pandas as pd
import time
import numpy as np

st.set_page_config(page_title="US Market Cap Pro", layout="wide")
st.title("🦅 美股動能矩陣 (修復版)")
st.caption("已修復 IntCastingNaNError | 市值 > $500M 篩選")

@st.cache_data(ttl=86400)
def get_all_raw_symbols():
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        tickers = pd.read_csv(url, header=None)[0].dropna().astype(str).tolist()
        return [t.strip().upper() for t in tickers if len(t) <= 4]
    except:
        return ["AAPL", "NVDA", "TSLA", "MSFT", "AMD", "META", "GOOGL", "AMZN"]

def filter_by_market_cap(symbols, min_cap_m):
    status_msg = st.empty()
    filtered_list = []
    batch_size = 200 # 縮小批次提高穩定性
    
    status_msg.info(f"🔍 正在初步掃描全市場市值... (目標 > ${min_cap_m}M)")
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_msg.text(f"📡 掃描進度: {i}/{len(symbols)} 隻股票")
        
        t = Ticker(batch)
        try:
            price_data = t.price
            for sym in batch:
                if isinstance(price_data, dict) and sym in price_data:
                    # 獲取市值，若無數據則給 0
                    m_cap = price_data[sym].get('marketCap', 0)
                    if isinstance(m_cap, (int, float)) and (m_cap / 1_000_000) >= min_cap_m:
                        filtered_list.append(sym)
        except:
            continue
            
    status_msg.success(f"✅ 篩選完成！找到 {len(filtered_list)} 隻優質股票。")
    return filtered_list

def get_sectors_safely(tickers, status_msg):
    sector_map = {}
    batch_size = 40 
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        status_msg.text(f"🏢 正在獲取行業分類... ({i}/{len(tickers)})")
        t = Ticker(batch)
        try:
            profiles = t.asset_profile
            for s in batch:
                if isinstance(profiles, dict) and s in profiles:
                    # 確保 profiles[s] 也是字典
                    p_info = profiles[s]
                    if isinstance(p_info, dict):
                        sector_map[s] = p_info.get('sector', 'Unknown')
                    else:
                        sector_map[s] = 'Unknown'
        except: pass
        time.sleep(1) 
    return sector_map

# --- UI 側邊欄 ---
with st.sidebar:
    st.header("⚙️ 篩選設定")
    m_cap_threshold = st.number_input("最低市值 (百萬 USD)", value=500, step=100)
    max_analyze = st.slider("深度分析股票數", 100, 1000, 400)

if st.button("🚀 啟動掃描"):
    start_time = time.time()
    
    all_symbols = get_all_raw_symbols()
    # 這裡限制搜尋範圍，避免 Streamlit 逾時，先搜前 3000 隻
    qualified_symbols = filter_by_market_cap(all_symbols[:3000], m_cap_threshold)
    
    if qualified_symbols:
        status = st.empty()
        status.info("🧮 正在運算動能指標...")
        
        # 只取前 max_analyze 隻進行歷史數據抓取
        t_final = Ticker(qualified_symbols[:max_analyze], asynchronous=True)
        h = t_final.history(period="1y", interval="1d")
        
        if not h.empty and 'close' in h.columns:
            close_prices = h['close'].unstack(level=0).ffill()
            
            # 計算回報率
            ret_now = (close_prices.iloc[-1] / close_prices.iloc[0]) - 1
            ret_3d = (close_prices.iloc[-4] / close_prices.iloc[0]) - 1
            
            # 建立 DataFrame 並處理空值
            df = pd.DataFrame({
                'Symbol': close_prices.columns,
                'Price': close_prices.iloc[-1],
                'RS_Now_Raw': ret_now,
                'RS_3D_Raw': ret_3d
            })
            
            # 【重要修復】：先 fillna 再 astype(int)
            df['RS_Now'] = (df['RS_Now_Raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
            df['RS_3D'] = (df['RS_3D_Raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
            df['RS_Change'] = df['RS_Now'] - df['RS_3D']
            
            # 獲取行業
            top_for_sector = df.sort_values(by='RS_Now', ascending=False).head(300)['Symbol'].tolist()
            s_map = get_sectors_safely(top_for_sector, status)
            df['Sector'] = df['Symbol'].map(s_map).fillna('Others')
            
            # 矩陣排名運算
            valid = df[~df['Sector'].isin(['Others', 'Unknown'])].copy()
            if not valid.empty:
                sec_avg = valid.groupby('Sector')['RS_Now'].mean()
                df['In_Sec_Rank_Now'] = valid.groupby('Sector')['RS_Now'].rank(ascending=False)
                df['In_Sec_Rank_3D'] = valid.groupby('Sector')['RS_3D'].rank(ascending=False)
                
                # 【重要修復】：處理排名變動的空值
                df['In_Sec_Rank_Chg'] = (df['In_Sec_Rank_3D'] - df['In_Sec_Rank_Now']).fillna(-999)
                df['Is_Leader'] = df.apply(lambda r: "👑" if r['Sector'] in sec_avg and r['RS_Now'] > sec_avg[r['Sector']] else "×", axis=1)

            # 整理輸出
            final = df.sort_values(by='In_Sec_Rank_Chg', ascending=False)
            
            display_df = final[['Symbol', 'In_Sec_Rank_Chg', 'Is_Leader', 'Sector', 'RS_Now', 'RS_Change', 'Price']].copy()
            
            # 美化符號格式化
            def arrow(v):
                if v == -999 or pd.isna(v): return "N/A"
                v_int = int(v)
                return f"▲ {v_int}" if v_int > 0 else (f"▼ {abs(v_int)}" if v_int < 0 else "0")
            
            display_df['In_Sec_Rank_Chg'] = display_df['In_Sec_Rank_Chg'].apply(arrow)
            display_df['RS_Change'] = display_df['RS_Change'].apply(arrow)
            
            st.dataframe(display_df, use_container_width=True, height=600)
            st.success(f"⚡ 完成！找到 {len(qualified_symbols)} 隻符合市值要求的股票。")
        else:
            st.error("無法獲取歷史股價數據。")
