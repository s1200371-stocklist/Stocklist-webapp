import streamlit as st
from yahooquery import Ticker
import pandas as pd
import time
import numpy as np

st.set_page_config(page_title="US Full Market Scanner", layout="wide")
st.title("🦅 全美股動能矩陣 (8,000+ 股票全掃描)")
st.warning("⚠️ 掃描全市場預計耗時 5-10 分鐘，請保持網頁開啟不要重新整理。")

# --- 1. 獲取全市場 8,000+ 代碼 ---
@st.cache_data(ttl=86400)
def get_all_raw_symbols():
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        # 獲取全市場代碼
        tickers = pd.read_csv(url, header=None)[0].dropna().astype(str).tolist()
        # 過濾掉長度過長或怪異的代碼，只保留美股主板常見代碼
        return [t.strip().upper() for t in tickers if t.isalpha() and len(t) <= 5]
    except:
        return ["AAPL", "NVDA", "TSLA", "MSFT", "AMD", "META", "GOOGL", "AMZN"]

# --- 2. 大規模市值篩選 (500M 門檻) ---
def filter_full_market_cap(symbols, min_cap_m):
    status_msg = st.empty()
    filtered_list = []
    batch_size = 500 # 大批次處理 8000 隻
    
    total = len(symbols)
    status_msg.info(f"🔍 正在掃描全市場 {total} 隻股票市值... (目標 > ${min_cap_m}M)")
    
    progress_cap = st.progress(0)
    
    for i in range(0, total, batch_size):
        batch = symbols[i:i+batch_size]
        percent = min(i / total, 1.0)
        progress_cap.progress(percent)
        status_msg.text(f"📡 目前進度: {i} / {total} 隻")
        
        t = Ticker(batch)
        try:
            price_data = t.price
            for sym in batch:
                if isinstance(price_data, dict) and sym in price_data:
                    p_info = price_data[sym]
                    if isinstance(p_info, dict):
                        m_cap = p_info.get('marketCap', 0)
                        # 進行市值校驗與過濾
                        if isinstance(m_cap, (int, float)) and (m_cap / 1_000_000) >= min_cap_m:
                            filtered_list.append(sym)
        except:
            continue
            
    progress_cap.empty()
    status_msg.success(f"✅ 篩選完成！從 {total} 隻中篩選出 {len(filtered_list)} 隻市值合格股票。")
    return filtered_list

# --- 3. 安全抓取行業 (防 Unknown) ---
def get_sectors_safely(tickers, status_msg):
    sector_map = {}
    batch_size = 40 
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        status_msg.text(f"🏢 正在同步強勢股行業... ({i}/{len(tickers)})")
        t = Ticker(batch)
        try:
            profiles = t.asset_profile
            for s in batch:
                if isinstance(profiles, dict) and s in profiles:
                    p_info = profiles[s]
                    if isinstance(p_info, dict):
                        sector_map[s] = p_info.get('sector', 'Unknown')
                    else:
                        sector_map[s] = 'Unknown'
        except: pass
        time.sleep(1.2) # 針對全市場掃描，稍微延長等待避免封鎖
    return sector_map

# --- UI 設定 ---
with st.sidebar:
    st.header("⚙️ 全市場設定")
    m_cap_threshold = st.number_input("最低市值門檻 (百萬 USD)", value=500, step=100)
    max_analyze = st.slider("分析強勢股上限", 100, 1500, 600, help="對市值合格且 RS 靠前的股票進行行業分析")

if st.button("🚀 啟動全美股 8,000+ 深度掃描"):
    start_time = time.time()
    
    # 第一階段：獲取全清單
    all_symbols = get_all_raw_symbols()
    
    # 第二階段：大規模市值過濾 (核心：不再有 [:3000])
    qualified_symbols = filter_full_market_cap(all_symbols, m_cap_threshold)
    
    if qualified_symbols:
        status = st.empty()
        status.info(f"🧮 正在抓取 {len(qualified_symbols)} 隻股票的歷史報價...")
        
        # 第三階段：批量抓取股價 (分批抓取防止 Ticker 過載)
        all_h = []
        hist_batch = 200
        for i in range(0, len(qualified_symbols), hist_batch):
            batch = qualified_symbols[i:i+hist_batch]
            t_hist = Ticker(batch, asynchronous=True)
            try:
                h = t_hist.history(period="1y", interval="1d")
                if not h.empty: all_h.append(h)
            except: continue
        
        if all_h:
            full_hist = pd.concat(all_h)
            close_prices = full_hist['close'].unstack(level=0).ffill()
            
            # 計算 RS
            ret_now = (close_prices.iloc[-1] / close_prices.iloc[0]) - 1
            ret_3d = (close_prices.iloc[-4] / close_prices.iloc[0]) - 1
            
            df = pd.DataFrame({
                'Symbol': close_prices.columns,
                'Price': close_prices.iloc[-1],
                'RS_Now_Raw': ret_now,
                'RS_3D_Raw': ret_3d
            })
            
            # 防空值轉型
            df['RS_Now'] = (df['RS_Now_Raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
            df['RS_3D'] = (df['RS_3D_Raw'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
            df['RS_Change'] = df['RS_Now'] - df['RS_3D']
            
            # 第四階段：獲取行業資料
            # 即使掃描 8000 隻，我們也只對「合格股票中 RS 較高」的部分抓行業，這才有效率
            top_for_sector = df.sort_values(by='RS_Now', ascending=False).head(max_analyze)['Symbol'].tolist()
            s_map = get_sectors_safely(top_for_sector, status)
            df['Sector'] = df['Symbol'].map(s_map).fillna('Others')
            
            # 第五階段：行業矩陣分析
            valid = df[~df['Sector'].isin(['Others', 'Unknown'])].copy()
            if not valid.empty:
                sec_avg = valid.groupby('Sector')['RS_Now'].mean()
                df['In_Sec_Rank_Now'] = valid.groupby('Sector')['RS_Now'].rank(ascending=False)
                df['In_Sec_Rank_3D'] = valid.groupby('Sector')['RS_3D'].rank(ascending=False)
                df['In_Sec_Rank_Chg'] = (df['In_Sec_Rank_3D'] - df['In_Sec_Rank_Now']).fillna(-999)
                df['Is_Leader'] = df.apply(lambda r: "👑" if r['Sector'] in sec_avg and r['RS_Now'] > sec_avg[r['Sector']] else "×", axis=1)

            # 最終排序與顯示
            final = df[df['In_Sec_Rank_Chg'] != -999].sort_values(by='In_Sec_Rank_Chg', ascending=False)
            
            st.divider()
            st.subheader(f"🎯 掃描結果 (符合市值門檻: {len(qualified_symbols)} 隻)")
            
            def arrow(v):
                if v == -999 or pd.isna(v): return "N/A"
                v_int = int(v)
                return f"▲ {v_int}" if v_int > 0 else (f"▼ {abs(v_int)}" if v_int < 0 else "0")
            
            display_df = final[['Symbol', 'In_Sec_Rank_Chg', 'Is_Leader', 'Sector', 'RS_Now', 'RS_Change', 'Price']].copy()
            display_df['In_Sec_Rank_Chg'] = display_df['In_Sec_Rank_Chg'].apply(arrow)
            display_df['RS_Change'] = display_df['RS_Change'].apply(arrow)
            
            st.dataframe(display_df, use_container_width=True, height=700)
            st.success(f"⚡ 全市場掃描完成！總耗時: {round((time.time()-start_time)/60, 2)} 分鐘。")
        else:
            st.error("歷史數據獲取失敗。")
    else:
        st.warning("沒有找到符合市值門檻的股票。")
