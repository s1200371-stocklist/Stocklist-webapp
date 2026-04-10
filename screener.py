import streamlit as st
from yahooquery import Ticker
import pandas as pd
import time

st.set_page_config(page_title="Full Market Matrix", layout="wide")
st.title("🦅 全美股動能矩陣 (全量顯示版)")
st.info("此版本將顯示所有市值 > $500M 的股票。沒被抓到行業的股票將排在後方。")

@st.cache_data(ttl=86400)
def get_all_raw_symbols():
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        tickers = pd.read_csv(url, header=None)[0].dropna().astype(str).tolist()
        return [t.strip().upper() for t in tickers if t.isalpha() and len(t) <= 5]
    except:
        return ["AAPL", "NVDA", "TSLA", "MSFT"]

def filter_full_market_cap(symbols, min_cap_m):
    status_msg = st.empty()
    filtered_list = []
    batch_size = 500 
    total = len(symbols)
    progress_cap = st.progress(0)
    
    for i in range(0, total, batch_size):
        batch = symbols[i:i+batch_size]
        progress_cap.progress(min(i / total, 1.0))
        status_msg.text(f"🔍 正在篩選市值: 已掃描 {i} / {total} 隻")
        
        t = Ticker(batch)
        try:
            price_data = t.price
            for sym in batch:
                if isinstance(price_data, dict) and sym in price_data:
                    m_cap = price_data[sym].get('marketCap', 0)
                    if isinstance(m_cap, (int, float)) and (m_cap / 1_000_000) >= min_cap_m:
                        filtered_list.append(sym)
        except: continue
            
    progress_cap.empty()
    status_msg.success(f"✅ 市值篩選完成！共找到 {len(filtered_list)} 隻合格股票。")
    return filtered_list

def get_sectors_safely(tickers, status_msg, limit):
    """只對前 N 隻強勢股抓行業，其餘標註為 Pending"""
    sector_map = {}
    # 只掃描 RS 最強的前 limit 隻，避免被 Yahoo 封鎖 IP
    to_scan = tickers[:limit]
    batch_size = 40 
    
    for i in range(0, len(to_scan), batch_size):
        batch = to_scan[i:i+batch_size]
        status_msg.text(f"🏢 正在同步核心行業資料... ({i}/{len(to_scan)})")
        t = Ticker(batch)
        try:
            profiles = t.asset_profile
            for s in batch:
                if isinstance(profiles, dict) and s in profiles:
                    p = profiles[s]
                    sector_map[s] = p.get('sector', 'Unknown') if isinstance(p, dict) else 'Unknown'
        except: pass
        time.sleep(1.2)
    return sector_map

if st.button("🚀 啟動 8,000 隻全市場掃描"):
    start_time = time.time()
    all_symbols = get_all_raw_symbols()
    
    # 1. 市值過濾
    qualified_symbols = filter_full_market_cap(all_symbols, 500)
    
    if qualified_symbols:
        status = st.empty()
        # 2. 獲取所有合格股票的報價 (這步不限數量，3000 隻也能跑)
        status.info(f"🧮 正在計算 {len(qualified_symbols)} 隻股票的動能...")
        
        all_h = []
        for i in range(0, len(qualified_symbols), 200):
            t_hist = Ticker(qualified_symbols[i:i+200], asynchronous=True)
            try:
                h = t_hist.history(period="1y", interval="1d")
                if not h.empty: all_h.append(h)
            except: continue
        
        if all_h:
            full_hist = pd.concat(all_h)
            close_prices = full_hist['close'].unstack(level=0).ffill()
            
            # 3. 計算 RS 指標
            ret_now = (close_prices.iloc[-1] / close_prices.iloc[0]) - 1
            ret_3d = (close_prices.iloc[-4] / close_prices.iloc[0]) - 1
            
            df = pd.DataFrame({
                'Symbol': close_prices.columns,
                'Price': close_prices.iloc[-1],
                'RS_Now': (ret_now.rank(pct=True) * 98 + 1).fillna(0).astype(int),
                'RS_3D': (ret_3d.rank(pct=True) * 98 + 1).fillna(0).astype(int)
            })
            df['RS_Change'] = df['RS_Now'] - df['RS_3D']
            
            # 4. 行業抓取 (我們設定只抓 RS 前 500 隻的行業，其餘顯示為 'Pending')
            # 這樣既能保證速度，又能讓你看到剩下的股票
            top_tickers = df.sort_values(by='RS_Now', ascending=False)['Symbol'].tolist()
            s_map = get_sectors_safely(top_tickers, status, limit=500)
            df['Sector'] = df['Symbol'].map(s_map).fillna('Waiting Scan')
            
            # 5. 計算排名變動 (僅限有行業資料的)
            valid = df[~df['Sector'].isin(['Waiting Scan', 'Unknown'])].copy()
            if not valid.empty:
                df['In_Sec_Rank_Now'] = valid.groupby('Sector')['RS_Now'].rank(ascending=False)
                df['In_Sec_Rank_3D'] = valid.groupby('Sector')['RS_3D'].rank(ascending=False)
                df['In_Sec_Rank_Chg'] = (df['In_Sec_Rank_3D'] - df['In_Sec_Rank_Now']).fillna(-999)
            
            # --- 最終顯示 (移除 head 限制) ---
            # 排序：有行業變動的排前面，其餘按 RS 排序
            final = df.sort_values(by=['In_Sec_Rank_Chg', 'RS_Now'], ascending=[False, False])
            
            st.divider()
            st.subheader(f"🎯 全量結果清單 (共 {len(final)} 隻股票符合市值要求)")
            
            def arrow(v):
                if v == -999 or pd.isna(v): return "-"
                return f"▲ {int(v)}" if v > 0 else (f"▼ {abs(int(v))}" if v < 0 else "0")
            
            display_df = final[['Symbol', 'In_Sec_Rank_Chg', 'Sector', 'RS_Now', 'RS_Change', 'Price']].copy()
            display_df['In_Sec_Rank_Chg'] = display_df['In_Sec_Rank_Chg'].apply(arrow)
            display_df['RS_Change'] = display_df['RS_Change'].apply(arrow)
            
            st.dataframe(display_df, use_container_width=True, height=800)
            st.success(f"⚡ 完成！顯示了所有 {len(final)} 隻符合條件的股票。")
