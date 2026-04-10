import streamlit as st
from yahooquery import Ticker
import pandas as pd
import time

st.set_page_config(page_title="US Market Cap Screener", layout="wide")
st.title("🦅 美股動能矩陣 (市值 > $500M 專業版)")
st.caption("自動過濾 8,000+ 股票 | 數據源：Yahoo Finance")

# --- 1. 獲取原始 8000 隻名單 ---
@st.cache_data(ttl=86400)
def get_all_raw_symbols():
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        # 獲取全市場約 8000+ 代碼
        tickers = pd.read_csv(url, header=None)[0].dropna().astype(str).tolist()
        return [t.strip().upper() for t in tickers if len(t) <= 4]
    except:
        return ["AAPL", "NVDA", "TSLA", "MSFT", "AMD", "META", "GOOGL", "AMZN"]

# --- 2. 市值篩選器 (核心邏輯) ---
def filter_by_market_cap(symbols, min_cap_m):
    status_msg = st.empty()
    filtered_list = []
    batch_size = 500 # 每次查詢 500 隻，效率最高
    
    status_msg.info(f"🔍 正在初步掃描全市場市值... (篩選 > ${min_cap_m}M)")
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_msg.text(f"📡 掃描進度: {i}/{len(symbols)} 隻股票")
        
        t = Ticker(batch)
        try:
            # 獲取價格與市值資料 (price 接口比較快，不易被封)
            price_data = t.price
            for sym in batch:
                if isinstance(price_data, dict) and sym in price_data:
                    m_cap = price_data[sym].get('marketCap', 0)
                    # 市值換算成 M (百萬)
                    if m_cap and (m_cap / 1_000_000) >= min_cap_m:
                        filtered_list.append(sym)
        except:
            continue
            
    status_msg.success(f"✅ 篩選完成！從 {len(symbols)} 隻中找到 {len(filtered_list)} 隻符合市值要求的股票。")
    return filtered_list

# --- 3. 獲取行業資料 (防 Unknown 修復版) ---
def get_sectors_safely(tickers, status_msg):
    sector_map = {}
    batch_size = 40 
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        status_msg.text(f"🏢 正在獲取強勢股行業... ({i}/{len(tickers)})")
        t = Ticker(batch)
        try:
            profiles = t.asset_profile
            for s in batch:
                if isinstance(profiles, dict) and s in profiles:
                    sector_map[s] = profiles[s].get('sector', 'Unknown') if isinstance(profiles[s], dict) else 'Unknown'
        except: pass
        time.sleep(1) # 保護 IP
    return sector_map

# --- UI 側邊欄 ---
with st.sidebar:
    st.header("⚙️ 篩選設定")
    m_cap_threshold = st.number_input("最低市值 (百萬 USD)", value=500, step=100)
    max_analyze = st.slider("最終分析股票數", 100, 1000, 500, help="從符合市值的股票中，按 RS 選出前 N 隻進行深度分析")

# --- 主程式 ---
if st.button("🚀 開始全市場深度篩選"):
    start_time = time.time()
    
    # 第一步：拿到全名單
    all_symbols = get_all_raw_symbols()
    
    # 第二步：市值篩選
    qualified_symbols = filter_by_market_cap(all_symbols, m_cap_threshold)
    
    if qualified_symbols:
        # 第三步：獲取歷史股價計算 RS (只對篩選後的股票)
        status = st.empty()
        status.info("🧮 正在獲取股價並計算動能...")
        
        t_final = Ticker(qualified_symbols[:max_analyze], asynchronous=True)
        h = t_final.history(period="1y", interval="1d")
        
        if not h.empty:
            close_prices = h['close'].unstack(level=0).ffill()
            
            # 計算 1Y RS
            ret_now = (close_prices.iloc[-1] / close_prices.iloc[0]) - 1
            ret_3d = (close_prices.iloc[-4] / close_prices.iloc[0]) - 1
            
            df = pd.DataFrame({
                'Symbol': close_prices.columns,
                'Price': close_prices.iloc[-1],
                'RS_Now': (ret_now.rank(pct=True) * 98 + 1).astype(int),
                'RS_3D': (ret_3d.rank(pct=True) * 98 + 1).astype(int)
            })
            df['RS_Change'] = df['RS_Now'] - df['RS_3D']
            
            # 第四步：安全獲取行業 (解決 Unknown)
            top_for_sector = df.sort_values(by='RS_Now', ascending=False).head(400)['Symbol'].tolist()
            s_map = get_sectors_safely(top_for_sector, status)
            df['Sector'] = df['Symbol'].map(s_map).fillna('Others')
            
            # 第五步：矩陣排名
            valid = df[df['Sector'] != 'Others'].copy()
            if not valid.empty:
                sec_avg = valid.groupby('Sector')['RS_Now'].mean()
                df['In_Sec_Rank_Now'] = valid.groupby('Sector')['RS_Now'].rank(ascending=False)
                df['In_Sec_Rank_3D'] = valid.groupby('Sector')['RS_3D'].rank(ascending=False)
                df['In_Sec_Rank_Chg'] = (df['In_Sec_Rank_3D'] - df['In_Sec_Rank_Now']).fillna(-999)
                df['Is_Leader'] = df.apply(lambda r: "👑" if r['Sector'] in sec_avg and r['RS_Now'] > sec_avg[r['Sector']] else "×", axis=1)

            # 最終顯示
            final = df.sort_values(by='In_Sec_Rank_Chg', ascending=False).head(max_analyze)
            
            st.divider()
            st.subheader(f"🎯 符合市值 > ${m_cap_threshold}M 的強勢名單")
            
            # 格式美化
            def arrow(v):
                if v == -999: return "N/A"
                return f"▲ {int(v)}" if v > 0 else (f"▼ {int(abs(v))}" if v < 0 else "0")
            
            display_df = final[['Symbol', 'In_Sec_Rank_Chg', 'Is_Leader', 'Sector', 'RS_Now', 'RS_Change', 'Price']].copy()
            display_df['In_Sec_Rank_Chg'] = display_df['In_Sec_Rank_Chg'].apply(arrow)
            display_df['RS_Change'] = display_df['RS_Change'].apply(arrow)
            
            st.dataframe(display_df, use_container_width=True, height=600)
            st.success(f"完成！耗時 {round(time.time()-start_time, 1)} 秒。")
