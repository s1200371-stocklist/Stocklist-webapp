import streamlit as st
from yahooquery import Ticker
import pandas as pd
import time

st.set_page_config(page_title="Ultimate Stock Matrix", layout="wide")

st.title("🚀 美股全市場矩陣 (行業資料強化版)")
st.markdown("""
* **掃描範圍**：全美股 8,000+ 股票
* **篩選條件**：市值 > $500M
* **優化點**：擴大行業抓取範圍至前 1,500 隻，並加入防封鎖延遲。
""")

@st.cache_data(ttl=86400)
def get_symbols():
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        df = pd.read_csv(url, header=None)
        # 只取純英文字母且長度合理的代碼
        full_list = df[0].dropna().astype(str).tolist()
        return [s.strip().upper() for s in full_list if s.isalpha() and len(s) <= 5]
    except:
        return ["AAPL", "NVDA", "TSLA", "MSFT", "AMD"]

def run_turbo_scan():
    all_symbols = get_symbols()
    qualified = []
    
    with st.status("正在執行全市場掃描...", expanded=True) as status:
        
        # --- 階段 1: 市值篩選 ---
        status.write("🔍 階段 1: 正在篩選全市場市值 (門檻: $500M)...")
        batch_size = 500
        for i in range(0, len(all_symbols), batch_size):
            batch = all_symbols[i:i+batch_size]
            try:
                p_data = Ticker(batch).price
                for s in batch:
                    if isinstance(p_data, dict) and s in p_data:
                        cap = p_data[s].get('marketCap', 0)
                        if isinstance(cap, (int, float)) and cap >= 500_000_000:
                            qualified.append(s)
                status.update(label=f"已掃描 {i+len(batch)} 隻... 發現合格股: {len(qualified)} 隻")
            except: continue
        
        if not qualified:
            status.update(label="❌ 未找到符合市值的股票", state="error")
            return None

        # --- 階段 2: 動能運算 ---
        status.write(f"📈 階段 2: 正在抓取 {len(qualified)} 隻股票的歷史價格...")
        all_h = []
        for i in range(0, len(qualified), 200):
            try:
                h = Ticker(qualified[i:i+200], asynchronous=True).history(period="1y", interval="1d")
                if not h.empty: all_h.append(h)
            except: continue
        
        if not all_h: return None
        df_h = pd.concat(all_h)
        prices = df_h['close'].unstack(level=0).ffill()
        
        ret_now = (prices.iloc[-1] / prices.iloc[0]) - 1
        ret_3d = (prices.iloc[-4] / prices.iloc[0]) - 1
        
        res_df = pd.DataFrame({
            'Price': prices.iloc[-1],
            'RS_Now': (ret_now.rank(pct=True) * 98 + 1).fillna(0).astype(int),
            'RS_3D': (ret_3d.rank(pct=True) * 98 + 1).fillna(0).astype(int)
        })
        res_df['RS_Change'] = res_df['RS_Now'] - res_df['RS_3D']

        # --- 階段 3: 行業同步 (擴大掃描範圍) ---
        # 這裡設定 limit=1500，這幾乎能覆蓋大部分市值 > 500M 且有成交量的股票
        limit = 1500
        status.write(f"🏢 階段 3: 正在深度抓取前 {limit} 隻強勢股的行業資料...")
        top_list = res_df.sort_values('RS_Now', ascending=False).head(limit).index.tolist()
        
        sector_map = {}
        sec_batch = 30 # 小批次以確保穩定
        for i in range(0, len(top_list), sec_batch):
            batch = top_list[i:i+sec_batch]
            try:
                # 使用 asset_profile 獲取精確行業
                profiles = Ticker(batch).asset_profile
                for s in batch:
                    if isinstance(profiles, dict) and s in profiles:
                        s_info = profiles[s]
                        if isinstance(s_info, dict):
                            sector_map[s] = s_info.get('sector', 'Unknown')
            except: pass
            # 加入微小延遲，防止 Yahoo 判定為惡意爬蟲
            time.sleep(0.6)
            status.update(label=f"行業抓取進度: {i+len(batch)} / {len(top_list)}")
            
        res_df['Sector'] = res_df.index.map(sector_map).fillna("Outside Top 1500")
        
        # 行內排名運算
        valid = res_df[~res_df['Sector'].isin(["Outside Top 1500", "Unknown"])].copy()
        if not valid.empty:
            res_df['In_Sec_Rank_Now'] = valid.groupby('Sector')['RS_Now'].rank(ascending=False)
            res_df['In_Sec_Rank_3D'] = valid.groupby('Sector')['RS_3D'].rank(ascending=False)
            res_df['In_Sec_Rank_Chg'] = (res_df['In_Sec_Rank_3D'] - res_df['In_Sec_Rank_Now']).fillna(-999)
        
        status.update(label="✅ 全市場分析完畢！", state="complete", expanded=False)
        return res_df

if st.button("🚀 啟動深度強化掃描"):
    data = run_turbo_scan()
    if data is not None:
        st.divider()
        # 排序邏輯：行內躍升名次越高越靠前
        final = data.sort_values(['In_Sec_Rank_Chg', 'RS_Now'], ascending=[False, False])
        
        def arrow(v):
            if v == -999 or pd.isna(v): return "-"
            return f"▲ {int(v)}" if v > 0 else (f"▼ {abs(int(v))}" if v < 0 else "0")

        display = final.reset_index().rename(columns={'index': 'Symbol'})
        display['In_Sec_Rank_Chg'] = display['In_Sec_Rank_Chg'].apply(arrow)
        display['RS_Change'] = display['RS_Change'].apply(arrow)
        
        st.dataframe(display[['Symbol', 'In_Sec_Rank_Chg', 'Sector', 'RS_Now', 'RS_Change', 'Price']], 
                     use_container_width=True, height=800)
