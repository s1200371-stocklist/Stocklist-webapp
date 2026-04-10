import streamlit as st
from yahooquery import Ticker
import pandas as pd
import numpy as np
import time

st.set_page_config(page_title="RS Matrix Terminal", layout="wide")
st.title("🦅 全美股 7 維度動能矩陣 — 終極穩定版")

@st.cache_data(ttl=86400)
def get_full_tickers():
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        tickers = pd.read_csv(url, header=None)[0].dropna().astype(str).tolist()
        return [t.strip().upper() for t in tickers if len(t) <= 4][:3000]
    except:
        return ["AAPL", "NVDA", "TSLA", "MSFT", "AMD", "META", "GOOGL", "AMZN"]

def run_momentum_scan(ticker_list):
    progress_bar = st.progress(0)
    status_msg = st.empty()
    
    # --- 階段 1: 批量獲取股價 ---
    batch_size = 150
    all_history = []
    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i:i+batch_size]
        status_msg.text(f"📡 階段 1/3: 抓取歷史數據 ({i}/{len(ticker_list)})")
        t = Ticker(batch, asynchronous=True)
        try:
            h = t.history(period="1y", interval="1d")
            if not h.empty: all_history.append(h)
        except: continue
        progress_bar.progress(min((i + batch_size) / len(ticker_list) * 0.6, 0.6))

    if not all_history:
        st.error("❌ 獲取數據失敗")
        return pd.DataFrame()

    # --- 階段 2: 核心動能運算 ---
    status_msg.text("🧮 階段 2/3: 運算全球相對強度排名...")
    full_h = pd.concat(all_history)
    close_prices = full_h['close'].unstack(level=0).ffill()
    
    # 時間點定義：今日(-1), 3日前(-4), 半年前(-126), 3日前視角的半年前(-129)
    ret_now = (close_prices.iloc[-1] / close_prices.iloc[-126]) - 1
    ret_3d_ago = (close_prices.iloc[-4] / close_prices.iloc[-129]) - 1
    
    # 指標 3: 3日價格緊湊度 (Max to Min Drop)
    recent_3d = close_prices.iloc[-4:]
    drop_3d = (recent_3d.min() - recent_3d.max()) / recent_3d.max()

    df = pd.DataFrame({
        'Symbol': close_prices.columns,
        'Price': close_prices.iloc[-1].values,
        'Ret_Now': ret_now.values,
        'Ret_3D': ret_3d_ago.values,
        'Drop_3D': drop_3d.values
    }).dropna()

    # 指標 1 & 2: 全場 RS 排名 (1-99)
    df['RS_Now'] = (df['Ret_Now'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_3D_Ago'] = (df['Ret_3D'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_Change'] = df['RS_Now'] - df['RS_3D_Ago']
    df['Tightness_3D'] = df['Drop_3D'].apply(lambda x: "✅" if x >= -0.05 else "❌")

    # --- 階段 3: 行業分析 (分批穩定抓取) ---
    top_n = 400
    top_tickers = df.sort_values(by='RS_Now', ascending=False).head(top_n)['Symbol'].tolist()
    sector_map = {}
    profile_batch = 50 
    
    for i in range(0, len(top_tickers), profile_batch):
        batch = top_tickers[i:i+profile_batch]
        status_msg.text(f"🏢 階段 3/3: 抓取強勢股行業資料 ({i}/{top_n})")
        try:
            t_prof = Ticker(batch)
            profiles = t_prof.asset_profile
            for s in batch:
                p = profiles.get(s, {})
                sector_map[s] = p.get('sector', 'Unknown') if isinstance(p, dict) else 'Unknown'
            time.sleep(0.6) # 禮貌延遲
        except: continue

    df['Sector'] = df['Symbol'].map(sector_map).fillna('Others')
    
    # 只針對有行業資料的計算內部指標
    valid_df = df[df['Sector'] != 'Others'].copy()
    if not valid_df.empty:
        # 指標 5 & 7: 行業大市表現
        sec_now = valid_df.groupby('Sector')['RS_Now'].mean()
        sec_3d = valid_df.groupby('Sector')['RS_3D_Ago'].mean()
        
        sec_rank_now = (sec_now.rank(pct=True) * 98 + 1).astype(int)
        sec_rank_3d = (sec_3d.rank(pct=True) * 98 + 1).astype(int)
        
        df['Sec_Mkt_Rank'] = df['Sector'].map(sec_rank_now).fillna(0).astype(int)
        df['Sec_Rank_Chg'] = df['Sec_Mkt_Rank'] - df['Sector'].map(sec_rank_3d).fillna(0).astype(int)
        
        # 指標 4: 行業領先地位
        df['Sec_Avg_RS'] = df['Sector'].map(sec_now)
        df['Is_Leader'] = df.apply(lambda r: "👑 是" if r['RS_Now'] > r['Sec_Avg_RS'] else "×", axis=1)
        
        # 指標 6: 行內排名變動 (舊名次 - 新名次)
        df['In_Sec_Rank_Now'] = valid_df.groupby('Sector')['RS_Now'].rank(ascending=False)
        df['In_Sec_Rank_3D'] = valid_df.groupby('Sector')['RS_3D_Ago'].rank(ascending=False)
        df['In_Sec_Rank_Chg'] = (df['In_Sec_Rank_3D'] - df['In_Sec_Rank_Now']).fillna(-999)

    progress_bar.progress(1.0)
    status_msg.text("✅ 全美股掃描成功完成！")
    return df

# --- UI 展示 ---
with st.sidebar:
    scan_limit = st.slider("掃描數量 (建議 1000)", 100, 3000, 1000)

if st.button("🔥 開始深度掃描"):
    start = time.time()
    res = run_momentum_scan(get_full_tickers()[:scan_limit])
    
    if not res.empty:
        # 關鍵排序：行內超車次數最多的排最前
        res = res.sort_values(by='In_Sec_Rank_Chg', ascending=False)
        
        # 整理與美化
        final = res[['Symbol', 'In_Sec_Rank_Chg', 'Is_Leader', 'Sector', 'RS_Now', 'RS_Change', 'Tightness_3D', 'Sec_Mkt_Rank', 'Sec_Rank_Chg', 'Price']].copy()
        
        def fmt(v):
            if v == -999: return "N/A"
            return f"▲ {int(v)}" if v > 0 else (f"▼ {int(abs(v))}" if v < 0 else "0")

        for c in ['In_Sec_Rank_Chg', 'RS_Change', 'Sec_Rank_Chg']:
            final[c] = final[c].apply(fmt)
            
        st.dataframe(final, use_container_width=True, height=600)
        st.success(f"耗時: {round(time.time()-start, 1)} 秒。已依行業內超車強度排序。")
