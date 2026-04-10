import streamlit as st
from yahooquery import Ticker
import pandas as pd
import numpy as np
import time

st.set_page_config(page_title="US Market Rank Momentum", layout="wide")
st.title("🦅 全美股 7 維度動能掃描器")
st.caption("排序邏輯：優先顯示在所屬行業中「名次上升最多」的黑馬股")

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
    
    # 階段 1: 批量獲取股價
    batch_size = 150
    all_history = []
    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i:i+batch_size]
        status_msg.text(f"📡 階段 1/3: 正在抓取股價數據... ({i}/{len(ticker_list)})")
        t = Ticker(batch, asynchronous=True)
        try:
            h = t.history(period="1y", interval="1d")
            if not h.empty: all_history.append(h)
        except: continue
        progress_bar.progress(min((i + batch_size) / len(ticker_list) * 0.6, 0.6))

    if not all_history:
        st.error("❌ 無法獲取股價數據")
        return pd.DataFrame()

    # 階段 2: 計算全域 RS 排名
    status_msg.text("🧮 階段 2/3: 正在運算全市場 RS 排名...")
    full_h = pd.concat(all_history)
    close_prices = full_h['close'].unstack(level=0).ffill()
    
    ret_now = (close_prices.iloc[-1] / close_prices.iloc[-126]) - 1
    ret_3d_ago = (close_prices.iloc[-4] / close_prices.iloc[-129]) - 1
    recent_3d = close_prices.iloc[-4:]
    drop_3d = (recent_3d.min() - recent_3d.max()) / recent_3d.max()

    df = pd.DataFrame({
        'Symbol': close_prices.columns,
        'Price': close_prices.iloc[-1].values,
        'Ret_Now': ret_now.values,
        'Ret_3D': ret_3d_ago.values,
        'Drop_3D': drop_3d.values
    }).dropna()

    df['RS_Now'] = (df['Ret_Now'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_3D_Ago'] = (df['Ret_3D'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_Change'] = df['RS_Now'] - df['RS_3D_Ago']
    df['Tightness_3D'] = df['Drop_3D'].apply(lambda x: "✅" if x >= -0.05 else "❌")

    # 階段 3: 行業分析 (只針對前 400 名，確保覆蓋率)
    top_400 = df.sort_values(by='RS_Now', ascending=False).head(400)['Symbol'].tolist()
    status_msg.text("🏢 階段 3/3: 正在分析強勢股行業排名變動...")
    t_top = Ticker(top_400, asynchronous=True)
    profiles = t_top.asset_profile
    
    sector_map = {s: profiles.get(s, {}).get('sector', 'Unknown') if isinstance(profiles.get(s), dict) else 'Unknown' for s in top_400}
    df['Sector'] = df['Symbol'].map(sector_map).fillna('Others')
    
    # 進行行業內部排名比較
    valid_sec_df = df[df['Sector'] != 'Others'].copy()
    if not valid_sec_df.empty:
        # 行業大市排名
        sec_group_now = valid_sec_df.groupby('Sector')['RS_Now'].mean()
        sec_group_3d = valid_sec_df.groupby('Sector')['RS_3D_Ago'].mean()
        sec_rank_now = (sec_group_now.rank(pct=True) * 98 + 1).astype(int)
        sec_rank_3d = (sec_group_3d.rank(pct=True) * 98 + 1).astype(int)
        
        df['Sec_Mkt_Rank'] = df['Sector'].map(sec_rank_now).fillna(0).astype(int)
        df['Sec_Rank_Chg'] = df['Sec_Mkt_Rank'] - df['Sector'].map(sec_rank_3d).fillna(0).astype(int)
        
        # 股票在行內排名變動 (計算方式：舊名次 - 新名次)
        # 數值越大代表進步越多 (例如從第 10 名變成第 2 名，+8)
        df['In_Sec_Rank_Now'] = valid_sec_df.groupby('Sector')['RS_Now'].rank(ascending=False)
        df['In_Sec_Rank_3D'] = valid_sec_df.groupby('Sector')['RS_3D_Ago'].rank(ascending=False)
        df['In_Sec_Rank_Chg'] = (df['In_Sec_Rank_3D'] - df['In_Sec_Rank_Now']).fillna(-99) # 沒數據的墊底
        
        df['Is_Leader'] = df.apply(lambda r: "👑 是" if r['RS_Now'] > df[df['Sector'] == r['Sector']]['RS_Now'].mean() else "×", axis=1)

    progress_bar.progress(1.0)
    status_msg.text("✅ 掃描完成！")
    return df

# UI 介面
with st.sidebar:
    scan_limit = st.slider("掃描數量", 100, 3000, 1000)

if st.button("🔥 啟動深度矩陣分析"):
    start_time = time.time()
    result_df = run_momentum_scan(get_full_tickers()[:scan_limit])
    
    if not result_df.empty:
        # 1. 核心排序：行內變動排名由大到小
        final_df = result_df.sort_values(by='In_Sec_Rank_Chg', ascending=False)
        
        # 2. 準備顯示用的欄位
        display_df = final_df[[
            'Symbol', 'In_Sec_Rank_Chg', 'Is_Leader', 'Sector', 
            'RS_Now', 'RS_Change', 'Tightness_3D', 'Sec_Mkt_Rank', 'Sec_Rank_Chg', 'Price'
        ]].copy()
        
        # 3. 數值美化 (加上箭頭)
        def arrow(v):
            if v == -99: return "N/A"
            return f"▲ {int(v)}" if v > 0 else (f"▼ {int(abs(v))}" if v < 0 else "0")

        for col in ['In_Sec_Rank_Chg', 'RS_Change', 'Sec_Rank_Chg']:
            display_df[col] = display_df[col].apply(arrow)
            
        st.dataframe(display_df, use_container_width=True, height=600)
        st.success(f"完成！耗時 {round(time.time()-start_time, 1)} 秒。頂部股票為行業內名次上升最快者。")
