import streamlit as st
from yahooquery import Ticker
import pandas as pd
import numpy as np
import time

st.set_page_config(page_title="US Market Full Scanner", layout="wide")
st.title("🦅 全美股 3,000 隻動能矩陣終端")

# 獲取美股代碼（這裡建議自備或從穩定來源獲取，例如市值前 3000 的名單）
@st.cache_data(ttl=86400)
def get_full_market_list():
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        tickers = pd.read_csv(url, header=None)[0].dropna().astype(str).tolist()
        return [t.strip().upper() for t in tickers if len(t) <= 4][:3000] # 取前 3000 隻
    except:
        return ["AAPL", "NVDA", "MSFT", "TSLA", "AMD"]

def run_full_scan(ticker_list):
    progress_bar = st.progress(0)
    status_msg = st.empty()
    
    batch_size = 100
    all_history = []
    all_profiles = {}
    
    # --- 第一階段：批次獲取數據 ---
    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i:i+batch_size]
        status_msg.text(f"📡 正在下載批次 {i//batch_size + 1}: {batch[0]}... ({i}/{len(ticker_list)})")
        
        t = Ticker(batch, asynchronous=True)
        try:
            # 獲取歷史數據
            h = t.history(period="1y", interval="1d")
            if not h.empty:
                all_history.append(h)
            
            # 獲取行業資料
            prof = t.summary_profile
            if isinstance(prof, dict):
                all_profiles.update(prof)
        except:
            continue
        
        progress_bar.progress(min((i + batch_size) / len(ticker_list), 1.0))

    if not all_history:
        st.error("無法獲取任何歷史數據。")
        return pd.DataFrame()

    # --- 第二階段：矩陣運算 ---
    status_msg.text("🧮 正在計算全市場 RS 排名與 7 大指標...")
    full_h = pd.concat(all_history)
    close_prices = full_h['close'].unstack(level=0).ffill()
    
    # 計算漲幅與動能
    ret_today = (close_prices.iloc[-1] / close_prices.iloc[-126]) - 1
    ret_3d = (close_prices.iloc[-4] / close_prices.iloc[-129]) - 1
    
    # 3日跌幅 (計算最高點到最低點)
    recent_3d = close_prices.iloc[-4:]
    drop_3d = (recent_3d.min() - recent_3d.max()) / recent_3d.max()

    # 建立主表
    df = pd.DataFrame({
        'Symbol': close_prices.columns,
        'Price': close_prices.iloc[-1].values,
        'Sector': [all_profiles.get(s, {}).get('sector', 'Unknown') for s in close_prices.columns],
        'Ret_Now': ret_today.values,
        'Ret_3d': ret_3d.values,
        'Drop_3d': drop_3d.values
    }).dropna()

    # 指標 1 & 2: 全域 RS 及 3日變化
    df['RS_Now'] = (df['Ret_Now'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_3d_Ago'] = (df['Ret_3d'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_Change'] = df['RS_Now'] - df['RS_3d_Ago']

    # 指標 3: 3日緊湊度
    df['Tightness'] = df['Drop_3d'].apply(lambda x: "✅" if x >= -0.05 else "❌")

    # 行業聚合 (指標 5 & 7)
    sec_rank = df.groupby('Sector')['RS_Now'].mean().rank(pct=True) * 98 + 1
    sec_rank_3d = df.groupby('Sector')['RS_3d_Ago'].mean().rank(pct=True) * 98 + 1
    
    df['Sector_Rank'] = df['Sector'].map(sec_rank).fillna(0).astype(int)
    df['Sector_Rank_Chg'] = df['Sector_Rank'] - df['Sector'].map(sec_rank_3d).fillna(0).astype(int)

    # 指標 4 & 6: 行業領先與排名變化
    df['Sector_Avg_RS'] = df['Sector'].map(df.groupby('Sector')['RS_Now'].mean())
    df['Leader'] = df.apply(lambda r: "👑" if r['RS_Now'] > r['Sector_Avg_RS'] else "×", axis=1)
    
    # 行內排名變化
    df['In_Sec_Rank_Now'] = df.groupby('Sector')['RS_Now'].rank(ascending=False)
    df['In_Sec_Rank_3d'] = df.groupby('Sector')['RS_3d_Ago'].rank(ascending=False)
    df['In_Sec_Rank_Chg'] = df['In_Sec_Rank_3d'] - df['In_Sec_Rank_Now']

    status_msg.text("✅ 全美股掃描完成！")
    return df.sort_values(by='RS_Now', ascending=False)

# UI 觸發
if st.button("🚀 開始 3,000 隻全市場深度掃描"):
    tickers = get_full_market_list()
    results = run_full_scan(tickers)
    st.dataframe(results)
