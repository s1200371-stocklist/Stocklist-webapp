import streamlit as st
from yahooquery import Ticker
import pandas as pd
import numpy as np
import time

st.set_page_config(page_title="RS 動能與行業矩陣", layout="wide")
st.title("🦅 專業級 RS 動能與行業矩陣 (YahooQuery)")

@st.cache_data(ttl=86400)
def get_sample_tickers():
    # 測試用嘅多行業強勢股名單
    return ["AAPL", "MSFT", "NVDA", "AVGO", "META", "GOOGL", "AMZN", "TSLA", "LLY", "UNH", 
            "V", "JPM", "WMT", "MA", "PG", "JNJ", "XOM", "HD", "COST", "MRK",
            "ABBV", "CVX", "CRM", "AMD", "BAC", "PEP", "KO", "TMO", "MCD", "ADBE"]

def calculate_advanced_rs(tickers):
    st_msg = st.empty()
    st_msg.text("🚀 正在通過 YahooQuery 批量獲取數據...")
    
    # 1. 獲取行業資料
    t = Ticker(tickers, asynchronous=True)
    profiles = t.summary_profile
    sector_map = {}
    for sym in tickers:
        try:
            if isinstance(profiles, dict) and sym in profiles and isinstance(profiles[sym], dict):
                sector_map[sym] = profiles[sym].get('sector', 'Unknown')
            else:
                sector_map[sym] = 'Unknown'
        except:
            sector_map[sym] = 'Unknown'

    # 2. 獲取一年歷史數據
    st_msg.text("📊 正在下載歷史股價並運算時間矩陣...")
    hist = t.history(period="1y", interval="1d")
    
    if hist.empty:
        st.error("無法獲取歷史數據")
        return pd.DataFrame()

    # 將數據表重組為 (日期 x 股票代碼) 格式
    close_prices = hist['close'].unstack(level=0).ffill()
    
    if len(close_prices) < 130:
        st.error("歷史數據不足半年，無法計算 RS")
        return pd.DataFrame()

    # --- 核心數據點 ---
    p_today = close_prices.iloc[-1]           # 今日收市
    p_3d_ago = close_prices.iloc[-4]          # 3日前收市
    p_6m_today = close_prices.iloc[-126]      # 半年前 (以今日計)
    p_6m_3d_ago = close_prices.iloc[-129]     # 半年前 (以3日前計)

    # 過去 3 日最高價與最低價 (用來計算跌幅)
    max_3d = close_prices.iloc[-4:].max()
    min_3d = close_prices.iloc[-4:].min()
    drop_3d = (min_3d - max_3d) / max_3d

    # --- 回報率計算 ---
    ret_today = (p_today - p_6m_today) / p_6m_today
    ret_3d_ago = (p_3d_ago - p_6m_3d_ago) / p_6m_3d_ago

    # 組合 DataFrame
    df = pd.DataFrame({
        '代碼': close_prices.columns,
        '現價': p_today.values,
        '行業': [sector_map.get(sym, 'Unknown') for sym in close_prices.columns],
        'ret_today': ret_today.values,
        'ret_3d_ago': ret_3d_ago.values,
        'drop_3d': drop_3d.values
    })

    st_msg.text("🧮 正在進行市場與行業排名交叉分析...")

    # --- 1 & 2. RS Rating (1-99) 及 3日變化 ---
    df['RS_今日'] = (df['ret_today'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_3日前'] = (df['ret_3d_ago'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_3日變化'] = df['RS_今日'] - df['RS_3日前']

    # --- 3. 過去3日股價下跌5%以內 ---
    df['3日跌幅<5%'] = df['drop_3d'].apply(lambda x: "✅ 是" if x >= -0.05 else "❌ 否")

    # --- 行業聚合運算 ---
    sector_rs_today = df.groupby('行業')['RS_今日'].mean()
    sector_rs_3d = df.groupby('行業')['RS_3日前'].mean()

    # --- 5 & 7. 該行業在整個大市排名 及 3日變化 ---
    sector_market_rank_today = (sector_rs_today.rank(pct=True) * 98 + 1).astype(int)
    sector_market_rank_3d = (sector_rs_3d.rank(pct=True) * 98 + 1).astype(int)
    
    df['行業大市排名'] = df['行業'].map(sector_market_rank_today)
    df['行業排名變化'] = df['行業大市排名'] - df['行業'].map(sector_market_rank_3d)
    
    # --- 4. 該股票在該行業是否領先 ---
    df['行業平均RS'] = df['行業'].map(sector_rs_today)
    df['行業領先?'] = df.apply(lambda row: "👑 是" if row['RS_今日'] > row['行業平均RS'] else "❌ 否", axis=1)

    # --- 6. 該股票在該行業的變動排名 ---
    # 先計今日行業內排名 (1為最好)
    df['行內排名_今日'] = df.groupby('行業')['RS_今日'].rank(ascending=False).astype(int)
    df['行內排名_3日前'] = df.groupby('行業')['RS_3日前'].rank(ascending=False).astype(int)
    # 排名數字越細越好，所以變動 = 舊排名 - 新排名 (例如由第5升到第2，+3)
    df['行內排名變化'] = df['行內排名_3日前'] - df['行內排名_今日']

    # --- 整理最終顯示格式 ---
    final_cols = [
        '代碼', '行業', '現價', 'RS_今日', 'RS_3日變化', '3日跌幅<5%', 
        '行業領先?', '行內排名變化', '行業大市排名', '行業排名變化'
    ]
    
    final_df = df[final_cols].sort_values(by='RS_今日', ascending=False).reset_index(drop=True)
    st_msg.text("✅ 運算完成！")
    return final_df

# --- 介面 ---
limit = st.slider("掃描數量", 10, 30, 30)

if st.button("🔥 執行 7 維度動能矩陣分析"):
    start_t = time.time()
    test_list = get_sample_tickers()[:limit]
    res_df = calculate_advanced_rs(test_list)
    
    if not res_df.empty:
        st.write(f"⏱️ 運算耗時: {round(time.time() - start_t, 2)} 秒")
        
        # 使用 Streamlit 內建的美化表格，為變化加上箭頭
        def format_change(val):
            if val > 0: return f"🟢 +{val}"
            elif val < 0: return f"🔴 {val}"
            return "➖ 0"
            
        res_df['RS_3日變化'] = res_df['RS_3日變化'].apply(format_change)
        res_df['行內排名變化'] = res_df['行內排名變化'].apply(format_change)
        res_df['行業排名變化'] = res_df['行業排名變化'].apply(format_change)
        
        st.dataframe(res_df, use_container_width=True)
