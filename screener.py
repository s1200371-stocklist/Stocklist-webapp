import streamlit as st
import requests
import pandas as pd
import numpy as np
import time

# 配置
API_KEY = "OAjgsk0vhIOmOu2SGhtjgKAsdWAxPVcx" # 已填入你的 Key
st.set_page_config(page_title="FMP RS Pro Terminal", layout="wide")

st.title("🦅 FMP 全美股 7 維度動能終端")
st.caption("數據源：Financial Modeling Prep (專業級) | 排序：行業內超車強度")

# --- 1. 核心數據抓取函數 ---
def get_fmp_market_data():
    # 抓取所有美股的基本面、價格、行業 (過濾市值 > 3億, 價格 > 2)
    screener_url = f"https://financialmodelingprep.com/api/v3/stock-screener?marketCapMoreThan=300000000&priceMoreThan=2&isEtf=false&limit=3000&apikey={API_KEY}"
    
    try:
        res = requests.get(screener_url).json()
        df = pd.DataFrame(res)
        return df[['symbol', 'price', 'sector', 'companyName', 'marketCap']]
    except Exception as e:
        st.error(f"API 抓取失敗: {e}")
        return pd.DataFrame()

def get_historical_rs_data(tickers):
    # FMP 的 EOD 數據通常需要個別抓取或批量接口
    # 為了最快速度，我們抓取「每日收盤漲跌幅」來模擬 RS
    status_msg = st.empty()
    status_msg.text("📊 正在同步全市場動能數據...")
    
    # 這裡我們利用 FMP 的 Daily Price Change 接口來快速獲取今日與之前的表現
    # 專業建議：計算真正的 RS 需要 1 年歷史，我們這裡取 FMP 預算的表現指標
    performance_url = f"https://financialmodelingprep.com/api/v3/stock-price-change/{','.join(tickers[:400])}?apikey={API_KEY}"
    
    try:
        res = requests.get(performance_url).json()
        perf_df = pd.DataFrame(res)
        return perf_df
    except:
        return pd.DataFrame()

# --- 2. 核心運算引擎 ---
if st.button("🚀 啟動 FMP 全局掃描"):
    start_time = time.time()
    
    # Step A: 獲取全市場基礎資料 (秒出)
    base_df = get_fmp_market_data()
    
    if not base_df.empty:
        # Step B: 獲取表現數據 (我們主要看 6M 和 1M 的表現來排 RS)
        # 為了效能，我們先拿前 1000 隻有潛力的
        tickers = base_df['symbol'].tolist()
        
        # 💡 專業優化：直接計算 RS
        # 在 FMP 中，我們利用 '1Y' 表現作為 RS 基底，'5D' 表現作為短期變動
        perf_url = f"https://financialmodelingprep.com/api/v3/stock-price-change/{','.join(tickers[:1000])}?apikey={API_KEY}"
        perf_data = requests.get(perf_url).json()
        perf_df = pd.DataFrame(perf_data)
        
        # 合併數據
        df = pd.merge(base_df, perf_df, on='symbol')
        
        # --- 7 維度運算 ---
        # 1. RS Rating (基於 1 年表現)
        df['RS_Now'] = (df['1Y'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        
        # 2. RS 3日變化 (用 1年表現 減去 5日表現 模擬 3日前位置)
        df['RS_3D_Ago']
