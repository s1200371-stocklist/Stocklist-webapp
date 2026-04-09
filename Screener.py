import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go

# --- 核心邏輯：計算相對強度 (Relative Strength) ---
def calculate_relative_strength(stock_df, benchmark_df):
    # 將股價與大盤基準化（起始點設為 1）
    stock_return = stock_df['Close'] / stock_df['Close'].iloc[0]
    bench_return = benchmark_df['Close'] / benchmark_df['Close'].iloc[0]
    # 相對強度線 = 個股回報 / 大盤回報
    rs_line = stock_return / bench_return
    return rs_line

# --- 網頁介面佈局 ---
st.set_page_config(page_title="專業強勢股獵人", layout="wide")
st.title("🛡️ 專業級強勢股偵測系統 2.0")

# 獲取標普 500 作為基準 (Benchmark)
@st.cache_data(ttl=3600)
def get_spy_data():
    return yf.download("SPY", period="1y")

spy_data = get_spy_data()

# --- 側邊欄：進階過濾 ---
with st.sidebar:
    st.header("🎯 狙擊條件設定")
    market = st.selectbox("選擇市場", ["美股 (US)", "港股 (HK)"])
    tickers_raw = st.text_area("代碼清單 (逗號分隔)", "NVDA, AAPL, TSLA, AMZN, GOOGL")
    
    st.divider()
    vol_min = st.number_input("最低日均成交量 (百萬)", value=1.0)
    rs_threshold = st.slider("RS 強度閾值 (高於大盤 %)", 0, 50, 5)

# --- 執行掃描 ---
if st.button("啟動掃描系統"):
    ticker_list = [t.strip() for t in tickers_raw.split(",")]
    if market == "港股 (HK)":
        ticker_list = [t + ".HK" if not t.endswith(".HK") else t for t in ticker_list]
    
    all_data = []
    
    for symbol in ticker_list:
        with st.status(f"分析中: {symbol}...", expanded=False):
            try:
                stock = yf.Ticker(symbol)
                hist = stock.history(period="1y")
                
                if len(hist) < 200: continue
                
                # 計算關鍵指標
                price = hist['Close'].iloc[-1]
                sma50 = hist['Close'].rolling(50).mean().iloc[-1]
                sma200 = hist['Close'].rolling(200).mean().iloc[-1]
                
                # 相對強度計算
                rs_line = calculate_relative_strength(hist, spy_data)
                rs_score = (rs_line.iloc[-1] - 1) * 100 # 超額收益百分比
                
                # 篩選邏輯
                is_minervini = price > sma50 > sma200
                is_rs_strong = rs_score > rs_threshold
                
                all_data.append({
                    "代碼": symbol,
                    "現價": round(price, 2),
                    "RS 指數": round(rs_score, 2),
                    "趨勢模板": "✅" if is_minervini else "❌",
                    "大盤領先": "✅" if is_rs_strong else "❌",
                    "成交量 (M)": round(stock.info.get('averageVolume', 0)/1e6, 2)
                })
            except Exception as e:
                st.error(f"{symbol} 數據獲取失敗: {e}")

    # --- 顯示結果表格 ---
    res_df = pd.DataFrame(all_data)
    if not res_df.empty:
        st.subheader("📊 掃描結果名單")
        st.dataframe(res_df.sort_values("RS 指數", ascending=False), use_container_width=True)
    else:
        st.info("目前沒有股票完全符合條件，請放寬參數或增加名單。")
