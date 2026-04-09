import streamlit as st
import yfinance as yf
import pandas as pd

st.set_page_config(page_title="專業選股器", layout="wide")
st.title("🏹 強勢股掃描儀 (穩定修復版)")

# 側邊欄設定
with st.sidebar:
    st.header("參數設定")
    min_sales = st.slider("最小營收增長 (%)", -10, 100, 20)
    tickers_input = st.text_area("輸入代碼 (逗號分隔)", "AAPL, NVDA, 0700.HK, 9988.HK")

def get_stock_data(symbol):
    symbol = symbol.strip().upper()
    # 自動處理港股格式：如果是 4 位數字，自動補上 .HK
    if symbol.isdigit() and len(symbol) <= 4:
        symbol = f"{symbol.zfill(4)}.HK"
    
    try:
        stock = yf.Ticker(symbol)
        # 抓取基礎資訊
        info = stock.info
        
        # 獲取價格數據來計算趨勢
        hist = stock.history(period="1y")
        if hist.empty:
            return None
            
        current_price = hist['Close'].iloc[-1]
        sma50 = hist['Close'].rolling(50).mean().iloc[-1]
        sma200 = hist['Close'].rolling(200).mean().iloc[-1]
        
        # 獲取成長數據 (增加多種備選欄位防止失效)
        sales_growth = info.get('revenueGrowth', 0)
        if sales_growth is None: sales_growth = 0
        
        eps_growth = info.get('earningsQuarterlyGrowth', 0)
        if eps_growth is None: eps_growth = 0

        return {
            "代碼": symbol,
            "名稱": info.get('shortName', 'N/A'),
            "產業": info.get('industry', 'N/A'),
            "現價": round(current_price, 2),
            "Sales 增長%": round(sales_growth * 100, 2),
            "EPS 增長%": round(eps_growth * 100, 2),
            "高於50MA": "✅" if current_price > sma50 else "❌",
            "多頭排列": "✅" if sma50 > sma200 else "❌"
        }
    except Exception as e:
        return {"代碼": symbol, "名稱": "抓取出錯", "產業": str(e)}

if st.button("開始掃描"):
    ticker_list = tickers_input.split(',')
    results = []
    
    progress_bar = st.progress(0)
    for i, t in enumerate(ticker_list):
        data = get_stock_data(t)
        if data:
            results.append(data)
        progress_bar.progress((i + 1) / len(ticker_list))
    
    df = pd.DataFrame(results)
    if not df.empty:
        # 過濾符合條件的股票
        filtered_df = df[df["Sales 增長%"] >= min_sales]
        st.subheader("符合條件名單")
        st.dataframe(filtered_df)
    else:
        st.error("所有代碼均抓取失敗，請檢查網路或代碼格式。")
