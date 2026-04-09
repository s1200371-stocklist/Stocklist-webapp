import streamlit as st
import yfinance as yf
import pandas as pd

st.set_page_config(page_title="均線大師選股器", layout="wide")
st.title("🏹 強勢股自選均線篩選器")

# --- 1. 側邊欄 UI 設定 ---
with st.sidebar:
    st.header("第一層：規模過濾")
    # 市值門檻預設為 500M
    min_mkt_cap = st.number_input("最小市值 (單位: 百萬 USD)", value=500)
    
    st.header("第二層：技術面均線過濾")
    # 讓用戶選擇要啟動哪些均線檢查
    ma_options = st.multiselect(
        "選擇必須符合的均線條件 (股價 > 均線):",
        ["50天線", "150天線", "200天線"],
        default=["50天線"] # 預設只選 50
    )
    
    st.header("第三層：成長過濾")
    min_sales = st.slider("最小營收增長 (%)", 0, 100, 20)
    
    st.divider()
    tickers_input = st.text_area("輸入代碼 (逗號分隔)", "AAPL, NVDA, TSLA, MSFT, AMD")

# --- 2. 核心抓取與判斷邏輯 ---
def get_stock_data(ticker, active_mas, min_cap, min_growth):
    try:
        stock = yf.Ticker(ticker.strip().upper())
        info = stock.info
        
        # --- 第一層：市值檢查 ---
        mkt_cap = info.get('marketCap', 0) or 0
        if mkt_cap < (min_cap * 1_000_000):
            return None
        
        # --- 第二層：均線檢查 ---
        # 抓取 1 年數據來計算均線 (150, 200 需要較長數據)
        hist = stock.history(period="1y")
        if hist.empty or len(hist) < 200:
            return None
            
        current_price = hist['Close'].iloc[-1]
        
        # 計算各條均線
        ma_results = {}
        if "50天線" in active_mas:
            ma50 = hist['Close'].rolling(50).mean().iloc[-1]
            if current_price <= ma50: return None
            ma_results["50MA"] = round(ma50, 2)
            
        if "150天線" in active_mas:
            ma150 = hist['Close'].rolling(150).mean().iloc[-1]
            if current_price <= ma150: return None
            ma_results["150MA"] = round(ma150, 2)
            
        if "200天線" in active_mas:
            ma200 = hist['Close'].rolling(200).mean().iloc[-1]
            if current_price <= ma200: return None
            ma_results["200MA"] = round(ma200, 2)

        # --- 第三層：營收增長檢查 ---
        sales_growth = info.get('revenueGrowth', 0) or 0
        if (sales_growth * 100) < min_growth:
            return None

        # 封裝結果
        res = {
            "代碼": ticker.upper(),
            "現價": round(current_price, 2),
            "Sales 增長%": f"{round(sales_growth * 100, 2)}%",
            "市值 (M)": f"{round(mkt_cap / 1_000_000, 2)}M",
        }
        res.update(ma_results) # 加入均線數值方便查看
        return res

    except Exception:
        return None

# --- 3. 執行與顯示 ---
if st.button("🚀 開始深度掃描"):
    ticker_list = [t.strip() for t in tickers_input.split(',')]
    results = []
    
    progress_bar = st.progress(0)
    for i, t in enumerate(ticker_list):
        data = get_stock_data(t, ma_options, min_mkt_cap, min_sales)
        if data:
            results.append(data)
        progress_bar.progress((i + 1) / len(ticker_list))
    
    if results:
        st.success(f"掃描完成！找到 {len(results)} 隻符合所有條件的強勢股。")
        df = pd.DataFrame(results)
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("沒有股票能通過這層層篩選，請嘗試放寬條件。")
