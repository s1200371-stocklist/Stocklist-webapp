import streamlit as st
import yfinance as yf
import pandas as pd
import time

# --- 頁面配置 ---
st.set_page_config(page_title="US Stock Master 500M+", layout="wide")
st.title("🦅 全美股市場自動篩選器 (市值 > 500M)")

# --- 1. 獲取全美股名單 (自動同步) ---
@st.cache_data(ttl=86400)
def get_full_us_list():
    # 使用另一個更穩定的數據源獲取全美股代碼 (包含 NYSE, NASDAQ, AMEX)
    try:
        # 這是 FTP 同步的標普全球代碼庫
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        tickers = pd.read_csv(url, header=None)[0].tolist()
        return [str(t).strip().upper() for t in tickers if len(str(t)) <= 4]
    except:
        return ["AAPL", "NVDA", "MSFT", "TSLA", "AMD", "META", "AMZN", "GOOGL"]

# --- 2. 側邊欄控制 ---
with st.sidebar:
    st.header("📌 篩選核心設定")
    # 強制市值門檻為 500M
    mkt_cap_threshold = st.number_input("強制市值下限 (百萬 USD)", value=500, min_value=1)
    
    st.header("📈 成長與技術面")
    min_growth = st.slider("最小營收增長 (%)", -20, 100, 20)
    
    ma_options = st.multiselect(
        "股價必須站上：",
        ["50天線", "150天線", "200天線"],
        default=["50天線"]
    )
    
    st.divider()
    all_symbols = get_full_us_list()
    # 這裡讓用戶選擇掃描多少隻，建議先測 300 隻
    limit = st.slider("掃描股票數量 (由市值大到小)", 50, len(all_symbols), 300)
    st.info(f"當前全美股庫存: {len(all_symbols)} 隻")

# --- 3. 核心過濾引擎 ---
def fast_screener(tickers, cap_limit, growth_limit, mas):
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # 為了不讓 Yahoo 封鎖，我們分批次小量處理
    for i, ticker in enumerate(tickers):
        try:
            status_text.text(f"🔍 掃描中 ({i+1}/{len(tickers)}): {ticker}")
            stock = yf.Ticker(ticker)
            
            # 第一步：只拿 Info (這最快)
            info = stock.info
            mkt_cap = info.get('marketCap', 0) or 0
            
            # 🟢 關鍵：如果不夠 500M，直接 Skip
            if mkt_cap < (cap_limit * 1_000_000):
                progress_bar.progress((i + 1) / len(tickers))
                continue
            
            # 第二步：檢查成長性
            rev_growth = info.get('revenueGrowth', 0) or 0
            if (rev_growth * 100) < growth_limit:
                progress_bar.progress((i + 1) / len(tickers))
                continue
                
            # 第三步：最後才處理最耗時的歷史數據
            hist = stock.history(period="1y")
            if hist.empty or len(hist) < 200:
                progress_bar.progress((i + 1) / len(tickers))
                continue
                
            curr_p = hist['Close'].iloc[-1]
            
            # 均線判定
            is_valid_ma = True
            for ma in mas:
                days = int(ma.replace("天線", ""))
                ma_val = hist['Close'].rolling(days).mean().iloc[-1]
                if curr_p < ma_val:
                    is_valid_ma = False
                    break
            
            if not is_valid_ma:
                progress_bar.progress((i + 1) / len(tickers))
                continue

            # 全部通過！
            results.append({
                "代碼": ticker,
                "名稱": info.get('shortName', 'N/A'),
                "現價": round(curr_p, 2),
                "市值 (M)": f"{round(mkt_cap/1_000_000, 1)}M",
                "營收增長": f"{round(rev_growth * 100, 1)}%",
                "產業": info.get('sector', 'N/A')
            })
            
        except Exception:
            pass
        
        progress_bar.progress((i + 1) / len(tickers))
    
    status_text.text("✅ 篩選完成！")
    return pd.DataFrame(results)

# --- 4. 執行與顯示 ---
if st.button("🚀 開始全美股市場掃描 (市值 500M+)"):
    # 排序名單 (這裡可以根據需要調整)
    selected_tickers = all_symbols[:limit]
    
    start_time = time.time()
    final_df = fast_screener(selected_tickers, mkt_cap_threshold, min_growth, ma_options)
    end_time = time.time()
    
    st.write(f"⏱️ 掃描耗時: {round(end_time - start_time, 1)} 秒")
    
    if not final_df.empty:
        st.success(f"🎯 成功找到 {len(final_df)} 隻符合條件的股票！")
        st.dataframe(final_df, use_container_width=True)
        
        # 下載功能
        csv = final_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 下載篩選結果", csv, "us_500m_stocks.csv", "text/csv")
    else:
        st.warning("在此批次中沒有股票符合條件，請嘗試減少「營收增長」或「均線」限制。")
