import streamlit as st
import pandas as pd
from yahooquery import Ticker
import time

# --- 1. 網頁配置 ---
st.set_page_config(page_title="美股 500M+ 篩選器", layout="wide")

st.title("📊 全美股實時篩選器")
st.markdown("自動合併 NASDAQ, NYSE, Other 名單，並篩選市值 > 500M USD 的股票。")

# --- 2. 核心功能：讀取 CSV 並合併 Symbol ---
@st.cache_data
def get_all_symbols():
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    all_symbols = []
    
    for f in files:
        try:
            # 讀取時考慮不同的 CSV 可能有不同的欄位名 (Symbol 或 Ticker)
            df = pd.read_csv(f)
            col = 'Symbol' if 'Symbol' in df.columns else 'Ticker'
            if col in df.columns:
                symbols = df[col].dropna().str.strip().unique().tolist()
                all_symbols.extend(symbols)
        except Exception as e:
            st.error(f"讀取 {f} 失敗: {e}")
            
    # 去除重複，並過濾掉不正常的代號
    return sorted(list(set([str(s) for s in all_symbols if len(str(s)) < 6])))

# --- 3. 核心功能：抓取 Yahoo 數據並篩選 ---
@st.cache_data(ttl=3600) # 數據快取 1 小時，避免頻繁請求被封 IP
def fetch_and_screen(symbols, min_mkt_cap=500_000_000):
    screened_list = []
    # 由於全美股數量龐大，我們分批處理 (每批 500 隻)
    batch_size = 500
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        status_text.text(f"正在抓取數據: {i} / {len(symbols)}...")
        progress_bar.progress(i / len(symbols))
        
        try:
            t = Ticker(batch, asynchronous=True)
            data = t.summary_detail
            
            for s in batch:
                s_data = data.get(s)
                if isinstance(s_data, dict):
                    mkt_cap = s_data.get('marketCap')
                    # 嚴格檢查市值是否為數字，防止 TypeError
                    if isinstance(mkt_cap, (int, float)) and mkt_cap >= min_mkt_cap:
                        screened_list.append({
                            "Symbol": s,
                            "Name": s_data.get('shortName', 'N/A'),
                            "MarketCap": mkt_cap,
                            "Price": s_data.get('previousClose', 0),
                            "Sector": s_data.get('sector', 'N/A'),
                            "Industry": s_data.get('industry', 'N/A')
                        })
        except Exception:
            continue # 跳過出錯的批次
            
    progress_bar.empty()
    status_text.empty()
    return pd.DataFrame(screened_list)

# --- 4. Web App 介面邏輯 ---

# 側邊欄控制
st.sidebar.header("篩選參數")
mkt_cap_threshold = st.sidebar.number_input("最低市值 (USD)", value=500_000_000, step=100_000_000)

if st.button("開始掃描全美股"):
    all_symbols = get_all_symbols()
    
    if not all_symbols:
        st.warning("⚠️ 沒找到任何股票代號，請檢查 CSV 檔案是否在資料夾中。")
    else:
        with st.spinner(f"正在掃描 {len(all_symbols)} 隻股票，這可能需要幾分鐘..."):
            result_df = fetch_and_screen(all_symbols, mkt_cap_threshold)
            
            if not result_df.empty:
                st.success(f"✅ 掃描完成！找到 {len(result_df)} 隻符合條件的股票。")
                
                # 數據統計
                c1, c2, c3 = st.columns(3)
                c1.metric("符合總數", len(result_df))
                c2.metric("平均股價", f"${result_df['Price'].mean():.2f}")
                
                # 顯示表格
                st.dataframe(
                    result_df,
                    column_config={
                        "MarketCap": st.column_config.Number_config("市值", format="$%.2e"),
                        "Price": st.column_config.Number_config("股價", format="$%.2f"),
                    },
                    use_container_width=True,
                    hide_index=True
                )
                
                # 下載按鈕
                csv = result_df.to_csv(index=False).encode('utf-8')
                st.download_button("下載篩選結果 CSV", data=csv, file_name="screened_results.csv", mime="text/csv")
            else:
                st.error("沒有股票符合目前的篩選條件。")
else:
    st.info("💡 請點擊上方按鈕開始從你的 CSV 名單中篩選數據。")
