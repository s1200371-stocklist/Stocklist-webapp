import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re

# --- 1. 網頁配置 ---
st.set_page_config(page_title="美股終極篩選器", layout="wide", page_icon="💰")

st.title("📈 美股市值與行業篩選器 (終極完美版)")
st.markdown("""
這個版本解決了：
1. **CSV 格式不一**：自動偵測 `,` 或 `|` 分隔符。
2. **欄位名稱混亂**：自動識別 `Symbol`, `Ticker`, `ACT Symbol` 等。
3. **熱門股遺漏**：提供「手動追加」功能，確保 PLTR 等股票不失蹤。
4. **數據穩定性**：保證市值優先，行業資料為輔。
""")

# --- 2. 智能讀取與清洗功能 ---
@st.cache_data
def load_all_symbols(manual_tickers=""):
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    raw_list = []
    
    # A. 讀取 CSV
    for f in files:
        if os.path.exists(f):
            try:
                # sep=None, engine='python' 會自動偵測逗號或直線分隔符
                df = pd.read_csv(f, sep=None, engine='python')
                # 尋找包含 symbol / ticker / act 的欄位
                target_cols = [c for c in df.columns if isinstance(c, str) and 
                               any(kw in c.lower() for kw in ['symbol', 'ticker', 'act', 'sign'])]
                
                if target_cols:
                    found_symbols = df[target_cols[0]].dropna().astype(str).str.strip().tolist()
                    raw_list.extend(found_symbols)
            except:
                continue

    # B. 處理手動追加的代號 (例如輸入 PLTR, NVDA)
    if manual_tickers:
        added_list = [t.strip().upper() for t in manual_tickers.split(',') if t.strip()]
        raw_list.extend(added_list)
                
    # C. 格式清洗與過濾
    clean_symbols = []
    for s in list(set(raw_list)):
        # 處理 Yahoo 格式：. 或 / 轉為 -
        s_clean = re.sub(r'[\./]', '-', s.upper())
        # 只保留 1-5 位的正規代號，剔除測試碼與權證
        if re.match(r'^[A-Z-]+$', s_clean) and 1 <= len(s_clean) <= 5:
            clean_symbols.append(s_clean)
            
    return sorted(list(set(clean_symbols)))

# --- 3. 核心抓取邏輯 ---
@st.cache_data(ttl=3600)
def fetch_data_pro(symbols, min_cap):
    results = []
    batch_size = 50 # 兼顧速度與穩定性
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_text.text(f"🚀 正在分析美股數據: {i} / {len(symbols)}...")
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            t = Ticker(batch, asynchronous=True)
            # 同時拿市值和公司資料
            all_info = t.get_modules(['summaryDetail', 'assetProfile'])
            
            for s in batch:
                data = all_info.get(s)
                if not isinstance(data, dict): continue
                
                summary = data.get('summaryDetail', {})
                profile = data.get('assetProfile', {})
                
                mkt_cap = summary.get('marketCap')
                # 硬指標：市值必須達標
                if isinstance(mkt_cap, (int, float)) and mkt_cap >= min_cap:
                    results.append({
                        "Symbol": s,
                        "Name": summary.get('shortName', s),
                        "MarketCap": mkt_cap,
                        "Price": summary.get('previousClose', 0.0),
                        "Sector": profile.get('sector', 'N/A') if isinstance(profile, dict) else 'N/A',
                        "Industry": profile.get('industry', 'N/A') if isinstance(profile, dict) else 'N/A'
                    })
        except:
            continue
            
    progress_bar.empty()
    status_text.empty()
    return pd.DataFrame(results)

# --- 4. 側邊欄與 UI ---
st.sidebar.header("🔍 篩選與補底")
target_cap = st.sidebar.number_input("最低市值門檻 (USD)", value=500_000_000, step=100_000_000)

st.sidebar.subheader("📌 手動補底 (防止遺漏)")
manual_input = st.sidebar.text_area("輸入想強制加入的代號 (用逗號隔開)", value="PLTR, NVDA, TSLA, AAPL", help="如果 CSV 太舊，可以在這裡手動輸入代號")

# --- 5. 執行主邏輯 ---
if st.button("🔥 開始全自動掃描篩選", use_container_width=True):
    # 1. 加載名單
    all_symbols = load_all_symbols(manual_input)
    
    if not all_symbols:
        st.error("無法載入代號，請檢查 CSV 檔案路徑。")
    else:
        st.info(f"成功加載 {len(all_symbols)} 個有效代號，正在向 Yahoo Finance 請求數據...")
        
        # 2. 抓取數據
        df = fetch_data_pro(all_symbols, target_cap)
        
        if not df.empty:
            st.success(f"✅ 篩選完成！共找到 {len(df)} 隻符合條件股票。")
            
            # 數據分析小統計
            col1, col2 = st.columns(2)
            with col1:
                st.metric("平均市值", f"${df['MarketCap'].mean():,.0f}")
            with col2:
                top_sector = df['Sector'].value_counts().idxmax()
                st.metric("最多的板塊", top_sector)

            # 顯示表格
            st.dataframe(
                df.sort_values("MarketCap", ascending=False),
                column_config={
                    "MarketCap": st.column_config.NumberColumn("市值 ($)", format="$%.2e"),
                    "Price": st.column_config.NumberColumn("股價 ($)", format="$%.2f")
                },
                use_container_width=True, hide_index=True, height=600
            )
            
            # 下載按鈕
            csv_data = df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 下載完整 CSV 結果", csv_data, "stock_results.csv", "text/csv")
        else:
            st.warning("符合條件的股票數量為 0。請檢查市值設定。")

# --- 補充小提示 ---
st.divider()
with st.expander("📝 關於搜尋不到特定股票的說明"):
    st.write("""
    1. **SNDK (SanDisk)**: 該公司已於 2016 年被 Western Digital (WDC) 收購並下市，因此無法搜到。
    2. **PLTR (Palantir)**: 如果 CSV 沒更新可能遺漏。本版本已在左側加入『手動補底』功能，確保它能被掃描到。
    3. **N/A 顯示**: ETF 或部分新上市公司在 Yahoo 數據庫中可能缺乏行業描述，這是正常現象。
    """)


st.markdown("---")
st.header("🎯 失蹤股票狙擊手 (測試專用)")
st.markdown("輸入你覺得「漏咗」嘅股票代號，睇下 Yahoo 到底回傳咗咩數據畀我哋。")

test_symbol = st.text_input("輸入失蹤股票代號 (例如: BRK-B, TSM, BABA)", value="BRK-B")

if st.button("🔍 狙擊這隻股票"):
    with st.spinner("正在向 Yahoo 查閱內部數據..."):
        try:
            t_test = Ticker(test_symbol, asynchronous=False)
            
            # 一次過攞晒所有模組，睇下市值收埋喺邊
            raw_summary = t_test.summary_detail.get(test_symbol, {})
            raw_price = t_test.price.get(test_symbol, {})
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("1. 檢查 SummaryDetail 模組")
                if isinstance(raw_summary, dict):
                    mkt_cap_1 = raw_summary.get('marketCap', '找不到')
                    st.write(f"**市值 (Market Cap):** {mkt_cap_1}")
                    st.json(raw_summary) # 顯示原始數據
                else:
                    st.error("Yahoo 完全沒有這隻股票的 Summary 數據")
                    
            with col2:
                st.subheader("2. 檢查 Price 模組")
                if isinstance(raw_price, dict):
                    mkt_cap_2 = raw_price.get('marketCap', '找不到')
                    st.write(f"**市值 (Market Cap):** {mkt_cap_2}")
                    st.json(raw_price) # 顯示原始數據
                else:
                    st.error("Yahoo 完全沒有這隻股票的 Price 數據")
                    
        except Exception as e:
            st.error(f"查詢失敗: {e}")
