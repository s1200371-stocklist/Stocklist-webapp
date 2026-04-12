import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re
import time

# ==========================================
# --- 1. 網頁基本配置 ---
# ==========================================
st.set_page_config(page_title="美股終極篩選器 (雙網版)", layout="wide", page_icon="🕸️")

st.title("🕸️ 美股市值與行業篩選器 (雙網全能版)")
st.markdown("""
這個終極版本包含了：
1. **最強 CSV 兼容**：自動識別不同格式，確保 NYSE 股票不遺漏。
2. **雙網救援機制**：大批次掃描後，自動對失蹤股票（如 TSLA, PLTR）進行單獨狙擊。
3. **系統重置功能**：一鍵清除快取，解決因 Yahoo API 斷線導致的「永久空表」問題。
""")

# ==========================================
# --- 2. 智能讀取與清洗 ---
# ==========================================
@st.cache_data
def load_all_symbols(manual_tickers=""):
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    raw_list = []
    
    # A. 讀取本地 CSV
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f, sep=None, engine='python')
                target_cols = [c for c in df.columns if isinstance(c, str) and 
                               any(kw in c.lower() for kw in ['symbol', 'ticker', 'act', 'sign'])]
                if target_cols:
                    raw_list.extend(df[target_cols[0]].dropna().astype(str).str.strip().tolist())
            except:
                continue

    # B. 加入手動輸入的代號
    if manual_tickers:
        raw_list.extend([t.strip().upper() for t in manual_tickers.split(',') if t.strip()])
                
    # C. 清洗與過濾 (只保留正常美股代號)
    clean_symbols = []
    for s in list(set(raw_list)):
        s_clean = re.sub(r'[\./]', '-', s.upper())
        if re.match(r'^[A-Z-]+$', s_clean) and 1 <= len(s_clean) <= 5:
            clean_symbols.append(s_clean)
            
    return sorted(list(set(clean_symbols)))

# ==========================================
# --- 3. 核心抓取邏輯 (雙網救援機制) ---
# ==========================================
@st.cache_data(ttl=3600)
def fetch_data_pro(symbols, min_cap):
    results = []
    missing_or_failed = []
    batch_size = 50 
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # 【第一重網】：大批次極速掃描
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_text.text(f"🚀 第一階段 (極速掃描): {i} / {len(symbols)}...")
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            t = Ticker(batch, asynchronous=True)
            data = t.get_modules(['summaryDetail', 'assetProfile'])
            
            for s in batch:
                s_data = data.get(s)
                if not isinstance(s_data, dict):
                    missing_or_failed.append(s)
                    continue
                    
                summary = s_data.get('summaryDetail', {})
                if isinstance(summary, dict):
                    mkt_cap = summary.get('marketCap')
                    if isinstance(mkt_cap, (int, float)):
                        if mkt_cap >= min_cap:
                            profile = s_data.get('assetProfile', {})
                            results.append({
                                "Symbol": s,
                                "Name": summary.get('shortName', s),
                                "MarketCap": mkt_cap,
                                "Price": summary.get('previousClose', 0.0),
                                "Sector": profile.get('sector', 'N/A') if isinstance(profile, dict) else 'N/A',
                                "Industry": profile.get('industry', 'N/A') if isinstance(profile, dict) else 'N/A'
                            })
                    else:
                        missing_or_failed.append(s)
                else:
                    missing_or_failed.append(s)
        except Exception:
            missing_or_failed.extend(batch)

    # 【第二重網】：VIP 單獨狙擊 (拯救漏網之魚)
    if missing_or_failed:
        missing_or_failed = list(set(missing_or_failed))
        
        for i, s in enumerate(missing_or_failed):
            status_text.text(f"🎯 第二階段 (VIP 補漏): 正在單獨拯救 {s} ({i+1}/{len(missing_or_failed)})...")
            # 確保進度條不會報錯
            progress_val = min((i + 1) / max(len(missing_or_failed), 1), 1.0)
            progress_bar.progress(progress_val)
            
            try:
                t_single = Ticker(s, asynchronous=False) # 單獨同步抓取
                s_data = t_single.get_modules(['summaryDetail', 'assetProfile']).get(s, {})
                
                if isinstance(s_data, dict):
                    summary = s_data.get('summaryDetail', {})
                    if isinstance(summary, dict):
                        mkt_cap = summary.get('marketCap')
                        if isinstance(mkt_cap, (int, float)) and mkt_cap >= min_cap:
                            profile = s_data.get('assetProfile', {})
                            results.append({
                                "Symbol": s,
                                "Name": summary.get('shortName', s),
                                "MarketCap": mkt_cap,
                                "Price": summary.get('previousClose', 0.0),
                                "Sector": profile.get('sector', 'N/A') if isinstance(profile, dict) else 'N/A',
                                "Industry": profile.get('industry', 'N/A') if isinstance(profile, dict) else 'N/A'
                            })
            except Exception:
                pass 
            time.sleep(0.05) # 防止被 Yahoo 封鎖
            
    progress_bar.empty()
    status_text.empty()
    return pd.DataFrame(results)

# ==========================================
# --- 4. 側邊欄介面與設定 ---
# ==========================================
st.sidebar.header("⚙️ 篩選與系統設定")
target_cap = st.sidebar.number_input("最低市值門檻 (USD)", value=500_000_000, step=100_000_000)

st.sidebar.markdown("---")
st.sidebar.subheader("📌 防漏補底名單")
manual_input = st.sidebar.text_area(
    "輸入想強制加入的代號 (用逗號隔開)", 
    value="PLTR, NVDA, TSLA, AAPL",
    help="即使 CSV 沒有這些股票，系統也會強制向 Yahoo 查詢"
)

st.sidebar.markdown("---")
st.sidebar.subheader("🛠️ 系統維護")
st.sidebar.info("如果發現掃描結果為 0，請務必先點擊下方按鈕清除快取！")
if st.sidebar.button("🧹 清除系統快取 (重置)"):
    st.cache_data.clear()
    st.sidebar.success("✅ 快取已完全清除！請重新點擊掃描按鈕。")

# 安全測試開關
is_test_mode = st.sidebar.checkbox("開啟測試模式 (只掃描首 30 隻股票，防止被 Block)", value=False)

# ==========================================
# --- 5. 主程式執行邏輯 ---
# ==========================================
if st.button("🔥 開始雙網全自動掃描", use_container_width=True):
    all_symbols = load_all_symbols(manual_input)
    
    if not all_symbols:
        st.error("❌ 無法載入代號，請檢查 CSV 檔案是否存在及格式。")
    else:
        # 如果開啟了測試模式，就縮減名單
        if is_test_mode:
            st.warning("⚠️ 現正處於測試模式，只會掃描少量股票。")
            target_symbols = all_symbols[:30] + [t.strip().upper() for t in manual_input.split(',') if t.strip()]
            target_symbols = list(set(target_symbols))
        else:
            target_symbols = all_symbols

        st.info(f"✅ 成功加載準備掃描。本次掃描數量：{len(target_symbols)} 隻。開始執行...")
        
        # 執行數據抓取
        df = fetch_data_pro(target_symbols, target_cap)
        
        # 顯示結果
        if not df.empty:
            # 確保不會有重複出現的股票
            df = df.drop_duplicates(subset=['Symbol'])
            
            st.success(f"🎉 篩選完成！成功突破限制，共找到 **{len(df)}** 隻符合條件的股票。")
            st.dataframe(
                df.sort_values("MarketCap", ascending=False),
                column_config={
                    "Symbol": "代號",
                    "Name": "公司名稱",
                    "MarketCap": st.column_config.NumberColumn("市值 ($)", format="$%.2e"),
                    "Price": st.column_config.NumberColumn("股價 ($)", format="$%.2f"),
                    "Sector": "板塊",
                    "Industry": "行業"
                },
                use_container_width=True, hide_index=True, height=600
            )
            
            csv_data = df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 下載完整 CSV 結果", csv_data, "stock_results_pro.csv", "text/csv")
        else:
            st.error("🚨 篩選結果依然為 0 隻！這通常是因為 Yahoo 暫時封鎖了你的 IP。")
            st.write("💡 **解決建議**：請先點擊左側的「🧹 清除系統快取」，勾選「開啟測試模式」，然後再試一次。")
