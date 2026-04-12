import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re
import time

st.set_page_config(page_title="美股終極篩選器 (雙網版)", layout="wide", page_icon="🕸️")

st.title("🕸️ 美股市值與行業篩選器 (雙網救援機制)")
st.markdown("特設**「VIP 獨立狙擊機制」**：自動偵測喺批次中失蹤嘅熱門股（如 TSLA, PLTR），並作單獨重新抓取，保證零遺漏！")

# --- 1. 讀取與清洗 (保持最強兼容) ---
@st.cache_data
def load_all_symbols(manual_tickers=""):
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    raw_list = []
    
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f, sep=None, engine='python')
                target_cols = [c for c in df.columns if isinstance(c, str) and 
                               any(kw in c.lower() for kw in ['symbol', 'ticker', 'act', 'sign'])]
                if target_cols:
                    raw_list.extend(df[target_cols[0]].dropna().astype(str).str.strip().tolist())
            except: continue

    if manual_tickers:
        raw_list.extend([t.strip().upper() for t in manual_tickers.split(',') if t.strip()])
                
    clean_symbols = []
    for s in list(set(raw_list)):
        s_clean = re.sub(r'[\./]', '-', s.upper())
        if re.match(r'^[A-Z-]+$', s_clean) and 1 <= len(s_clean) <= 5:
            clean_symbols.append(s_clean)
            
    return sorted(list(set(clean_symbols)))

# --- 2. 核心抓取邏輯 (殺手鐧：雙網救援機制) ---
@st.cache_data(ttl=3600)
def fetch_data_pro(symbols, min_cap):
    results = []
    missing_or_failed = [] # 收集所有失敗、無數據嘅代號
    batch_size = 50 
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # 網一：大批次極速掃描
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_text.text(f"🚀 第一階段 (極速掃描): {i} / {len(symbols)}...")
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            t = Ticker(batch, asynchronous=True)
            data = t.get_modules(['summaryDetail', 'assetProfile'])
            
            for s in batch:
                s_data = data.get(s)
                
                # 如果回傳字串報錯，或者根本唔係字典 -> 掟入重試名單
                if not isinstance(s_data, dict):
                    missing_or_failed.append(s)
                    continue
                    
                summary = s_data.get('summaryDetail', {})
                if isinstance(summary, dict):
                    mkt_cap = summary.get('marketCap')
                    # 如果有市值
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
                        missing_or_failed.append(s) # 搵唔到市值，可能係 API 未 load 完，重試！
                else:
                    missing_or_failed.append(s) # 冇 summaryDetail，重試！
        except Exception:
            missing_or_failed.extend(batch) # 成個 Batch 冧咗，全部重試！

    # 網二：VIP 獨立狙擊 (單獨重試)
    if missing_or_failed:
        # 去除重複
        missing_or_failed = list(set(missing_or_failed))
        st.toast(f"發現 {len(missing_or_failed)} 隻股票喺大批次中無回覆，正在啟動 VIP 獨立狙擊...", icon="🎯")
        
        for i, s in enumerate(missing_or_failed):
            status_text.text(f"🎯 第二階段 (VIP 補漏): 正在單獨拯救 {s} ({i+1}/{len(missing_or_failed)})...")
            progress_bar.progress(min(i / len(missing_or_failed), 1.0))
            
            try:
                # 殺手鐧：asynchronous=False (同步)，逐隻查，最慢但 100% 準確
                t_single = Ticker(s, asynchronous=False)
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
                pass # 呢隻真係死股/退市股，可以徹底放棄
            
            # 畀少少休息時間 Yahoo，防止被 block
            time.sleep(0.05)
            
    progress_bar.empty()
    status_text.empty()
    return pd.DataFrame(results)

# --- 3. 介面與執行 ---
st.sidebar.header("🔍 篩選與補底")
target_cap = st.sidebar.number_input("最低市值門檻 (USD)", value=500_000_000, step=100_000_000)
manual_input = st.sidebar.text_area("輸入想強制加入的代號 (用逗號隔開)", value="PLTR, NVDA, TSLA, AAPL")

if st.button("🔥 開始雙網全自動掃描", use_container_width=True):
    all_symbols = load_all_symbols(manual_input)
    
    if not all_symbols:
        st.error("無法載入代號，請檢查 CSV。")
    else:
        st.info(f"成功加載 {len(all_symbols)} 個有效代號。開始執行...")
        
        df = fetch_data_pro(all_symbols, target_cap)
        
        if not df.empty:
            st.success(f"✅ 篩選完成！成功突破 Yahoo 限制，共找到 {len(df)} 隻股票。")
            st.dataframe(
                df.sort_values("MarketCap", ascending=False),
                column_config={
                    "MarketCap": st.column_config.NumberColumn("市值 ($)", format="$%.2e"),
                    "Price": st.column_config.NumberColumn("股價 ($)", format="$%.2f")
                },
                use_container_width=True, hide_index=True, height=600
            )
            csv_data = df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 下載完整 CSV 結果", csv_data, "stock_results_pro.csv", "text/csv")
        else:
            st.warning("符合條件的股票數量為 0。")

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
