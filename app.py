import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re
import time

# ==========================================
# --- 1. 網頁基本配置 ---
# ==========================================
st.set_page_config(page_title="美股終極篩選器 (無快取防護版)", layout="wide", page_icon="🕸️")

st.title("🕸️ 美股市值與行業篩選器 (無快取堅固版)")
st.markdown("已移除 API 請求的快取機制，保證每次掃描都是最新狀態，徹底解決「卡死空表」的靈異問題。")

# ==========================================
# --- 2. 智能讀取 CSV (這個保留快取，因為文件不會變) ---
# ==========================================
@st.cache_data
def load_all_symbols(manual_tickers=""):
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    raw_list = []
    
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f, sep=None, engine='python')
                cols = [c for c in df.columns if isinstance(c, str) and 
                        any(kw in c.lower() for kw in ['symbol', 'ticker', 'act', 'sign'])]
                if cols:
                    raw_list.extend(df[cols[0]].dropna().astype(str).str.strip().tolist())
            except: continue

    if manual_tickers:
        raw_list.extend([t.strip().upper() for t in manual_tickers.split(',') if t.strip()])
                
    clean_symbols = []
    for s in list(set(raw_list)):
        s_clean = re.sub(r'[\./]', '-', s.upper())
        if re.match(r'^[A-Z-]+$', s_clean) and 1 <= len(s_clean) <= 5:
            clean_symbols.append(s_clean)
            
    return sorted(list(set(clean_symbols)))

# ==========================================
# --- 3. 核心抓取邏輯 (移除了 @st.cache_data，每次真跑！) ---
# ==========================================
def fetch_data_pro(symbols, min_cap):
    # 防護：如果傳入空名單，直接返回
    if not symbols:
        return pd.DataFrame(), ["錯誤：沒有提供股票代號"]
        
    results = []
    missing_or_failed = []
    batch_size = 50 
    
    progress_bar = st.progress(0.0)
    status_text = st.empty()
    debug_logs = []
    
    # 【階段一：批次極速掃描】
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_text.text(f"🚀 第一階段: 掃描中 {i} / {len(symbols)}...")
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            t = Ticker(batch, asynchronous=True)
            data = t.get_modules(['summaryDetail', 'assetProfile'])
            
            if not isinstance(data, dict):
                debug_logs.append(f"Batch 失敗 (Yahoo回傳字串): {str(data)[:100]}")
                missing_or_failed.extend(batch)
                continue
            
            for s in batch:
                s_data = data.get(s)
                if not isinstance(s_data, dict):
                    missing_or_failed.append(s)
                    continue
                    
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
                else:
                    missing_or_failed.append(s)
        except Exception as e:
            debug_logs.append(f"Batch 系統報錯: {e}")
            missing_or_failed.extend(batch)

    # 【階段二：VIP 獨立拯救 (減慢速度，提高成功率)】
    if missing_or_failed:
        missing_or_failed = list(set(missing_or_failed))
        status_text.text(f"🎯 第二階段: 發現 {len(missing_or_failed)} 隻漏網之魚，開始單獨拯救...")
        
        for i, s in enumerate(missing_or_failed):
            progress_bar.progress(min((i + 1) / max(len(missing_or_failed), 1), 1.0))
            try:
                t_single = Ticker(s, asynchronous=False)
                raw_data = t_single.get_modules(['summaryDetail', 'assetProfile'])
                
                if isinstance(raw_data, dict):
                    s_data = raw_data.get(s, {})
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
            except Exception as e: 
                debug_logs.append(f"單獨拯救 {s} 失敗: {e}")
            
            # 放慢腳步，避免被封鎖 (非常關鍵)
            time.sleep(0.5) 
            
    progress_bar.empty()
    status_text.empty()
    return pd.DataFrame(results), debug_logs

# ==========================================
# --- 4. 側邊欄與執行 ---
# ==========================================
st.sidebar.header("⚙️ 篩選設定")
target_cap = st.sidebar.number_input("最低市值門檻 (USD)", value=500_000_000, step=100_000_000)

st.sidebar.subheader("📌 必查補底名單")
manual_input = st.sidebar.text_area("確保這些代號必查 (用逗號隔開)", value="PLTR, NVDA, TSLA, AAPL")

# 保留清除按鈕以防萬一
if st.sidebar.button("🧹 強制重置系統狀態"):
    st.cache_data.clear()
    st.sidebar.success("系統已重置！")

is_test_mode = st.sidebar.checkbox("開啟測試模式 (只測首 50 隻 + 補底，防止 Block)", value=True)

if st.button("🔥 開始穩健掃描", use_container_width=True):
    all_symbols = load_all_symbols(manual_input)
    
    if not all_symbols:
        st.error("❌ 無法載入代號，請檢查 CSV 檔案。")
    else:
        # 決定掃描名單
        if is_test_mode:
            target_symbols = all_symbols[:50] + [t.strip().upper() for t in manual_input.split(',') if t.strip()]
        else:
            target_symbols = all_symbols
            
        target_symbols = list(set(target_symbols))

        st.info(f"✅ 準備掃描 {len(target_symbols)} 隻股票...")
        
        # 執行抓取 (因為無快取，每次都會真實運行)
        df, logs = fetch_data_pro(target_symbols, target_cap)
        
        if not df.empty:
            df = df.drop_duplicates(subset=['Symbol'])
            st.success(f"🎉 篩選完成！共找到 **{len(df)}** 隻股票。")
            st.dataframe(
                df.sort_values("MarketCap", ascending=False),
                column_config={
                    "Symbol": "代號",
                    "Name": "名稱",
                    "MarketCap": st.column_config.NumberColumn("市值 ($)", format="$%.2e"),
                    "Price": st.column_config.NumberColumn("股價 ($)", format="$%.2f")
                },
                use_container_width=True, hide_index=True
            )
        else:
            st.error("🚨 篩選結果為 0！這通常代表 Yahoo Finance 暫時限制了你的網絡請求。")
            
        if logs:
            with st.expander("🔍 查看系統除錯報告 (Yahoo 攔截記錄)"):
                for log in logs:
                    st.write(log)
