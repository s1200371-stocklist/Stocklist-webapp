import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re

# --- 1. 網頁基本配置 ---
st.set_page_config(page_title="美股 500M+ 專業篩選器", layout="wide", page_icon="📈")

st.title("📈 全美股市值篩選器 (行業與板塊增強版)")
st.markdown("自動合併三大交易所名單，並精準抓取市值、板塊及行業數據。")

# --- 2. 智能符號讀取與清洗 ---
@st.cache_data
def load_and_clean_symbols():
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    raw_symbols = []
    
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f)
                cols = [c for c in df.columns if c.lower() in ['symbol', 'ticker']]
                if cols:
                    symbols = df[cols[0]].dropna().astype(str).str.strip().tolist()
                    raw_symbols.extend(symbols)
            except Exception as e:
                st.warning(f"讀取 {f} 時發生錯誤: {e}")
                
    clean_symbols = []
    for s in list(set(raw_symbols)):
        formatted_s = re.sub(r'[\./]', '-', s.upper())
        if re.match(r'^[A-Z-]+$', formatted_s) and len(formatted_s) < 8:
            clean_symbols.append(formatted_s)
            
    return sorted(list(set(clean_symbols)))

# --- 3. 數據抓取與篩選核心 (重點優化部分) ---
@st.cache_data(ttl=3600)
def fetch_and_screen_data(symbols, min_cap):
    results = []
    failed_logs = []
    batch_size = 150 # 稍微縮小批次，因為一次抓兩個模組數據量較大
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_text.text(f"🚀 正在分析: {i} / {len(symbols)} (正在提取市值及行業資料)...")
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            # 同時抓取 summaryDetail (市值) 和 assetProfile (板塊/行業)
            t = Ticker(batch, asynchronous=True)
            all_data = t.get_modules(['summaryDetail', 'assetProfile'])
            
            for s in batch:
                # 取得該符號的所有模組數據
                s_all = all_data.get(s)
                if not isinstance(s_all, dict):
                    continue
                
                s_summary = s_all.get('summaryDetail', {})
                s_profile = s_all.get('assetProfile', {})
                
                # 處理市值
                if isinstance(s_summary, dict):
                    mkt_cap = s_summary.get('marketCap')
                    
                    if isinstance(mkt_cap, (int, float)) and mkt_cap >= min_cap:
                        # 成功篩選，整理數據
                        results.append({
                            "Symbol": s,
                            "Name": s_summary.get('shortName', 'N/A') if isinstance(s_summary.get('shortName'), str) else s,
                            "MarketCap": mkt_cap,
                            "Price": s_summary.get('previousClose', 0),
                            # 從 assetProfile 拿 Sector 和 Industry
                            "Sector": s_profile.get('sector', 'N/A'),
                            "Industry": s_profile.get('industry', 'N/A')
                        })
                    else:
                        failed_logs.append({"Symbol": s, "Reason": "市值不足或查無數據"})
                else:
                    failed_logs.append({"Symbol": s, "Reason": "API 回傳格式錯誤"})
                    
        except Exception as e:
            continue
            
    progress_bar.empty()
    status_text.empty()
    return pd.DataFrame(results), failed_logs

# --- 4. 側邊欄介面 ---
st.sidebar.header("⚙️ 篩選設定")
target_cap = st.sidebar.number_input("最低市值門檻 (USD)", value=500_000_000, step=100_000_000, format="%d")
show_debug = st.sidebar.checkbox("🛠️ 開啟 Debug 模式", value=False)

# --- 5. 主程式邏輯 ---
if st.button("🔍 開始掃描全市場", use_container_width=True):
    try:
        all_symbols = load_and_clean_symbols()
        if not all_symbols:
            st.error("❌ 找不到 CSV 檔案。")
        else:
            with st.spinner(f"正在掃描 {len(all_symbols)} 隻股票..."):
                df_result, logs = fetch_and_screen_data(all_symbols, target_cap)
                
                if not df_result.empty:
                    st.success(f"✅ 找到 {len(df_result)} 隻符合條件股票")
                    
                    # 顯示數據表格，新增行業欄位
                    st.dataframe(
                        df_result.sort_values("MarketCap", ascending=False),
                        column_config={
                            "Symbol": "代號",
                            "Name": "公司名稱",
                            "MarketCap": st.column_config.NumberColumn("市值 ($)", format="$%.2e"),
                            "Price": st.column_config.NumberColumn("股價", format="$%.2f"),
                            "Sector": "板塊 (Sector)",
                            "Industry": "行業 (Industry)"
                        },
                        use_container_width=True,
                        hide_index=True,
                        height=600
                    )
                    
                    csv = df_result.to_csv(index=False).encode('utf-8')
                    st.download_button("📥 下載結果 CSV", data=csv, file_name="stock_screener_results.csv", mime="text/csv")
                else:
                    st.warning("⚠️ 沒有股票符合條件。")
                
                if show_debug and logs:
                    with st.expander("查看被排除的原因"):
                        st.write(pd.DataFrame(logs))
    except Exception as e:
        st.error(f"🚨 出錯了: {e}")
else:
    st.info("👈 請點擊按鈕開始掃描。")
