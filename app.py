import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re

# --- 1. 網頁基本配置 ---
st.set_page_config(page_title="美股專業篩選器 (Pro)", layout="wide", page_icon="📈")

st.title("📈 全美股市值與行業篩選器 (Pro Version)")
st.markdown("自動合併三大交易所名單，過濾無效代號，並抓取 Yahoo Finance 最新數據。")

# --- 2. 智能符號讀取與清洗 (終極修正版) ---
@st.cache_data
def load_and_clean_symbols():
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    raw_symbols = []
    
    # 讀取 CSV，自動適應逗號或 | 分隔符
    for f in files:
        if os.path.exists(f):
            try:
                # engine='python' 和 sep=None 讓 Pandas 自動偵測分隔符
                df = pd.read_csv(f, sep=None, engine='python')
                # 模糊搜尋欄位：只要欄位名稱包含 symbol, ticker 或 act 就接受
                cols = [c for c in df.columns if isinstance(c, str) and any(kw in c.lower() for kw in ['symbol', 'ticker', 'act'])]
                if cols:
                    symbols = df[cols[0]].dropna().astype(str).str.strip().tolist()
                    raw_symbols.extend(symbols)
            except Exception as e:
                st.warning(f"讀取 {f} 時發生錯誤: {e}")
                
    clean_symbols = []
    # 符號清洗與嚴格過濾邏輯
    for s in list(set(raw_symbols)):
        # 將 . 和 / 替換為 Yahoo 支援的 - (解決 BRK.B 變 BRK-B 的問題)
        formatted_s = re.sub(r'[\./]', '-', s.upper())
        
        # 嚴格限制：只保留純英文字母和橫線，且長度在 1 到 5 之間 (過濾 Warrants 及垃圾代號)
        if re.match(r'^[A-Z-]+$', formatted_s) and 1 <= len(formatted_s) <= 5:
            clean_symbols.append(formatted_s)
            
    return sorted(list(set(clean_symbols)))

# --- 3. 數據抓取與篩選核心 (市值保證 + 行業補完) ---
@st.cache_data(ttl=3600) # 快取 1 小時
def fetch_and_screen_data(symbols, min_cap):
    results = []
    batch_size = 50 # 每批 50 隻，確保 API 穩定性
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_text.text(f"🚀 正在請求 Yahoo 數據: {i} / {len(symbols)}...")
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            t = Ticker(batch, asynchronous=True)
            # 同時抓取市值與公司簡介
            all_data = t.get_modules(['summaryDetail', 'assetProfile'])
            
            for s in batch:
                s_data = all_data.get(s)
                if not isinstance(s_data, dict):
                    continue
                
                summary = s_data.get('summaryDetail', {})
                profile = s_data.get('assetProfile', {})
                
                if isinstance(summary, dict):
                    mkt_cap = summary.get('marketCap')
                    
                    # 只要市值過關，就記錄下來
                    if isinstance(mkt_cap, (int, float)) and mkt_cap >= min_cap:
                        results.append({
                            "Symbol": s,
                            "Name": summary.get('shortName', s),
                            "MarketCap": mkt_cap,
                            "Price": summary.get('previousClose', 0.0),
                            "Sector": profile.get('sector', 'N/A') if isinstance(profile, dict) else 'N/A',
                            "Industry": profile.get('industry', 'N/A') if isinstance(profile, dict) else 'N/A'
                        })
        except Exception:
            continue
            
    progress_bar.empty()
    status_text.empty()
    return pd.DataFrame(results)

# --- 4. 側邊欄介面 ---
st.sidebar.header("⚙️ 篩選設定")
target_cap = st.sidebar.number_input("最低市值門檻 (USD)", value=500_000_000, step=100_000_000, format="%d")

st.sidebar.markdown("---")
st.sidebar.info("💡 **提示**\n\nETF 及部分基金可能不會顯示行業 (Industry) 及板塊 (Sector) 資料，會以 N/A 顯示。")

# --- 5. 主程式執行邏輯 ---
if st.button("🔍 開始掃描全市場", use_container_width=True):
    try:
        all_symbols = load_and_clean_symbols()
        
        if not all_symbols:
            st.error("❌ 找不到任何有效的股票代號！請確認 CSV 檔案存在且格式正確。")
        else:
            with st.spinner(f"已成功載入 {len(all_symbols)} 隻有效股票代號，正在執行篩選..."):
                df_result = fetch_and_screen_data(all_symbols, target_cap)
                
                if not df_result.empty:
                    st.success(f"✅ 掃描完成！找到 **{len(df_result)}** 隻市值 > {target_cap/1e6:.0f}M 的股票。")
                    st.balloons()
                    
                    # 顯示數據表格
                    st.dataframe(
                        df_result.sort_values("MarketCap", ascending=False),
                        column_config={
                            "Symbol": "代號",
                            "Name": "公司名稱",
                            "MarketCap": st.column_config.NumberColumn("市值 ($)", format="$%.2e"),
                            "Price": st.column_config.NumberColumn("股價 ($)", format="$%.2f"),
                            "Sector": "板塊",
                            "Industry": "行業"
                        },
                        use_container_width=True,
                        hide_index=True,
                        height=600
                    )
                    
                    # 提供 CSV 下載
                    csv = df_result.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 下載篩選結果 (CSV)",
                        data=csv,
                        file_name="us_stocks_screened.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.warning("⚠️ 沒有任何股票符合目前的篩選條件。")

    except Exception as e:
        st.error("🚨 程式運行出現非預期錯誤！")
        st.exception(e)
else:
    st.info("👈 請點擊上方的「開始掃描全市場」按鈕。")
