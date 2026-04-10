import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re

# --- 1. 網頁基本配置 ---
st.set_page_config(page_title="美股 500M+ 專業篩選器", layout="wide", page_icon="📈")

st.title("📈 全美股市值篩選器 (Pro Version)")
st.markdown("自動合併三大交易所名單，智能清洗代號，並極速抓取 Yahoo Finance 數據。")

# --- 2. 智能符號讀取與清洗 ---
@st.cache_data
def load_and_clean_symbols():
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    raw_symbols = []
    
    # 讀取所有 CSV
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f)
                # 自動尋找包含 symbol 或 ticker 的欄位
                cols = [c for c in df.columns if c.lower() in ['symbol', 'ticker']]
                if cols:
                    symbols = df[cols[0]].dropna().astype(str).str.strip().tolist()
                    raw_symbols.extend(symbols)
            except Exception as e:
                st.warning(f"讀取 {f} 時發生錯誤: {e}")
                
    clean_symbols = []
    # 符號清洗邏輯 (解決 BRK.B 變 BRK-B 的問題)
    for s in list(set(raw_symbols)):
        # 將 . 和 / 替換為 Yahoo 支援的 -
        formatted_s = re.sub(r'[\./]', '-', s.upper())
        # 只保留英文字母和橫線，且長度合理 (<8)
        if re.match(r'^[A-Z-]+$', formatted_s) and len(formatted_s) < 8:
            clean_symbols.append(formatted_s)
            
    return sorted(list(set(clean_symbols)))

# --- 3. 數據抓取與篩選核心 ---
@st.cache_data(ttl=3600) # 快取 1 小時
def fetch_and_screen_data(symbols, min_cap):
    results = []
    failed_logs = [] # 記錄失敗或被篩走的股票
    batch_size = 200 # 每批 200 隻，確保穩定
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        status_text.text(f"🚀 正在分析並向 Yahoo 請求數據: {i} / {len(symbols)}...")
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            t = Ticker(batch, asynchronous=True)
            data = t.summary_detail
            
            if not isinstance(data, dict):
                continue
                
            for s in batch:
                s_info = data.get(s)
                
                # 檢查是否有回傳正常字典數據
                if isinstance(s_info, dict):
                    mkt_cap = s_info.get('marketCap')
                    
                    # 嚴格檢查市值是否為數字 (防 TypeError)
                    if isinstance(mkt_cap, (int, float)):
                        if mkt_cap >= min_cap:
                            results.append({
                                "Symbol": s,
                                "Name": s_info.get('shortName', 'N/A'),
                                "MarketCap": mkt_cap,
                                "Price": s_info.get('previousClose', 0),
                                "Sector": s_info.get('sector', 'N/A')
                            })
                        else:
                            failed_logs.append({"Symbol": s, "Reason": f"市值過低 ({mkt_cap/1e6:.1f}M)"})
                    else:
                        failed_logs.append({"Symbol": s, "Reason": "Yahoo 未提供市值數據 (如 ETF/ADR)"})
                else:
                    failed_logs.append({"Symbol": s, "Reason": "Yahoo 查無此代號或 API 請求超時"})
                    
        except Exception as e:
            failed_logs.append({"Symbol": "BATCH_ERROR", "Reason": str(e)})
            continue
            
    progress_bar.empty()
    status_text.empty()
    
    return pd.DataFrame(results), failed_logs

# --- 4. 側邊欄控制介面 ---
st.sidebar.header("⚙️ 篩選設定")
target_cap = st.sidebar.number_input("最低市值門檻 (USD)", value=500_000_000, step=100_000_000, format="%d")
show_debug = st.sidebar.checkbox("🛠️ 開啟 Debug 模式 (查看被篩走的股票)", value=False)

st.sidebar.markdown("---")
st.sidebar.info("💡 **小提示**\n\n如果發現名單漏了某些股票，請開啟 Debug 模式查看原因。")

# --- 5. 主程式執行邏輯 ---
if st.button("🔍 開始掃描全市場", use_container_width=True):
    try:
        all_symbols = load_and_clean_symbols()
        
        if not all_symbols:
            st.error("❌ 找不到任何股票代號！請確認 `nasdaq-listed.csv` 等三個檔案與 `app.py` 放在同一資料夾內。")
        else:
            with st.spinner(f"已準備 {len(all_symbols)} 隻股票名單，正在執行大數據篩選..."):
                # 執行篩選
                df_result, logs = fetch_and_screen_data(all_symbols, target_cap)
                
                if not df_result.empty:
                    st.success(f"✅ 掃描完成！找到 **{len(df_result)}** 隻市值 > {target_cap/1e6:.0f}M 的股票。")
                    st.balloons()
                    
                    # 顯示數據表格 (使用安全的 NumberColumn 防 AttributeError)
                    st.dataframe(
                        df_result.sort_values("MarketCap", ascending=False),
                        column_config={
                            "Symbol": "代號",
                            "Name": "公司名稱",
                            "MarketCap": st.column_config.NumberColumn("市值 ($)", format="$%.2e"),
                            "Price": st.column_config.NumberColumn("股價", format="$%.2f"),
                            "Sector": "板塊"
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
                        file_name="screened_stocks.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.warning("⚠️ 沒有任何股票符合目前的篩選條件。")
                
                # 如果開啟了 Debug 模式，顯示失敗日誌
                if show_debug and logs:
                    st.markdown("---")
                    st.subheader("🛠️ Debug 日誌 (未入選名單)")
                    with st.expander("點擊展開查看被排除的股票及原因"):
                        log_df = pd.DataFrame(logs)
                        st.dataframe(log_df, use_container_width=True)

    except Exception as e:
        st.error("🚨 程式運行出現非預期錯誤！詳細資訊如下：")
        st.exception(e)
else:
    st.info("👈 請點擊上方的「開始掃描全市場」按鈕。")
