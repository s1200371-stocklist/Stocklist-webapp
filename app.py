import streamlit as st
import pandas as pd
from yahooquery import Ticker
import os
import re

# --- 1. 網頁配置 ---
st.set_page_config(page_title="美股篩選除錯診斷工具", layout="wide")

st.title("🔍 美股篩選：全流程除錯診斷版")
st.markdown("""
呢個版本專門用嚟排查點解股票數量變少。佢會話你知：
1. **CSV 讀到幾多隻？**
2. **Yahoo 認得幾多隻？**
3. **最後過到市值門檻有幾多隻？**
""")

# --- 2. 數據源診斷 (CSV 讀取) ---
@st.cache_data
def load_and_diagnose_symbols():
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    diag_info = {}
    all_raw_list = []
    
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f)
                cols = [c for c in df.columns if c.lower() in ['symbol', 'ticker']]
                if cols:
                    s_list = df[cols[0]].dropna().astype(str).str.strip().unique().tolist()
                    diag_info[f] = len(s_list)
                    all_raw_list.extend(s_list)
                else:
                    diag_info[f] = "錯誤：找不到 Symbol 欄位"
            except Exception as e:
                diag_info[f] = f"錯誤：{str(e)}"
        else:
            diag_info[f] = "不存在"

    # 清洗格式
    clean_symbols = []
    for s in list(set(all_raw_list)):
        formatted_s = re.sub(r'[\./]', '-', s.upper())
        if re.match(r'^[A-Z-]+$', formatted_s) and len(formatted_s) < 8:
            clean_symbols.append(formatted_s)
            
    return sorted(clean_symbols), diag_info, len(set(all_raw_list))

# --- 3. API 診斷 (數據抓取) ---
@st.cache_data(ttl=3600)
def fetch_with_diagnostics(symbols, min_cap):
    results = []
    log_data = {
        "api_not_found": [],   # Yahoo 查無此人
        "missing_mkt_cap": [], # 搵到股，但冇市值數據
        "below_threshold": [], # 市值唔夠
        "success": []          # 成功入選
    }
    
    batch_size = 100
    progress_bar = st.progress(0)
    
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        progress_bar.progress(min(i / len(symbols), 1.0))
        
        try:
            t = Ticker(batch, asynchronous=True)
            data = t.summary_detail
            
            for s in batch:
                s_info = data.get(s)
                
                # 診斷 A: Yahoo 認唔認得
                if not isinstance(s_info, dict) or not s_info:
                    log_data["api_not_found"].append(s)
                    continue
                
                mkt_cap = s_info.get('marketCap')
                
                # 診斷 B: 有冇市值數據
                if not isinstance(mkt_cap, (int, float)):
                    log_data["missing_mkt_cap"].append(s)
                    continue
                
                # 診斷 C: 門檻檢查
                if mkt_cap >= min_cap:
                    log_data["success"].append(s)
                    results.append({
                        "Symbol": s,
                        "Name": s_info.get('shortName', 'N/A'),
                        "MarketCap": mkt_cap,
                        "Price": s_info.get('previousClose', 0)
                    })
                else:
                    log_data["below_threshold"].append(s)
                    
        except Exception:
            continue
            
    progress_bar.empty()
    return pd.DataFrame(results), log_data

# --- 4. 介面與顯示 ---
target_cap = st.sidebar.number_input("最低市值門檻 (USD)", value=500_000_000, step=100_000_000)

if st.button("🚀 開始全流程診斷"):
    # 第一步：CSV 診斷
    clean_list, csv_diag, raw_total = load_and_diagnose_symbols()
    
    st.subheader("第一階段：數據源 (CSV) 診斷")
    col1, col2, col3 = st.columns(3)
    col1.metric("CSV 原始代號總數", raw_total)
    col2.metric("清洗後有效格式", len(clean_list))
    col3.metric("格式被篩走數量", raw_total - len(clean_list))
    
    with st.expander("查看各個 CSV 讀取詳情"):
        st.write(csv_diag)

    # 第二步：API 診斷
    st.markdown("---")
    st.subheader("第二階段：Yahoo API 抓取診斷")
    
    with st.spinner("正在逐一排查..."):
        df_result, logs = fetch_with_diagnostics(clean_list, target_cap)
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("最終入選", len(logs["success"]), delta_color="normal")
        c2.metric("Yahoo 查無此股", len(logs["api_not_found"]), delta="-❌")
        c3.metric("缺失市值數據", len(logs["missing_mkt_cap"]), delta="-⚠️")
        c4.metric("市值未達標", len(logs["below_threshold"]), delta="-📉")

        # 顯示原因分析
        st.info(f"💡 **診斷結論：** 你的 CSV 提供咗 {len(clean_list)} 隻股票，但 Yahoo 只認得其中 {len(clean_list) - len(logs['api_not_found'])} 隻。")

        if not df_result.empty:
            st.dataframe(df_result.sort_values("MarketCap", ascending=False), use_container_width=True)
            
            # 提供 Debug 詳細名單
            with st.expander("查看『查無此股』嘅名單 (前 50 隻)"):
                st.write(logs["api_not_found"][:50])
            
            with st.expander("查看『有股但冇市值數據』嘅名單 (前 50 隻)"):
                st.write(logs["missing_mkt_cap"][:50])
        else:
            st.error("掃描結束，但沒有任何股票符合條件。")
else:
    st.info("請點擊按鈕開始執行診斷程序。")
