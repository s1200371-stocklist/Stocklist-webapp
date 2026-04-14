import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime
import time

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="美股全量動能掃描器", page_icon="📈", layout="wide")

# --- 2. 強健的數據轉換函數 ---
def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except:
        return 0.0

# --- 3. Finviz 數據獲取 (快取 1 小時) ---
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        # 抓取所有大於 3 億美金嘅股票，包攬巨型股
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"連線至 Finviz 失敗: {e}")
        return pd.DataFrame()

# --- 4. yfinance 批量計算引擎 (包含防禦機制) ---
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_rs(tickers, benchmark="^GSPC", batch_size=200):
    """將龐大名單分批下載，降低記憶體壓力並防止 IP 封鎖"""
    rs_signals = {}
    
    # 步驟一：獨立下載基準指數
    bench_data = yf.download(benchmark, period="3mo", progress=False)['Close']
    if bench_data.empty: return rs_signals
    bench_norm = bench_data / bench_data.iloc[0]

    # 步驟二：分批處理股票
    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i+batch_size]
        
        try:
            data = yf.download(batch_tickers, period="3mo", progress=False)['Close']
            
            # 處理單隻股票回傳 Series 的特例
            if isinstance(data, pd.Series):
                data = data.to_frame(name=batch_tickers[0])
                
            data = data.ffill().dropna(how='all')
            
            # 運算 RS
            for ticker in batch_tickers:
                if ticker in data.columns and not data[ticker].dropna().empty:
                    stock_price = data[ticker].dropna()
                    if len(stock_price) > 25:
                        stock_norm = stock_price / stock_price.iloc[0]
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        
                        latest_rs = rs_line.iloc[-1]
                        latest_ma = rs_ma_25.iloc[-1]
                        rs_signals[ticker] = bool(latest_rs > latest_ma) # 確保是原生 bool
                    else:
                        rs_signals[ticker] = False
                else:
                    rs_signals[ticker] = False
        except Exception:
            # 如果某個批次報錯，直接將該批次設為 False，防止全盤崩潰
            for ticker in batch_tickers:
                rs_signals[ticker] = False
                
        # 強迫系統休息，保護 IP
        time.sleep(0.5) 
        
    return rs_signals

# --- 5. UI 介面 ---
st.title("🚀 美股全量強勢股掃描器")
st.caption(f"數據最後更新時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with st.sidebar:
    st.header("⚙️ 篩選參數")
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    
    st.markdown("---")
    enable_rs = st.checkbox("啟用 RS > 25D MA 技術過濾", value=False)
    
    if enable_rs:
        st.warning("⏳ 提示：全市場掃描涉及數千隻股票，因 Streamlit 雲端限制，首次運算可能需時 3-5 分鐘。請耐心等候，切勿重新整理。")

# --- 6. 執行主邏輯 ---
with st.spinner("正在從 Finviz 獲取並清洗基礎數據..."):
    raw_data = fetch_finviz_data()

if not raw_data.empty:
    df_processed = raw_data.copy()
    df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
    
    # 第一層：過濾市值
    final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].sort_values(by='Mcap_Numeric', ascending=False)
    
    # 第二層：運算並過濾 RS 動能
    if enable_rs:
        target_tickers = final_df['Ticker'].tolist()
        
        with st.spinner(f"正在全速運算 {len(target_tickers)} 隻股票嘅 RS 動能... ☕"):
            rs_results = calculate_all_rs(target_tickers)
            
            # 將運算結果 Map 回 DataFrame
            final_df['RS_Strong'] = final_df['Ticker'].map(rs_results)
            
            # 【關鍵修復】：處理 Pandas Boolean Ambiguity 錯誤
            # 確保欄位沒有 NaN，並強制轉換為 bool 陣列進行過濾
            mask = final_df['RS_Strong'].fillna(False).astype(bool)
            final_df = final_df[mask]
            
            st.success("✅ RS 動能篩選完成！")

    # 顯示結果
    st.metric(label="符合所有條件的股票數量", value=len(final_df))
    
    # 移除輔助欄位以美化表格
    display_cols = [c for c in final_df.columns if c not in ['Mcap_Numeric', 'RS_Strong']]
    st.dataframe(final_df[display_cols], use_container_width=True, height=600)
    
    # CSV 下載按鈕
    csv = final_df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 匯出清單 (CSV)", data=csv, file_name="strong_stocks.csv", mime="text/csv")
    
else:
    st.error("未能獲取初始數據，請檢查 Finviz 網絡狀態。")
