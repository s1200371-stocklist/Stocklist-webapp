import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime
import time

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 美股全量強勢股掃描器", page_icon="📈", layout="wide")

# --- 2. 強健的數據轉換函數 ---
def convert_mcap_to_float(val):
    """將 Finviz 嘅字串市值轉換為浮點數 (單位：Million)"""
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

# --- 4. yfinance 批量計算引擎 (終極防禦版) ---
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_rs(tickers, benchmark="SPY", batch_size=200):
    """使用 SPY 代替 ^GSPC，加入時區修復與防封鎖機制"""
    rs_signals = {}
    
    # 步驟一：獨立下載基準指數
    bench_data = yf.download(benchmark, period="3mo", progress=False)['Close']
    if bench_data.empty: 
        st.error(f"⚠️ 嚴重錯誤：無法下載基準指數 {benchmark}！請稍後再試。")
        return rs_signals
        
    # 【關鍵防禦】：強制消除基準數據的時區，避免 Pandas 報錯
    if bench_data.index.tz is not None:
        bench_data.index = bench_data.index.tz_localize(None)
        
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
            
            # 【關鍵防禦】：強制消除個股數據的時區
            if data.index.tz is not None:
                data.index = data.index.tz_localize(None)
            
            # 運算 RS 動能
            for ticker in batch_tickers:
                if ticker in data.columns and not data[ticker].dropna().empty:
                    stock_price = data[ticker].dropna()
                    if len(stock_price) > 25:
                        stock_norm = stock_price / stock_price.iloc[0]
                        # 確保日期完美對齊
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        
                        # 強制轉換為標準 float 及 bool
                        latest_rs = float(rs_line.iloc[-1])
                        latest_ma = float(rs_ma_25.iloc[-1])
                        rs_signals[ticker] = bool(latest_rs > latest_ma)
                    else:
                        rs_signals[ticker] = False
                else:
                    rs_signals[ticker] = False
        except Exception as e:
            # 遇錯不崩潰，將該批次設為 False
            for ticker in batch_tickers:
                rs_signals[ticker] = False
                
        # 強迫系統休息 0.5 秒，保護 IP
        time.sleep(0.5) 
        
    return rs_signals

# --- 5. UI 介面配置 ---
st.title("🚀 美股全量強勢股掃描器")
st.caption(f"數據最後更新時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with st.sidebar:
    st.header("⚙️ 篩選參數")
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    
    st.markdown("---")
    enable_rs = st.checkbox("📈 啟用 RS > 25D MA 篩選", value=False)
    
    if enable_rs:
        st.warning("⏳ 提示：全市場掃描涉及數千隻股票。因伺服器限制，首次運算可能需時 2-5 分鐘。請耐心等候，**切勿重新整理網頁**。")

# --- 6. 執行主邏輯與漏斗顯示 ---
with st.spinner("正在從 Finviz 獲取並清洗基礎數據..."):
    raw_data = fetch_finviz_data()

if not raw_data.empty:
    df_processed = raw_data.copy()
    df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
    
    # [顯示] 初始獲取數量
    st.markdown(f"**📊 第一關 (基礎名單)：** 成功從 Finviz 獲取 `{len(df_processed)}` 隻股票")
    
    # 第一層：過濾市值
    final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].sort_values(by='Mcap_Numeric', ascending=False)
    
    # [顯示] 市值過濾後數量
    st.markdown(f"**📊 第二關 (市值 > {min_mcap}M)：** 剩餘 `{len(final_df)}` 隻股票")
    
    if len(final_df) == 0:
        st.error("⚠️ 錯誤：市值過濾後剩下 0 隻！請檢查 Finviz 網站是否更改了格式。")
        st.stop()
    
    # 第二層：運算並過濾 RS 動能
    if enable_rs:
        target_tickers = final_df['Ticker'].tolist()
        
        with st.spinner(f"正在全速運算 {len(target_tickers)} 隻股票嘅 RS 動能... ☕"):
            rs_results = calculate_all_rs(target_tickers)
            
            # 檢查運算結果
            success_count = sum(1 for v in rs_results.values() if v is True)
            st.markdown(f"**📊 第三關 (RS 動能突破)：** 尋找到 `{success_count}` 隻強勢股")
            
            # 將結果寫入 DataFrame
            final_df['RS_Strong'] = final_df['Ticker'].map(rs_results)
            
            # 【終極防禦】：解決 Boolean Ambiguity 報錯
            mask = final_df['RS_Strong'].fillna(False).astype(bool)
            final_df = final_df[mask]
            
            if len(final_df) > 0:
                st.success("✅ 全市場篩選完成！")
            else:
                st.warning("⚠️ 警告：目前沒有股票符合 RS 動能條件。可能是大盤近期暴跌，覆巢之下無完卵。")

    st.markdown("---")
    
    # 顯示結果與表格
    if len(final_df) > 0:
        st.subheader(f"🎯 最終符合條件的股票清單 (共 {len(final_df)} 隻)")
        
        # 移除後台輔助欄位，讓畫面更乾淨
        display_cols = [c for c in final_df.columns if c not in ['Mcap_Numeric', 'RS_Strong']]
        st.dataframe(final_df[display_cols], use_container_width=True, height=600)
        
        # CSV 下載功能
        csv = final_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 匯出精選強勢股 (CSV)", data=csv, file_name="super_stocks_scan.csv", mime="text/csv")
        
else:
    st.error("未能獲取初始數據，請檢查網絡連線或稍後再試。")
