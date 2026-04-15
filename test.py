import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime
import time

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 納指超級增長股掃描器", page_icon="📈", layout="wide")

# --- 2. 數據清洗函數 ---
def convert_mcap_to_float(val):
    """將 Finviz 的市值轉換為浮點數 (單位：百萬美元)"""
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except:
        return 0.0

# --- 3. Finviz 基礎數據獲取 (快取 1 小時) ---
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        # 直接拿取 >300M 的股票，減輕後續運算壓力
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"連線至 Finviz 失敗，請稍後再試: {e}")
        return pd.DataFrame()

# --- 4. yfinance 批量計算引擎 (🔥 納指專用防彈版) ---
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_rs(tickers, batch_size=200):
    rs_signals = {}
    
    # 【防禦機制 A】：納指專用基準輪換
    benchmarks_to_try = ["QQQ", "^NDX", "QQQM"]
    bench_data = pd.DataFrame()
    used_bench = ""

    for b in benchmarks_to_try:
        try:
            temp_data = yf.download(b, period="3mo", progress=False)
            if not temp_data.empty and 'Close' in temp_data.columns:
                close_data = temp_data['Close']
                # 兼容 yfinance 不同版本的回傳格式
                if isinstance(close_data, pd.Series):
                    bench_data = close_data.to_frame(name=b)
                else:
                    bench_data = close_data
                used_bench = b
                break # 成功獲取即跳出迴圈
        except Exception:
            continue
            
    if bench_data.empty: 
        st.error("⚠️ 嚴重錯誤：無法下載任何納指基準 (QQQ/NDX)！Yahoo API 可能已暫時封鎖你的 IP，請休息 15 分鐘後再試。")
        return rs_signals

    # 【防禦機制 B】：強制消除基準數據時區
    if bench_data.index.tz is not None:
        bench_data.index = bench_data.index.tz_localize(None)
        
    # 計算基準標準化
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]

    # 【防禦機制 C】：分批處理與限速
    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i+batch_size]
        
        try:
            data = yf.download(batch_tickers, period="3mo", progress=False)
            if data.empty or 'Close' not in data.columns:
                raise ValueError("No Close Data")
                
            close_prices = data['Close']
            if isinstance(close_prices, pd.Series):
                close_prices = close_prices.to_frame(name=batch_tickers[0])
                
            close_prices = close_prices.ffill().dropna(how='all')
            
            # 【防禦機制 D】：強制消除個股數據時區
            if close_prices.index.tz is not None:
                close_prices.index = close_prices.index.tz_localize(None)
            
            # 運算 RS 動能
            for ticker in batch_tickers:
                if ticker in close_prices.columns and not close_prices[ticker].dropna().empty:
                    stock_price = close_prices[ticker].dropna()
                    
                    if len(stock_price) > 25:
                        stock_norm = stock_price / stock_price.iloc[0]
                        # 確保日期完美對齊
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        
                        latest_rs = float(rs_line.iloc[-1])
                        latest_ma = float(rs_ma_25.iloc[-1])
                        rs_signals[ticker] = bool(latest_rs > latest_ma)
                    else:
                        rs_signals[ticker] = False
                else:
                    rs_signals[ticker] = False
                    
        except Exception as e:
            # 遇錯不崩潰，跳過該批次
            for ticker in batch_tickers:
                rs_signals[ticker] = False
                
        # 強迫系統休息 0.5 秒，保護 IP
        time.sleep(0.5) 
        
    return rs_signals

# --- 5. UI 側邊欄 ---
st.title("🚀 美股納指增長股掃描器")
st.caption(f"數據最後更新時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with st.sidebar:
    st.header("⚙️ 篩選參數")
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    
    st.markdown("---")
    enable_rs = st.checkbox("📈 啟用 RS > 25D MA (對比納指)", value=False)
    
    if enable_rs:
        st.warning("⏳ 提示：首次運算需從 Yahoo 獲取數千隻股票數據，預計需時 2-5 分鐘，請耐心等候，切勿重新整理網頁。")

# --- 6. 主程式邏輯與漏斗追蹤 ---
with st.spinner("正在連接 Finviz 獲取基礎名單..."):
    raw_data = fetch_finviz_data()

if not raw_data.empty:
    df_processed = raw_data.copy()
    df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
    
    # 【偵錯漏斗 1】
    st.info(f"📊 **第一關 (Finviz 基礎名單)**：獲取了 {len(df_processed)} 隻股票")
    
    final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].sort_values(by='Mcap_Numeric', ascending=False)
    
    # 【偵錯漏斗 2】
    st.info(f"📊 **第二關 (市值 > {min_mcap}M)**：剩餘 {len(final_df)} 隻股票")
    
    if len(final_df) == 0:
        st.error("⚠️ 錯誤：市值過濾後剩下 0 隻！")
        st.stop()
    
    if enable_rs:
        target_tickers = final_df['Ticker'].tolist()
        
        with st.spinner(f"正在全速運算 {len(target_tickers)} 隻股票的 RS 動能... ☕"):
            rs_results = calculate_all_rs(target_tickers)
            
            # 【偵錯漏斗 3】檢查 API 實際有幾多隻 True
            success_count = sum(1 for v in rs_results.values() if v is True)
            st.info(f"📊 **第三關 (跑贏納指)**：發現 {success_count} 隻超級強勢股")
            
            final_df['RS_Strong'] = final_df['Ticker'].map(rs_results)
            
            # 【防禦機制 E】：完美處理 Pandas Boolean Ambiguity
            mask = final_df['RS_Strong'].fillna(False).astype(bool)
            final_df = final_df[mask]
            
            if len(final_df) > 0:
                st.success("✅ 全市場技術掃描完成！")
            else:
                st.warning("⚠️ 警告：目前沒有股票能跑贏納指。可能科技股極度強勢，其他板塊疲弱。")

    st.markdown("---")
    
    # --- 7. 結果展示與匯出 ---
    if len(final_df) > 0:
        st.subheader(f"🎯 最終精選清單 (共 {len(final_df)} 隻)")
        
        # 移除輔助運算的欄位，讓表格保持乾淨
        display_cols = [c for c in final_df.columns if c not in ['Mcap_Numeric', 'RS_Strong']]
        st.dataframe(final_df[display_cols], use_container_width=True, height=600)
        
        # CSV 下載按鈕
        csv = final_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 匯出精選強勢股 (CSV)", data=csv, file_name="super_growth_stocks.csv", mime="text/csv")
        
else:
    st.error("未能獲取初始數據，請檢查網絡連線或稍後再試。")
