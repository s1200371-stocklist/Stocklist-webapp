import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
import yfinance as yf
import datetime
import time

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 美股全階段動能狙擊手", page_icon="🎯", layout="wide")

# --- 2. 數據清洗函數 ---
def convert_mcap_to_float(val):
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
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"連線至 Finviz 失敗，請稍後再試: {e}")
        return pd.DataFrame()

# --- 4. yfinance 批量計算引擎 (🔥 3重訊號精準判定版) ---
@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_rs(tickers, batch_size=200):
    rs_signals = {} 
    
    # 納指基準後備機制
    benchmarks_to_try = ["QQQ", "^NDX", "QQQM"]
    bench_data = pd.DataFrame()
    used_bench = ""

    for b in benchmarks_to_try:
        try:
            temp_data = yf.download(b, period="3mo", progress=False)
            if not temp_data.empty and 'Close' in temp_data.columns:
                close_data = temp_data['Close']
                if isinstance(close_data, pd.Series):
                    bench_data = close_data.to_frame(name=b)
                else:
                    bench_data = close_data
                used_bench = b
                break 
        except Exception:
            continue
            
    if bench_data.empty: 
        st.error("⚠️ 無法下載納指基準，請稍後再試。")
        return rs_signals

    # 消除基準時區
    if bench_data.index.tz is not None:
        bench_data.index = bench_data.index.tz_localize(None)
        
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]

    # 分批處理個股
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
            
            # 消除個股時區
            if close_prices.index.tz is not None:
                close_prices.index = close_prices.index.tz_localize(None)
            
            for ticker in batch_tickers:
                if ticker in close_prices.columns and not close_prices[ticker].dropna().empty:
                    stock_price = close_prices[ticker].dropna()
                    
                    # 確保有足夠日數計算 25MA 及獲取「昨日」數據 (最少 30 日防止新股報錯)
                    if len(stock_price) > 30: 
                        stock_norm = stock_price / stock_price.iloc[0]
                        aligned_bench = bench_norm.reindex(stock_norm.index).ffill()
                        
                        rs_line = stock_norm / aligned_bench * 100
                        rs_ma_25 = rs_line.rolling(window=25).mean()
                        
                        # 獲取「今日」及「尋日」的數據
                        latest_rs = float(rs_line.iloc[-1])
                        latest_ma = float(rs_ma_25.iloc[-1])
                        prev_rs = float(rs_line.iloc[-2])
                        prev_ma = float(rs_ma_25.iloc[-2])
                        
                        # 【核心判定邏輯：3大階段】
                        if latest_rs > latest_ma:
                            if prev_rs <= prev_ma:
                                rs_signals[ticker] = "🚀 剛剛突破"
                            else:
                                rs_signals[ticker] = "🔥 已經突破"
                        elif latest_rs >= latest_ma * 0.95:
                            rs_signals[ticker] = "🎯 即將突破 (<5%)"
                        else:
                            rs_signals[ticker] = "無"
                    else:
                        rs_signals[ticker] = "無"
                else:
                    rs_signals[ticker] = "無"
                    
        except Exception as e:
            for ticker in batch_tickers:
                rs_signals[ticker] = "無"
                
        time.sleep(0.5) # 保護 IP，防止封鎖
        
    return rs_signals

# --- 5. UI 側邊欄 ---
st.title("🎯 美股全階段動能狙擊手 (對比納指)")
st.caption(f"數據最後更新時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with st.sidebar:
    st.header("⚙️ 篩選參數")
    min_mcap = st.number_input("最低市值 (Million USD)", min_value=0.0, value=500.0, step=50.0)
    
    st.markdown("---")
    enable_rs = st.checkbox("📈 啟動 3 重階段動能掃描", value=False)
    
    if enable_rs:
        st.warning("⏳ 提示：正在運算全市場的動能階段。需時 2-5 分鐘，請耐心等候，切勿重新整理。")

# --- 6. 主程式邏輯與漏斗追蹤 ---
with st.spinner("正在連接 Finviz 獲取基礎名單..."):
    raw_data = fetch_finviz_data()

if not raw_data.empty:
    df_processed = raw_data.copy()
    df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
    
    st.info(f"📊 **第一關 (基礎名單)**：獲取了 {len(df_processed)} 隻股票")
    
    final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].sort_values(by='Mcap_Numeric', ascending=False)
    st.info(f"📊 **第二關 (市值 > {min_mcap}M)**：剩餘 {len(final_df)} 隻股票")
    
    if len(final_df) == 0:
        st.error("⚠️ 錯誤：市值過濾後剩下 0 隻！")
        st.stop()
    
    if enable_rs:
        target_tickers = final_df['Ticker'].tolist()
        
        with st.spinner(f"正在全速運算 {len(target_tickers)} 隻股票的動能階段... ☕"):
            rs_results = calculate_all_rs(target_tickers)
            
            # 寫入標籤
            final_df['RS_階段'] = final_df['Ticker'].map(rs_results).fillna("無")
            
            # 統計 3 種狀態
            count_sustained = len(final_df[final_df['RS_階段'] == "🔥 已經突破"])
            count_crossup = len(final_df[final_df['RS_階段'] == "🚀 剛剛突破"])
            count_potential = len(final_df[final_df['RS_階段'] == "🎯 即將突破 (<5%)"])
            
            st.info(f"📊 **第三關 (精準分類)**：尋找到 **{count_sustained}** 隻已經突破，**{count_crossup}** 隻剛剛突破，以及 **{count_potential}** 隻即將突破。")
            
            # 剔除沒有訊號的弱勢股
            final_df = final_df[final_df['RS_階段'] != "無"]
            
            if len(final_df) > 0:
                st.success("✅ 3 重動能掃描完成！")
            else:
                st.warning("⚠️ 警告：目前大盤可能處於極度弱勢，沒有發現任何突破或潛伏訊號。")

    st.markdown("---")
    
    # --- 7. 結果展示與匯出 ---
    if len(final_df) > 0:
        st.subheader(f"🎯 最終階段清單 (共 {len(final_df)} 隻)")
        
        # 【完美避開 KeyError 嘅智能排序】
        cols = ['Ticker']
        
        # 如果有開啟掃描先至加 'RS_階段' 入去
        if 'RS_階段' in final_df.columns:
            cols.append('RS_階段')
            
        # 將其餘欄位加入 (同時過濾走唔需要睇嘅 Mcap_Numeric 以及重複嘅 Ticker)
        cols += [c for c in final_df.columns if c not in cols and c != 'Mcap_Numeric']
        
        # 使用自訂排序，讓表格更實用
        st.dataframe(final_df[cols], use_container_width=True, height=600)
        
        csv = final_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 匯出狙擊清單 (CSV)", data=csv, file_name="rs_stages_stocks.csv", mime="text/csv")
        
else:
    st.error("未能獲取初始數據，請檢查網絡連線或稍後再試。")
