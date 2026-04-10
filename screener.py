import streamlit as st
from yahooquery import Ticker
import pandas as pd
import numpy as np
import time

# 頁面配置
st.set_page_config(page_title="US Market RS Matrix", layout="wide")
st.title("🦅 全美股 3,000 隻 — 7 維度動能矩陣終端")

# --- 1. 獲取美股名單 (建議限制在 3000 隻以保證速度) ---
@st.cache_data(ttl=86400)
def get_full_tickers():
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        # 下載全名單並過濾掉異常代碼
        tickers = pd.read_csv(url, header=None)[0].dropna().astype(str).tolist()
        return [t.strip().upper() for t in tickers if len(t) <= 4][:3000]
    except:
        # 備用名單
        return ["AAPL", "NVDA", "TSLA", "MSFT", "AMD", "META", "GOOGL", "AMZN"]

# --- 2. 核心分析引擎 ---
def run_momentum_scan(ticker_list):
    progress_bar = st.progress(0)
    status_msg = st.empty()
    
    # A. 批量獲取歷史股價 (分批處理以防 Timeout)
    batch_size = 150
    all_history = []
    
    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i:i+batch_size]
        status_msg.text(f"📡 階段 1/3: 正在抓取股價數據... ({i}/{len(ticker_list)})")
        
        t = Ticker(batch, asynchronous=True)
        try:
            h = t.history(period="1y", interval="1d")
            if not h.empty:
                all_history.append(h)
        except:
            continue
        progress_bar.progress(min((i + batch_size) / len(ticker_list) * 0.6, 0.6))

    if not all_history:
        st.error("❌ 無法獲取股價數據，請稍後再試。")
        return pd.DataFrame()

    # B. 運算全域 RS 排名
    status_msg.text("🧮 階段 2/3: 正在運算全市場 RS 排名 (1-99)...")
    full_h = pd.concat(all_history)
    # 重組數據表：行是日期，列是代碼
    close_prices = full_h['close'].unstack(level=0).ffill()
    
    # 計算漲幅與動能 (今日 vs 6個月前, 3日前 vs 6個月前)
    ret_now = (close_prices.iloc[-1] / close_prices.iloc[-126]) - 1
    ret_3d_ago = (close_prices.iloc[-4] / close_prices.iloc[-129]) - 1
    
    # 指標 3: 過去 3 日跌幅 (緊湊度)
    recent_3d = close_prices.iloc[-4:]
    drop_3d = (recent_3d.min() - recent_3d.max()) / recent_3d.max()

    df = pd.DataFrame({
        'Symbol': close_prices.columns,
        'Price': close_prices.iloc[-1].values,
        'Ret_Now': ret_today.values if 'ret_today' in locals() else ret_now.values,
        'Ret_3d': ret_3d_ago.values,
        'Drop_3d': drop_3d.values
    }).dropna()

    # 指標 1 & 2: RS 今日與 3 日變化
    df['RS_Now'] = (df['Ret_Now'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_3D_Ago'] = (df['Ret_3d'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_Change'] = df['RS_Now'] - df['RS_3D_Ago']
    df['Tightness_3D'] = df['Drop_3d'].apply(lambda x: "✅" if x >= -0.05 else "❌")

    # C. 行業深度分析 (只針對前 300 名，防止全部 Unknown)
    top_300 = df.sort_values(by='RS_Now', ascending=False).head(300)['Symbol'].tolist()
    status_msg.text("🏢 階段 3/3: 正在分析強勢股行業矩陣...")
    
    t_top = Ticker(top_300, asynchronous=True)
    profiles = t_top.asset_profile
    
    sector_map = {}
    for s in top_300:
        prof = profiles.get(s, {})
        if isinstance(prof, dict): # 解決 AttributeError
            sector_map[s] = prof.get('sector', 'Unknown')
        else:
            sector_map[s] = 'Unknown'
            
    df['Sector'] = df['Symbol'].map(sector_map).fillna('Others/Below Top 300')
    
    # 只針對有行業資料的股票計算行業排名 (指標 4, 5, 6, 7)
    valid_sec_df = df[df['Sector'] != 'Others/Below Top 300'].copy()
    
    if not valid_sec_df.empty:
        # 指標 5 & 7: 行業在大市的排名與變化
        sec_group_now = valid_sec_df.groupby('Sector')['RS_Now'].mean()
        sec_group_3d = valid_sec_df.groupby('Sector')['RS_3D_Ago'].mean()
        
        sec_rank_now = (sec_group_now.rank(pct=True) * 98 + 1).astype(int)
        sec_rank_3d = (sec_group_3d.rank(pct=True) * 98 + 1).astype(int)
        
        df['Sec_Mkt_Rank'] = df['Sector'].map(sec_rank_now).fillna(0).astype(int)
        df['Sec_Rank_Chg'] = df['Sec_Mkt_Rank'] - df['Sector'].map(sec_rank_3d).fillna(0).astype(int)
        
        # 指標 4: 該股票是否在行內領先
        df['Sec_Avg_RS'] = df['Sector'].map(sec_group_now)
        df['Is_Leader'] = df.apply(lambda r: "👑" if r['RS_Now'] > r['Sec_Avg_RS'] else "×", axis=1)
        
        # 指標 6: 該股票在行內的排名變動
        df['In_Sec_Rank_Now'] = valid_sec_df.groupby('Sector')['RS_Now'].rank(ascending=False)
        df['In_Sec_Rank_3D'] = valid_sec_df.groupby('Sector')['RS_3D_Ago'].rank(ascending=False)
        df['In_Sec_Rank_Chg'] = (df['In_Sec_Rank_3D'] - df['In_Sec_Rank_Now']).fillna(0).astype(int)

    progress_bar.progress(1.0)
    status_msg.text("✅ 全美股 7 維度掃描完成！")
    
    return df.sort_values(by='RS_Now', ascending=False)

# --- 3. Streamlit UI 介面 ---
with st.sidebar:
    st.header("⚙️ 掃描參數")
    scan_limit = st.slider("掃描股票數量", 100, 3000, 1000)
    st.info("提示：掃描 3,000 隻約需 2-4 分鐘，請耐心等候。")

if st.button("🚀 啟動全美股深度掃描"):
    all_symbols = get_full_tickers()[:scan_limit]
    start_time = time.time()
    
    result_df = run_momentum_scan(all_symbols)
    
    if not result_df.empty:
        st.divider()
        st.subheader("🎯 掃描結果 (按 RS 今日排名)")
        
        # 整理欄位名稱讓 User 易睇
        display_df = result_df[[
            'Symbol', 'Price', 'RS_Now', 'RS_Change', 'Tightness_3D', 
            'Is_Leader', 'In_Sec_Rank_Chg', 'Sector', 'Sec_Mkt_Rank', 'Sec_Rank_Chg'
        ]].copy()
        
        # 格式化顯示 (加上箭頭)
        def format_arrow(val):
            if val > 0: return f"▲ {val}"
            elif val < 0: return f"▼ {abs(val)}"
            return f"{val}"

        for col in ['RS_Change', 'In_Sec_Rank_Chg', 'Sec_Rank_Chg']:
            display_df[col] = display_df[col].apply(format_arrow)
            
        st.dataframe(display_df, use_container_width=True, height=600)
        st.success
