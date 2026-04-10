import streamlit as st
from yahooquery import Ticker
import pandas as pd
import time

st.set_page_config(page_title="RS Momentum Matrix", layout="wide")
st.title("🦅 全美股 7 維度動能矩陣 (防彈穩定版)")
st.caption("數據源：Yahoo Finance | 排序：尋找行業內名次躍升最強的黑馬")

@st.cache_data(ttl=86400)
def get_full_tickers():
    """獲取全美股名單"""
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
    try:
        tickers = pd.read_csv(url, header=None)[0].dropna().astype(str).tolist()
        return [t.strip().upper() for t in tickers if len(t) <= 4][:2000] # 限制 2000 確保速度
    except:
        return ["AAPL", "NVDA", "TSLA", "MSFT", "AMD", "META", "GOOGL", "AMZN"]

def get_sectors_safely(tickers, status_msg):
    """安全獲取行業資料，防範 Yahoo 封鎖"""
    sector_map = {}
    batch_size = 40  # 關鍵：小批次，不使用異步
    
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        status_msg.text(f"🏢 階段 2/3: 正在安全抓取行業資料... ({min(i+batch_size, len(tickers))}/{len(tickers)})")
        
        t = Ticker(batch) # 注意：這裡絕對不能加 asynchronous=True
        try:
            profiles = t.asset_profile
            for symbol in batch:
                if isinstance(profiles, dict) and symbol in profiles:
                    data = profiles[symbol]
                    if isinstance(data, dict):
                        sector_map[symbol] = data.get('sector', 'Unknown')
                    else:
                        sector_map[symbol] = 'Unknown_API_Error'
                else:
                    sector_map[symbol] = 'Unknown_Not_Found'
        except:
            for symbol in batch:
                sector_map[symbol] = 'Unknown_Network_Error'
        
        time.sleep(1.0) # 禮貌性延遲，保護 IP
    return sector_map

def run_momentum_scan(ticker_list):
    progress_bar = st.progress(0)
    status_msg = st.empty()
    
    # --- 階段 1: 批量獲取歷史股價 (可異步) ---
    batch_size = 150
    all_history = []
    
    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i:i+batch_size]
        status_msg.text(f"📡 階段 1/3: 抓取歷史報價... ({min(i+batch_size, len(ticker_list))}/{len(ticker_list)})")
        t = Ticker(batch, asynchronous=True)
        try:
            h = t.history(period="1y", interval="1d")
            if not h.empty and 'close' in h.columns:
                all_history.append(h)
        except:
            continue
        progress_bar.progress(min((i + batch_size) / len(ticker_list) * 0.4, 0.4))

    if not all_history:
        st.error("❌ 無法獲取股價數據，請稍後再試。")
        return pd.DataFrame()

    status_msg.text("🧮 正在運算市場動能...")
    full_h = pd.concat(all_history)
    close_prices = full_h['close'].unstack(level=0).ffill().dropna(how='all', axis=1)
    
    # 計算表現 (需確保數據足夠長)
    try:
        # 抓取首尾數據計算 1Y RS
        ret_now = (close_prices.iloc[-1] / close_prices.iloc[0]) - 1
        # 抓取 3 日前數據計算過往 RS
        ret_3d_ago = (close_prices.iloc[-4] / close_prices.iloc[0]) - 1
        # 計算近 5 日最大回撤 (緊湊度)
        recent_5d = close_prices.iloc[-5:]
        drop_3d = (recent_5d.min() - recent_5d.max()) / recent_5d.max()
    except Exception as e:
        st.error("數據長度不足以計算動能。")
        return pd.DataFrame()

    df = pd.DataFrame({
        'Symbol': close_prices.columns,
        'Price': close_prices.iloc[-1].values,
        'Ret_Now': ret_now.values,
        'Ret_3D': ret_3d_ago.values,
        'Drop_3D': drop_3d.values
    }).dropna()

    # 指標 1 & 2: 計算全域 RS
    df['RS_Now'] = (df['Ret_Now'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_3D_Ago'] = (df['Ret_3D'].rank(pct=True) * 98 + 1).astype(int)
    df['RS_Change'] = df['RS_Now'] - df['RS_3D_Ago']
    df['Tightness_3D'] = df['Drop_3D'].apply(lambda x: "✅" if x >= -0.05 else "❌")

    # --- 階段 2: 安全獲取強勢股行業 ---
    progress_bar.progress(0.6)
    # 只取全市場 RS 前 500 名來分析行業，節省時間且提高準確率
    top_tickers = df.sort_values(by='RS_Now', ascending=False).head(500)['Symbol'].tolist()
    sector_map = get_sectors_safely(top_tickers, status_msg)
    
    df['Sector'] = df['Symbol'].map(sector_map).fillna('Others')
    
    # 過濾掉錯誤或沒有行業的股票
    valid_df = df[~df['Sector'].str.contains('Unknown|Others')].copy()
    
    status_msg.text("🧠 階段 3/3: 構建矩陣排名...")
    progress_bar.progress(0.9)
    
    if not valid_df.empty:
        # 指標 5 & 7: 行業大市排名
        sec_now = valid_df.groupby('Sector')['RS_Now'].mean()
        sec_3d = valid_df.groupby('Sector')['RS_3D_Ago'].mean()
        
        sec_rank_now = (sec_now.rank(pct=True) * 98 + 1).astype(int)
        sec_rank_3d = (sec_3d.rank(pct=True) * 98 + 1).astype(int)
        
        df['Sec_Mkt_Rank'] = df['Sector'].map(sec_rank_now).fillna(0).astype(int)
        df['Sec_Rank_Chg'] = df['Sec_Mkt_Rank'] - df['Sector'].map(sec_rank_3d).fillna(0).astype(int)
        
        # 指標 4: 行業領頭羊
        df['Sec_Avg_RS'] = df['Sector'].map(sec_now)
        df['Is_Leader'] = df.apply(lambda r: "👑 是" if pd.notna(r['Sec_Avg_RS']) and r['RS_Now'] > r['Sec_Avg_RS'] else "×", axis=1)
        
        # 指標 6: 行內排名變動 (舊名次 - 新名次)
        df['In_Sec_Rank_Now'] = valid_df.groupby('Sector')['RS_Now'].rank(ascending=False)
        df['In_Sec_Rank_3D'] = valid_df.groupby('Sector')['RS_3D_Ago'].rank(ascending=False)
        # 如果沒有行業資料，設為 -999 以便排到最後
        df['In_Sec_Rank_Chg'] = (df['In_Sec_Rank_3D'] - df['In_Sec_Rank_Now']).fillna(-999)

    progress_bar.progress(1.0)
    status_msg.text("✅ 掃描成功完成！")
    return df

# --- UI 介面 ---
with st.sidebar:
    st.header("⚙️ 掃描設定")
    scan_limit = st.slider("掃描股票數量", 100, 2000, 500, help="數量越多，獲取行業資料越久。建議設在 500~1000 之間。")

if st.button("🚀 啟動深度矩陣分析"):
    start = time.time()
    
    res = run_momentum_scan(get_full_tickers()[:scan_limit])
    
    if not res.empty:
        st.divider()
        st.subheader("🎯 掃描結果 (按行業內躍升排序)")
        
        # 1. 核心排序：行內超車次數最多的排最前
        res = res.sort_values(by='In_Sec_Rank_Chg', ascending=False)
        
        # 2. 整理顯示欄位
        final = res[[
            'Symbol', 'In_Sec_Rank_Chg', 'Is_Leader', 'Sector', 
            'RS_Now', 'RS_Change', 'Tightness_3D', 'Sec_Mkt_Rank', 'Sec_Rank_Chg', 'Price'
        ]].copy()
        
        final.columns = ['代碼', '行內超車名次', '板塊領軍', '行業', 'RS 分數', 'RS 變動', '3日緊湊度', '行業大盤排名', '行業排名變動', '現價']
        
        # 3. 數值美化
        def fmt(v):
            if v == -999: return "N/A"
            try:
                if v > 0: return f"▲ {int(v)}"
                if v < 0: return f"▼ {int(abs(v))}"
                return "0"
            except:
                return "N/A"

        for c in ['行內超車名次', 'RS 變動', '行業排名變動']:
            final[c] = final[c].apply(fmt)
            
        st.dataframe(final, use_container_width=True, height=700)
        st.success(f"耗時: {round(time.time()-start, 1)} 秒。排在最前方的股票是近期資金突襲的對象。")
