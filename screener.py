import streamlit as st
import requests
import pandas as pd
import time

# --- 核心配置 ---
API_KEY = "OAjgsk0vhIOmOu2SGhtjgKAsdWAxPVcx"
# 建議使用標準 v3 路徑，避免 stable 路徑兼容性問題
BASE_URL = "https://financialmodelingprep.com/api/v3"

st.set_page_config(page_title="FMP Matrix Pro", layout="wide")
st.title("🦅 FMP 7 維度動能矩陣 (修復版)")
st.caption("已修復 JSONDecodeError | 專注於行內名次超車排序")

def safe_get_json(url):
    """安全獲取 JSON，避免 JSONDecodeError"""
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"API 請求失敗，狀態碼: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"連線出錯: {e}")
        return None

def fetch_data():
    with st.spinner('📡 正在同步數據...'):
        # 1. 獲取篩選後的股票與行業資料 (Screener 是最穩定的 Sector 來源)
        # 設定市值 > 3億，確保有足夠流動性
        screener_url = f"{BASE_URL}/stock-screener?marketCapMoreThan=300000000&isEtf=false&isActivelyTrading=true&limit=1000&apikey={API_KEY}"
        stocks_data = safe_get_json(screener_url)
        
        if not stocks_data:
            return None
        
        df_base = pd.DataFrame(stocks_data)
        
        # 2. 批量獲取價格變動 (RS 核心數據)
        tickers = df_base['symbol'].tolist()
        # 為防 URL 過長，我們分批抓取
        all_perf = []
        batch_size = 300
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            perf_url = f"{BASE_URL}/stock-price-change/{','.join(batch)}?apikey={API_KEY}"
            p_data = safe_get_json(perf_url)
            if p_data and isinstance(p_data, list):
                all_perf.extend(p_data)
        
        if not all_perf:
            st.error("無法獲取表現數據。")
            return None
            
        df_perf = pd.DataFrame(all_perf)
        
        # 3. 合併數據 (以 symbol 為準)
        final_df = pd.merge(df_base, df_perf, on='symbol', how='inner')
        return final_df

# --- UI 與 運算 ---
if st.button("🚀 執行全美股深度分析"):
    start_time = time.time()
    data = fetch_data()
    
    if data is not None and not data.empty:
        # --- 7 維度運算 ---
        # 1. RS Rating
        data['RS_Now'] = (data['1Y'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        # 2. RS 3日變化 (模擬)
        data['RS_3D_Ago'] = (data['max'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        data['RS_Change'] = data['RS_Now'] - data['RS_3D_Ago']
        # 3. 3日緊湊度
        data['Tightness_3D'] = data['5D'].apply(lambda x: "✅" if x >= -5 else "❌")
        
        # --- 行業分析 ---
        sector_group = data.groupby('sector')['RS_Now'].mean()
        sector_group_3d = data.groupby('sector')['RS_3D_Ago'].mean()
        
        # 5. 行業大市排名
        data['Sec_Mkt_Rank'] = data['sector'].map((sector_group.rank(pct=True) * 98 + 1).astype(int))
        # 7. 行業排名變動
        sec_rank_3d = (sector_group_3d.rank(pct=True) * 98 + 1).astype(int)
        data['Sec_Rank_Chg'] = data['Sec_Mkt_Rank'] - data['sector'].map(sec_rank_3d)
        # 4. 板塊領頭羊
        data['Is_Leader'] = data.apply(lambda r: "👑 是" if r['RS_Now'] > sector_group[r['sector']] else "×", axis=1)
        # 6. 行內排名變動 (主要排序)
        data['In_Sec_Rank_Now'] = data.groupby('sector')['RS_Now'].rank(ascending=False)
        data['In_Sec_Rank_3D'] = data.groupby('sector')['RS_3D_Ago'].rank(ascending=False)
        data['In_Sec_Rank_Chg'] = (data['In_Sec_Rank_3D'] - data['In_Sec_Rank_Now']).astype(int)

        # --- 排序與顯示 ---
        result = data.sort_values(by='In_Sec_Rank_Chg', ascending=False)
        
        display = result[[
            'symbol', 'In_Sec_Rank_Chg', 'Is_Leader', 'sector', 
            'RS_Now', 'RS_Change', 'Tightness_3D', 'Sec_Mkt_Rank', 'Sec_Rank_Chg', 'price'
        ]].copy()
        
        display.columns = ['代碼', '行內超車名次', '板塊領軍', '行業', 'RS 分數', 'RS 變動', '3日緊湊度', '行業大市排名', '行業排名變動', '現價']

        def fmt(v):
            if v > 0: return f"▲ {int(v)}"
            if v < 0: return f"▼ {int(abs(v))}"
            return "0"
        
        for col in ['行內超車名次', 'RS 分動', '行業排名變動']:
            if col in display.columns:
                display[col] = display[col].apply(fmt)

        st.dataframe(display, use_container_width=True, height=700)
        st.success(f"✅ 掃描完成！耗時: {round(time.time() - start_time, 2)} 秒")
    else:
        st.warning("沒有找到符合條件的數據。")
