import streamlit as st
import requests
import pandas as pd
import time

# --- 核心配置 ---
API_KEY = "OAjgsk0vhIOmOu2SGhtjgKAsdWAxPVcx"
STABLE_URL = "https://financialmodelingprep.com/stable"

st.set_page_config(page_title="FMP Matrix Pro", layout="wide")
st.title("🦅 FMP Stable 7 維度動能矩陣")
st.caption("使用接口: /stock-list & /stock-price-change | 排序: 行內超車名次")

def fetch_data():
    with st.spinner('📡 正在從 Stable 節點同步全市場數據...'):
        # 1. 攞全美股名單 (使用你提供的 /stock-list 接口)
        list_url = f"{STABLE_URL}/stock-list?apikey={API_KEY}"
        all_stocks = requests.get(list_url).json()
        
        # 過濾：只要股票 (Stock)，唔要 ETF 或其他，並限制前 3000 隻確保速度
        df_list = pd.DataFrame(all_stocks)
        df_list = df_list[df_list['type'] == 'stock'].head(3000)
        
        if df_list.empty:
            st.error("名單獲取失敗，請檢查 API Key。")
            return None

        # 2. 獲取行業資料 (Screener 接口喺攞 Sector 方面最快)
        # 我哋用 Screener 攞翻呢 3000 隻嘅行業，因為 stock-list 唔自帶 Sector
        screener_url = f"{STABLE_URL}/stock-screener?isEtf=false&limit=3000&apikey={API_KEY}"
        sector_data = requests.get(screener_url).json()
        df_sectors = pd.DataFrame(sector_data)[['symbol', 'sector', 'industry']]
        
        # 3. 批量獲取價格表現 (RS 核心)
        tickers = df_list['symbol'].tolist()
        all_perf = []
        batch_size = 500 
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]
            perf_url = f"{STABLE_URL}/stock-price-change/{','.join(batch)}?apikey={API_KEY}"
            p_data = requests.get(perf_url).json()
            if isinstance(p_data, list):
                all_perf.extend(p_data)
        
        df_perf = pd.DataFrame(all_perf)
        
        # --- 數據大合併 ---
        final_df = pd.merge(df_list, df_sectors, on='symbol', how='inner')
        final_df = pd.merge(final_df, df_perf, on='symbol', how='inner')
        return final_df

if st.button("🚀 執行全美股 3,000 隻深度分析"):
    start_time = time.time()
    data = fetch_data()
    
    if data is not None:
        # --- 7 維度動能運算中心 ---
        
        # 1. RS Rating (1Y 表現)
        data['RS_Now'] = (data['1Y'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        
        # 2. RS 3日變化 (模擬)
        data['RS_3D_Ago'] = (data['max'].rank(pct=True) * 98 + 1).fillna(0).astype(int)
        data['RS_Change'] = data['RS_Now'] - data['RS_3D_Ago']
        
        # 3. 3日緊湊度 (5日波幅)
        data['Tightness_3D'] = data['5D'].apply(lambda x: "✅" if x >= -5 else "❌")
        
        # --- 行業矩陣 ---
        sector_avg = data.groupby('sector')['RS_Now'].mean()
        sector_avg_3d = data.groupby('sector')['RS_3D_Ago'].mean()
        
        # 5. 行業大市排名
        data['Sec_Mkt_Rank'] = data['sector'].map((sector_avg.rank(pct=True) * 98 + 1).astype(int))
        
        # 7. 行業排名變動
        sec_rank_3d = (sector_avg_3d.rank(pct=True) * 98 + 1).astype(int)
        data['Sec_Rank_Chg'] = data['Sec_Mkt_Rank'] - data['sector'].map(sec_rank_3d)
        
        # 4. 板塊領頭羊
        data['Is_Leader'] = data.apply(lambda r: "👑 是" if r['RS_Now'] > sector_avg[r['sector']] else "×", axis=1)
        
        # 6. 行內排名變動 (排序關鍵)
        data['In_Sec_Rank_Now'] = data.groupby('sector')['RS_Now'].rank(ascending=False)
        data['In_Sec_Rank_3D'] = data.groupby('sector')['RS_3D_Ago'].rank(ascending=False)
        data['In_Sec_Rank_Chg'] = (data['In_Sec_Rank_3D'] - data['In_Sec_Rank_Now']).astype(int)

        # --- 最終結果與排序 ---
        result = data.sort_values(by='In_Sec_Rank_Chg', ascending=False)
        
        # 顯示欄位
        display = result[[
            'symbol', 'In_Sec_Rank_Chg', 'Is_Leader', 'sector', 
            'RS_Now', 'RS_Change', 'Tightness_3D', 'Sec_Mkt_Rank', 'Sec_Rank_Chg', 'price'
        ]].copy()
        
        display.columns = ['代碼', '行內超車名次', '板塊領軍', '行業', 'RS 分數', 'RS 變動', '3日緊湊度', '行業大市排名', '行業排名變動', '現價']

        def fmt(v):
            if v > 0: return f"▲ {int(v)}"
            if v < 0: return f"▼ {int(abs(v))}"
            return "0"
        
        for col in ['行內超車名次', 'RS 變動', '行業排名變動']:
            display[col] = display[col].apply(fmt)

        st.dataframe(display, use_container_width=True, height=700)
        st.success(f"✅ 完成！耗時: {round(time.time() - start_time, 2)} 秒")
