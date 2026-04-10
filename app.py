import streamlit as st
import pandas as pd

# 1. 設定網頁基本佈局
st.set_page_config(page_title="美股 500M+ 篩選器", layout="wide")

st.title("📈 美股篩選結果 (市值 > 500M USD)")
st.markdown("呢個 Web App 直接讀取你頭先篩選好嘅本地 CSV 檔案，速度極快！")

# 2. 讀取你剛剛生成的 CSV
try:
    # 嘗試讀取檔案
    df = pd.read_csv('screened_stocks_500M.csv')
    
    # 稍微格式化一下數字，等佢易睇啲 (將 MarketCap 轉為 Billion / Million)
    def format_market_cap(val):
        if val >= 1e9:
            return f"${val/1e9:.2f}B"
        else:
            return f"${val/1e6:.2f}M"
            
    # 複製一個 DataFrame 嚟做顯示，以免改動原始數據
    display_df = df.copy()
    display_df['MarketCap_Formatted'] = display_df['MarketCap'].apply(format_market_cap)
    
    # 重新排位，將格式化後嘅市值放前面
    display_df = display_df[['Symbol', 'Name', 'MarketCap_Formatted', 'Price', 'Sector']]
    display_df.columns = ['代號', '公司名稱', '市值', '股價', '板塊']

    # 3. 顯示數據面板
    col1, col2 = st.columns(2)
    col1.metric("符合條件股票總數", f"{len(df)} 隻")
    
    st.divider()
    
    # 4. 顯示互動表格 (內建排序、放大功能)
    st.dataframe(
        display_df,
        use_container_width=True, # 佔滿全螢幕闊度
        height=600,               # 設定高度
        hide_index=True           # 隱藏左邊無用嘅數字 index
    )

except FileNotFoundError:
    # 如果找不到 CSV，顯示錯誤提示
    st.error("⚠️ 搵唔到 `screened_stocks_500M.csv` 檔案！請確保你已經成功執行咗頭先個篩選腳本，並且 `app.py` 係同個 CSV 擺喺同一個 Folder。")
