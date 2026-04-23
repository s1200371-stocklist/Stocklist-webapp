
import os
import re
import json
import time
import random
import datetime
import requests
import pandas as pd
import streamlit as st
import yfinance as yf
import concurrent.futures
from datetime import timedelta
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance

# ==========================================
# 1. 頁面設定與 UI 樣式
# ==========================================
st.set_page_config(
    page_title='🚀 美股全方位量化與 AI 平台',
    page_icon='📈',
    layout='wide'
)

# ==========================================
# 2. 核心工具函數
# ==========================================
def get_headers():
    """模擬真實瀏覽器，防止被 AWS S3 及 OpenInsider 封鎖"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/json,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive'
    }

def clean_ai_response(text):
    """清理 AI 回覆，刪除思考過程同英文草稿"""
    raw = text.strip()
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL | re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    lines = [line for line in raw.split('\n') if bool(re.search(r'[\u4e00-\u9fff]', line)) or len(line) < 60]
    return "\n".join(lines).strip()

def call_pollinations(messages):
    """調用 AI 分析引擎 (OpenAI 模式)"""
    try:
        response = requests.post('https://text.pollinations.ai/', json={'messages': messages, 'model': 'openai'}, timeout=60)
        return clean_ai_response(response.text)
    except Exception as e: return f"⚠️ AI 分析目前無法使用: {e}"

# ==========================================
# 3. 真實另類數據模組 (修復連線版)
# ==========================================

@st.cache_data(ttl=1800)
def fetch_reddit_data():
    """Reddit ApeWisdom: 增加 Trend 欄位"""
    try:
        res = requests.get('https://apewisdom.io/api/v1.0/filter/all-stocks/page/1', headers=get_headers(), timeout=10)
        if res.status_code == 200:
            results = res.json().get('results', [])
            rows = []
            for item in results[:10]:
                m = item.get('mentions', 0)
                m24 = item.get('mentions_24h_ago', m)
                diff = m - m24
                trend = f"▲ +{diff}" if diff > 0 else (f"▼ {diff}" if diff < 0 else "▶ 0")
                rows.append({
                    'Ticker': str(item.get('ticker')).upper(),
                    'Mentions': m,
                    'Trend (24h)': trend,
                    'Sentiment': 'Bullish' if m > m24 else 'Neutral'
                })
            return pd.DataFrame(rows), "🟢 ApeWisdom 實時數據"
    except: pass
    return pd.DataFrame(), "🔴 Reddit 數據連線失敗"

@st.cache_data(ttl=1800)
def fetch_x_sentiment(api_key=None):
    """X / FinTwit: 接收使用者輸入的 API Key"""
    if not api_key:
        return pd.DataFrame(), "🔴 無 X API 授權 (為保證準確，已拒絕顯示假數據)"
    try:
        url = "https://api.adanos.org/x-stocks/sentiment"
        res = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, params={"limit": 10}, timeout=12)
        if res.status_code == 200:
            stocks = res.json().get("stocks", [])
            if stocks:
                rows = [{"Ticker": str(i.get("ticker", "")).upper(), "Sentiment": "Bullish" if i.get("sentiment_score", 0)>=0.25 else "Bearish", "Mentions": i.get("mentions", 0)} for i in stocks[:10]]
                return pd.DataFrame(rows), "🟢 X / FinTwit API 正常"
    except: pass
    return pd.DataFrame(), "🔴 X API 連線失敗或 Key 無效"

@st.cache_data(ttl=3600)
def fetch_insider_data():
    """真實內部交易: OpenInsider + YFinance SEC 雙重備援"""
    # 嘗試 1: OpenInsider
    try:
        url = 'http://openinsider.com/insider-purchases-25k'
        res = requests.get(url, headers=get_headers(), timeout=15)
        dfs = pd.read_html(res.text)
        for df in dfs:
            if 'Ticker' in df.columns and 'Trade Type' in df.columns:
                buys = df[df['Trade Type'].str.contains('Purchase', na=False, case=False)].copy()
                if not buys.empty:
                    buys = buys[['Filing Date', 'Ticker', 'Insider Name', 'Value']].head(10)
                    buys.columns = ['Date', 'Ticker', 'Insider', 'Value']
                    buys['Source'] = 'OpenInsider'
                    return buys.reset_index(drop=True), "🟢 OpenInsider 官方 SEC 數據"
    except: pass
    
    # 嘗試 2: 若 OpenInsider 封鎖 IP，自動調用 YFinance 真實 SEC 數據做備援
    try:
        target_tickers = ['NVDA', 'TSLA', 'AAPL', 'MSFT', 'AMZN', 'META', 'PLTR', 'AMD', 'COIN', 'MSTR']
        results = []
        cutoff_date = pd.Timestamp.now(tz=None) - timedelta(days=60)
        
        for t in target_tickers:
            tkr = yf.Ticker(t)
            trades = tkr.insider_transactions
            if trades is not None and not trades.empty:
                df = trades.reset_index()
                # 尋找日期與交易類型欄位
                date_col = next((c for c in df.columns if 'date' in str(c).lower() or 'Start' in str(c)), None)
                text_col = next((c for c in df.columns if 'text' in str(c).lower() or 'trans' in str(c).lower()), None)
                
                if date_col and text_col:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.tz_localize(None)
                    df = df[df[date_col] >= cutoff_date]
                    buys = df[df[text_col].astype(str).str.contains('Buy|Purchase', case=False, na=False)]
                    
                    for _, row in buys.head(2).iterrows():
                        val = row.get('Value')
                        if pd.notna(val) and float(val) > 0:
                            results.append({
                                'Date': row[date_col].strftime('%Y-%m-%d'),
                                'Ticker': t,
                                'Insider': str(row.get('Insider', row.get('Name', '高層'))).title(),
                                'Value': f"${float(val):,.0f}",
                                'Source': 'YF SEC Data'
                            })
        if results:
            df_final = pd.DataFrame(results).sort_values('Date', ascending=False).head(10)
            return df_final.reset_index(drop=True), "🟡 YFinance 備援 (官方 SEC 申報)"
    except: pass
    return pd.DataFrame(), "🔴 OpenInsider 及備援均無法連線 (已拒絕顯示假數據)"

@st.cache_data(ttl=3600)
def fetch_congress_data():
    """國會交易數據: 已加入 headers 偽裝，修復 403 Forbidden 報錯"""
    trades = []
    try:
        # Senate & House 數據 (必須帶 Headers)
        s_res = requests.get('https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json', headers=get_headers(), timeout=10).json()
        h_res = requests.get('https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json', headers=get_headers(), timeout=10).json()
        
        for t in s_res + h_res:
            # 確保有 transaction_date 且 type 為 purchase
            if t.get('transaction_date') and 'purchase' in str(t.get('type')).lower():
                name = t.get('senator') if t.get('senator') else t.get('representative')
                source = 'Senate' if t.get('senator') else 'House'
                trades.append({'Date': t.get('transaction_date'), 'Ticker': t.get('ticker'), 'Politician': name, 'Source': source})
    except Exception as e: 
        return pd.DataFrame(), f"🔴 國會交易 API 報錯: {e}"
    
    if trades:
        df = pd.DataFrame(trades)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date', 'Ticker'])
        df = df[df['Ticker'].str.isalpha() == True] # 剔除亂碼 Ticker
        df = df.sort_values('Date', ascending=False)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        return df[['Date', 'Ticker', 'Politician', 'Source']].head(10).reset_index(drop=True), "🟢 國會官方披露數據"
        
    return pd.DataFrame(), "🔴 國會交易 API 無法連線"

# ==========================================
# 4. 量化分析與選股引擎 (RS/MACD)
# ==========================================

def calculate_indicators(tickers):
    """計算技術面指標"""
    results = {}
    if not tickers: return results
    try:
        data = yf.download(tickers, period='1y', progress=False)
        close = data['Close']
        if isinstance(close, pd.Series): close = close.to_frame(name=tickers[0])
        for t in tickers:
            if t in close.columns:
                s = close[t].dropna()
                if len(s) > 60:
                    sma20 = s.rolling(20).mean().iloc[-1]
                    sma50 = s.rolling(50).mean().iloc[-1]
                    curr = s.iloc[-1]
                    is_breakout = curr > sma20 and s.iloc[-2] <= s.rolling(20).mean().iloc[-2]
                    results[t] = {
                        'Status': '🚀 啱啱突破' if is_breakout else ('🔥 持續強勢' if curr > sma20 > sma50 else '⚖️ 整理中'),
                        'Signal': is_breakout
                    }
    except: pass
    return results

# ==========================================
# 5. Streamlit 主頁面邏輯
# ==========================================

def main():
    st.sidebar.title("🛠️ 功能導航")
    mode = st.sidebar.radio("請選擇模組:", ["另類數據雷達", "動能選股系統", "個股 AI 驗證"])
    
    # X API Key 輸入框 (可選)
    st.sidebar.markdown("---")
    user_x_api_key = st.sidebar.text_input("🔑 輸入 X API Key (選填，用於解鎖社交熱度)", type="password")

    if mode == "另類數據雷達":
        st.title("🕵️ 另類數據雷達 (100% 真實數據)")
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("1. Reddit 討論趨勢 (Trend)")
            df_r, msg_r = fetch_reddit_data()
            st.caption(msg_r)
            st.dataframe(df_r, use_container_width=True, hide_index=True)

            st.subheader("2. 國會議員買入 (最新)")
            df_c, msg_c = fetch_congress_data()
            st.caption(msg_c)
            st.dataframe(df_c, use_container_width=True, hide_index=True)

        with col2:
            st.subheader("3. 內部人士買入 (Insider)")
            df_i, msg_i = fetch_insider_buying()
            st.caption(msg_i)
            st.dataframe(df_i, use_container_width=True, hide_index=True)

            st.subheader("4. X / FinTwit 社交熱度")
            df_x, msg_x = fetch_x_sentiment(user_x_api_key)
            st.caption(msg_x)
            if not df_x.empty:
                st.dataframe(df_x, use_container_width=True, hide_index=True)

    elif mode == "動能選股系統":
        st.title("🎯 RS x SMA 動能選股")
        if st.button("執行全市場掃描 (Mid-Cap+)"):
            with st.spinner("掃描技術面中..."):
                screen = Overview()
                screen.set_filter(filters_dict={'Market Cap.': '+Mid (over $2bln)'})
                raw = screen.screener_view()
                if not raw.empty:
                    tickers = raw['Ticker'].head(50).tolist()
                    tech = calculate_indicators(tickers)
                    raw['技術狀態'] = raw['Ticker'].map(lambda x: tech.get(x, {}).get('Status', 'N/A'))
                    st.dataframe(raw[raw['技術狀態'].str.contains('突破|強勢', na=False)], use_container_width=True)

    elif mode == "個股 AI 驗證":
        st.title("🔍 個股 AI 深度驗證")
        ticker = st.text_input("輸入股票代碼:", "TSLA").upper()
        if st.button("開始 AI 分析") and ticker:
            with st.spinner("抓取消息中..."):
                tkr = yf.Ticker(ticker)
                news = "\n".join([f"- {n['title']}" for n in tkr.news[:5]])
                prompt = [{"role": "system", "content": "你係香港頂級股評人。只可以用廣東話，禁止輸出英文及JSON。"},
                          {"role": "user", "content": f"用香港廣東話總結 {ticker} 最新新聞並給出評分：\n{news}"}]
                st.markdown(call_pollinations(prompt))

if __name__ == "__main__":
    main()


