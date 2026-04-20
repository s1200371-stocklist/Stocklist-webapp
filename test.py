
import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance
import yfinance as yf
import datetime
import time
import concurrent.futures
import requests
import random
import re
import json

# --- 1. 專業版面配置 ---
st.set_page_config(page_title="🚀 美股全方位量化與 AI 平台", page_icon="📈", layout="wide")

# --- 2. 輔助/清洗函數 ---
def get_headers():
    """模擬真實瀏覽器 Header 防止被封鎖"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    ]
    return {'User-Agent': random.choice(user_agents)}

def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except: return 0.0

def clean_ai_response(text):
    if not isinstance(text, str): return str(text)
    text = text.strip()
    if text.startswith('{'):
        try:
            parsed = json.loads(text)
            text = parsed.get('content', parsed.get('choices', [{}])[0].get('message', {}).get('content', text))
        except: pass
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    marker = "【"
    if marker in text: text = text[text.find(marker):]
    return text.replace('\\n', '\n').replace('\\"', '"').strip()

# ==========================================
#        模組 C：另類數據雷達 (修復版)
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_sentiment():
    """抓取 Reddit WSB 熱門股票 (加入重試與 Header)"""
    url = "https://tradestie.com/api/v1/apps/reddit"
    for _ in range(3):
        try:
            response = requests.get(url, headers=get_headers(), timeout=15)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    return pd.DataFrame(data)
            time.sleep(1)
        except: continue
    return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    """獲取 Insider 買入 (增加錯誤處理)"""
    try:
        from finvizfinance.insider import Insider
        # 嘗試獲取最熱門的內部買入
        finsider = Insider(option='top insider trading recent buy')
        df = finsider.get_insider()
        if df is not None and not df.empty:
            return df
    except Exception as e:
        # 如果庫失效，嘗試直接透過 API 請求 (這裡作為備用提示)
        pass
    return pd.DataFrame()

# ==========================================
#        模組 A & B：量化與 AI 引擎 (完整還原)
# ==========================================
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except: return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, close_condition, batch_size=100, _progress_bar=None, _status_text=None):
    results = {}
    try:
        bench = yf.download("QQQ", period="2y", progress=False)['Close']
        if isinstance(bench, pd.DataFrame): bench = bench.iloc[:, 0]
        bench_norm = bench / bench.iloc[0]
    except: return results

    total = len(tickers)
    for i in range(0, total, batch_size):
        batch = tickers[i:i+batch_size]
        if _status_text: _status_text.markdown(f"**階段 2/3**: 計緊指標... (`{min(i+batch_size, total)}` / `{total}`)")
        if _progress_bar: _progress_bar.progress(min(1.0, (i + batch_size) / total))
        try:
            data = yf.download(batch, period="2y", progress=False)['Close']
            if isinstance(data, pd.Series): data = data.to_frame(name=batch[0])
            data = data.ffill()
            for t in batch:
                if t in data.columns:
                    s = data[t].dropna()
                    if len(s) > max(sma_short, sma_long):
                        sn = s / s.iloc[0]
                        aligned_bench = bench_norm.reindex(sn.index).ffill()
                        rs = (sn / aligned_bench) * 100
                        rs_ma = rs.rolling(25).mean()
                        rs_s = "🚀 啱啱突破" if rs.iloc[-1] > rs_ma.iloc[-1] and rs.iloc[-2] <= rs_ma.iloc[-2] else ("🔥 已經突破" if rs.iloc[-1] > rs_ma.iloc[-1] else "無")
                        
                        ema12, ema26 = s.ewm(span=12).mean(), s.ewm(span=26).mean()
                        m, sig = ema12 - ema26, (ema12 - ema26).ewm(span=9).mean()
                        macd_s = "🚀 啱啱突破" if m.iloc[-1] > sig.iloc[-1] and m.iloc[-2] <= sig.iloc[-2] else ("🔥 已經突破" if m.iloc[-1] > sig.iloc[-1] else "無")
                        
                        sm_s, sm_l = s.rolling(sma_short).mean(), s.rolling(sma_long).mean()
                        trend = sm_s.iloc[-1] > sm_l.iloc[-1]
                        results[t] = {'RS': rs_s, 'MACD': macd_s, 'SMA_Trend': trend}
        except: pass
    return results

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    results = []
    total = len(tickers)
    for idx, t in enumerate(tickers):
        try:
            tkr = yf.Ticker(t)
            q = tkr.quarterly_financials
            if q is None or q.empty: q = tkr.quarterly_income_stmt
            if not q.empty:
                eps = q.loc['Diluted EPS'].iloc[:2].values if 'Diluted EPS' in q.index else [0,0]
                rev = q.loc['Total Revenue'].iloc[:2].values if 'Total Revenue' in q.index else [0,0]
                results.append({'Ticker': t, 'EPS (最新)': f"{eps[0]:.2f}", 'Rev (最新)': f"{rev[0]/1e9:.1f}B"})
        except: pass
        if _progress_bar: _progress_bar.progress((idx+1)/total)
    return pd.DataFrame(results) if results else pd.DataFrame(columns=['Ticker', 'EPS (最新)', 'Rev (最新)'])

def analyze_news_ai(news_list):
    news_text = "".join([f"- {item['新聞標題']}\n" for item in news_list[:15]])
    system_prompt = "你係香港金融專家，用廣東話寫報告。第一句必須係：【📉 近月市場焦點總結】。"
    user_prompt = f"分析以下新聞並推薦潛力股代號：\n{news_text}"
    try:
        r = requests.post("https://text.pollinations.ai/", json={"messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], "model": "openai"}, timeout=30)
        return clean_ai_response(r.text)
    except: return "⚠️ AI 暫時忙碌。"

def analyze_alt_data_ai(reddit_df, insider_df):
    system_prompt = "你係香港策略分析師，用廣東話寫報告。第一句必須係：【🕵️ 另類數據 AI 偵測報告】。"
    r_str = reddit_df.head(10).to_string() if not reddit_df.empty else "無數據"
    i_str = insider_df.head(10).to_string() if not insider_df.empty else "無數據"
    user_prompt = f"分析 Reddit 與 Insider 數據：\nReddit:\n{r_str}\nInsider:\n{i_str}"
    try:
        r = requests.post("https://text.pollinations.ai/", json={"messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}], "model": "openai"}, timeout=30)
        return clean_ai_response(r.text)
    except: return "⚠️ AI 暫時忙碌。"

# ==========================================
#        UI 流程控制
# ==========================================
with st.sidebar:
    st.title("🧰 投資雙引擎")
    app_mode = st.radio("功能模組", ["🎯 狙擊手", "📰 AI 洞察", "🕵️ 另類數據"])

if app_mode == "🎯 狙擊手":
    st.title("🎯 RS x MACD 狙擊手")
    if st.button("🚀 開始掃描", use_container_width=True):
        raw = fetch_finviz_data()
        if not raw.empty:
            res = calculate_all_indicators(raw['Ticker'].tolist()[:100], 25, 125, "唔揀")
            st.write("掃描完成 (Demo 前 100 隻)")
            st.dataframe(pd.DataFrame(res).T)

elif app_mode == "📰 AI 洞察":
    st.title("📰 AI 新聞分析")
    if st.button("攞報告", use_container_width=True):
        with st.spinner("AI 讀緊新聞..."):
            st.markdown(analyze_news_ai([{"新聞標題": "Fed keeps rates steady", "來源": "Reuters"}]))

elif app_mode == "🕵️ 另類數據":
    st.title("🕵️ 另類數據雷達")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🌐 Reddit WSB 熱度")
        r_df = fetch_reddit_sentiment()
        if not r_df.empty: st.dataframe(r_df[['ticker', 'sentiment', 'no_of_comments']].head(10), hide_index=True)
        else: st.warning("⚠️ Reddit API 暫時拒絕連線，請稍後再試。")
    with col2:
        st.subheader("🏛️ Insider 內部買入")
        i_df = fetch_insider_buying()
        if not i_df.empty: st.dataframe(i_df.head(10), hide_index=True)
        else: st.warning("⚠️ Finviz Insider 模組連線受阻。")
    
    if st.button("🚀 AI 交叉分析", use_container_width=True):
        st.markdown(analyze_alt_data_ai(r_df, i_df))


