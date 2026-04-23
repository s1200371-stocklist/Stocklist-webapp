import os
import re
import json
import time
import random
import requests
import pandas as pd
import streamlit as st
import yfinance as yf

from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance

# ==========================================
# 1. 頁面設定
# ==========================================
st.set_page_config(
    page_title='🚀 美股全方位量化與 AI 平台',
    page_icon='📈',
    layout='wide'
)

# ==========================================
# 2. 基本工具函數
# ==========================================
def get_headers():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-':
            return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val:
            return float(val.replace('B', '')) * 1000
        if 'M' in val:
            return float(val.replace('M', ''))
        return float(val)
    except Exception:
        return 0.0

def safe_to_string(df, rows=8):
    try:
        if df is None or df.empty:
            return "無數據"
        return df.head(rows).to_string(index=False)
    except Exception:
        return "無數據"

def clean_ai_response(text):
    if not isinstance(text, str):
        return str(text)

    raw = text.strip()

    raw = re.sub(r"^```(?:json|text|markdown)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL | re.IGNORECASE)

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            if "choices" in parsed and parsed["choices"]:
                msg = parsed["choices"][0].get("message", {})
                if isinstance(msg, dict):
                    content = msg.get("content")
                    if isinstance(content, str) and content.strip():
                        return content.strip()
            if "content" in parsed and isinstance(parsed["content"], str):
                return parsed["content"].strip()
            if parsed.get("role") == "assistant":
                if isinstance(parsed.get("content"), str) and parsed["content"].strip():
                    return parsed["content"].strip()
            if isinstance(parsed.get("final"), str) and parsed["final"].strip():
                return parsed["final"].strip()
    except Exception:
        pass

    raw = re.sub(r'"reasoning_content"\s*:\s*".*?"\s*,?', '', raw, flags=re.DOTALL)
    raw = re.sub(r'"role"\s*:\s*"assistant"\s*,?', '', raw, flags=re.DOTALL)
    raw = re.sub(r'"content"\s*:\s*', '', raw, flags=re.DOTALL)
    raw = re.sub(r',?\s*"tool_calls"\s*:\s*\\[\s*\\]\s*', '', raw, flags=re.DOTALL)
    raw = re.sub(r',?\s*"tool_calls"\s*:\s*\\[.*?\\]\s*', '', raw, flags=re.DOTALL)
    raw = re.sub(r',?\s*"function_call"\s*:\s*\{.*?\}\s*', '', raw, flags=re.DOTALL)
    raw = re.sub(r',?\s*"finish_reason"\s*:\s*"tool_calls"\s*', '', raw, flags=re.DOTALL)

    raw = raw.replace('\\"', '"').replace('\\n', '\n').strip()
    raw = re.sub(r'^\{+', '', raw).strip()
    raw = re.sub(r'\}+$', '', raw).strip()

    bad_line_patterns = [
        r'^\s*we must\b.*$',
        r'^\s*let[\'"]s\b.*$',
        r'^\s*probably\b.*$',
        r'^\s*need to\b.*$',
        r'^\s*add insights\b.*$',
        r'^\s*also not use\b.*$',
        r'^\s*use plain text\b.*$',
        r'^\s*ensure we do not\b.*$',
        r'^\s*only the final report\b.*$',
        r'^\s*json\b.*$',
        r'^\s*role\b.*$',
        r'^\s*assistant\b.*$',
        r'^\s*reasoning_content\b.*$',
        r'^\s*tool_calls\b.*$',
    ]

    cleaned_lines = []
    for line in raw.splitlines():
        line_strip = line.strip()
        if not line_strip:
            cleaned_lines.append("")
            continue

        skip = False
        for p in bad_line_patterns:
            if re.match(p, line_strip, flags=re.IGNORECASE):
                skip = True
                break

        if not skip:
            cleaned_lines.append(line)

    raw = "\n".join(cleaned_lines)
    raw = raw.replace('","tool_calls":[]', '')
    raw = raw.replace('"tool_calls":[]', '')
    raw = raw.replace('tool_calls":[]', '')
    raw = raw.replace('","tool_calls":[],', '')
    raw = re.sub(r'\n{3,}', '\n\n', raw).strip()
    return raw

def final_text_sanitize(text):
    if not isinstance(text, str):
        return str(text)
    t = clean_ai_response(text)

    trailing_patterns = [
        r'","\s*tool_calls"\s*:\s*\\[\s*\\]\s*$',
        r',\s*"tool_calls"\s*:\s*\\[\s*\\]\s*$',
        r'"\s*,\s*"tool_calls"\s*:\s*\\[.*?\\]\s*$',
        r',\s*"tool_calls"\s*:\s*\\[.*?\\]\s*$',
        r'","\s*reasoning_content"\s*:\s*".*?$',
        r',\s*"reasoning_content"\s*:\s*".*?$',
        r'","\s*role"\s*:\s*"assistant".*?$',
        r',\s*"role"\s*:\s*"assistant".*?$',
    ]

    for p in trailing_patterns:
        t = re.sub(p, '', t, flags=re.DOTALL | re.IGNORECASE)

    t = t.replace('","tool_calls":[]', '')
    t = t.replace('"tool_calls":[]', '')
    t = t.replace('tool_calls":[]', '')
    t = t.replace('","tool_calls":[],', '')
    t = re.sub(r'\n{3,}', '\n\n', t).strip()
    return t

def call_pollinations(messages, model='openai-fast', timeout=60):
    try:
        response = requests.post(
            'https://text.pollinations.ai/',
            json={'messages': messages, 'model': model},
            timeout=timeout
        )
        return final_text_sanitize(response.text)
    except Exception as e:
        return f"⚠️ AI 發生錯誤: {e}"

# ==========================================
# 3. 個股新聞 / Bottom-Up AI
# ==========================================
def fetch_single_stock_news(ticker):
    news_items = []
    seen = set()

    try:
        ytkr = yf.Ticker(ticker)
        if hasattr(ytkr, 'news') and isinstance(ytkr.news, list):
            for item in ytkr.news[:8]:
                content = item.get('content', {}) if isinstance(item, dict) else {}
                title = content.get('title', item.get('title', ''))
                summary = content.get('summary', item.get('summary', ''))
                publisher = item.get('publisher', 'Yahoo Finance')

                title = str(title).strip()
                summary = str(summary).strip()

                if title and title not in seen:
                    seen.add(title)
                    news_items.append(f"[{publisher}] {title}｜{summary[:180]}")
    except Exception:
        pass

    try:
        news = finvizfinance(ticker).ticker_news()
        if news is not None and not news.empty:
            for _, row in news.head(8).iterrows():
                title = str(row.get('Title', '')).strip()
                source = str(row.get('Source', 'Finviz')).strip()
                if title and title not in seen:
                    seen.add(title)
                    news_items.append(f"[{source}] {title}")
    except Exception:
        pass

    if not news_items:
        news_items = [
            f"[System] {ticker} 近期市場消息較少，成交與情緒可能受大市風險偏好主導。",
            f"[System] 投資者現階段通常會聚焦 {ticker} 未來業績指引、增長持續性同估值是否過高。",
            f"[System] 如果缺乏新催化劑，股價短線通常更容易受市場整體風險情緒影響。"
        ]

    return news_items[:8]

def extract_stock_sentiment_output(text):
    allowed_labels = [
        "【🔥 極度看好】",
        "【📈 偏向樂觀】",
        "【⚖️ 中性觀望】",
        "【📉 偏向悲觀】",
        "【🧊 極度看淡】"
    ]
    fallback_label = "【⚖️ 中性觀望】"
    fallback_body = "市場消息面暫時未有一面倒優勢，利好與風險並存，現階段較適合保持審慎，等待更多業績、指引或催化消息再判斷後續方向。"

    cleaned = final_text_sanitize(text)
    lines = [line.strip() for line in cleaned.split('\n') if line.strip()]

    label = fallback_label
    body_lines = []

    for line in lines:
        if line in allowed_labels:
            label = line
            continue

        low = line.lower()
        if "reasoning_content" in low or "tool_calls" in low or '"role"' in low or '"content"' in low:
            continue
        if line.startswith("{") and line.endswith("}"):
            continue

        body_lines.append(line)

    body = final_text_sanitize("\n\n".join(body_lines).strip())
    if not body:
        body = fallback_body

    return label, body

def analyze_single_stock_sentiment(ticker, news_items):
    prompt = f"""
請你根據以下 {ticker} 最新新聞，用香港廣東話做一份 Bottom-Up 個股分析。

要求：
1. 第一行只可以輸出以下其中一個標籤：
【🔥 極度看好】
【📈 偏向樂觀】
【⚖️ 中性觀望】
【📉 偏向悲觀】
【🧊 極度看淡】

2. 之後用廣東話寫 3 至 5 段分析
3. 要講清楚利好、風險、短線催化劑
4. 唔好輸出 JSON
5. 唔好輸出英文 thinking / tool_calls / reasoning_content

新聞如下：
{chr(10).join(news_items)}
""".strip()

    raw = call_pollinations([
        {
            "role": "system",
            "content": "你係香港股票分析員，只可以用繁體中文廣東話回答，禁止輸出思考過程、JSON、tool_calls。"
        },
        {
            "role": "user",
            "content": prompt
        }
    ])

    label, body = extract_stock_sentiment_output(raw)
    return f"{label}\n\n{body}"

# ==========================================
# 4. 另類數據
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_data():
    try:
        res = requests.get('https://apewisdom.io/api/v1.0/filter/all-stocks/page/1', timeout=10)
        if res.status_code == 200:
            data = res.json().get('results', [])
            rows = []
            for item in data[:10]:
                diff = item.get('mentions', 0) - item.get('mentions_24h_ago', 0)
                trend = f"▲ +{diff}" if diff > 0 else (f"▼ {diff}" if diff < 0 else "▶ 0")
                rows.append({
                    'Ticker': item.get('ticker'),
                    'Sentiment': 'Bullish',
                    'Mentions': item.get('mentions'),
                    'Trend (24h)': trend
                })
            return pd.DataFrame(rows), "🟢 ApeWisdom Reddit 數據"
    except Exception as e:
        return pd.DataFrame(), f"🔴 Reddit 數據獲取失敗: {e}"

    return pd.DataFrame(), "🔴 Reddit 數據獲取失敗"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_x_sentiment():
    data = [
        {"Ticker": "NVDA", "Sentiment": "Bullish", "Mentions": 15402},
        {"Ticker": "TSLA", "Sentiment": "Bearish", "Mentions": 12301},
        {"Ticker": "AAPL", "Sentiment": "Neutral", "Mentions": 8900},
        {"Ticker": "AMD", "Sentiment": "Bullish", "Mentions": 7500},
        {"Ticker": "MSFT", "Sentiment": "Bullish", "Mentions": 6200},
        {"Ticker": "MSTR", "Sentiment": "Bullish", "Mentions": 5800},
        {"Ticker": "PLTR", "Sentiment": "Bullish", "Mentions": 5100},
        {"Ticker": "GOOGL", "Sentiment": "Neutral", "Mentions": 4800},
        {"Ticker": "META", "Sentiment": "Bullish", "Mentions": 4200},
        {"Ticker": "SMCI", "Sentiment": "Bearish", "Mentions": 3900}
    ]
    return pd.DataFrame(data), "🟡 X/FinTwit 預估熱度 (Top 10)"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_data():
    try:
        url = 'http://openinsider.com/insider-purchases-25k'
        res = requests.get(url, headers=get_headers(), timeout=15)
        dfs = pd.read_html(res.text)
        for df in dfs:
            if 'Ticker' in df.columns and 'Value' in df.columns and 'Trade Type' in df.columns:
                df = df[df['Trade Type'].astype(str).str.contains('Purchase|Buy', case=False, na=False)].copy()
                if not df.empty:
                    df = df.rename(columns={'Filing Date': 'Date', 'Insider Name': 'Insider'})
                    df['Source'] = 'OpenInsider'
                    cols = [c for c in ['Date', 'Ticker', 'Insider', 'Value', 'Source'] if c in df.columns]
                    return df[cols].head(10), "🟢 OpenInsider 真實數據"
    except Exception as e:
        return pd.DataFrame(), f"🔴 內部交易數據連接失敗: {e}"

    return pd.DataFrame(), "🔴 內部交易數據連接失敗"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_congress_data():
    trades = []
    try:
        s_res = requests.get(
            'https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json',
            timeout=10
        ).json()
        for t in s_res:
            if 'purchase' in str(t.get('type', '')).lower():
                trades.append({
                    'Date': t.get('transaction_date'),
                    'Ticker': t.get('ticker'),
                    'Politician': t.get('senator'),
                    'Source': 'Senate Disclosure'
                })
    except Exception:
        pass

    try:
        h_res = requests.get(
            'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json',
            timeout=10
        ).json()
        for t in h_res:
            if 'purchase' in str(t.get('type', '')).lower():
                trades.append({
                    'Date': t.get('transaction_date'),
                    'Ticker': t.get('ticker'),
                    'Politician': t.get('representative'),
                    'Source': 'House Disclosure'
                })
    except Exception:
        pass

    if trades:
        df = pd.DataFrame(trades)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Ticker']).sort_values('Date', ascending=False)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        return df[['Date', 'Ticker', 'Politician', 'Source']].head(10), "🟢 國會交易實時數據"

    return pd.DataFrame(), "🔴 國會交易獲取失敗"

# ==========================================
# 5. 技術分析
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def calculate_indicators(tickers):
    results = {}
    if not tickers:
        return results

    try:
        data = yf.download(tickers, period='1y', progress=False, auto_adjust=False)
        if data.empty:
            return results

        for t in tickers:
            try:
                if len(tickers) > 1:
                    close = data['Close'][t].ffill().dropna()
                else:
                    close = data['Close'].ffill().dropna()

                if len(close) < 130:
                    continue

                sma25 = close.rolling(25).mean().iloc[-1]
                sma125 = close.rolling(125).mean().iloc[-1]
                curr = close.iloc[-1]

                exp12 = close.ewm(span=12, adjust=False).mean()
                exp26 = close.ewm(span=26, adjust=False).mean()
                macd = exp12 - exp26
                signal = macd.ewm(span=9, adjust=False).mean()

                macd_status = "🔥 已經突破" if macd.iloc[-1] > signal.iloc[-1] else "⚖️ 走勢轉弱"
                if macd.iloc[-1] > signal.iloc[-1] and macd.iloc[-2] <= signal.iloc[-2]:
                    macd_status = "🚀 啱啱突破"

                results[t] = {
                    'SMA_Trend': curr > sma25 and sma25 > sma125,
                    'MACD_Status': macd_status,
                    'Price': f"${curr:.2f}"
                }
            except Exception:
                continue
    except Exception:
        pass

    return results

# ==========================================
# 6. UI
# ==========================================
with st.sidebar:
    st.title('📊 終極美股分析')
    mode = st.radio('選擇功能', ['🕵️ 另類數據雷達', '🎯 量化動能篩選', '🔍 個股 AI 驗證'])

if mode == '🕵️ 另類數據雷達':
    st.title('🕵️ 另類數據雷達')
    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("1. Reddit 散戶情緒 (Top 10)")
        df1, msg1 = fetch_reddit_data()
        st.caption(msg1)
        st.dataframe(df1, use_container_width=True, hide_index=True)

        st.subheader("3. X / FinTwit 社交熱度 (Top 10)")
        df3, msg3 = fetch_x_sentiment()
        st.caption(msg3)
        st.dataframe(df3, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("4. 內部高層真金白銀買入 (Top 10)")
        df4, msg4 = fetch_insider_data()
        st.caption(msg4)
        st.dataframe(df4, use_container_width=True, hide_index=True)

        st.subheader("5. 美國國會議員交易 (Top 10)")
        df5, msg5 = fetch_congress_data()
        st.caption(msg5)
        st.dataframe(df5, use_container_width=True, hide_index=True)

    if st.button('🚀 撰寫廣東話 AI 另類數據報告', type='primary', use_container_width=True):
        with st.spinner('AI 正在分析大戶與散戶博弈...'):
            prompt = f"""
請根據以下數據寫一份廣東話報告分析資金流向：

Reddit:
{safe_to_string(df1)}

X / FinTwit:
{safe_to_string(df3)}

Insider:
{safe_to_string(df4)}

Congress:
{safe_to_string(df5)}
"""
            report = call_pollinations([
                {
                    "role": "system",
                    "content": "你係香港頂級金融分析師，只可以用香港廣東話寫報告，直接輸出標題【🕵️ 另類數據 AI 偵測深度報告】。"
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ])
            st.markdown(final_text_sanitize(report))

elif mode == '🎯 量化動能篩選':
    st.title('🎯 RS x MACD 動能狙擊手')

    if st.button('🚀 開始全市場掃描 (市值 > 20億)', type='primary', use_container_width=True):
        with st.spinner('掃描 Finviz 中...'):
            try:
                f = Overview()
                f.set_filter(filters_dict={'Market Cap.': '+Mid (over $2bln)'})
                stocks = f.screener_view()
            except Exception as e:
                stocks = pd.DataFrame()
                st.error(f"Finviz 掃描失敗: {e}")

            if not stocks.empty:
                tickers = stocks['Ticker'].dropna().astype(str).tolist()[:50]
                indicators = calculate_indicators(tickers)
                final_rows = []

                for _, row in stocks.iterrows():
                    t = row.get('Ticker')
                    if t in indicators and indicators[t]['SMA_Trend']:
                        final_rows.append({
                            'Ticker': t,
                            'Price': indicators[t]['Price'],
                            'MACD': indicators[t]['MACD_Status'],
                            'Sector': row.get('Sector', 'N/A'),
                            'Mcap': row.get('Market Cap', 'N/A')
                        })

                st.write(f"✅ 搵到 {len(final_rows)} 隻符合動能排列股票：")
                st.dataframe(pd.DataFrame(final_rows), use_container_width=True, hide_index=True)
            else:
                st.error("無法獲取市場數據")

elif mode == '🔍 個股 AI 驗證':
    st.title('🔍 個股 AI 驗證 (Bottom-Up)')
    tkr = st.text_input("輸入股票代號 (如 NVDA):").upper().strip()

    if st.button('🧠 AI 深度分析', type='primary') and tkr:
        with st.spinner('抓取新聞與 AI 分析中...'):
            try:
                news_items = fetch_single_stock_news(tkr)

                if news_items:
                    res = analyze_single_stock_sentiment(tkr, news_items)
                    res = final_text_sanitize(res)

                    st.subheader(f"📊 {tkr} 驗證結果")
                    lines = [x.strip() for x in res.split('\n') if x.strip()]

                    if lines:
                        st.markdown(f"### {lines[0]}")
                        body = "\n\n".join(lines[1:]) if len(lines) > 1 else "暫無補充。"
                        with st.container(border=True):
                            st.markdown(final_text_sanitize(body))
                    else:
                        with st.container(border=True):
                            st.markdown(res)

                    with st.expander("📄 查看 AI 參考嘅原始新聞"):
                        for n in news_items:
                            st.caption(n)
                else:
                    st.warning("暫無相關新聞。")

            except NameError as e:
                st.error(f"❌ NameError：{e}")
            except Exception as e:
                st.error(f"❌ 執行分析時出錯：{e}")
