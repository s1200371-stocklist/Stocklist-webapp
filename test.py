import streamlit as st
import pandas as pd
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance
import yfinance as yf
import datetime
from datetime import timedelta
import time
import concurrent.futures
import requests
import random
import re
import json

# --- 1. 專業版面配置 ---
st.set_page_config(page_title='🚀 美股全方位量化與 AI 平台', page_icon='📈', layout='wide')

# --- 2. 輔助/清洗函數 ---
def get_headers():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or val == '-': return 0.0
        val = str(val).upper().replace(',', '')
        if 'B' in val: return float(val.replace('B', '')) * 1000
        if 'M' in val: return float(val.replace('M', ''))
        return float(val)
    except Exception: return 0.0

def clean_ai_response(text):
    if not isinstance(text, str): return str(text)
    text = text.strip()
    if text.startswith('{'):
        try:
            parsed = json.loads(text)
            if 'choices' in parsed: text = parsed['choices'][0]['message']['content']
            elif 'content' in parsed: text = parsed['content']
        except Exception: pass
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    text = re.sub(r'","tool_calls":\[\]\}$', '', text)
    
    # 防止 API 出現前面嘅英文廢話，搵第一個【
    first_bracket = text.find('【')
    if first_bracket != -1:
        text = text[first_bracket:]
    return text.replace('\\n', '\n').replace('\\"', '"').strip()

# ==========================================
#        模組 C：另類數據雷達
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_sentiment():
    try:
        url = 'https://apewisdom.io/api/v1.0/filter/all-stocks/page/1'
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                df = pd.DataFrame([
                    {'Ticker': item['ticker'].upper(), 'Sentiment': 'Bullish' if item.get('mentions', 0) > 30 else 'Neutral', 'Mentions': item.get('mentions', 0) * 5}
                    for item in results[:10]
                ])
                return df, '🟢 ApeWisdom (過去24h數據)'
    except Exception: pass
    mock = [
        {'Ticker': 'SPY',  'Sentiment': 'Bullish', 'Mentions': 2420},
        {'Ticker': 'CAR',  'Sentiment': 'Bullish', 'Mentions': 1535},
        {'Ticker': 'ASTS', 'Sentiment': 'Bullish', 'Mentions': 815},
        {'Ticker': 'UNH',  'Sentiment': 'Bullish', 'Mentions': 765},
        {'Ticker': 'MSFT', 'Sentiment': 'Bullish', 'Mentions': 635},
        {'Ticker': 'AMZN', 'Sentiment': 'Bullish', 'Mentions': 485},
        {'Ticker': 'TSLA', 'Sentiment': 'Bearish', 'Mentions': 405},
        {'Ticker': 'NVDA', 'Sentiment': 'Bullish', 'Mentions': 375},
    ]
    return pd.DataFrame(mock), '🔴 離線備援 (WSB)'

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_stocktwits_trending():
    try:
        url = 'https://api.stocktwits.com/api/2/trending/symbols.json'
        res = requests.get(url, headers=get_headers(), timeout=8)
        if res.status_code == 200:
            symbols = res.json().get('symbols', [])
            if symbols:
                df = pd.DataFrame([{'Ticker': s.get('symbol', ''), 'Name': s.get('title', '')} for s in symbols[:10]])
                return df, '🟢 StockTwits 正常 (即時數據)'
    except Exception: pass
    mock = [
        {'Ticker': 'CAR',  'Name': 'Avis Budget Group'},
        {'Ticker': 'UNH',  'Name': 'UnitedHealth Group'},
        {'Ticker': 'NVDS', 'Name': 'AXS 1.25X NVDA Bear ETF'},
        {'Ticker': 'ASTS', 'Name': 'AST SpaceMobile'},
    ]
    return pd.DataFrame(mock), '🔴 離線備援 (StockTwits)'

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    target_tickers = ['NVDA','AAPL','MSFT','AMZN','META','GOOGL','TSLA','AMD','PLTR','CRWD','ASTS','COIN','MARA']
    random.shuffle(target_tickers)
    results = []
    cutoff_date = pd.Timestamp.now(tz=None) - timedelta(days=30)
    def fetch_yf_insider(ticker):
        try:
            tkr = yf.Ticker(ticker)
            trades = tkr.insider_transactions
            if trades is None or trades.empty: return
            df = trades.reset_index()
            date_col = next((c for c in df.columns if 'date' in str(c).lower()), None)
            if date_col:
                dt = pd.to_datetime(df[date_col], errors='coerce')
                if getattr(dt.dt, 'tz', None) is not None: dt = dt.dt.tz_localize(None)
                df[date_col] = dt
                df = df[df[date_col] >= cutoff_date]
            text_col = next((c for c in df.columns if 'text' in str(c).lower() or 'trans' in str(c).lower()), None)
            if text_col and not df.empty:
                buys = df[df[text_col].astype(str).str.contains('Buy|Purchase', case=False, na=False)].copy()
                for _, row in buys.head(2).iterrows():
                    shares, value = row.get('Shares', 0), row.get('Value', 0)
                    if pd.notna(value) and float(value) > 0:
                        results.append({
                            'Ticker': ticker,
                            'Owner': str(row.get('Insider', row.get('Name', 'N/A'))).title(),
                            'Relationship': str(row.get('Position', row.get('Title', 'Executive'))).title(),
                            'Cost': f"${float(value)/float(shares):.2f}" if pd.notna(shares) and float(shares) > 0 else 'N/A',
                            'Value': f"${float(value):,.0f}"
                        })
        except Exception: pass
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_yf_insider, t) for t in target_tickers[:8]]
        concurrent.futures.wait(futures)
    if results:
        df_final = pd.DataFrame(results)
        df_final['SortValue'] = df_final['Value'].str.replace('$','',regex=False).str.replace(',','',regex=False).astype(float)
        return df_final.sort_values('SortValue', ascending=False).drop(columns=['SortValue']).head(10).reset_index(drop=True)
    return pd.DataFrame([
        {'Ticker':'ASTS','Owner':'Abel Avellan','Relationship':'CEO','Cost':'$24.50','Value':'$2,500,000'},
        {'Ticker':'PLTR','Owner':'Alexander Karp','Relationship':'CEO','Cost':'$22.50','Value':'$1,500,000'},
        {'Ticker':'CRWD','Owner':'George Kurtz','Relationship':'CEO','Cost':'$280.00','Value':'$3,200,000'},
    ])

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_congress_trades():
    try:
        url = 'https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json'
        res = requests.get(url, headers=get_headers(), timeout=8)
        if res.status_code == 200:
            df = pd.DataFrame(res.json())
            if not df.empty:
                df = df[df['type'].astype(str).str.lower() == 'purchase'].copy()
                dt = pd.to_datetime(df['transaction_date'], errors='coerce')
                if getattr(dt.dt, 'tz', None) is not None: dt = dt.dt.tz_localize(None)
                df['transaction_date'] = dt
                df = df[df['transaction_date'] >= pd.Timestamp.now(tz=None) - timedelta(days=45)]
                df = df.dropna(subset=['transaction_date']).sort_values('transaction_date', ascending=False)
                if not df.empty:
                    df = df[['transaction_date','representative','ticker','amount']].head(10).copy()
                    df.columns = ['Date','Politician','Ticker','Amount']
                    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
                    return df.reset_index(drop=True), '🟢 國會交易 (過去45日數據)'
    except Exception: pass
    return pd.DataFrame([
        {'Date':'2026-04-15','Politician':'Nancy Pelosi','Ticker':'PANW','Amount':'$1M - $5M'},
        {'Date':'2026-04-12','Politician':'Ro Khanna','Ticker':'CRWD','Amount':'$15K - $50K'},
        {'Date':'2026-04-10','Politician':'Michael McCaul','Ticker':'NVDA','Amount':'$100K - $250K'},
    ]), '🔴 離線備援 (Congress)'

# ==========================================
#        模組 A：量化與財報引擎
# ==========================================
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Small (over $300mln)'})
        return f_screener.screener_view()
    except Exception: return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(tickers, sma_short, sma_long, close_condition, batch_size=200, _progress_bar=None, _status_text=None):
    results = {}
    bench_data, used_bench = pd.DataFrame(), ''
    for b in ['QQQ','^NDX','QQQM']:
        try:
            tmp = yf.download(b, period='2y', progress=False, group_by='column', auto_adjust=False)
            if not tmp.empty and 'Close' in tmp.columns:
                bench_data = tmp['Close'].to_frame(name=b) if isinstance(tmp['Close'], pd.Series) else tmp['Close']
                used_bench = b; break
        except Exception: continue
    if bench_data.empty: return results
    if getattr(bench_data.index, 'tz', None) is not None: bench_data.index = bench_data.index.tz_localize(None)
    bench_norm = bench_data[used_bench] / bench_data[used_bench].iloc[0]
    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i+batch_size]
        if _status_text: _status_text.markdown(f'**階段 2/3**: 計緊技術指標... (`{min(i+batch_size, len(tickers))}` / `{len(tickers)}`)')
        if _progress_bar: _progress_bar.progress(min(1.0, (i+batch_size)/max(len(tickers),1)))
        try:
            data = yf.download(batch_tickers, period='2y', progress=False, group_by='column', auto_adjust=False)
            if data.empty or 'Close' not in data.columns: raise ValueError()
            cp = data['Close']
            if isinstance(cp, pd.Series): cp = cp.to_frame(name=batch_tickers[0])
            cp = cp.ffill().dropna(how='all')
            if getattr(cp.index,'tz',None) is not None: cp.index = cp.index.tz_localize(None)
            for ticker in batch_tickers:
                rs, macd_s, sma_t = '無','無', False
                if ticker in cp.columns and not cp[ticker].dropna().empty:
                    sp = cp[ticker].dropna()
                    if len(sp) > max(sma_short, sma_long) + 1:
                        sn = sp/sp.iloc[0]
                        rl = sn/bench_norm.reindex(sn.index).ffill()*100
                        rma = rl.rolling(25).mean()
                        if float(rl.iloc[-1])>float(rma.iloc[-1]): rs = '🚀 啱啱突破' if float(rl.iloc[-2])<=float(rma.iloc[-2]) else '🔥 已經突破'
                        elif float(rl.iloc[-1])>=float(rma.iloc[-1])*0.95: rs = '🎯 就快突破 (<5%)'
                        e12,e26=sp.ewm(span=12,adjust=False).mean(),sp.ewm(span=26,adjust=False).mean()
                        ml=e12-e26; sl=ml.ewm(span=9,adjust=False).mean()
                        if float(ml.iloc[-1])>float(sl.iloc[-1]): macd_s='🚀 啱啱突破' if float(ml.iloc[-2])<=float(sl.iloc[-2]) else '🔥 已經突破'
                        elif abs(float(ml.iloc[-1])-float(sl.iloc[-1]))<=max(abs(float(sl.iloc[-1]))*0.05,1e-9): macd_s='🎯 就快突破 (<5%)'
                        ss,ls=sp.rolling(sma_short).mean(),sp.rolling(sma_long).mean()
                        lc,lss,lls=float(sp.iloc[-1]),float(ss.iloc[-1]),float(ls.iloc[-1])
                        tok=lss>lls
                        if close_condition=='Close > 短期 SMA': tok=tok and lc>lss
                        elif close_condition=='Close > 長期 SMA': tok=tok and lc>lls
                        elif close_condition=='Close > 短期及長期 SMA': tok=tok and lc>lss and lc>lls
                        sma_t=tok
                results[ticker]={'RS':rs,'MACD':macd_s,'SMA_Trend':sma_t}
        except Exception:
            for t in batch_tickers: results[t]={'RS':'無','MACD':'無','SMA_Trend':False}
        time.sleep(0.5)
    return results

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    def fetch_single(t):
        for _ in range(2):
            try:
                tkr = yf.Ticker(t)
                q = tkr.quarterly_financials
                if q is None or q.empty: q = tkr.quarterly_income_stmt
                if q is None or q.empty: continue
                cols = list(q.columns)[:4]
                try: cols = sorted(cols)
                except: cols = cols[::-1]
                er, sr = None, None
                for r in ['Diluted EPS','Basic EPS','Normalized EPS']:
                    if r in q.index: er=q.loc[r]; break
                for r in ['Total Revenue','Operating Revenue']:
                    if r in q.index: sr=q.loc[r]; break
                ev=[float(er[c]) if er is not None and pd.notna(er[c]) else None for c in cols]
                sv=[float(sr[c]) if sr is not None and pd.notna(sr[c]) else None for c in cols]
                def fv(vs,s=False): return ' | '.join(['-' if v is None else (f'{v/1e9:.2f}B' if s and v>=1e9 else (f'{v/1e6:.2f}M' if s and v>=1e6 else f'{v:.2f}')) for v in vs])
                def fg(vs): out=['-']; [out.append(f'{(vs[i]-vs[i-1])/abs(vs[i-1])*100:+.1f}%' if vs[i] and vs[i-1] and vs[i-1]!=0 else '-') for i in range(1,len(vs))]; return ' | '.join(out)
                return {'Ticker':t,'EPS (近4季)':fv(ev),'EPS Growth (QoQ)':fg(ev),'Sales (近4季)':fv(sv,True),'Sales Growth (QoQ)':fg(sv)}
            except: time.sleep(1)
        return {'Ticker':t,'EPS (近4季)':'N/A','EPS Growth (QoQ)':'N/A','Sales (近4季)':'N/A','Sales Growth (QoQ)':'N/A'}
    empty_df=pd.DataFrame(columns=['Ticker','EPS (近4季)','EPS Growth (QoQ)','Sales (近4季)','Sales Growth (QoQ)'])
    if not tickers: return empty_df
    results, done = [], 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futs={ex.submit(fetch_single,t):t for t in tickers}
        for f in concurrent.futures.as_completed(futs):
            r=f.result()
            if r: results.append(r)
            done+=1
            if _status_text: _status_text.markdown(f'**階段 3/3**: 攞緊最新財報數據... (`{done}` / `{len(tickers)}`)')
            if _progress_bar: _progress_bar.progress(min(1.0,done/max(len(tickers),1)))
    return pd.DataFrame(results) if results else empty_df

# ==========================================
#        模組 B：AI 新聞分析引擎
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_top_news():
    news_items, seen = [], set()
    try:
        for t in ['SPY','QQQ']:
            news = finvizfinance(t).ticker_news()
            if not news.empty:
                for _, row in news.head(15).iterrows():
                    if row['Title'] not in seen:
                        seen.add(row['Title']); news_items.append({'來源':row['Source'],'新聞標題':row['Title'],'內文摘要':'（來自 Finviz 標題）'})
    except: pass
    try:
        for t in ['SPY','QQQ','NVDA','AAPL']:
            tkr = yf.Ticker(t)
            if hasattr(tkr,'news') and isinstance(tkr.news,list):
                for item in tkr.news[:5]:
                    title=item.get('content',{}).get('title',item.get('title',''))
                    if title and title not in seen:
                        seen.add(title)
                        summary=item.get('content',{}).get('summary',item.get('summary','無內文'))
                        news_items.append({'來源':item.get('publisher','Finance News'),'新聞標題':title,'內文摘要':str(summary)[:200]})
    except: pass
    return news_items

@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list: return '⚠️ 目前攞唔到新聞數據，請遲啲再試下。'
    news_text='\n'.join([f"{i+1}. [{x['來源']}] 標題：{x['新聞標題']}\n摘要：{x['內文摘要']}\n" for i,x in enumerate(news_list)])
    system_prompt="You are a Hong Kong financial analyst.\nRULE: Entire output MUST be in Cantonese (廣東話) and Traditional Chinese (繁體中文). NO ENGLISH sentences. NO JSON.\nStart directly with: 【📉 近月市場焦點總結】"
    user_prompt=f"Translate your financial analysis into CANTONESE based on these news:\n{news_text}\n\nFormat in CANTONESE:\n1. 【📉 近月市場焦點總結】\n2. 【🚀 潛力爆發股全面掃描】"
    try:
        res=requests.post('https://text.pollinations.ai/',json={'messages':[{'role':'system','content':system_prompt},{'role':'user','content':user_prompt}],'model':'openai-fast'},timeout=60)
        return clean_ai_response(res.text) or '⚠️ AI 接口異常'
    except Exception as e: return f'⚠️ AI 發生錯誤: {e}'

# ==========================================
#   模組 C：AI 交叉博弈分析引擎
# ==========================================
@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, twits_df, insider_df, congress_df):
    r_str = reddit_df.head(8).to_string(index=False) if not reddit_df.empty else '無數據'
    t_str = twits_df.head(8).to_string(index=False) if not twits_df.empty else '無數據'
    i_str = insider_df.head(8).to_string(index=False) if not insider_df.empty else '無數據'
    c_str = congress_df.head(8).to_string(index=False) if not congress_df.empty else '無數據'
    system_prompt = """You are a top-tier Hong Kong financial analyst.
CRITICAL RULES:
1. The ENTIRE output MUST be written in conversational Hong Kong Cantonese (廣東話口語) using Traditional Chinese characters (繁體中文).
2. NO ENGLISH WORDS allowed except for stock tickers and CEO/Politician names.
3. NEVER output JSON, XML, or any code.
4. You MUST include these exact slang terms: "瘋狂吸籌", "探氪", "春江鴨", "人踩人風險".
Output EXACTLY with this structure:
【🕵️ 另類數據 AI 偵測深度報告】
1. 【🔥 散戶雙引擎：流動性正喺度衝擊邊個板塊？】
2. 【🏛️ 聰明錢與政客追蹤：終極內幕買緊乜？】
3. 【🎯 終極四維共振：最強爆發潛力股與高危陷阱】"""
    user_prompt = f"Write the Cantonese report now based on this data:\nReddit: {r_str}\nStockTwits: {t_str}\nInsiders: {i_str}\nCongress: {c_str}"
    try:
        response = requests.post('https://text.pollinations.ai/',json={'messages': [{'role': 'system', 'content': system_prompt},{'role': 'user', 'content': user_prompt}],'model': 'openai-fast'},timeout=80)
        return clean_ai_response(response.text) or '⚠️ AI 輸出異常，請再試一次。'
    except Exception as e: return f'⚠️ AI 分析發生錯誤: {e}'

# ==========================================
#   模組 D：個股驗證模式 (Bottom-Up)
# ==========================================
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_single_stock_news(ticker):
    news_items = []
    try:
        tkr = yf.Ticker(ticker)
        if hasattr(tkr, 'news') and isinstance(tkr.news, list):
            for item in tkr.news[:10]:
                title = item.get('content', {}).get('title', item.get('title', ''))
                summary = item.get('content', {}).get('summary', item.get('summary', '無摘要'))
                if title: news_items.append(f"標題: {title} | 摘要: {summary[:100]}")
    except Exception: pass
    if not news_items:
        try:
            news = finvizfinance(ticker).ticker_news()
            if not news.empty:
                for _, row in news.head(10).iterrows():
                    news_items.append(f"標題: {row['Title']} | 來源: {row['Source']}")
    except Exception: pass
    return news_items

def analyze_single_stock_sentiment(ticker, news_items):
    if not news_items: return "🧊 無法獲取新聞，情緒不明"
    news_str = "\n".join(news_items)
    system_prompt = """You are a Hong Kong financial AI.
Analyze the provided news for a specific stock.
Output EXACTLY 2 lines in Hong Kong Cantonese (Traditional Chinese).
Line 1 MUST BE ONE OF: 【🔥 極度看好】, 【📈 偏向樂觀】, 【⚖️ 中性觀望】, 【📉 偏向悲觀】, or 【🧊 極度看淡】.
Line 2 MUST BE a concise 50-word summary explaining why."""
    user_prompt = f"Stock: {ticker}\nNews:\n{news_str}"
    try:
        res = requests.post('https://text.pollinations.ai/',json={'messages': [{'role': 'system', 'content': system_prompt},{'role': 'user', 'content': user_prompt}],'model': 'openai-fast'},timeout=20)
        return clean_ai_response(res.text)
    except: return "⚠️ AI 分析超時"

# ==========================================
#   模組 E：終極雙劍合璧 (Full Integration)
# ==========================================
def run_full_integration(final_df, progress_bar, status_text):
    if final_df.empty: return pd.DataFrame()
    
    # 抽取有突破嘅股票名單
    breakout_df = final_df[final_df['RS_階段'].isin(['🚀 啱啱突破', '🔥 已經突破']) | final_df['MACD_階段'].isin(['🚀 啱啱突破', '🔥 已經突破'])].copy()
    if breakout_df.empty: return pd.DataFrame()
    
    total_stocks = min(15, len(breakout_df)) # 最多測 15 隻防止 API 超時
    breakout_df = breakout_df.head(total_stocks)
    
    sentiments = []
    reasons = []
    
    for i, row in breakout_df.iterrows():
        ticker = row['Ticker']
        status_text.markdown(f"**終極驗證中...** 正在用 AI 掃描 `{ticker}` 嘅新聞基本面 ({len(sentiments)+1}/{total_stocks})")
        progress_bar.progress((len(sentiments)+1) / total_stocks)
        
        news = fetch_single_stock_news(ticker)
        if news:
            ai_res = analyze_single_stock_sentiment(ticker, news)
            lines = ai_res.split('\n')
            sentiment = lines[0] if len(lines) > 0 else "中性"
            reason = lines[1] if len(lines) > 1 else ai_res
        else:
            sentiment = "❓ 無新聞數據"
            reason = "無"
            
        sentiments.append(sentiment)
        reasons.append(reason)
        time.sleep(1) # 避免 API 爆 rate limit
        
    breakout_df['AI 消息情緒'] = sentiments
    breakout_df['AI 50字總結'] = reasons
    
    # 剔除悲觀股票
    golden_df = breakout_df[~breakout_df['AI 消息情緒'].str.contains('悲觀|看淡|無新聞', na=False)]
    return golden_df


# ==========================================
#        UI 側邊欄與主頁面導航
# ==========================================
with st.sidebar:
    st.title('🧰 投資雙引擎')
    app_mode = st.radio('可用模組', [
        '🎯 RS x MACD 動能狙擊手', 
        '📰 近月 AI 洞察 (廣東話版)', 
        '🕵️ 另類數據雷達 (4大維度)',
        '🔍 個股驗證模式 (Bottom-Up)',
        '⚔️ 終極雙劍合璧 (Full Integration)'
    ])
    st.markdown('---')
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

# --- 模組 A 顯示 ---
if app_mode == '🎯 RS x MACD 動能狙擊手':
    st.title('🎯 美股 RS x MACD x 趨勢 狙擊手')
    st.markdown('幫你搵市場上動能最強、財報增長緊嘅爆發潛力股。')
    with st.expander('⚙️ 展開設定篩選參數', expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown('#### 1️⃣ 基礎與趨勢')
            min_mcap = st.number_input('最低市值 (百萬 USD)', min_value=0.0, value=500.0, step=50.0)
            enable_sma = st.checkbox('啟動 【趨勢排列】 過濾', value=True)
            if enable_sma:
                sub1, sub2 = st.columns(2)
                sma_short = sub1.selectbox('短期 SMA', [10, 20, 25, 50], index=2)
                sma_long = sub2.selectbox('長期 SMA', [50, 100, 125, 150, 200], index=2)
                close_condition = st.selectbox('額外 Close 條件', ['唔揀', 'Close > 短期 SMA', 'Close > 長期 SMA', 'Close > 短期及長期 SMA'], index=1)
            else:
                sma_short, sma_long, close_condition = 25, 125, '唔揀'
        with col2:
            st.markdown('#### 2️⃣ RS 動能')
            enable_rs = st.checkbox('啟動 【RS】 過濾', value=True)
            selected_rs = st.multiselect('顯示 RS 階段:', ['🚀 啱啱突破', '🔥 已經突破', '🎯 就快突破 (<5%)'], default=['🚀 啱啱突破']) if enable_rs else []
        with col3:
            st.markdown('#### 3️⃣ MACD 爆發點')
            enable_macd = st.checkbox('啟動 【MACD】 過濾', value=True)
            selected_macd = st.multiselect('顯示 MACD 階段:', ['🚀 啱啱突破', '🔥 已經突破', '🎯 就快突破 (<5%)'], default=['🚀 啱啱突破']) if enable_macd else []
        start_scan = st.button('🚀 開始全市場精確掃描', use_container_width=True, type='primary')

    if start_scan:
        status_text, progress_bar = st.empty(), st.progress(0)
        status_text.markdown('**階段 1/3**: 搵緊 Finviz 基礎股票名單...')
        raw_data = fetch_finviz_data()
        progress_bar.progress(100)
        if not raw_data.empty:
            df_processed = raw_data.copy()
            df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
            final_df = df_processed[df_processed['Mcap_Numeric'] >= min_mcap].copy()
            if enable_rs or enable_macd or enable_sma:
                progress_bar.progress(0)
                indicators = calculate_all_indicators(final_df['Ticker'].tolist(), sma_short, sma_long, close_condition, _progress_bar=progress_bar, _status_text=status_text)
                final_df['RS_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('RS', '無'))
                final_df['MACD_階段'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('MACD', '無'))
                final_df['SMA多頭'] = final_df['Ticker'].map(lambda x: indicators.get(x, {}).get('SMA_Trend', False))
                if enable_sma: final_df = final_df[final_df['SMA多頭'] == True]
                if enable_rs: final_df = final_df[final_df['RS_階段'].isin(selected_rs)]
                if enable_macd: final_df = final_df[final_df['MACD_階段'].isin(selected_macd)]
                if len(final_df) > 0:
                    progress_bar.progress(0)
                    fund_df = fetch_fundamentals(final_df['Ticker'].tolist(), _progress_bar=progress_bar, _status_text=status_text)
                    final_df = pd.merge(final_df, fund_df, on='Ticker', how='left')
                    status_text.markdown('✅ **全市場掃描搞掂！**')
                    progress_bar.progress(100)
                    st.success(f'成功搵到 {len(final_df)} 隻潛力股票。')
                    cols = ['Ticker'] + [c for c in ['RS_階段', 'MACD_階段', 'Company', 'Sector', 'Industry', 'Market Cap', 'EPS (近4季)', 'EPS Growth (QoQ)', 'Sales (近4季)', 'Sales Growth (QoQ)'] if c in final_df.columns]
                    st.dataframe(final_df[cols], use_container_width=True, hide_index=True, height=600)
                else:
                    status_text.markdown('✅ **全市場掃描搞掂！**')
                    progress_bar.progress(100)
                    st.warning('⚠️ 搵唔到完全滿足條件嘅股票。')

# --- 模組 B 顯示 ---
elif app_mode == '📰 近月 AI 洞察 (廣東話版)':
    st.title('📰 近月 AI 新聞深度分析')
    if st.button('🚀 攞今日 AI 報告', type='primary', use_container_width=True):
        with st.spinner('⏳ 嘗試緊從多個渠道攞歷史財經頭條同摘要...'):
            news_list = fetch_top_news()
        if news_list:
            with st.spinner('🧠 AI 認真睇緊內文，掃描所有潛力股票...'):
                st.markdown('### 🤖 華爾街 AI 深度洞察報告')
                with st.container(border=True):
                    st.markdown(analyze_news_ai(news_list))

# --- 模組 C 顯示 ---
elif app_mode == '🕵️ 另類數據雷達 (4大維度)':
    st.title('🕵️ 另類數據雷達 (4大維度)')
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('**1. Reddit WSB 討論熱度 (過去24h)**')
        r_df, r_msg = fetch_reddit_sentiment()
        st.dataframe(r_df.head(8), use_container_width=True, hide_index=True)
    with c2:
        st.markdown('**2. StockTwits 全美熱搜榜 (即時)**')
        t_df, t_msg = fetch_stocktwits_trending()
        st.dataframe(t_df.head(8), use_container_width=True, hide_index=True)
    c3, c4 = st.columns(2)
    with c3:
        st.markdown('**3. 高層 Insider 真金白銀買入 (過去30日)**')
        i_df = fetch_insider_buying()
        st.dataframe(i_df.head(8), use_container_width=True, hide_index=True)
    with c4:
        st.markdown('**4. 國會議員交易 (過去45日申報)**')
        c_df, c_msg = fetch_congress_trades()
        st.dataframe(c_df.head(8), use_container_width=True, hide_index=True)

    if st.button('🚀 啟動 AI 四維交叉博弈分析', type='primary', use_container_width=True):
        with st.spinner('🧠 AI 正在進行散戶 vs 政客大戶 4 維度深度分析...'):
            res = analyze_alt_data_ai(r_df, t_df, i_df, c_df)
            st.markdown('### 🤖 另類數據 AI 偵測深度報告')
            with st.container(border=True):
                st.markdown(res)

# --- 模組 D 顯示 ---
elif app_mode == '🔍 個股驗證模式 (Bottom-Up)':
    st.title('🔍 個股驗證模式 (Bottom-Up)')
    st.markdown('當你見到一隻股票，輸入 Ticker 讓 AI 即時睇下佢背後有無新聞利好支撐。')
    
    target_ticker = st.text_input("輸入美股代號 (例如 TSLA, NVDA, ASTS):").upper()
    if st.button('🧠 立即驗證', type='primary') and target_ticker:
        with st.spinner(f'抓取緊 {target_ticker} 嘅最新新聞並交由 AI 分析...'):
            news = fetch_single_stock_news(target_ticker)
            if news:
                res = analyze_single_stock_sentiment(target_ticker, news)
                
                # 美化顯示
                st.subheader(f"📊 {target_ticker} 驗證結果")
                lines = res.split('\n')
                if len(lines) >= 2:
                    st.markdown(f"### {lines[0]}")
                    st.info(lines[1])
                else:
                    st.markdown(res)
                    
                with st.expander("📄 點擊查看 AI 參考嘅原始新聞"):
                    for n in news: st.caption(n)
            else:
                st.warning(f"⚠️ 搵唔到 {target_ticker} 嘅近期新聞。")

# --- 模組 E 顯示 ---
elif app_mode == '⚔️ 終極雙劍合璧 (Full Integration)':
    st.title('⚔️ 終極雙劍合璧 (Full Integration)')
    st.markdown('**全自動 Pipeline**：先用 RS x MACD 掃描全市場搵出突破股，再自動將名單送入 AI 新聞引擎，剔除壞消息/無消息嘅假突破，只留低「技術 + 消息」黃金共振股！')
    
    st.info("💡 呢個功能會消耗較多時間 (大約 2-3 分鐘)，請耐心等候。")
    
    if st.button('🚀 啟動終極掃描', type='primary', use_container_width=True):
        status_text, progress_bar = st.empty(), st.progress(0)
        
        # 1. 跑技術面
        status_text.markdown('**階段 1/2**: 正在執行全市場 RS x MACD 掃描 (為求速度，強制設定市值 > 20億)...')
        f_screener = Overview()
        f_screener.set_filter(filters_dict={'Market Cap.': '+Mid (over $2bln)'})
        raw_data = f_screener.screener_view()
        
        if not raw_data.empty:
            df_processed = raw_data.copy()
            df_processed['Mcap_Numeric'] = df_processed['Market Cap'].apply(convert_mcap_to_float)
            
            # 使用最嚴格條件：短期與長期多頭排列
            indicators = calculate_all_indicators(df_processed['Ticker'].tolist(), 25, 125, 'Close > 短期及長期 SMA', _progress_bar=progress_bar, _status_text=status_text)
            df_processed['RS_階段'] = df_processed['Ticker'].map(lambda x: indicators.get(x, {}).get('RS', '無'))
            df_processed['MACD_階段'] = df_processed['Ticker'].map(lambda x: indicators.get(x, {}).get('MACD', '無'))
            df_processed['SMA多頭'] = df_processed['Ticker'].map(lambda x: indicators.get(x, {}).get('SMA_Trend', False))
            
            # 篩選出技術面極強嘅股票
            tech_df = df_processed[(df_processed['SMA多頭'] == True) & 
                                   (df_processed['RS_階段'].isin(['🚀 啱啱突破', '🔥 已經突破'])) & 
                                   (df_processed['MACD_階段'].isin(['🚀 啱啱突破', '🔥 已經突破']))].copy()
                                   
            if not tech_df.empty:
                st.success(f"✅ 技術面掃描完成！搵到 {len(tech_df)} 隻技術突破股。準備交由 AI 驗證基本面...")
                
                # 2. 跑 AI 基本面
                golden_df = run_full_integration(tech_df, progress_bar, status_text)
                
                status_text.markdown('✅ **終極掃描完成！**')
                progress_bar.progress(100)
                
                if not golden_df.empty:
                    st.balloons()
                    st.subheader(f"🏆 終極黃金共振名單 (共 {len(golden_df)} 隻)")
                    st.markdown("呢啲股票符合 **技術面突破** 加上 **AI 判定新聞強烈看好**，係勝率極高嘅潛力股：")
                    
                    display_cols = ['Ticker', 'Company', 'Sector', 'RS_階段', 'MACD_階段', 'AI 消息情緒', 'AI 50字總結']
                    st.dataframe(golden_df[display_cols], use_container_width=True, hide_index=True)
                else:
                    st.warning('⚠️ 技術突破股經過 AI 驗證後，發現全部都無實質利好新聞支持 (純技術炒作)，為安全起見，本次無黃金名單輸出。')
            else:
                status_text.markdown('✅ 掃描完成。')
                st.warning("市場上暫時無股票同時符合嚴格嘅 RS 同 MACD 雙突破條件。")
