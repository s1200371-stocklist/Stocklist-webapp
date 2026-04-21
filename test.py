import io
import re
import json
import time
import random
import datetime
import concurrent.futures

import pandas as pd
import requests
import streamlit as st
import yfinance as yf
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance


# ─────────────────────────────────────────
# 版面配置
# ─────────────────────────────────────────
st.set_page_config(
    page_title="🚀 美股全方位量化與 AI 平台",
    page_icon="📈",
    layout="wide"
)


# ─────────────────────────────────────────
# 輔助函數
# ─────────────────────────────────────────
def get_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }


def convert_mcap_to_float(val):
    try:
        if pd.isna(val) or str(val).strip() in ("-", ""):
            return 0.0
        val = str(val).upper().replace(",", "").strip()
        if "B" in val:
            return float(val.replace("B", "")) * 1000
        if "M" in val:
            return float(val.replace("M", ""))
        if "K" in val:
            return float(val.replace("K", "")) / 1000
        return float(val)
    except Exception:
        return 0.0


def clean_ai_response(text):
    if not isinstance(text, str):
        return str(text)
    text = text.strip()

    if text.startswith("{"):
        try:
            parsed = json.loads(text)
            text = parsed.get(
                "content",
                parsed.get("choices", [{}])[0].get("message", {}).get("content", text)
            )
        except Exception:
            pass

    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)

    for marker in ["【📉", "【🕵️", "【"]:
        if marker in text:
            text = text[text.find(marker):]
            break

    text = re.sub(r'","tool_calls":\[\]\}$', "", text)
    text = text.replace("\\n", "\n").replace('\\"', '"')
    return text.strip()


def safe_download(tickers, period="2y"):
    """統一 yfinance 下載入口，自動處理 MultiIndex"""
    try:
        df = yf.download(
            tickers,
            period=period,
            progress=False,
            group_by="column",
            auto_adjust=False,
            timeout=20,
            multi_level_index=False,
            threads=True,
        )
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join([str(x) for x in col if x]).strip("_")
                for col in df.columns.to_flat_index()
            ]
        return df
    except Exception:
        return pd.DataFrame()


def call_free_ai(system_prompt, user_prompt):
    try:
        response = requests.post(
            "https://text.pollinations.ai/",
            json={
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "model": "openai",
            },
            timeout=(10, 50),
        )
        if response.status_code == 200:
            return clean_ai_response(response.text)
        return f"⚠️ AI 接口狀態異常 (HTTP {response.status_code})，請遲啲再試。"
    except Exception as e:
        return f"⚠️ AI 發生錯誤: {e}"


# ─────────────────────────────────────────
# 模組 C：另類數據（Reddit + Insider）
# ─────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_reddit_sentiment():
    """
    方案 1：ApeWisdom API（免帳號、免 API Key）
    方案 2：Tradestie API（備援）
    回傳欄位：ticker / sentiment / mentions / upvotes
    """
    # ── 方案 1：ApeWisdom ──────────────────
    try:
        url = "https://apewisdom.io/api/v1.0/filter/all-stocks/page/1"
        res = requests.get(url, headers=get_headers(), timeout=(5, 15))
        if res.status_code == 200:
            results = res.json().get("results", [])
            if results:
                rows = []
                for item in results[:15]:
                    ticker   = str(item.get("ticker", "")).upper().strip()
                    mentions = int(item.get("mentions", 0) or 0)
                    upvotes  = int(item.get("upvotes", 0) or 0)
                    rank     = int(item.get("rank", 99) or 99)
                    sentiment = "Bullish" if mentions >= 10 else "Neutral"
                    rows.append({
                        "ticker":    ticker,
                        "sentiment": sentiment,
                        "mentions":  mentions,
                        "upvotes":   upvotes,
                        "rank":      rank,
                    })
                if rows:
                    return pd.DataFrame(rows)
    except Exception:
        pass

    # ── 方案 2：Tradestie（備援）──────────
    try:
        url = "https://tradestie.com/api/v1/apps/reddit"
        res = requests.get(url, headers=get_headers(), timeout=(5, 15))
        if res.status_code == 200:
            data = res.json()
            if isinstance(data, list) and data:
                df = pd.DataFrame(data).head(15)
                df["ticker"] = df["ticker"].astype(str).str.upper()
                # 統一欄位名，補上 ApeWisdom 沒有的欄位
                if "no_of_comments" in df.columns:
                    df = df.rename(columns={"no_of_comments": "mentions"})
                df["upvotes"] = 0
                df["rank"] = range(1, len(df) + 1)
                return df[["ticker", "sentiment", "mentions", "upvotes", "rank"]]
    except Exception:
        pass

    return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_insider_buying():
    """
    方案 1：OpenInsider screener（免費、HTML table 解析）
    方案 2：Finviz Insider（備援，易被限流）
    回傳欄位：Ticker / Company / Insider / Title / Value
    """
    # ── 方案 1：OpenInsider ────────────────
    try:
        url = (
            "http://openinsider.com/screener?"
            "s=&o=&pl=&ph=&ll=&lh=&fd=30&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago="
            "&xp=1&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0"
            "&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h="
            "&sortcol=0&cnt=50&page=1"
        )
        res = requests.get(url, headers=get_headers(), timeout=(5, 20))
        if res.status_code == 200:
            tables = pd.read_html(io.StringIO(res.text))
            if tables:
                df = tables[0].copy()
                df.columns = [str(c).strip() for c in df.columns]

                # OpenInsider 實際欄位：
                # X | Filing Date | Trade Date | Ticker | Company Name |
                # Insider Name | Title | Trade Type | Price | Qty | Owned |
                # ΔOwn | Value | 1d | 1w | 1m | 6m
                rename_map = {}
                for c in df.columns:
                    cl = c.lower().strip()
                    if cl == "ticker":
                        rename_map[c] = "Ticker"
                    elif cl in ("company name", "company"):
                        rename_map[c] = "Company"
                    elif cl in ("insider name", "insider"):
                        rename_map[c] = "Insider"
                    elif cl == "title":
                        rename_map[c] = "Title"
                    elif cl == "price":
                        rename_map[c] = "Price"
                    elif cl == "value":
                        rename_map[c] = "Value"
                    elif cl in ("trade date", "filing date"):
                        rename_map[c] = "Trade Date"

                df = df.rename(columns=rename_map)

                need = ["Ticker", "Company", "Insider", "Title", "Value"]
                available = [c for c in need if c in df.columns]

                if "Ticker" in df.columns and len(available) >= 3:
                    df = df[available].copy()
                    # 移除非股票代號嘅列（例如表頭重複）
                    df = df[df["Ticker"].astype(str).str.match(r"^[A-Z]{1,6}$")]
                    return df.head(15).reset_index(drop=True)
    except Exception:
        pass

    # ── 方案 2：Finviz Insider（備援）─────
    try:
        from finvizfinance.insider import Insider
        finsider = Insider(option="top insider trading recent buy")
        df = finsider.get_insider()
        if df is not None and not df.empty:
            need = ["Ticker", "Owner", "Relationship", "Cost", "Value"]
            available = [c for c in need if c in df.columns]
            return df[available].head(15).reset_index(drop=True)
    except Exception:
        pass

    return pd.DataFrame()


# ─────────────────────────────────────────
# 模組 A：Finviz 篩選 + 技術指標 + 財報
# ─────────────────────────────────────────
@st.cache_data(ttl=3600)
def fetch_finviz_data():
    try:
        f_screener = Overview()
        f_screener.set_filter(filters_dict={"Market Cap.": "+Small (over $300mln)"})
        return f_screener.screener_view()
    except Exception as e:
        st.error(f"⚠️ 連唔到 Finviz，請陣間再試: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def calculate_all_indicators(
    tickers, sma_short, sma_long, close_condition,
    batch_size=100, _progress_bar=None, _status_text=None
):
    results = {}

    # ── 下載基準（QQQ 系列）────────────────
    bench_series = None
    for bench in ["QQQ", "^NDX", "QQQM"]:
        temp = safe_download(bench, period="2y")
        if temp.empty:
            continue
        for col_name in ["Close", f"Close_{bench}"]:
            if col_name in temp.columns:
                ser = temp[col_name].dropna()
                if not ser.empty:
                    bench_series = ser
                    break
        if bench_series is not None:
            break

    if bench_series is None:
        st.error("⚠️ 下載唔到基準數據，請檢查網絡。")
        return results

    if getattr(bench_series.index, "tz", None) is not None:
        bench_series.index = bench_series.index.tz_localize(None)

    bench_norm = bench_series / bench_series.iloc[0]
    total_tickers = len(tickers)

    for i in range(0, total_tickers, batch_size):
        batch = tickers[i:i + batch_size]

        if _status_text:
            _status_text.markdown(
                f"**階段 2/3**: 計緊技術指標... (`{min(i + batch_size, total_tickers)}` / `{total_tickers}`)"
            )
        if _progress_bar:
            _progress_bar.progress(min(1.0, (i + batch_size) / total_tickers))

        try:
            data = safe_download(batch, period="2y")
            if data.empty:
                raise ValueError("No data")

            # 統一提取 Close 價格
            close_prices = pd.DataFrame(index=data.index)
            if "Close" in data.columns and len(batch) == 1:
                close_prices[batch[0]] = data["Close"]
            else:
                for t in batch:
                    col = f"Close_{t}"
                    if col in data.columns:
                        close_prices[t] = data[col]

            close_prices = close_prices.ffill().dropna(how="all")
            if getattr(close_prices.index, "tz", None) is not None:
                close_prices.index = close_prices.index.tz_localize(None)

            for ticker in batch:
                rs_stage = macd_stage = "無"
                sma_trend = False

                if ticker not in close_prices.columns:
                    results[ticker] = {"RS": rs_stage, "MACD": macd_stage, "SMA_Trend": sma_trend}
                    continue

                stock_price = close_prices[ticker].dropna()
                if len(stock_price) <= max(sma_short, sma_long) + 5:
                    results[ticker] = {"RS": rs_stage, "MACD": macd_stage, "SMA_Trend": sma_trend}
                    continue

                # ── RS ──────────────────────────────
                stock_norm  = stock_price / stock_price.iloc[0]
                aligned_bench = bench_norm.reindex(stock_norm.index).ffill().dropna()
                stock_norm    = stock_norm.reindex(aligned_bench.index).dropna()

                if len(stock_norm) > 30:
                    rs_line    = stock_norm / aligned_bench * 100
                    rs_ma_25   = rs_line.rolling(window=25).mean()
                    latest_rs  = float(rs_line.iloc[-1])
                    prev_rs    = float(rs_line.iloc[-2])
                    latest_rma = float(rs_ma_25.iloc[-1])
                    prev_rma   = float(rs_ma_25.iloc[-2])

                    if pd.notna(latest_rma) and pd.notna(prev_rma):
                        if latest_rs > latest_rma:
                            rs_stage = "🚀 啱啱突破" if prev_rs <= prev_rma else "🔥 已經突破"
                        elif latest_rs >= latest_rma * 0.95:
                            rs_stage = "🎯 就快突破 (<5%)"

                # ── MACD ─────────────────────────────
                ema12       = stock_price.ewm(span=12, adjust=False).mean()
                ema26       = stock_price.ewm(span=26, adjust=False).mean()
                macd_line   = ema12 - ema26
                signal_line = macd_line.ewm(span=9, adjust=False).mean()
                l_macd  = float(macd_line.iloc[-1])
                p_macd  = float(macd_line.iloc[-2])
                l_sig   = float(signal_line.iloc[-1])
                p_sig   = float(signal_line.iloc[-2])

                if l_macd > l_sig:
                    macd_stage = "🚀 啱啱突破" if p_macd <= p_sig else "🔥 已經突破"
                elif abs(l_sig) > 0.0001 and abs(l_macd - l_sig) <= abs(l_sig) * 0.05:
                    macd_stage = "🎯 就快突破 (<5%)"

                # ── SMA 趨勢 ─────────────────────────
                sma_s     = stock_price.rolling(window=sma_short).mean()
                sma_l     = stock_price.rolling(window=sma_long).mean()
                l_close   = float(stock_price.iloc[-1])
                l_sma_s   = float(sma_s.iloc[-1])
                l_sma_l   = float(sma_l.iloc[-1])
                trend_ok  = l_sma_s > l_sma_l

                if close_condition == "Close > 短期 SMA":
                    trend_ok = trend_ok and (l_close > l_sma_s)
                elif close_condition == "Close > 長期 SMA":
                    trend_ok = trend_ok and (l_close > l_sma_l)
                elif close_condition == "Close > 短期及長期 SMA":
                    trend_ok = trend_ok and (l_close > l_sma_s) and (l_close > l_sma_l)

                sma_trend = trend_ok
                results[ticker] = {"RS": rs_stage, "MACD": macd_stage, "SMA_Trend": sma_trend}

        except Exception:
            for t in batch:
                results[t] = {"RS": "無", "MACD": "無", "SMA_Trend": False}

        time.sleep(0.3 + random.random() * 0.4)

    return results


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fundamentals(tickers, _progress_bar=None, _status_text=None):
    empty_df = pd.DataFrame(
        columns=["Ticker", "EPS (近4季)", "EPS Growth (QoQ)", "Sales (近4季)", "Sales Growth (QoQ)"]
    )
    if not tickers:
        return empty_df

    def fetch_single(t):
        time.sleep(0.4 + random.random() * 0.8)
        for attempt in range(3):
            try:
                if attempt > 0:
                    time.sleep(1.5)
                tkr   = yf.Ticker(t)
                q_inc = tkr.quarterly_financials
                if q_inc is None or q_inc.empty:
                    q_inc = tkr.quarterly_income_stmt
                if q_inc is None or q_inc.empty:
                    continue

                cols = list(q_inc.columns)[:4]
                try:
                    cols = sorted(cols)
                except Exception:
                    cols = cols[::-1]
                if not cols:
                    continue

                eps_row = sales_row = None
                for r in ["Diluted EPS", "Basic EPS", "Normalized EPS"]:
                    if r in q_inc.index:
                        eps_row = q_inc.loc[r]
                        break
                for r in ["Total Revenue", "Operating Revenue"]:
                    if r in q_inc.index:
                        sales_row = q_inc.loc[r]
                        break

                eps_vals   = [float(eps_row[c])   if eps_row   is not None and pd.notna(eps_row[c])   else None for c in cols]
                sales_vals = [float(sales_row[c]) if sales_row is not None and pd.notna(sales_row[c]) else None for c in cols]

                def fmt_val(vals, is_sales=False):
                    out = []
                    for v in vals:
                        if v is None:
                            out.append("-")
                        elif is_sales:
                            if v >= 1e9:   out.append(f"{v/1e9:.2f}B")
                            elif v >= 1e6: out.append(f"{v/1e6:.2f}M")
                            else:          out.append(f"{v:.0f}")
                        else:
                            out.append(f"{v:.2f}")
                    return " | ".join(out)

                def fmt_growth(vals):
                    out = ["-"]
                    for i in range(1, len(vals)):
                        if vals[i] is None or vals[i-1] is None or vals[i-1] == 0:
                            out.append("-")
                        else:
                            out.append(f"{(vals[i]-vals[i-1])/abs(vals[i-1])*100:+.1f}%")
                    return " | ".join(out)

                return {
                    "Ticker": t,
                    "EPS (近4季)":       fmt_val(eps_vals, False),
                    "EPS Growth (QoQ)":  fmt_growth(eps_vals),
                    "Sales (近4季)":     fmt_val(sales_vals, True),
                    "Sales Growth (QoQ)": fmt_growth(sales_vals),
                }
            except Exception:
                pass

        return {"Ticker": t, "EPS (近4季)": "N/A", "EPS Growth (QoQ)": "N/A",
                "Sales (近4季)": "N/A", "Sales Growth (QoQ)": "N/A"}

    results  = []
    completed = 0
    total     = len(tickers)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_single, t): t for t in tickers}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            completed += 1
            if _status_text:
                _status_text.markdown(f"**階段 3/3**: 攞緊最新財報數據... (`{completed}` / `{total}`)")
            if _progress_bar:
                _progress_bar.progress(min(1.0, completed / total))

    return pd.DataFrame(results) if results else empty_df


# ─────────────────────────────────────────
# 模組 B：AI 新聞分析
# ─────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_top_news():
    news_items  = []
    seen_titles = set()

    # 來源 1：Finviz
    try:
        for t in ["SPY", "QQQ"]:
            stock = finvizfinance(t)
            news  = stock.ticker_news()
            if news is not None and not news.empty:
                for _, row in news.head(20).iterrows():
                    title = row.get("Title", "")
                    if title and title not in seen_titles:
                        seen_titles.add(title)
                        news_items.append({
                            "來源":   row.get("Source", "Finviz"),
                            "新聞標題": title,
                            "內文摘要": "（來自 Finviz 標題）",
                        })
    except Exception:
        pass

    # 來源 2：yfinance
    try:
        for t in ["SPY", "QQQ", "NVDA", "AAPL"]:
            tkr  = yf.Ticker(t)
            news = getattr(tkr, "news", None) or []
            for item in news[:6]:
                title     = item.get("title", "")
                summary   = item.get("summary", "")
                publisher = item.get("publisher", "Finance News")

                content = item.get("content", {})
                if isinstance(content, dict):
                    title     = content.get("title", title)
                    summary   = content.get("summary", summary)
                    provider  = content.get("provider", {})
                    if isinstance(provider, dict):
                        publisher = provider.get("displayName", publisher)
                    elif isinstance(provider, str):
                        publisher = provider

                if title and title not in seen_titles:
                    seen_titles.add(title)
                    clean_summary = summary.replace("\n", " ")
                    if len(clean_summary) > 250:
                        clean_summary = clean_summary[:250] + "..."
                    news_items.append({
                        "來源":   publisher,
                        "新聞標題": title,
                        "內文摘要": clean_summary or "無提供內文",
                    })
    except Exception as e:
        if "Too Many Requests" in str(e):
            st.warning("⚠️ Yahoo Finance 暫時限制訪問，改用 Finviz 新聞庫。")

    return news_items


@st.cache_data(ttl=3600, show_spinner=False)
def analyze_news_ai(news_list):
    if not news_list:
        return "⚠️ 目前攞唔到新聞數據，請遲啲再試下。"

    news_text = "\n".join(
        f"{i+1}. [{item['來源']}] 標題：{item['新聞標題']}\n摘要：{item['內文摘要']}"
        for i, item in enumerate(news_list)
    )

    system_prompt = (
        "你係一位身處香港中環嘅頂級金融分析師。\n"
        "【絕對強制規範】：\n"
        "1. 你必須用「香港廣東話口語（Cantonese）」寫呢份報告。\n"
        "2. 絕對禁止輸出任何 JSON、字典、編程代碼或非中文內容。\n"
        "3. 絕對禁止輸出思考過程、英文草稿。\n"
        "4. 開頭第一句必須係：「【📉 近月市場焦點總結】」。"
    )
    user_prompt = (
        f"請睇下呢堆近期美股新聞：\n{news_text}\n\n"
        "請用廣東話完成：\n"
        "1. 【📉 近月市場焦點總結】：150-200 字總結大市走勢同情緒。\n"
        "2. 【🚀 潛力爆發股全面掃描】：列出所有有潛力嘅股票代號，每隻用 1-2 句廣東話解釋。"
    )
    return call_free_ai(system_prompt, user_prompt)


@st.cache_data(ttl=3600, show_spinner=False)
def analyze_alt_data_ai(reddit_df, insider_df):
    system_prompt = (
        "你係香港中環頂級策略分析師。\n"
        "【絕對強制規範】：\n"
        "1. 必須用地道「香港廣東話口語（Cantonese）」寫報告。\n"
        "2. 絕對禁止輸出任何英文思考過程或 JSON。\n"
        "3. 第一句必須係：「【🕵️ 另類數據 AI 偵測報告】」。"
    )
    r_str = reddit_df.head(10).to_string(index=False) if not reddit_df.empty else "無數據"
    i_str = insider_df.head(10).to_string(index=False) if not insider_df.empty else "無數據"

    user_prompt = (
        f"分析以下美股另類數據：\n\n"
        f"[Reddit WallStreetBets 熱門名單]\n{r_str}\n\n"
        f"[內部人士買入名單]\n{i_str}\n\n"
        "請用廣東話完成：\n"
        "1. 【🔥 散戶正喺度瘋傳啲咩？】：用 100 字總結 Reddit 網民情緒同最關注嘅 Meme 股。\n"
        "2. 【🏛️ 大佬真金白銀入緊邊隻？】：分析 Insider 買入名單，邊啲股票連高層都忍唔住入貨。\n"
        "3. 【🎯 終極爆發潛力股】：對比兩份名單，搵出大戶散戶齊齊入或最具轉機嘅股票。"
    )
    return call_free_ai(system_prompt, user_prompt)


# ─────────────────────────────────────────
# 側邊欄導航
# ─────────────────────────────────────────
with st.sidebar:
    st.title("🧰 投資雙引擎")
    st.markdown("揀個你想用嘅模組：")
    app_mode = st.radio(
        "可用模組",
        ["🎯 RS x MACD 動能狙擊手", "📰 近月 AI 洞察 (廣東話版)", "🕵️ 另類數據雷達"],
    )
    st.markdown("---")
    st.caption(f"數據最後更新: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")


# ─────────────────────────────────────────
# 模組 A：RS x MACD 動能狙擊手
# ─────────────────────────────────────────
if app_mode == "🎯 RS x MACD 動能狙擊手":
    st.title("🎯 美股 RS x MACD x 趨勢 狙擊手")
    st.markdown("幫你搵市場上動能最強、財報增長緊嘅爆發潛力股。")

    with st.expander("⚙️ 展開設定篩選參數", expanded=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("#### 1️⃣ 基礎與趨勢")
            min_mcap   = st.number_input("最低市值 (百萬 USD)", min_value=0.0, value=500.0, step=50.0)
            enable_sma = st.checkbox("啟動 【趨勢排列】 過濾", value=True)

            if enable_sma:
                s1, s2 = st.columns(2)
                sma_short       = s1.selectbox("短期 SMA", [10, 20, 25, 50], index=2)
                sma_long        = s2.selectbox("長期 SMA", [50, 100, 125, 150, 200], index=2)
                close_options   = ["唔揀", "Close > 短期 SMA", "Close > 長期 SMA", "Close > 短期及長期 SMA"]
                close_condition = st.selectbox("額外 Close 條件", options=close_options, index=1)

                captions = {
                    "唔揀":              f"✅ 條件：SMA `{sma_short}` > SMA `{sma_long}`",
                    "Close > 短期 SMA":  f"✅ 條件：`Close` > SMA `{sma_short}` > SMA `{sma_long}`",
                    "Close > 長期 SMA":  f"✅ 條件：SMA `{sma_short}` > SMA `{sma_long}` 且 `Close` > SMA `{sma_long}`",
                    "Close > 短期及長期 SMA": "✅ 條件：`Close` > 雙均線，且短線高於長線",
                }
                st.caption(captions.get(close_condition, ""))
            else:
                sma_short, sma_long, close_condition = 25, 125, "唔揀"

        with col2:
            st.markdown("#### 2️⃣ RS 動能 (對比納指)")
            enable_rs = st.checkbox("啟動 【RS】 過濾", value=True)
            if enable_rs:
                selected_rs = st.multiselect(
                    "顯示 RS 階段:",
                    options=["🚀 啱啱突破", "🔥 已經突破", "🎯 就快突破 (<5%)"],
                    default=["🚀 啱啱突破"],
                )
            else:
                selected_rs = []

        with col3:
            st.markdown("#### 3️⃣ MACD 爆發點")
            enable_macd = st.checkbox("啟動 【MACD】 過濾", value=True)
            if enable_macd:
                selected_macd = st.multiselect(
                    "顯示 MACD 階段:",
                    options=["🚀 啱啱突破", "🔥 已經突破", "🎯 就快突破 (<5%)"],
                    default=["🚀 啱啱突破"],
                )
            else:
                selected_macd = []

        st.markdown("---")
        start_scan = st.button("🚀 開始全市場精確掃描", use_container_width=True, type="primary")

    if start_scan:
        st.markdown("### ⏳ 系統運算進度")
        status_text  = st.empty()
        progress_bar = st.progress(0)

        status_text.markdown("**階段 1/3**: 搵緊 Finviz 基礎股票名單...")
        raw_data = fetch_finviz_data()
        progress_bar.progress(100)

        final_df = pd.DataFrame()

        if not raw_data.empty:
            df_processed = raw_data.copy()

            if "Market Cap" in df_processed.columns:
                df_processed["Mcap_Numeric"] = df_processed["Market Cap"].apply(convert_mcap_to_float)
                final_df = df_processed[df_processed["Mcap_Numeric"] >= min_mcap].copy()
            else:
                final_df = df_processed.copy()

            if (enable_rs or enable_macd or enable_sma) and not final_df.empty:
                target_tickers = final_df["Ticker"].dropna().astype(str).tolist()

                progress_bar.progress(0)
                indicators = calculate_all_indicators(
                    target_tickers, sma_short, sma_long, close_condition,
                    _progress_bar=progress_bar, _status_text=status_text,
                )

                final_df["RS_階段"]  = final_df["Ticker"].map(lambda x: indicators.get(x, {}).get("RS", "無"))
                final_df["MACD_階段"] = final_df["Ticker"].map(lambda x: indicators.get(x, {}).get("MACD", "無"))
                final_df["SMA多頭"]   = final_df["Ticker"].map(lambda x: indicators.get(x, {}).get("SMA_Trend", False))

                if enable_sma:
                    final_df = final_df[final_df["SMA多頭"] == True]
                if enable_rs:
                    final_df = final_df[final_df["RS_階段"].isin(selected_rs)]
                if enable_macd:
                    final_df = final_df[final_df["MACD_階段"].isin(selected_macd)]

                if not final_df.empty:
                    progress_bar.progress(0)
                    fund_df  = fetch_fundamentals(
                        final_df["Ticker"].tolist(),
                        _progress_bar=progress_bar,
                        _status_text=status_text,
                    )
                    final_df = pd.merge(final_df, fund_df, on="Ticker", how="left")
                    status_text.markdown("✅ **全市場掃描同過濾搞掂！**")
                    progress_bar.progress(100)
                    st.success(f"成功搵到 **{len(final_df)}** 隻符合條件嘅潛力股票。")
                else:
                    status_text.markdown("✅ **全市場掃描搞掂！**")
                    progress_bar.progress(100)
                    st.warning("⚠️ 掃描完成，但搵唔到股票同時滿足你嘅條件，試下放寬設定。")

        st.markdown("---")
        if not final_df.empty:
            st.subheader("🎯 終極精選清單")
            priority_cols = [
                "Ticker", "RS_階段", "MACD_階段",
                "Company", "Sector", "Industry", "Market Cap",
                "EPS (近4季)", "EPS Growth (QoQ)",
                "Sales (近4季)", "Sales Growth (QoQ)",
            ]
            display_cols = [c for c in priority_cols if c in final_df.columns]
            st.dataframe(final_df[display_cols], use_container_width=True, height=600)

            csv = final_df[display_cols].to_csv(index=False).encode("utf-8")
            st.download_button(
                "📥 下載終極清單 (CSV)",
                data=csv,
                file_name="rs_macd_trend_sniper.csv",
                mime="text/csv",
            )


# ─────────────────────────────────────────
# 模組 B：AI 新聞洞察
# ─────────────────────────────────────────
elif app_mode == "📰 近月 AI 洞察 (廣東話版)":
    st.title("📰 近月 AI 新聞深度分析")
    st.markdown("系統自動爬取近期財經熱門新聞，交俾 AI 用廣東話幫你掃描大市熱點同潛力股。")

    if st.button("🚀 攞今日 AI 報告", type="primary", use_container_width=True):
        with st.spinner("⏳ 攞緊財經頭條同摘要..."):
            news_list = fetch_top_news()

        if news_list:
            st.success(f"✅ 成功攞到 **{len(news_list)}** 條近期財經資訊！")

            with st.expander("📄 撳開睇下 AI 讀緊咩原始新聞"):
                for idx, item in enumerate(news_list):
                    st.markdown(f"**{idx+1}. {item['新聞標題']}**")
                    st.caption(f"📰 來源: `{item['來源']}`")
                    st.write(f"📝 摘要: *{item['內文摘要']}*")
                    st.markdown("---")

            with st.spinner("🧠 AI 認真睇緊內文，掃描潛力股票... (大約 15-30 秒)"):
                ai_result = analyze_news_ai(news_list)

            st.markdown("---")
            st.markdown("### 🤖 華爾街 AI 深度洞察報告")
            with st.container(border=True):
                st.markdown(ai_result)
        else:
            st.error("❌ 攞唔到新聞，可能俾伺服器 block 咗，請等陣再試。")


# ─────────────────────────────────────────
# 模組 C：另類數據雷達
# ─────────────────────────────────────────
elif app_mode == "🕵️ 另類數據雷達":
    st.title("🕵️ 另類數據雷達 (Alt-Data Radar)")
    st.markdown("追蹤 **聰明錢（Insider 大戶）** 同 **散戶熱度（Reddit WSB via ApeWisdom）**。")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("🌐 Reddit WSB 散戶熱度榜")
        with st.spinner("攞緊 Reddit 熱度數據..."):
            r_df = fetch_reddit_sentiment()

        if not r_df.empty:
            show_cols = [c for c in ["rank", "ticker", "sentiment", "mentions", "upvotes"] if c in r_df.columns]
            st.dataframe(r_df[show_cols].head(15), use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ 暫時攞唔到 Reddit 熱度數據（ApeWisdom 及 Tradestie 都無回應），請稍後再試。")

    with col_r:
        st.subheader("🏛️ 近期高層 Insider 買入")
        with st.spinner("攞緊 Insider 數據..."):
            i_df = fetch_insider_buying()

        if not i_df.empty:
            show_cols = [c for c in ["Ticker", "Company", "Insider", "Title", "Value"] if c in i_df.columns]
            st.dataframe(i_df[show_cols].head(15), use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ 暫時攞唔到 Insider 數據（OpenInsider 及 Finviz 都無回應），請稍後再試。")

    st.markdown("---")

    if st.button("🚀 啟動 AI 大戶散戶交叉博弈分析", type="primary", use_container_width=True):
        if r_df.empty and i_df.empty:
            st.error("⚠️ 兩邊數據都攞唔到，AI 無嘢可以分析，請稍後再試。")
        else:
            with st.spinner("🧠 AI 分析大戶同散戶嘅博弈... (大約 15-20 秒)"):
                res = analyze_alt_data_ai(r_df, i_df)
            st.markdown("### 🤖 另類數據 AI 偵測報告")
            with st.container(border=True):
                st.markdown(res)
