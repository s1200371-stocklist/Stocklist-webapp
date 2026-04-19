import yfinance as yf
from duckduckgo_search import DDGS
import time

def fetch_top_news():
    """從 yfinance 抓取 SPY (標普500) 和 QQQ (納指) 的最新熱門新聞"""
    print("⏳ 正在從 Yahoo Finance 獲取最新大盤新聞...")
    try:
        spy = yf.Ticker("SPY")
        qqq = yf.Ticker("QQQ")
        
        news_items = []
        # 提取 SPY 和 QQQ 的新聞 (大盤新聞通常涵蓋當日最重要的宏觀與科技巨頭資訊)
        if spy.news:
            news_items.extend(spy.news[:5])
        if qqq.news:
            news_items.extend(qqq.news[:5])
            
        # 簡單去重 (避免 SPY 和 QQQ 新聞重複)
        seen_links = set()
        formatted_news = ""
        for item in news_items:
            link = item.get('link', '')
            if link not in seen_links:
                seen_links.add(link)
                title = item.get('title', '無標題')
                publisher = item.get('publisher', '未知來源')
                formatted_news += f"- [{publisher}] {title}\n"
                
        return formatted_news
    except Exception as e:
        print(f"⚠️ 獲取新聞失敗: {e}")
        return ""

def analyze_with_free_ai(news_text):
    """調用 DuckDuckGo Search 的免費 LLM 接口進行語意分析與繁體中文總結"""
    print("⏳ 正在調用免費 AI (GPT-4o-mini / Claude 級別) 進行語意分析，請稍候...")
    
    prompt = f"""
    你是華爾街的頂級量化與基本面分析師。請閱讀以下今天最新的美股新聞標題：
    
    {news_text}
    
    請完成以下任務，並嚴格以「繁體中文」輸出：
    1. 【今日市場熱點總結】：用 100 字以內精煉總結目前市場的核心敘事（例如：AI 熱潮、降息預期等）。
    2. 【🚀 Top 3 潛力股】：從新聞中挑選出最具正面催化劑（Catalyst）的 3 隻美股代號（Ticker），並用一句話解釋看好原因。如果新聞中沒有明確提及足夠的個股，請根據熱點板塊推斷最相關的龍頭股（例如新聞提到晶片短缺，可提及 NVDA 或 TSM）。

    排版請簡潔、清晰、專業。
    """
    
    try:
        # 使用 DDGS 的免費 chat 功能
        # model 可選: 'gpt-4o-mini', 'claude-3-haiku', 'llama-3.1-70b', 'mixtral-8x7b'
        with DDGS() as ddgs:
            response = ddgs.chat(prompt, model='gpt-4o-mini')
            return response
    except Exception as e:
        return f"⚠️ AI 分析失敗，可能是免費接口暫時被限制 (Rate Limit)。錯誤訊息: {e}\n建議：過幾分鐘後再試，或更換網絡環境。"

if __name__ == "__main__":
    print("====================================")
    print("📈 財經新聞 AI 潛力股分析 - 測試啟動")
    print("====================================\n")
    
    # 1. 抓取新聞
    news_data = fetch_top_news()
    
    if news_data:
        print("\n✅ 成功獲取以下英文原生新聞：")
        print("------------------------------------")
        print(news_data)
        print("------------------------------------\n")
        
        # 2. AI 分析與中文翻譯
        ai_analysis = analyze_with_free_ai(news_data)
        
        print("\n🤖 AI 分析結果 (繁體中文)：")
        print("====================================")
        print(ai_analysis)
        print("====================================\n")
        print("測試完成！如果結果滿意，我們將把這段邏輯整合到 Streamlit Web App 中。")
    else:
        print("❌ 未能獲取新聞，測試終止。")
