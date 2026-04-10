import pandas as pd
from yahooquery import Ticker
import time

def run_screener():
    # 1. 讀取檔案
    files = ['nasdaq-listed.csv', 'nyse-listed.csv', 'other-listed.csv']
    all_tickers = []
    
    for file in files:
        try:
            df = pd.read_csv(file)
            # 獲取 Symbol 列，並去除空白字符
            tickers = df['Symbol'].dropna().str.strip().tolist()
            all_tickers.extend(tickers)
            print(f"從 {file} 讀取了 {len(tickers)} 個代號")
        except Exception as e:
            print(f"讀取 {file} 失敗: {e}")

    # 去除重複項
    all_tickers = list(set(all_tickers))
    print(f"總共有 {len(all_tickers)} 個唯一代號待掃描")

    # 2. 分批處理 (Batching) - 每批 500 個
    batch_size = 500
    screened_results = []
    min_mkt_cap = 500_000_000

    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i:i+batch_size]
        print(f"正在處理第 {i} 到 {i+len(batch)} 隻股票...")
        
        try:
            t = Ticker(batch, asynchronous=True)
            details = t.summary_detail
            
            for symbol in batch:
                data = details.get(symbol)
                if isinstance(data, dict):
                    mkt_cap = data.get('marketCap', 0)
                    if mkt_cap > min_mkt_cap:
                        screened_results.append({
                            'Symbol': symbol,
                            'Name': data.get('shortName', 'N/A'),
                            'MarketCap': mkt_cap,
                            'Price': data.get('previousClose', 0),
                            'Sector': data.get('sector', 'N/A')
                        })
        except Exception as e:
            print(f"處理批次時出錯: {e}")
        
        # 稍微暫停，避免被 Yahoo 封鎖
        time.sleep(1)

    # 3. 儲存結果
    result_df = pd.DataFrame(screened_results)
    result_df.to_csv('screened_stocks_500M.csv', index=False)
    print(f"\n篩選完成！共有 {len(result_df)} 隻股票符合條件。結果已存至 'screened_stocks_500M.csv'")

if __name__ == "__main__":
    run_screener()
