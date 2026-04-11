st.markdown("---")
st.header("🎯 失蹤股票狙擊手 (測試專用)")
st.markdown("輸入你覺得「漏咗」嘅股票代號，睇下 Yahoo 到底回傳咗咩數據畀我哋。")

test_symbol = st.text_input("輸入失蹤股票代號 (例如: BRK-B, TSM, BABA)", value="BRK-B")

if st.button("🔍 狙擊這隻股票"):
    with st.spinner("正在向 Yahoo 查閱內部數據..."):
        try:
            t_test = Ticker(test_symbol, asynchronous=False)
            
            # 一次過攞晒所有模組，睇下市值收埋喺邊
            raw_summary = t_test.summary_detail.get(test_symbol, {})
            raw_price = t_test.price.get(test_symbol, {})
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("1. 檢查 SummaryDetail 模組")
                if isinstance(raw_summary, dict):
                    mkt_cap_1 = raw_summary.get('marketCap', '找不到')
                    st.write(f"**市值 (Market Cap):** {mkt_cap_1}")
                    st.json(raw_summary) # 顯示原始數據
                else:
                    st.error("Yahoo 完全沒有這隻股票的 Summary 數據")
                    
            with col2:
                st.subheader("2. 檢查 Price 模組")
                if isinstance(raw_price, dict):
                    mkt_cap_2 = raw_price.get('marketCap', '找不到')
                    st.write(f"**市值 (Market Cap):** {mkt_cap_2}")
                    st.json(raw_price) # 顯示原始數據
                else:
                    st.error("Yahoo 完全沒有這隻股票的 Price 數據")
                    
        except Exception as e:
            st.error(f"查詢失敗: {e}")
