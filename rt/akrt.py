import akshare as ak

# stock_zh_index_spot_em_df = ak.stock_zh_index_spot_em(symbol="上证系列指数")
# print(stock_zh_index_spot_em_df)

# import akshare as ak
#
# stock_js_weibo_report_df = ak.stock_js_weibo_report(time_period="CNHOUR12")
# print(stock_js_weibo_report_df)
# print(stock_js_weibo_report_df.sort_values('rate', ascending=False))

import akshare as ak


index_stock_cons_df = ak.index_stock_cons(symbol="000300")  # 主要调用 ak.stock_a_code_to_symbol() 来进行转换
index_stock_cons_df['symbol'] = index_stock_cons_df['品种代码'].apply(ak.stock_a_code_to_symbol)
print(index_stock_cons_df)


# index_zh_a_hist_min_em_df = ak.index_zh_a_hist_min_em(symbol="000001", period="30", start_date="2024-08-11 09:30:00", end_date="2024-08-11 19:00:00")
# print(index_zh_a_hist_min_em_df)
#
# # stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sh000919")
# # print(stock_zh_index_daily_df)
# #
# #
# # stock_zh_index_daily_tx_df = ak.stock_zh_index_daily_tx(symbol="sh000919")
# # print(stock_zh_index_daily_tx_df)
#
# stock_zh_index_daily_em_df = ak.stock_zh_index_daily_em(symbol="sh000919")
# print(stock_zh_index_daily_em_df)

