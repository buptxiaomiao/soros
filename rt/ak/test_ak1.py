# import akshare as ak
#
# stock_bj_a_spot_em_df = ak.stock_bj_a_spot_em()
# print(stock_bj_a_spot_em_df)
#
# import akshare as ak
#
# stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()
# print(stock_sz_a_spot_em_df)
#
# import akshare as ak
#
# stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
# print(stock_sh_a_spot_em_df)

# import akshare as ak
#
# stock_zh_a_spot_df = ak.stock_zh_a_spot()
# print(stock_zh_a_spot_df)
#
# import akshare as ak
#
# stock_individual_spot_xq_df = ak.stock_individual_spot_xq(symbol="SH600000")
# print(stock_individual_spot_xq_df)
# import akshare as ak
#
# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20170301", end_date='20240528', adjust="")
# print(stock_zh_a_hist_df)


# import akshare as ak
#
# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20170301", end_date='20240528', adjust="qfq")
# print(stock_zh_a_hist_df)


# # ok
# import akshare as ak   ## ok
# stock_zh_a_hist_tx_df = ak.stock_zh_a_hist_tx(symbol="sz000001", start_date="20200101", end_date="20231027", adjust="")
# print(stock_zh_a_hist_tx_df)

# import akshare as ak
#
# stock_zh_a_minute_df = ak.stock_zh_a_minute(symbol='sh600751', period='1', adjust="qfq")
# print(stock_zh_a_minute_df)

# import akshare as ak
#
# # 注意：该接口返回的数据只有最近一个交易日的有开盘价，其他日期开盘价为 0
# stock_zh_a_hist_min_em_df = ak.stock_zh_a_hist_min_em(symbol="000001", start_date="2024-03-20 09:30:00", end_date="2024-03-20 15:00:00", period="1", adjust="")
# print(stock_zh_a_hist_min_em_df)


# import akshare as ak
#
# stock_zh_a_hist_pre_min_em_df = ak.stock_zh_a_hist_pre_min_em(symbol="000001", start_time="09:00:00", end_time="15:40:00")
# print(stock_zh_a_hist_pre_min_em_df)

# # ok tick
# import akshare as ak
#
# stock_zh_a_tick_tx_js_df = ak.stock_zh_a_tick_tx_js(symbol="sz000001")
# print(stock_zh_a_tick_tx_js_df)
#
# # ok. 历史
# import akshare as ak
#
# stock_gsrl_gsdt_em_df = ak.stock_gsrl_gsdt_em(date="20251211")
# print(stock_gsrl_gsdt_em_df.columns)
# print(stock_gsrl_gsdt_em_df)

import akshare as ak

stock_zh_ah_spot_em_df = ak.stock_zh_ah_spot_em()
print(stock_zh_ah_spot_em_df)


import akshare as ak

stock_zh_ah_name_df = ak.stock_zh_ah_name()
print(stock_zh_ah_name_df)

import akshare as ak

stock_board_concept_cons_em_df = ak.stock_board_concept_cons_em(symbol="融资融券")
print(stock_board_concept_cons_em_df)
import akshare as ak

stock_rank_lxsz_ths_df = ak.stock_rank_lxsz_ths()
print(stock_rank_lxsz_ths_df)




