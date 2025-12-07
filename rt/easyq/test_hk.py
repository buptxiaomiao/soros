import easyquotation
import pandas as pd
from easyquotation.hkquote import HKQuote

quotation = easyquotation.use('hkquote') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
# res = quotation.market_snapshot(prefix=True)
HKQuote().format_response_data('')
print(quotation.stock_list)
print(len(quotation.stock_list))

# res = quotation.market_snapshot()
res = quotation.real('00001')
print(res)
print('finish0.')
# data_list = [value for value in res.values()[:10]]
# for i in data_list:
#     print(i)

# res = quotation.market_snapshot(prefix=True)
res = quotation.real(['00001', '03690'])
print(res)
print('finish1.')
# data_list = [value for value in res.values()]
# for i in data_list:
#     print(i)

# quotation = easyquotation.use('jsl')  # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
# res = quotation.etfindex(index_id="", min_volume=0, max_discount=None, min_discount=None)
# 将字典值转换为列表，然后创建DataFrame
data_list = [value for value in res.values()]
for i in data_list:
    print(i)
# etf = pd.DataFrame(data_list)
# etf.to_csv("etf_20251207.csv")
#
# # print(etf)
# # for code in ETF_LIST:
# d = etf[etf['fund_id'] == '510300'].iloc[0].to_dict()
# print(d)
# # pct_change = round(float(d['price']) / float(d['pre_close']) - 1, 4) * 100
# s_msg = f" {d['fund_nm'].replace('ETF', '')}{d['increase_rt']} p={d['price']} "
# # msg += s_msg
# print(s_msg)