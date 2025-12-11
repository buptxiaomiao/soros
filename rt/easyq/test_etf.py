import easyquotation
import pandas as pd

quotation = easyquotation.use('jsl') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
# res = quotation.market_snapshot(prefix=True)
res = quotation.etfindex(index_id="", min_volume=0, max_discount=None, min_discount=None)

print(res)
print(res.keys())
print('finish.')

# quotation = easyquotation.use('jsl')  # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
# res = quotation.etfindex(index_id="", min_volume=0, max_discount=None, min_discount=None)
# 将字典值转换为列表，然后创建DataFrame
data_list = [value for value in res.values()]
etf = pd.DataFrame(data_list)
etf.to_csv("etf_20251210.csv")

# print(etf)
# for code in ETF_LIST:
d = etf[etf['fund_id'] == '510300'].iloc[0].to_dict()
print(d)
# pct_change = round(float(d['price']) / float(d['pre_close']) - 1, 4) * 100
s_msg = f" {d['fund_nm'].replace('ETF', '')}{d['increase_rt']} p={d['price']} "
# msg += s_msg
print(s_msg)