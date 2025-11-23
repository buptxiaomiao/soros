import easyquotation

quotation = easyquotation.use('tencent') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
res = quotation.market_snapshot(prefix=True)
print(res)
print('finish.')

