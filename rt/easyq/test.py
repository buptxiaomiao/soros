import easyquotation

from monkey_easyq import monkey_easyq_wrapper

quotation = easyquotation.use('tencent') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
quotation = monkey_easyq_wrapper(quotation)
res = quotation.market_snapshot(prefix=True)
print(len(res))
print(list(res.values())[:10])
print(list(res.keys()))
print(len(res))
print('finish.')

print(res['sz000002'])
print(res['sz000987'])

"""
1. aliyun创建每分钟的任务，数据留存下来
    - 卡死情况处理
    - 20s/30s如何使用？
    - while true + sleep + supervisor + crontab restart.
2. boll线 + 行情更新飞书文档
3. 1min、5min、15min、30分钟、60min、120min 线更新下载：策略
4. 
4.  todo-db-sync etf信息更新飞书文档
"""
