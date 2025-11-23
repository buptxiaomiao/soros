import time
import os
from datetime import datetime
import tushare as ts
import easyquotation
import pandas as pd

from rt.api.stock_minutes_rt import get_stock_minutes_rt, get_sell_msg

TOKEN = os.getenv("TOKEN")
# FMT = "%m-%d %H:%M:%S"
FMT = "%H:%M:%S"
SLEEP_SECONDS = 20
ts.set_token(TOKEN)
STOCK_LIST = ['乐鑫科技', '伯特利', '雅克科技']
ETF_LIST = ['513090']


def func():
    # df = get_stock_list_rt()
    quotation = easyquotation.use('tencent') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
    res = quotation.market_snapshot(prefix=False)
    # 将字典值转换为列表，然后创建DataFrame
    data_list = [value for value in res.values()]
    df = pd.DataFrame(data_list)
    # print(df)
    dt = datetime.now().strftime(FMT)

    code_set = list()
    msg = f"[{dt}]中位={round(df['涨跌(%)'].median(), 2)} "
    for name in STOCK_LIST:
        d = df[df['name'] == name].iloc[0].to_dict()
        code_set.append(d['code'])
        s_msg = f"{name[:2]}{d['涨跌(%)']} "
        msg += s_msg


    quotation = easyquotation.use('jsl') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
    res = quotation.etfindex(index_id="", min_volume=0, max_discount=None, min_discount=None)
    # 将字典值转换为列表，然后创建DataFrame
    data_list = [value for value in res.values()]
    etf = pd.DataFrame(data_list)

    for code in ETF_LIST:
        d = etf[etf['fund_id'] == code].iloc[0].to_dict()
        s_msg = f"{d['fund_nm'].replace('ETF', '')}{d['increase_rt']} p={d['price']} "
        msg += s_msg
    print(msg)
    return code_set + ETF_LIST


def main():
    func()
    i = 0
    while 1:
        i += 1
        time.sleep(SLEEP_SECONDS)
        code_set = func()
        msg = ""
        if i % 2 == 0:
            for c in code_set:
                try:
                    df = get_stock_minutes_rt(c)
                    sell_msg = get_sell_msg(df)
                    if sell_msg:
                        msg += f" ●{sell_msg}● "
                except Exception as e:
                    print(e)
                    pass
        if msg:
            print(msg)


if __name__ == '__main__':
    main()

