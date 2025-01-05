import time
import os
from datetime import datetime
from api.etf_realtime import get_rt_etf_all_a_dc
import tushare as ts

from rt.api.stock_minutes_rt import get_stock_minutes_rt, get_sell_msg

TOKEN = os.getenv("TOKEN")
FMT = "%m-%d %H:%M:%S"
SLEEP_SECONDS = 10
ts.set_token(TOKEN)
STOCK_LIST = ['乐鑫科技', '伯特利', '雅克科技']
ETF_LIST = ['513090', '513000']


def func():
    df = ts.realtime_list()
    dt = datetime.now().strftime(FMT)

    code_set = list()

    msg = f"【{dt}】 中位={df.PCT_CHANGE.median()}"
    for name in STOCK_LIST:
        d = df[df['NAME'] == name].iloc[0].to_dict()
        code_set.append(d['TS_CODE'].split('.')[0])
        s_msg = f" {name[:2]}{d['PCT_CHANGE']} "
        msg += s_msg

    etf = get_rt_etf_all_a_dc()
    for code in ETF_LIST:
        d = etf[etf['TS_CODE'] == code].iloc[0].to_dict()
        s_msg = f" {d['NAME'].replace('ETF', '')}{d['PCT_CHANGE']} p={d['PRICE']} gap={d['RT_VALUE_GAP']} "
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

