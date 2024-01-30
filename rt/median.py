import time
import os
from datetime import datetime
from api.etf_realtime import get_rt_etf_all_a_dc
import tushare as ts
TOKEN = os.getenv("TOKEN")
FMT = "%m-%d %H:%M:%S"
SLEEP_SECONDS = 10
ts.set_token(TOKEN)
STOCK_LIST = ['乐鑫科技', '伯特利']
ETF_LIST = ['513090', '513000']


def func():
    df = ts.realtime_list()
    dt = datetime.now().strftime(FMT)

    msg = f"【{dt}】 中位={df.PCT_CHANGE.median()}"
    for name in STOCK_LIST:
        d = df[df['NAME'] == name].iloc[0].to_dict()
        s_msg = f" {name[:2]}{d['PCT_CHANGE']} "
        msg += s_msg

    etf = get_rt_etf_all_a_dc()
    for code in ETF_LIST:
        d = etf[etf['TS_CODE'] == code].iloc[0].to_dict()
        s_msg = f" {d['NAME'].replace('ETF', '')}{d['PCT_CHANGE']} p={d['PRICE']} gap={d['RT_VALUE_GAP']} "
        msg += s_msg
    print(msg)


def main():
    func()
    while 1:
        time.sleep(SLEEP_SECONDS)
        func()


if __name__ == '__main__':
    main()

