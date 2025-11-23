import time
import uuid
import pandas as pd
import requests
from random import randint
from tushare.util.format_stock_code import format_stock_code
from retrying import retry
from stock_list_rt import get_stock_list_rt
from stock_minutes_rt import get_stock_minutes_rt, StockKlineRt

from rt.api.thread_pool_executor import ThreadPoolExecutorBase

def func():
    df = get_stock_list_rt()
    print(df.head(3))
    print(df.columns)
    print(df.shape)
    return df

def f(code):
    print(f"f.code=${code}")
    df = get_stock_minutes_rt(code, klt=101)
    return df

if __name__ == '__main__':
    stock_df = func()
    stock_list = stock_df['ts_code'].to_list()
    stock_list_raw = [i.split('.')[0] for i in stock_list]
    print(stock_list_raw)

    st = time.time()
    # stock_list_raw = ['603083', '600839', '605068']

    # result = ThreadPoolExecutorBase.run_by_pool_pro(f, [[i] for i in stock_list_raw])
    # print(len(result))
    # print(result[0].shape)
    # print(result[0].columns)
    # print(result[0].head(3))

    result = StockKlineRt.run(stock_list_raw)
    et = time.time()
    print(result.shape)
    print(result.columns)
    print(result.head(3))
    print(f"st={st}, et={et}, total={et-st}")

