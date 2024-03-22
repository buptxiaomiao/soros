# coding: utf-8

import os
import sys
import time

import pandas

sys.path.append('..')
from utils.ts_util import pro
from utils.path_util import PathUtil
from utils.template_util import TemplateUtil
from utils.local_dim_util import LocalDimUtil
from functools import reduce
from utils.local_pick_util import LocalPickleUtil
import pandas as pd

m = {
    '000001.SH': '上证指数',
    '000300.SH': '沪深300',
    '399006.SZ': '创业板指',
    '000688.SH': '科创50',
    '000698.SH': '科创100',
    '000905.SH': '中证500',
    '000852.SH': '中证1000',
    '932000.CSI': '中证2000',
}


def get_index_df(index):
    print(index)
    df = pro.index_daily(**{
        "ts_code": index,
        "trade_date": "",
        "start_date": "20240101",
        "end_date": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "close",
        "open",
        "high",
        "low",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount"
    ])
    return df


df = None
for index, name in m.items():
    index_df = get_index_df(index)
    index_df[name] = index_df['pct_chg']
    tmp_df = index_df[['trade_date', name]]
    if df is None:
        df = tmp_df
    else:
        df = df.join(tmp_df.set_index('trade_date'), on='trade_date')

print(df)
df.to_excel('output.xlsx')


