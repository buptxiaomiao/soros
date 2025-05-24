#!/usr/bin/env python
# -*- coding:utf-8 -*-

import time
import uuid
from random import randint

import pandas as pd
import requests
from tushare.util.format_stock_code import format_stock_code

from rt.api.thread_pool_executor import ThreadPoolExecutorBase


def get_stock_list_rt():
    df = get_stock_list_rt_cn()
    df.columns = [
        'ts_code', 'name', 'price', 'change_pct', 'amount', 'high', 'low', 'vol_ratio', 'pre_close', 'total_mv', 'float_mv',
        'pe_ttm', 'turnover_rate', 'change', 'volume', 'swing', 'open', 'pb', 'change_pct_60', 'change_pct_this_year'
    ]
    return df

def get_stock_list_rt_cn():
    df = StockListRT.run()
    return df[
        ['代码', '名称', '价格', '涨幅', '成交额', '最高价', '最低价', '量比', '昨收价', '总市值', '流通市值',
         '市盈率-动态', '换手', '涨跌金额', '成交量', '振幅', '开盘价', '市净率', '60日涨幅', '今年涨幅']
    ]

sort_fields = (
    '代码', '名称', '价格', '涨幅', '成交额', '最高价', '最低价', '量比', '昨收价', '总市值', '流通市值',
    '市盈率-动态', '换手',
)

def sort_df_columns(df: pd.DataFrame):
    new_columns = [i for i in sort_fields if i in df.columns] + [i for i in df.columns if i not in sort_fields]
    return df[new_columns]


class StockListRT(ThreadPoolExecutorBase):

    @classmethod
    def run(cls):
        func = cls.realtime_list
        total_page = cls.get_total_page()
        result_df_list = cls.run_by_pool(fetch_func=func, total_page=total_page)

        # 拼接所有分页数据
        final_df = pd.concat(result_df_list, ignore_index=True)
        final_df = final_df.sort_values(by='涨幅', ascending=False).reset_index(drop=True)
        return final_df

    @classmethod
    def get_total_page(cls):
        if hasattr(cls, 'total_page'):
            return getattr(cls, 'total_page')
        (df, total_cnt) = cls.realtime_list()
        page_size = df.shape[0]
        total_page = int(total_cnt / page_size + 1)
        setattr(cls, 'total_page', total_page)
        return total_page

    divide_100_func = lambda x: x / 100
    divide_100_million_func = lambda x: round(x / 100000000, 1)

    field_config = (
        ('f2', '价格', divide_100_func),
        ('f3', '涨幅', divide_100_func),
        ('f4', '涨跌金额', divide_100_func),
        ('f5', '成交量'),
        ('f6', '成交额', divide_100_million_func),
        ('f7', '振幅', divide_100_func),
        ('f8', '换手', divide_100_func),
        ('f9', '市盈率-动态', divide_100_func),
        ('f10', '量比', divide_100_func),
        ('f12', '代码', format_stock_code),
        ('f14', '名称'),
        ('f15', '最高价', divide_100_func),
        ('f16', '最低价', divide_100_func),
        ('f17', '开盘价', divide_100_func),
        ('f18', '昨收价', divide_100_func),
        ('f20', '总市值', divide_100_million_func),
        ('f21', '流通市值', divide_100_million_func),
        ('f23', '市净率', divide_100_func),
        ('f24', '60日涨幅', divide_100_func),
        ('f25', '今年涨幅', divide_100_func),
    )

    @classmethod
    def realtime_list(cls, page_no = None) -> (pd.DataFrame, int):
        """
        东方财富网-沪深京 A 股-实时行情
        https://quote.eastmoney.com/center/gridlist.html#hs_a_board
        :return: 实时行情
        :rtype: pandas.DataFrame、总数
        """
        ut = str(uuid.uuid4()).replace('-', '')

        url = f"http://{randint(1, 100)}.push2.eastmoney.com/api/qt/clist/get"
        fields_str = ",".join([i[0] for i in cls.field_config])
        params = {
            "pn":  "1" if not page_no else str(page_no), # 页码
            "pz": "200", # 单页返回
            "po": "1",
            "np": "1",
            "ut": ut,
            "fltt": "1",
            "invt": "2", # 市场
            "fid": "f12",
            # 京沪深A股 https://quote.eastmoney.com/center/gridlist.html#hs_a_board
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
            # "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
            "fields": fields_str,
            "_": str(time.time()),
        }
        r = requests.get(url, params=params, proxies=cls.get_proxy_conf())
        data_json = r.json()
        if not data_json["data"]["diff"]:
            return pd.DataFrame(), 0
        total_num = data_json['data']['total']

        temp_df = pd.DataFrame(data_json["data"]["diff"])
        column_dict = {i[0]: i[1] for i in cls.field_config}
        apply_func_dict = {i[1]: i[2] for i in cls.field_config if len(i) > 2}
        new_columns = [column_dict.get(i, '-') for i in temp_df.columns]
        temp_df.columns = new_columns
        temp_df.reset_index(inplace=True)
        for col in new_columns:
            if col not in ('代码', '名称'):
                temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")

            if col in apply_func_dict:
                temp_df[col] = temp_df[col].apply(apply_func_dict[col])

        temp_df = temp_df.dropna(subset=['价格'])
        temp_df = sort_df_columns(temp_df)
        df_sorted = temp_df.sort_values(by='涨幅', ascending=False).reset_index(drop=True)
        return df_sorted, total_num


if __name__ == '__main__':

    pd.set_option('display.max_columns', None)  # 显示所有列
    res = get_stock_list_rt()
    print(res)
    print(res.shape)
    print(res.columns)

    res = get_stock_list_rt()
    print(res)
    print(res.shape)
    print(res.columns)

