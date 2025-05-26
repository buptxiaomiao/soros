#!/usr/bin/env python
# -*- coding:utf-8 -*-

import time
import uuid
import pandas as pd
import requests
from random import randint
from tushare.util.format_stock_code import format_stock_code
from retrying import retry

from rt.api.thread_pool_executor import ThreadPoolExecutorBase



sort_fields = (
    '板块代码', '板块名称', '涨幅', '成交额', '总市值', '上涨家数', '下跌家数', '最新价'
)

def sort_df_columns(df: pd.DataFrame):
    new_columns = [i for i in sort_fields if i in df.columns] + [i for i in df.columns if i not in sort_fields]
    return df[new_columns]

class IndustryBKListRT(ThreadPoolExecutorBase):

    @classmethod
    def get_bk_tuple_list(cls):
        if hasattr(cls, '_bk_tuple_list'):
            return getattr(cls, '_bk_tuple_list')
        df = cls.run()
        # 使用 zip 合并为元组列表
        tuple_list = list(zip(df['板块代码'], df['板块名称']))
        # print(tuple_list)
        # 输出示例：
        # [('BK0422', '物流行业'), ('BK1043', '专业服务'), ...]
        setattr(cls, '_bk_tuple_list', tuple_list)
        return tuple_list

    @classmethod
    def run(cls):
        func = cls.industry_bk_list
        total_page = cls.get_total_page()
        result_df_list = cls.run_by_pool(fetch_func=func, total_page=total_page)

        # 拼接所有分页数据
        final_df = pd.concat(result_df_list, ignore_index=True)
        final_df = final_df.sort_values(by='涨幅', ascending=False).reset_index(drop=True)
        final_df = sort_df_columns(final_df)
        return final_df

    @classmethod
    def get_total_page(cls, page_size=20):
        if hasattr(cls, 'total_page'):
            return getattr(cls, 'total_page')
        (df, total_cnt) = cls.industry_bk_list()
        actual_size = df.shape[0]
        total_page = int(total_cnt / actual_size + 1) if actual_size > 0 else 0
        setattr(cls, 'total_page', total_page)
        return total_page

    divide_100_func = lambda x: x / 100
    divide_10k_func = lambda x: x / 10000  # 成交量单位处理
    divide_100_million_func = lambda x: round(x / 100000000, 1)  # 成交额/市值单位处理

    field_config = (
        ('f12', '板块代码'),
        ('f14', '板块名称'),
        ('f3', '涨幅'),
        ('f2', '最新价'),
        ('f4', '涨跌额'),
        # ('f5', '成交量', divide_10k_func),
        ('f6', '成交额', divide_100_million_func),
        ('f104', '上涨家数'),
        ('f105', '下跌家数'),
        ('f128', '领涨股票'),
        ('f140', '领涨股代码', format_stock_code),
        ('f207', '领跌股票'),
        ('f208', '领跌股代码', format_stock_code),
        ('f20', '总市值', divide_100_million_func),
        ('f136', '换手率'),
        ('f8', '振幅'),
        ('f222', '净流入'),
    )

    @classmethod
    @retry(stop_max_attempt_number=3, wait_fixed=200)
    def industry_bk_list(cls, page_no=None) -> (pd.DataFrame, int):
        """
        东方财富网-板块实时行情
        https://quote.eastmoney.com/center/boardlist.html#boards-BK0465
        """
        ut = str(uuid.uuid4()).replace('-', '')

        url = f"http://{randint(1, 100)}.push2.eastmoney.com/api/qt/clist/get"
        fields_str = ",".join([i[0] for i in cls.field_config])
        params = {
            "pn": "1" if not page_no else str(page_no),
            "pz": "20",  # 接口默认分页大小
            "po": "1",
            "np": "1",
            "ut": ut,
            "fltt": "2",
            "invt": "2",
            "fid": "f3",
            # 板块特殊参数：fs:m:90+t:2（行业板块）可根据需要调整
            "fs": "m:90+t:2",
            "fields": fields_str,
            "_": str(int(time.time() * 1000)),
        }

        r = requests.get(url, params=params, proxies=cls.get_proxy_conf())
        data_json = r.json()

        if data_json is None or not data_json.get("data") or not data_json["data"].get("diff"):
            return pd.DataFrame(), 0

        total_num = data_json['data']['total']
        temp_df = pd.DataFrame(data_json["data"]["diff"])

        # 字段处理
        column_map = {item[0]: item[1] for item in cls.field_config}
        temp_df.rename(columns=column_map, inplace=True)

        # 应用转换函数
        for item in cls.field_config:
            if len(item) > 2 and item[1] in temp_df.columns:
                temp_df[item[1]] = temp_df[item[1]].apply(item[2])

        # 类型转换
        numeric_cols = ['涨幅', '最新价', '涨跌额', '成交额', '上涨家数',
                        '下跌家数', '总市值', '换手率', '振幅', '净流入']
        for col in numeric_cols:
            if col in temp_df.columns:
                temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")

        return temp_df, total_num


# 使用示例
def get_plate_list_rt():
    df = IndustryBKListRT.run()
    # df.columns = [
    #     'plate_code', 'plate_name', 'change_pct', 'price', 'change_amt',
    #     'volume', 'amount', 'up_count', 'down_count', 'leader_stock',
    #     'leader_code', 'loser_stock', 'loser_code', 'total_mv', 'turnover',
    #     'swing', 'net_inflow'
    # ]
    return df

if __name__ == '__main__':

    pd.set_option('display.max_columns', None)  # 显示所有列
    df = get_plate_list_rt()
    print(df)
    print(df.shape)
    print(df.columns)

    bk_map = IndustryBKListRT.get_bk_tuple_list()
    print(bk_map)

