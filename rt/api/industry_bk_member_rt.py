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
from rt.api.industry_bk_list_rt import IndustryBKListRT


def get_bk_member_df():
    industry_bk_tuple_list = IndustryBKListRT.get_bk_tuple_list()
    return BKMemberRT.run(industry_bk_tuple_list)


sort_fields = (
    'bk_code', 'bk_name', '股票代码', '股票名称', '主力净流入', '超大单净额'
)

def sort_df_columns(df: pd.DataFrame):
    new_columns = [i for i in sort_fields if i in df.columns] + [i for i in df.columns if i not in sort_fields]
    return df[new_columns]


class BKMemberRT(ThreadPoolExecutorBase):

    @classmethod
    def run(cls, bk_tuple_list=(('BK0910', '通用设备'), )):
        """获取指定板块的所有成分股
        :param bk_tuple_list: 板块代码，如 BK0910 表示通用设备
        :return: 成分股的DataFrame
        """

        func = cls.component_stocks
        # total_page = cls.get_total_page()
        args = ([list(t) + [1] for t in bk_tuple_list]
                + [list(t) + [2] for t in bk_tuple_list]
                + [list(t) + [3] for t in bk_tuple_list])
        result_df_list = cls.run_by_pool_pro(fetch_func=func, args=args)

        final_df = pd.concat(result_df_list, ignore_index=True)
        final_df = final_df.sort_values(by='涨幅', ascending=False)
        final_df = final_df.drop_duplicates(subset=['股票代码']).reset_index(drop=True)
        final_df = sort_df_columns(final_df)
        print(f"BKMemberRT.final_df.shape={final_df.shape}")
        return final_df

    # @classmethod
    # def get_total_page(cls, page_size=50):
    #     if hasattr(cls, 'total_page'):
    #         return getattr(cls, 'total_page')
    #
    #     df, total_cnt = cls.component_stocks()
    #     actual_size = len(df)
    #     print(f"actual_size={actual_size}")
    #     total_page = (total_cnt // actual_size) + 1 if actual_size > 0 else 0
    #     setattr(cls, 'total_page', total_page)
    #     return total_page

    # 字段处理函数
    divide_100 = lambda x: x / 100 if x != "-" else None
    divide_10k = lambda x: x / 10000 if x != "-" else None
    code_format = lambda x: format_stock_code(x) if x != "-" else None

    # 字段映射配置
    field_config = (
        ('f12', '股票代码', code_format),
        ('f14', '股票名称'),
        ('f2', '最新价', divide_100),
        ('f3', '涨幅', divide_100),  # 百分比值
        ('f62', '主力净流入', divide_10k),  # 单位：万元
        ('f75', '换手率', divide_100),  # 百分比
        ('f78', '超大单净额', divide_10k),  # 单位：万元
        ('f81', '超大单占比', divide_100),  # 百分比
        ('f84', '大单净额', divide_10k),  # 单位：万元
        ('f87', '大单占比', divide_100),  # 百分比
        ('f184', '中单占比', divide_100),  # 百分比
        ('f66', '小单净额', divide_10k),  # 单位：万元
        ('f69', '小单占比', divide_100),  # 百分比
        ('f124', '更新时间戳'),
        ('f13', '市场类型'),  # 0: 深市/A股，1: 沪市
    )

    @classmethod
    @retry(stop_max_attempt_number=3, wait_fixed=500)
    def component_stocks(cls, bk_code='BK0910', bk_name='专用设备', page_no=None) -> (pd.DataFrame, int):
        ut = str(uuid.uuid4()).replace('-', '')
        url = f"http://{randint(1, 100)}.push2.eastmoney.com/api/qt/clist/get"

        # print(f"component_stocks begin. bk_code={bk_code}, bk_name={bk_name}")

        # 构造请求参数
        params = {
            "pn": "1" if not page_no else str(page_no),
            # "pn": "1",
            "pz": "100",
            "po": "1",
            "np": "1",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": f"b:{bk_code}",  # 动态板块代码
            "fields": ",".join([f[0] for f in cls.field_config]),
            "ut": ut,
            "_": str(int(time.time() * 1000)),
        }

        # 发送请求
        r = requests.get(url, params=params, proxies=cls.get_proxy_conf())
        data_json = r.json()
        # print(data_json)

        # 解析JSONP格式
        # json_str = r.text.split('(', 1)[1].rsplit(')', 1)[0]
        # data = json.loads(json_str)

        if data_json is None or not data_json.get('data') or not data_json['data'].get('diff'):
            return pd.DataFrame(), 0

        total_num = data_json['data']['total']
        # print(f"total_num={total_num}")
        temp_df = pd.DataFrame(data_json['data']['diff'])
        # if temp_df.shape[0] < total_num:
        #     print(f"component_stocks. bk_code={bk_code}, bk_name={bk_name} total_num={total_num}, but temp_df.num={temp_df.shape[0]}")

        # 字段处理
        for config in cls.field_config:
            if len(config) > 2:
                col_name = config[1]
                temp_df[col_name] = temp_df[config[0]].apply(config[2])
            else:
                temp_df.rename(columns={config[0]: config[1]}, inplace=True)

        # 保留需要的列
        keep_columns = [c[1] for c in cls.field_config]
        temp_df = temp_df[keep_columns]

        # 处理特殊值
        numeric_cols = ['最新价', '涨幅', '主力净流入', '换手率',
                        '超大单净额', '超大单占比', '大单净额', '大单占比',
                        '中单占比', '小单净额', '小单占比']
        for col in numeric_cols:
            temp_df[col] = pd.to_numeric(temp_df[col], errors='coerce')

        # 时间戳转换
        if '更新时间戳' in temp_df.columns:
            temp_df['更新时间'] = pd.to_datetime(
                temp_df['更新时间戳'], unit='s', errors='coerce'
            )

        temp_df['bk_code'] = bk_code
        temp_df['bk_name'] = bk_name
        return temp_df, total_num


# 使用示例
if __name__ == '__main__':

    pd.set_option('display.max_columns', None)  # 显示所有列

    # 获取通用设备板块(BK0910)成分股
    df = BKMemberRT.run(bk_tuple_list=(('BK0910', '通用设备'), ))
    # print(f"共获取 {len(df)} 只成分股")
    # print(df[['股票代码', '股票名称', '最新价', '涨幅', '主力净流入']].head())
    print(df)
    print(df.shape)
    print(df.columns)

    df = get_bk_member_df()
    print(df)
    print(df.shape)
    print(df.columns)
