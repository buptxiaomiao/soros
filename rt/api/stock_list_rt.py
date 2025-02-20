#!/usr/bin/env python
# -*- coding:utf-8 -*-

import time
import uuid
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from random import randint

import pandas as pd
import requests
from tushare.util.format_stock_code import format_stock_code

from rt.api.thread_pool_executor import ThreadPoolExecutorBase


def get_stock_list_rt():
    return StockListRT.run()

class StockListRT(ThreadPoolExecutorBase):

    @classmethod
    def run(cls):
        func = cls.realtime_list
        total_page = cls.get_total_page()
        result_df_list = cls.run_by_pool(fetch_func=func, total_page=total_page)

        # 拼接所有分页数据
        final_df = pd.concat(result_df_list, ignore_index=True)
        final_df = final_df.sort_values(by='PCT_CHANGE', ascending=False).reset_index(drop=True)
        final_df['PRICE'] = final_df['PRICE'] / 100
        final_df['PCT_CHANGE'] = final_df['PCT_CHANGE'] / 100
        final_df['CHANGE'] = final_df['CHANGE'] / 100
        final_df['AMOUNT'] = final_df['AMOUNT'] / 100000000
        final_df['SWING'] = final_df['SWING'] / 100
        final_df['HIGH'] = final_df['HIGH'] / 100
        final_df['LOW'] = final_df['LOW'] / 100
        final_df['OPEN'] = final_df['OPEN'] / 100
        final_df['CLOSE'] = final_df['CLOSE'] / 100
        final_df['VOL_RATIO'] = final_df['VOL_RATIO'] / 100
        final_df['TURNOVER_RATE'] = final_df['TURNOVER_RATE'] / 100
        return final_df

    @classmethod
    def get_total_page(cls):
        if hasattr(cls, 'total_page'):
            return getattr(cls, 'total_page')
        _res = cls.realtime_list()
        return int(_res[1] / 200 + 1)

    @classmethod
    def realtime_list(cls, page_no = None) -> (pd.DataFrame, int):
        """
        东方财富网-沪深京 A 股-实时行情
        https://quote.eastmoney.com/center/gridlist.html#hs_a_board
        :return: 实时行情
        :rtype: pandas.DataFrame、总数、
            1、序号:RANK
            2、代码:TS_CODE
            3、名称:NAME
            4、最新价:PRICE
            5、涨跌幅:PCT_CHANGE
            6、涨跌额:CHANGE
            7、成交量:VOLUME
            8、成交额:AMOUNT
            9、振幅:SWING
            10、最高:HIGH
            11、最低:LOW
            12、今开:OPEN
            13、昨收:CLOSE
            14、量比:VOL_RATIO
            15、换手率:TURNOVER_RATE
            16、市盈率-动态:PE
            17、市净率:PB
            18、总市值:TOTAL_MV
            19、流通市值:FLOAT_MV
            20、涨速:RISE
            21、5分钟涨跌:5MIN
            22、60日涨跌幅:60DAY
            23、年初至今涨跌幅:1YEAR
        """
        ut = str(uuid.uuid4()).replace('-', '')

        url = f"http://{randint(1, 100)}.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn":  "1" if not page_no else str(page_no), # 页码
            "pz": "200", # 单页返回
            "po": "1",
            "np": "1",
            "ut": ut,
            "fltt": "1",
            "invt": "2",
            "fid": "f12",
            "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
            "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152",
            "_": str(time.time()),
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        if not data_json["data"]["diff"]:
            return pd.DataFrame(), 0
        total_num = data_json['data']['total']

        temp_df = pd.DataFrame(data_json["data"]["diff"])
        temp_df.columns = [
            "_",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "成交量",
            "成交额",
            "振幅",
            "换手率",
            "市盈率-动态",
            "量比",
            "5分钟涨跌",
            "代码",
            "_",
            "名称",
            "最高",
            "最低",
            "今开",
            "昨收",
            "总市值",
            "流通市值",
            "涨速",
            "市净率",
            "60日涨跌幅",
            "年初至今涨跌幅",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
        ]
        temp_df.reset_index(inplace=True)
        # temp_df["index"] = temp_df.index + 1
        # temp_df.rename(columns={"index": "序号"}, inplace=True)
        temp_df = temp_df[
            [
                # "序号",
                "代码",
                "名称",
                "最新价",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "振幅",
                "最高",
                "最低",
                "今开",
                "昨收",
                "量比",
                "换手率",
                "市盈率-动态",
                "市净率",
                "总市值",
                "流通市值",
                "涨速",
                "5分钟涨跌",
                "60日涨跌幅",
                "年初至今涨跌幅",
            ]
        ]

        temp_df["代码"] = temp_df["代码"].apply(format_stock_code)
        temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
        temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
        temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
        temp_df["今开"] = pd.to_numeric(temp_df["今开"], errors="coerce")
        temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce")
        temp_df["量比"] = pd.to_numeric(temp_df["量比"], errors="coerce")
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
        temp_df["市盈率-动态"] = pd.to_numeric(temp_df["市盈率-动态"], errors="coerce")
        temp_df["市净率"] = pd.to_numeric(temp_df["市净率"], errors="coerce")
        temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
        temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")
        temp_df["涨速"] = pd.to_numeric(temp_df["涨速"], errors="coerce")
        temp_df["5分钟涨跌"] = pd.to_numeric(temp_df["5分钟涨跌"], errors="coerce")
        temp_df["60日涨跌幅"] = pd.to_numeric(temp_df["60日涨跌幅"], errors="coerce")
        temp_df["年初至今涨跌幅"] = pd.to_numeric(temp_df["年初至今涨跌幅"], errors="coerce")
        temp_df.columns = [
            # "RANK",
            "TS_CODE",
            "NAME",
            "PRICE",
            "PCT_CHANGE",
            "CHANGE",
            "VOLUME",
            "AMOUNT",
            "SWING",
            "HIGH",
            "LOW",
            "OPEN",
            "CLOSE",
            "VOL_RATIO",
            "TURNOVER_RATE",
            "PE",
            "PB",
            "TOTAL_MV",
            "FLOAT_MV",
            "RISE",
            "5MIN",
            "60DAY",
            "1YEAR",
        ]
        temp_df = temp_df.dropna(subset=['PRICE'])
        df_sorted = temp_df.sort_values(by='PCT_CHANGE', ascending=False).reset_index(drop=True)
        return df_sorted, total_num


if __name__ == '__main__':

    pd.set_option('display.max_columns', None)  # 显示所有列
    res = get_stock_list_rt()
    print(res)
    print(res.shape)
    # print(res[0].shape)
    # print(res[1])
