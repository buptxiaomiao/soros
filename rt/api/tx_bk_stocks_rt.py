#!/usr/bin/env python
# -*- coding:utf-8 -*-

import hashlib
import pandas as pd
import requests
import time
from retrying import retry

from rt.api.tx_sw_hy_rt import get_tx_sw_hy1_rt, get_tx_sw_hy2_rt
from rt.api.thread_pool_executor import ThreadPoolExecutorBase

# 接口
# https://bisheng.tenpay.com/fcgi-bin/xg_plate_stocks.fcgi?exchange=12&plate_code=01801733&sort_type=1&source=zxg&stocks_type=3&time=1770043389166&user_type=4&sign=e36ba88617a2261b7241c33f515a5264
base_url = 'https://bisheng.tenpay.com/fcgi-bin/xg_plate_stocks.fcgi'


def get_tx_bk_stocks_rt_by_plate_code(plate_code):
    """获取腾讯财经板块成分股实时行情
    
    :param plate_code: 板块代码，如 '01801733'
    :return: DataFrame
    """
    return TXBkStocksRT.get_tx_bk_stocks_list(plate_code=plate_code)

def get_sw_hy1_stocks_rt_all():
    """获取申万一级行业板块成分股实时行情"""
    return TXBkStocksRT.get_sw_hy_stocks_rt_all()

def get_sw_hy2_stocks_rt_all():
    """获取申万二级行业板块成分股实时行情"""
    return TXBkStocksRT.get_sw_hy_stocks_rt_all(2)


class TXBkStocksRT(ThreadPoolExecutorBase):
    """
    腾讯财经-板块成分股实时行情
    https://bisheng.tenpay.com/fcgi-bin/xg_plate_stocks.fcgi
    """

    @classmethod
    def get_sw_hy_stocks_rt_all(cls, level=1):
        if level == 2:
            hy_res = get_tx_sw_hy2_rt()
        else:
            hy_res = get_tx_sw_hy1_rt()
        hy_df = hy_res[0]
        bk_code_list = list(hy_df['板块代码'])
        print(bk_code_list)
        args = [[i] for i in bk_code_list]
        result_df_list = cls.run_by_pool_pro(fetch_func=cls.get_tx_bk_stocks_list, args=args)
        print(f"get_sw_hy_stocks_rt_all len(result_df_list)={len(result_df_list)}")
        final_df = pd.concat(result_df_list, ignore_index=True)
        return final_df

    divide_100_func = lambda x: x / 100
    divide_10000_func = lambda x: x / 10000  # 成交量单位处理
    divide_10000_round_func = lambda x: round(x / 10000, 2)  # 成交额/市值单位处理

    field_config = (
        ('stock_code', '股票代码m'),
        ('stock_name', '股票名称'),
        ('price', '最新价'),
        ('change_percent', '涨跌幅'),
        ('market_cap', '总市值'),
        ('main_net_inflow', '主力净流入', divide_10000_round_func),
        ('stock_type', '股票类型'),  # GP-A-CYB
        ('price_change', '涨跌额'),
        ('weight', '板块权重'),
        ('tags', '标签'), # 数组
        ('turnover_money', '成交额', divide_10000_round_func), # 亿
        ('turnover_amount', '成交量'),
        ('turnover_ratio', '换手率'),
        ('amplitude', '振幅'),
        ('quantity_ratio', '量比'),
        ('liutong_cap', '流通市值'), # 亿
        ('pe_ttm', '市盈率(TTM)'),
    )

    @staticmethod
    def get_sign(params):
        # 1. 准备参数
        # 注意：这里的顺序必须和 JS 拼接时的顺序完全一致
        """https://st.gtimg.com/quotes_center/assets/index.d2403baf.js"""

        # 2. 按照 JS 逻辑拼接字符串
        # 遍历 params 拼接 key=value
        temp_list = [f"{k}={v}" for k, v in params.items()]
        # 3. 加上硬编码的 key
        temp_list.append("key=7ad247390dafce0cf9911de0f2083eba")
        raw_str = "&".join(temp_list)
        # 4. MD5 加密
        sign = hashlib.md5(raw_str.encode(encoding='UTF-8')).hexdigest().lower()
        return sign


    @classmethod
    @retry(stop_max_attempt_number=3, wait_fixed=200)
    def get_tx_bk_stocks_list(cls, plate_code, req=None):
        """
        腾讯财经-板块成分股实时行情
        :param plate_code: 板块代码，如 '01801733'
        :param req: 封装的request.Session()实例
        :return: 实时行情 DataFrame、总数
        """
        """https://bisheng.tenpay.com/fcgi-bin/xg_plate_stocks.fcgi?exchange=12&plate_code=01801733&sort_type=1&source=zxg&stocks_type=3&time=1770043389166&user_type=4&sign=e36ba88617a2261b7241c33f515a5264"""

        params = {
            "exchange": '12',           # 交易所
            "plate_code": plate_code,       # 板块代码
            "sort_type": "1",               # 排序类型：1=涨跌幅
            "source": "zxg",                # 来源
            "stocks_type": "3",             # 股票类型
            "time": str(int(time.time() * 1000)),  # 时间戳（毫秒）
            "user_type": "4",
        }
        sign = cls.get_sign(params)
        params["sign"] = sign

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.4668.55 Safari/537.36',
            'Referer': 'https://stockapp.finance.qq.com/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
        }

        if req is None:
            r = cls.session.get(base_url, params=params, proxies=cls.get_proxy_conf(),headers=headers)
        else:
            r = req.get(base_url, params=params, proxies=cls.get_proxy_conf(),headers=headers)
        data_json = r.json()
        print(data_json)

        quote_statis = data_json['quote_statis']
        down_cnt = quote_statis['down_cnt']
        up_cnt = quote_statis['up_cnt']
        even_cnt = quote_statis['even_cnt']
        limit_up = quote_statis['limit_up']
        limit_down = quote_statis['limit_down']

        stocks = data_json.get("stocks", {})

        total_num = stocks.get("total_num", 0)
        stock_list = stocks.get("stocks_list", [])
        # "stock_code":"300870.SZ",
        # "stock_name":"欧陆通",
        # "price":242.500000,
        # "change_percent":-2.830000,
        # "market_cap":266.410000,
        # "main_net_inflow":35360400.000000,
        # "stock_type":"GP-A-CYB",
        # "price_change":-7.070000,
        # "weight":3.194552,
        # "tags":[
        # ],
        # "turnover_money":158950.000000,
        # "turnover_amount":63242.000000,
        # "turnover_ratio":5.760000,
        # "amplitude":6.570000,
        # "quantity_ratio":0.830000,
        # "liutong_cap":266.410000,
        # "pe_ttm":79.990000

        if not stock_list:
            return pd.DataFrame(), 0

        # 转换为 DataFrame
        temp_df = pd.DataFrame(stock_list)

        # 字段重命名
        column_map = {item[0]: item[1] for item in cls.field_config if item[0] in temp_df.columns}
        temp_df.rename(columns=column_map, inplace=True)

        print(temp_df)

        # 数值类型转换（必须在应用转换函数之前）
        numeric_cols = ['最新价', '涨跌额', '涨跌幅', '5日涨跌幅', '20日涨跌幅',
                        '60日涨跌幅', '年初至今涨跌幅', '换手率', '流通市值',
                        '总市值', '成交量', '成交额', '主力净流入',
                        '主力5日净流入', '主力20日净流入', '主力流出', '主力流入',
                        '涨速', '量比', '市净率', '市盈率', '总股本', '流通股本', '板块权重',
                        '振幅', '市盈率(TTM)']
        for col in numeric_cols:
            if col in temp_df.columns:
                print(f"begin. {col}")
                temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")

        # 应用转换函数（此时已是数值类型）
        for item in cls.field_config:
            if len(item) > 2 and item[1] in temp_df.columns:
                temp_df[item[1]] = temp_df[item[1]].apply(item[2])

        # 处理领涨股_code：新增领涨股_mcode保存原始值，code去掉前两位市场前缀
        if '股票代码m' in temp_df.columns:
            temp_df['股票代码'] = temp_df['股票代码m'].astype(str).str[:6]

        if '标签' in temp_df.columns:
            temp_df['标签'] = (temp_df['标签'].astype(str).str.strip('[]')
                               .str.replace('"', '').str.replace("'", '').str.replace(',', '、'))

        if '股票名称' in temp_df.columns:
            temp_df['股票名称'] = temp_df['股票名称'].str.replace('\u3000', '').str.replace(' ', '')

        temp_df['板块代码'] = plate_code

        all_columns = [
            '板块代码',
            # 基础信息
            '股票代码m', '股票代码', '股票名称',
            # 价格指标
            '最新价', '涨跌额', '涨跌幅', '板块权重',
            # 资金指标
            '主力净流入',
            # 成交量指标
            '成交额',  '量比', '振幅',  '成交量', '换手率',
            # 市值指标
            '总市值', '流通市值', '市盈率(TTM)',
            '股票类型', '标签',
        ]
        # 筛选存在的列
        new_columns = [col for col in all_columns if col in temp_df.columns]
        temp_df = temp_df[new_columns]

        return temp_df, total_num


def test_sign():
    """https://bisheng.tenpay.com/fcgi-bin/xg_plate_stocks.fcgi?exchange=12&plate_code=01801733&sort_type=1&source=zxg&stocks_type=3&time=1770043389166&user_type=4&sign=e36ba88617a2261b7241c33f515a5264"""
    params = {
        "exchange": "12",
        "plate_code": '01801733',
        "sort_type": "1",
        "source": "zxg",
        "stocks_type": "3",
        # "time": str(int(time.time() * 1000)),  # 13位毫秒时间戳
        "time": "1770043389166",  # 13位毫秒时间戳
        "user_type": "4"
    }
    test_res = TXBkStocksRT.get_sign(params)
    print(test_res)
    print(test_res=='e36ba88617a2261b7241c33f515a5264')

def test_func():

    # 示例：获取某个板块的成分股
    # https://stockapp.finance.qq.com/mstats/#mod=list&id=pt01801733&typename=%E5%85%B6%E4%BB%96%E7%94%B5%E6%BA%90%E8%AE%BE%E5%A4%87%E2%85%A1&sign=web
    p_code = '01801733'  # 替换为实际的板块代码 其他电源设备Ⅱ
    res = get_tx_bk_stocks_rt_by_plate_code(plate_code=p_code)
    df = res[0]
    print(df)
    print(df.shape)
    print(df.columns)
    df.to_csv('tx_bk_stocks_rt.csv', index=False, header=True)

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)  # 显示所有列

    res1 = get_sw_hy1_stocks_rt_all()
    res1.to_csv('sw_hy1_stocks_rt.csv', index=False, header=True)

    res2 = get_sw_hy2_stocks_rt_all()
    res2.to_csv('sw_hy2_stocks_rt.csv', index=False, header=True)
    pass
