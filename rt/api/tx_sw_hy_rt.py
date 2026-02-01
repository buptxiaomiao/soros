#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pandas as pd
import requests
from retrying import retry

from rt.api.thread_pool_executor import ThreadPoolExecutorBase

# 页面
# https://stockapp.finance.qq.com/mstats/#mod=list&id=hy_first&module=hy&type=first&sort=8&page=1&max=20
# 接口
base_url = 'https://proxy.finance.qq.com/cgi/cgi-bin/rank/pt/getRank'


def get_tx_sw_hy1_rt():
    """获取腾讯财经申万一级行业实时行情"""
    return TXSwHyRT.get_tx_sw_hy_list()


def get_tx_sw_hy2_rt():
    """获取腾讯财经申万二级行业板块实时行情"""
    return TXSwHyRT.get_tx_sw_hy_list(2)


class TXSwHyRT(ThreadPoolExecutorBase):
    """
    腾讯财经-申万行业板块实时行情
    """

    @classmethod
    def run(cls):
        func = cls.get_tx_sw_hy_list
        total_page = cls.get_total_page()
        result_df_list = cls.run_by_pool(fetch_func=func, total_page=total_page)

        # 拼接所有分页数据
        final_df = pd.concat(result_df_list, ignore_index=True)
        final_df = final_df.sort_values(by='涨跌幅', ascending=False).reset_index(drop=True)
        return final_df

    @classmethod
    def get_total_page(cls, page_size=20):
        if hasattr(cls, 'total_page'):
            return getattr(cls, 'total_page')
        (df, total_cnt) = cls.get_tx_sw_hy_list(page_no=1)
        actual_size = df.shape[0]
        total_page = int(total_cnt / actual_size + 1) if actual_size > 0 else 0
        setattr(cls, 'total_page', total_page)
        return total_page

    divide_100_func = lambda x: x / 100
    divide_10000_func = lambda x: x / 10000  # 成交量单位处理
    divide_10000_round_func = lambda x: round(x / 10000, 2)  # 成交额/市值单位处理

    field_config = (
        ('code', '板块代码'),
        ('name', '板块名称'),
        ('zxj', '最新价'),
        ('zd', '涨跌额'),
        ('zdf', '涨跌幅'),
        ('zdf_d5', '5日涨跌幅'),
        ('zdf_d20', '20日涨跌幅'),
        ('zdf_d60', '60日涨跌幅'),
        ('zdf_y', '年初至今涨跌幅'),
        ('hsl', '换手率'),
        ('ltsz', '流通市值', divide_10000_round_func),
        ('zsz', '总市值', divide_10000_round_func),
        ('volume', '成交量', divide_10000_func),
        ('turnover', '成交额', divide_10000_round_func),
        ('zljlr', '主力净流入', divide_10000_round_func),
        ('zljlr_d5', '主力5日净流入', divide_10000_round_func),
        ('zljlr_d20', '主力20日净流入', divide_10000_round_func),
        ('zllc', '主力流出', divide_10000_round_func),
        ('zllr', '主力流入', divide_10000_round_func),
        ('speed', '涨速'),
        ('lb', '量比'),
        ('zgb', '个股数量'),
    )

    @classmethod
    @retry(stop_max_attempt_number=3, wait_fixed=200)
    def get_tx_sw_hy_list(cls, hy_level=1, req=None):
        """
        腾讯财经-申万行业板块实时行情
        swbk: 申万板块类型
        req: 封装的request.Session()实例
        :return: 实时行情 DataFrame、总数
        """

        bk_type_map = {
            1: 'hy',  # stock_type='BK-HY-1'
            2: 'hy2'  # stock_type='BK-HY-2'
        }

        params = {
            "board_type": bk_type_map.get(hy_level, 'hy'),             # 板块类型：hy=行业
            "sort_type": "priceRatioW52",   # 排序类型：52周涨幅
            "direct": "down",               # 排序方向：down=降序
            "offset": 0,               # 偏移量
            "count": 200,                 # 每页数量
        }

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.4668.55 Safari/537.36',
            'Referer': 'https://stockapp.finance.qq.com/'
        }

        if req is None:
            r = requests.get(base_url, params=params, proxies=cls.get_proxy_conf()
                             , headers=headers
                             )
        else:
            r = req.get(base_url, params=params, proxies=cls.get_proxy_conf()
                        , headers=headers
                        )
        data_json = r.json()

        if data_json is None or data_json.get("code") != 0 or not data_json.get("data"):
            return pd.DataFrame(), 0

        data = data_json['data']
        total_num = data.get('total', 0)
        rank_list = data.get('rank_list', [])

        if not rank_list:
            return pd.DataFrame(), total_num

        # 转换为 DataFrame
        temp_df = pd.DataFrame(rank_list)

        # 处理领涨股信息（lzg字段是对象，需要展开）
        if 'lzg' in temp_df.columns:
            # 展开领涨股信息
            lzg_df = pd.json_normalize(temp_df['lzg'])
            lzg_df.columns = [f'领涨股_{col}' for col in lzg_df.columns]
            temp_df = pd.concat([temp_df.drop('lzg', axis=1), lzg_df], axis=1)

        # 字段重命名
        column_map = {item[0]: item[1] for item in cls.field_config if item[0] in temp_df.columns}
        temp_df.rename(columns=column_map, inplace=True)

        # 数值类型转换（必须在应用转换函数之前）
        numeric_cols = ['最新价', '涨跌额', '涨跌幅', '5日涨跌幅', '20日涨跌幅',
                        '60日涨跌幅', '年初至今涨跌幅', '换手率', '流通市值',
                        '总市值', '成交量', '成交额', '主力净流入',
                        '主力5日净流入', '主力20日净流入', '主力流出', '主力流入',
                        '涨速', '量比']

        for col in numeric_cols:
            if col in temp_df.columns:
                temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")

        # 应用转换函数（此时已是数值类型）
        for item in cls.field_config:
            if len(item) > 2 and item[1] in temp_df.columns:
                temp_df[item[1]] = temp_df[item[1]].apply(item[2])

        # 处理领涨股_code：新增领涨股_mcode保存原始值，code去掉前两位市场前缀
        if '领涨股_code' in temp_df.columns:
            temp_df['领涨股_mcode'] = temp_df['领涨股_code']  # 保存原始值
            temp_df['领涨股_code'] = temp_df['领涨股_code'].astype(str).str[2:]  # 去掉前两位

        # 拆解个股数量字段（格式如 "83/335"）
        if '个股数量' in temp_df.columns:
            # 拆分上涨数量和下跌数量
            temp_df[['上涨数量', '下跌数量']] = temp_df['个股数量'].astype(str).str.split('/', expand=True)
            temp_df['上涨数量'] = pd.to_numeric(temp_df['上涨数量'], errors='coerce')
            temp_df['下跌数量'] = pd.to_numeric(temp_df['下跌数量'], errors='coerce')
            # 计算上涨下跌个数比（下跌为0时固定为999，保留两位小数）
            temp_df['上涨下跌个数比'] = temp_df.apply(
                lambda row: 999 if row['下跌数量'] == 0 else round(row['上涨数量'] / row['下跌数量'], 2),
                axis=1
            )

        # 计算主力流入流出比（主力流出为0时为空值，保留两位小数）
        if '主力流入' in temp_df.columns and '主力流出' in temp_df.columns:
            temp_df['主力流入流出比'] = temp_df.apply(
                lambda row: None if row['主力流出'] == 0 else round(row['主力流入'] / row['主力流出'], 2),
                axis=1
            )

        # 完整版字段列表（按业务逻辑分组排序）
        # ======================================
        # 基础信息
        #   板块代码      - 申万行业板块代码
        #   板块名称      - 行业板块名称
        # 价格指标
        #   最新价        - 板块指数最新价格
        #   涨跌额        - 板块指数涨跌金额
        # 涨跌幅指标（单位：%，如 2.5 表示 2.5%）
        #   涨跌幅        - 当日涨跌幅
        #   涨速          - 上涨速度（近期涨跌斜率）
        #   5日涨跌幅     - 近5个交易日累计涨跌幅
        #   20日涨跌幅    - 近20个交易日累计涨跌幅
        #   60日涨跌幅    - 近60个交易日累计涨跌幅
        #   年初至今涨跌幅 - 当年累计涨跌幅
        #   zdf_w52       - 52周涨跌幅
        # 成交量指标
        #   成交量        - 当日成交股数（单位：万股）
        #   成交额        - 当日成交金额（单位：亿元）
        #   换手率        - 板块整体换手率（单位：%）
        #   量比          - 当日成交量/近5日平均成交量（>1表示放量）
        # 市值指标（单位：亿元）
        #   流通市值      - 板块流通市值合计  万亿
        #   总市值        - 板块总市值合计   万亿
        # 资金指标（单位：亿元，正数表示流入）
        #   主力净流入    - 主力资金净流入金额
        #   主力5日净流入      - 5日主力净流入 亿元
        #   主力20日净流入     - 20日主力净流入 亿元
        #   主力流出          - 主力流出金额 亿元
        #   主力流入          - 主力流入金额 亿元
        # 板块统计
        #   个股数量          - 板块内股票总数（原始格式：上涨数/下跌数，如 "83/335"）
        #   上涨数量          - 上涨个股数量（从个股数量拆解）
        #   下跌数量          - 下跌个股数量（从个股数量拆解）
        #   上涨下跌个数比    - 上涨数量/下跌数量（下跌为0时固定为999）
        #   stock_type        - 股票类型标识（腾讯内部类型代码）BK-HY-1 / BK-HY-2
        # 资金指标补充
        #   主力流入流出比    - 主力流入/主力流出（主力流出为0时为空值）
        # 领涨股信息
        #   领涨股_mcode  - 领涨股代码（含市场前缀，如 sh600001）
        #   领涨股_code   - 领涨股代码（纯代码，去掉前两位市场前缀，如 600001）
        #   领涨股_name   - 领涨股名称
        #   领涨股_zd     - 领涨股涨跌额
        #   领涨股_zdf    - 领涨股涨跌幅（单位：%）
        #   领涨股_zxj    - 领涨股最新价
        # ======================================
        all_columns = [
            # 基础信息
            '板块代码', '板块名称',
            # 价格指标
            '最新价', '涨跌额',
            # 涨跌幅指标
            '涨跌幅', '涨速',
            '5日涨跌幅', '20日涨跌幅', '60日涨跌幅', '年初至今涨跌幅', 'zdf_w52',
            # 成交量指标
            '成交量', '成交额', '换手率', '量比',
            # 市值指标
            '流通市值', '总市值',
            # 资金指标
            '主力净流入', '主力5日净流入', '主力20日净流入', '主力流出', '主力流入',
            # 板块统计
            '个股数量', '上涨数量', '下跌数量', '上涨下跌个数比', 'stock_type',
            # 资金指标补充
            '主力流入流出比',
            # 领涨股信息
            '领涨股_mcode', '领涨股_code', '领涨股_name', '领涨股_zd', '领涨股_zdf', '领涨股_zxj',
        ]
        # 筛选存在的列
        new_columns = [col for col in all_columns if col in temp_df.columns]
        temp_df = temp_df[new_columns]

        return temp_df, total_num


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)  # 显示所有列
    res1 = get_tx_sw_hy1_rt()
    df1 = res1[0]
    print(df1)
    print(df1.shape)
    print(df1.columns)
    df1.to_csv('tx_sw_hy1_rt.csv', index=False, header=True)

    res2 = get_tx_sw_hy2_rt()
    df2 = res2[0]
    print(df2)
    print(df2.shape)
    print(df2.columns)
    df2.to_csv('tx_sw_hy2_rt.csv', index=False, header=True)
