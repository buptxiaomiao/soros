# coding: utf-8
import datetime

import numpy as np
import pandas as pd
import requests
import sys
import os


sys.path.append('..')
sys.path.append('../..')

from utils.setting import BARK_KEY
from utils.cache_manager import CacheManager
from rt.api.daily import Daily
from rt.api.stock_list_rt import get_stock_list_rt
from rt.api.industry_bk_member_rt import get_bk_member_df
from utils.now import Now

pd.set_option('display.max_rows', None)  # 设置为 None 表示不限制行数
pd.set_option('display.max_columns', None)  # 设置为 None 表示不限制列数

# 发送通知的函数
def send_notification(msg):
    return True
    # 这里需要替换为你的Bark应用的device_key
    device_key = BARK_KEY
    print(BARK_KEY)
    group = '日线BOLL'
    url = f'https://api.day.app/{device_key}?group={group}&title={group}&body={msg}'
    # headers = {'Content-Type': 'application/json'}
    # response = requests.post(url, headers=headers, json={'content': msg})
    response = requests.get(url)
    return response.status_code == 200

class Boll:

    cache_manager = CacheManager(f"/tmp/boll_{Now().date}.pickle")

    @classmethod
    def get_data_remote(cls):
        # 这个函数是一个占位符，你需要实现它来从远程数据源获取数据
        # 这里只是一个示例，返回一个空的DataFrame
        now = Now()
        end_date = str(now.delta(1).date)
        start_date = str(now.delta(40).date)
        df = Daily.get_df(start_date, end_date)
        df['dt'] = df['trade_date'].astype(str)
        df['price'] = df['close']
        df['name'] = ''
        df['amount'] = df['amount'] / 100000
        return df[ ['ts_code', 'dt', 'price', 'high', 'low', 'name', 'amount'] ]

    @classmethod
    def save_dataframe_to_csv(cls, dataframe, filename):
        # 将DataFrame保存到CSV文件
        dataframe.to_csv(filename, index=False)
        print(f"Data saved to {filename}")

    @classmethod
    def load_dataframe_from_csv(cls, filename):
        # 从CSV文件加载DataFrame
        return pd.read_csv(filename)

    @classmethod
    def ensure_dataframe(cls, filename):
        # 检查本地CSV文件是否存在
        if not os.path.exists(filename):
            print("CSV file not found, fetching data remotely...")
            # 如果文件不存在，从远程获取数据
            dataframe = cls.get_data_remote()
            # 确保DataFrame包含所需的列
            required_columns = ['ts_code', 'dt', 'price', 'high', 'low', 'name', 'amount']
            if not all(column in dataframe.columns for column in required_columns):
                raise ValueError("The fetched data does not contain all required columns.")
            # 格式化DataFrame（这里可以根据需要添加具体的格式化代码）
            # 保存DataFrame到CSV
            cls.save_dataframe_to_csv(dataframe, filename)
        else:
            print(f"CSV file found, loading data...:{filename}")
            # 如果文件存在，从CSV加载数据
            dataframe = cls.load_dataframe_from_csv(filename)
            dataframe['dt'] = dataframe['dt'].astype(str)

        return dataframe

    @classmethod
    def run(cls, new_df):
        now = Now()
        end_date = now.delta(1).date
        new_df['dt'] = str(now).replace('-', '')
        new_df_to_merge = new_df[ ['ts_code', 'dt', 'price', 'high', 'low', 'name', 'amount'] ]
        filename = f'/tmp/boll_day_his_{end_date}.csv'
        his_df = cls.ensure_dataframe(filename)
        df = his_df
        if cls.check_add_new_df(his_df, new_df_to_merge):
            print(f"now:{now.now}, Boll his_df['dt'].max()={his_df['dt'].max()} add new df:{new_df_to_merge.shape}")
            df = pd.concat([his_df, new_df_to_merge])
        else:
            print(f"now:{now.now}, Boll his_df['dt'].max()={his_df['dt'].max()} NOT add new df:{new_df_to_merge.shape}")

        df = cls.cal_boll(df)
        notice_data_list = cls.get_notice_list(df)
        notice_df = cls.fill_stock_info(notice_data_list, new_df)
        # print(notice_df)
        # print(notice_df.shape)
        # print(notice_df.columns)
        # print(notice_df['dt'].max())

        from rt.mail_tool import MailTool
        time_str = str(notice_df['日期'].max())[4:14] # 20250523 12:34
        MailTool.send(f"日线BOLL{time_str}", [(notice_df, '接近BOLL下限')])

    @classmethod
    def check_add_new_df(cls, his_df, new_df):
        his_df_dt = his_df['dt'].max()
        now = Now()
        if his_df_dt == str(now.datekey):
            return False
        if not now.is_trade_date:
            return False
        if now.now.hour < 9 or now.now.hour >= 18:
            return False
        return True


    @classmethod
    def cal_boll(cls, df, window=20, num_std=2):
        df = df.sort_values(by=['ts_code', 'dt'], ascending=[True, True])[
            ['ts_code', 'dt', 'price', 'high', 'low', 'name', 'amount']
        ]
        # print(df)

        df['for_true_col_ma'] = df['price']
        df['for_true_col_std'] = df['price']
        # 对每个分组，将abc列的最后一个元素设置为该分组low列的最后一个值
        for name, group in df.groupby('ts_code'):
            last_index = group.index[-1]  # 获取每个分组的最后一个索引
            df.loc[last_index, 'for_true_col_ma'] = group['high'].iloc[-1]
            df.loc[last_index, 'for_true_col_std'] = group['low'].iloc[-1]
        # print(df.shape)
        # mei_de_group_row = df.loc[df['ts_code'] == '000333.SZ']
        # # print(mei_de_group_row)
        # print(mei_de_group_row.iloc[-1])

        df['amount_avg10'] = df.groupby('ts_code')['amount'].transform(
            lambda x: x.rolling(window=window, min_periods=window).mean())

        df['ma'] = df.groupby('ts_code')['for_true_col_ma'].transform(
            lambda x: x.rolling(window=window, min_periods=window).mean())
        df['std'] = df.groupby('ts_code')['for_true_col_ma'].transform(
            lambda x: x.rolling(window=window, min_periods=window).std())
        df['ma-now'] = df.groupby('ts_code')['price'].transform(
            lambda x: x.rolling(window=window, min_periods=window).mean())
        df['std-now'] = df.groupby('ts_code')['price'].transform(
            lambda x: x.rolling(window=window, min_periods=window).std())
        df['ub'] = df['ma'] + num_std * df['std']
        df['lb'] = df['ma'] - num_std * df['std']
        df['lb-now'] = df['ma-now'] - num_std * df['std-now']
        df['high_gt_ub'] = df['high'] >= df['ub']
        df['low_lt_lb'] = df['low'] <= df['lb']
        df['price_lt_lb'] = df['price'] <= df['lb']
        return df

    @classmethod
    def get_notice_list(cls, df):
        res = list()
        for ts_code, group in df.groupby('ts_code'):
            if group.shape[0] > 1:
                group['low_lt_lb_prev'] = group['low_lt_lb'].shift(1)
                group['amount_avg10_prev'] = group['amount_avg10'].shift(1)

                row = group.iloc[-1]
                name = row['name']
                dt = row['dt']
                price = row['price']
                low = row['low']
                lb = row['lb']
                lb_now = row['lb-now'] # 当前价格的BOLL
                price_lt_lb = row['price_lt_lb']
                low_lt_lb = row['low_lt_lb']
                low_lt_lb_prev = row['low_lt_lb_prev']
                if low_lt_lb and not low_lt_lb_prev:
                    msg = f"{name} {ts_code}在{dt}最低价{low}接近BOLL下限{round(lb_now, 2)}. 当前价格{price}"
                    data = {
                        'ts_code': ts_code,
                        'dt': dt,
                        'lb_now': round(lb_now, 2),
                        'low': low,
                        'price': price,
                        'amount_avg10_prev': row['amount_avg10_prev']
                    }
                    res.append(data)
                    # print(msg)
                    # if cls.cache_manager.get(f"{name}{ts_code}"):
                    #     continue
                    # else:
                    #     cls.cache_manager.update(f"{name}{ts_code}", msg)
                    #     send_notification(msg)
        return res

    @classmethod
    def fill_stock_info(cls, data_list, new_df):
        boll_df = pd.DataFrame(data_list)
        print(f"fill_stock_info.boll_df.shape={boll_df.shape} new_df.shape={new_df.shape}")
        merged_df = pd.merge(
            boll_df[ ['ts_code', 'dt', 'low', 'lb_now', 'price', 'amount_avg10_prev'] ],
            new_df[ ['ts_code', 'name', 'change_pct', 'total_mv', 'float_mv', 'amount'] ],
            on=['ts_code'],  # 合并键
            how='left'  # 按需调整合并方式，如 'left', 'right', 'outer'
        )
        print(f"fill_stock_info.merged_df.shape={merged_df.shape}")

        # print(merged_df)
        merged_df = merged_df[
            (merged_df['price'] > 5)
            & (~merged_df["name"].str.contains("ST", na=False))
            & (~merged_df["name"].str.contains("退", na=False))
            & ((merged_df['amount_avg10_prev'] > 0.8) | (merged_df['amount'] > 1))
        ]

        print(f"fill_stock_info.merged_df.filter.shape={merged_df.shape}")

        merged_df['破线'] = np.where(merged_df['low'] <= merged_df['lb_now'], '●', '')
        merged_df = merged_df[
            ['dt', 'ts_code', 'name', 'change_pct', 'price', 'lb_now', 'amount', 'float_mv', '破线']
        ]
        merged_df.columns = [
            '日期', '代码', '名称', '涨跌', '价格', 'BOLL下限', '成交额(亿)', '流动市值(亿)', '破线'
        ]

        bk_df = get_bk_member_df()
        bk_df['代码'] = bk_df['股票代码']
        bk_df['行业板块'] = bk_df['bk_name']
        bk_df['主力净流入(万)'] = bk_df['主力净流入'].round(1)
        bk_df = bk_df[
            ['代码', '行业板块', '股票名称', '主力净流入(万)']
        ]

        final_df = pd.merge(
            merged_df[ ['日期', '代码', '名称', '涨跌', '价格', 'BOLL下限', '破线', '成交额(亿)', '流动市值(亿)'] ],
            bk_df[ ['代码', '行业板块', '主力净流入(万)'] ],
            on=['代码'],  # 合并键
            how='left'  # 按需调整合并方式，如 'left', 'right', 'outer'
        )
        print(f"fill_stock_info.行业板块final_df.shape={final_df.shape}")

        final_df = final_df.sort_values(by=['行业板块', '主力净流入(万)'], ascending=False)
        final_df = final_df[
            ['日期', '代码', '名称', '涨跌', '行业板块', '价格', 'BOLL下限', '破线', '主力净流入(万)', '成交额(亿)', '流动市值(亿)']
        ]
        print(final_df)
        return final_df

if __name__ == '__main__':
    import tushare as ts
    from utils.setting import TOKEN

    ts.set_token(TOKEN)
    print(TOKEN)
    df = get_stock_list_rt()
    Boll.run(df)

    # now = Now()
    # print(now)
    # print(str(now))
    # print('2025-05-25 02:17:36' > '20250501')
    # print('2025-05-25 02:17:36' > '20251201')



