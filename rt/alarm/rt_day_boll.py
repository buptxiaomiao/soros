# coding: utf-8
import datetime

import pandas as pd
import requests
import sys
import os

sys.path.append('..')
sys.path.append('../..')

from utils.setting import BARK_KEY
from rt.api.daily import Daily
from utils.now import Now

pd.set_option('display.max_rows', None)  # 设置为 None 表示不限制行数
pd.set_option('display.max_columns', None)  # 设置为 None 表示不限制列数

# 发送通知的函数
def send_notification(msg):
    # 这里需要替换为你的Bark应用的device_key
    device_key = BARK_KEY
    group = '日线BOLL'
    url = f'https://api.day.app/{device_key}?group={group}&title={group}&body={msg}'
    # headers = {'Content-Type': 'application/json'}
    # response = requests.post(url, headers=headers, json={'content': msg})
    response = requests.get(url)
    return response.status_code == 200

class Boll:

    def __init__(self):
        pass

    @classmethod
    def get_data_remote(cls):
        # 这个函数是一个占位符，你需要实现它来从远程数据源获取数据
        # 这里只是一个示例，返回一个空的DataFrame
        now = Now()
        end_date = str(now.delta(1).date)
        start_date = str(now.delta(40).date)
        df = Daily.get_df(start_date, end_date)
        df['dt'] = df['trade_date']
        df['price'] = df['close']
        df['name'] = ''
        return df[ ['ts_code', 'dt', 'price', 'high', 'low', 'name'] ]

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
            required_columns = ['ts_code', 'dt', 'price', 'high', 'low', 'name']
            if not all(column in dataframe.columns for column in required_columns):
                raise ValueError("The fetched data does not contain all required columns.")
            # 格式化DataFrame（这里可以根据需要添加具体的格式化代码）
            # 保存DataFrame到CSV
            cls.save_dataframe_to_csv(dataframe, filename)
        else:
            print("CSV file found, loading data...")
            # 如果文件存在，从CSV加载数据
            dataframe = cls.load_dataframe_from_csv(filename)

        return dataframe

    @classmethod
    def run(cls, new_df):
        now = Now()
        end_date = now.delta(1).date
        new_df['dt'] = str(now)
        new_df['ts_code'] = new_df['TS_CODE']
        new_df['price'] = new_df['PRICE']
        new_df['high'] = new_df['HIGH']
        new_df['low'] = new_df['LOW']
        new_df['name'] = new_df['NAME']
        new_df = new_df[ ['ts_code', 'dt', 'price', 'high', 'low', 'name'] ]
        filename = f'/tmp/boll_day_his_{end_date}.csv'
        his_df = cls.ensure_dataframe(filename)

        df = pd.concat([his_df, new_df])
        df = cls.cal_boll(df)
        cls.check_and_notice(df)

    @classmethod
    def cal_boll(cls, df, window=20, num_std=2):
        df = df.sort_values(by=['ts_code', 'dt'], ascending=[True, True])[ ['ts_code', 'dt', 'price', 'high', 'low', 'name'] ]
        # print(df)
        df['ma'] = df.groupby('ts_code')['price'].transform(
            lambda x: x.rolling(window=window, min_periods=window).mean())
        df['std'] = df.groupby('ts_code')['price'].transform(
            lambda x: x.rolling(window=window, min_periods=window).std())
        df['ub'] = df['ma'] + num_std * df['std']
        df['lb'] = df['ma'] - num_std * df['std']
        df['high_gt_ub'] = df['high'] >= df['ub']
        df['low_lt_lb'] = df['low'] <= df['lb']
        return df

    @classmethod
    def check_and_notice(cls, df):
        notified = set()
        for ts_code, group in df.groupby('ts_code'):
            if ts_code not in notified:
                if group.shape[0] > 1:
                    group['low_lt_lb_prev'] = group['low_lt_lb'].shift(1)
                    row = group.iloc[-1]
                    name = row['name']
                    dt = row['dt']
                    price = row['price']
                    low = row['low']
                    lb = row['lb']
                    low_lt_lb = row['low_lt_lb']
                    low_lt_lb_prev = row['low_lt_lb_prev']
                    if low_lt_lb and not low_lt_lb_prev:
                        msg = f"{name} {ts_code}在{dt}最低价{low}低于BOLL下限{round(lb, 2)}. 当前价格{price}"
                        print(msg)
                        notified.add(ts_code)
                        # send_notification(msg)

if __name__ == '__main__':
    import tushare as ts
    from utils.setting import TOKEN

    ts.set_token(TOKEN)
    print(TOKEN)
    df = ts.realtime_list()
    Boll.run(df)
    pass

