from traceback import print_last

from requests import request
from datetime import datetime, timedelta
import random
import requests
import pandas as pd

from rt.api.thread_pool_executor import ThreadPoolExecutorBase

base_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"


def get_stock_minutes_rt(code, klt=30, window=20, num_std=2, end=""):
    res, _ = StockKlineRt.get_stock_minutes_rt(code, klt, window, num_std, end)
    return res

class StockKlineRt(ThreadPoolExecutorBase):

    @classmethod
    def run(cls, stock_list):

        func = cls.get_stock_minutes_rt
        # total_page = cls.get_total_page()
        args = [[i] for i in stock_list]
        result_df_list = cls.run_by_pool_pro(fetch_func=func, args=args)

        print("len(result_df_list)")
        print(len(result_df_list))
        final_df = pd.concat(result_df_list, ignore_index=True)
        final_df = final_df.sort_values(by='时间', ascending=False)
        print(f"BKMemberRT.final_df.shape={final_df.shape}")
        return final_df

    """
        code支持股票、ETF
    """
    @classmethod
    def get_stock_minutes_rt(cls, code, klt=30, window=20, num_std=2, end=""):


        # secid：股票代码，格式为市场代码+股票代码，例如沪市股票的市场代码为1，深市股票的市场代码为0，股票代码为6位数字，如1.600000表示沪市股票600000。
        mkt = '1' if code.startswith('6') else \
            '1' if code.startswith('5') else '0'
        end_date_str = (datetime.now() + + timedelta(days=random.randint(1, 30))).strftime('%Y%m%d')
        if end:
            end_date_str = end
        print(end_date_str)

        url = (base_url +
               f"?secid={mkt}.{code}"
               f"&ut=fa5fd1943c7b386f172d6893dbfba10b"
               f"&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6"
               f"&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61"
               f"&klt={klt}" # klt：K线类型，例如1表示1分钟，5表示5分钟，101表示日K线等。
               f"&end={end_date_str}"
               f"&lmt=240"
               f"&fqt=1" # fqt：复权类型，例如0表示不复权，1表示前复权，2表示后复权。
               f"")
        print(url)
        # response = request("GET", url)
        response = requests.get(url,  proxies=cls.get_proxy_conf())
        res_json = response.json()
        print(res_json)
        data = res_json['data']

        market = data.get('market', 1)
        name = data.get('name', '赛力斯')
        decimal = data.get('decimal', 2)
        dktotal = data.get('dktotal', 2056)
        preKPrice = data.get('preKPrice', 128.82)
        klines = data.get('klines', [])

        # "2025-01-03 15:00,126.22,124.98,126.22,124.85,48077,602495916.00,1.09,-0.97,-1.23,0.32"
        col_names = ['时间', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '-1', '涨跌幅', '-2', '-3']
        klines_list = [
            dict(zip(col_names, s.split(',')))
            for s in klines
        ]
        df = pd.DataFrame(klines_list).astype({
            '开盘': float,
            '收盘': float,
            '最高': float,
            '最低': float,
            '成交量': float,
            '成交额': float,
            '涨跌幅': float
        })
        df['name'] = name
        df['klt'] = klt

        df['mean'] = df['收盘'].rolling(window=window, min_periods=window).mean()
        df['std'] = df['收盘'].rolling(window=window, min_periods=window).std()
        df['ub'] = df['mean'] + num_std * df['std']
        df['lb'] = df['mean'] - num_std * df['std']
        df['low_lt_lb'] = df['最低'] <= df['lb']
        df['low_lt_lb_prev1'] = df['low_lt_lb'].shift(1)
        df['low_lt_lb_prev2'] = df['low_lt_lb'].shift(2)
        df['sell_flag'] = df['low_lt_lb'] & (df['low_lt_lb_prev1'] | df['low_lt_lb_prev2'])

        # low_lt_lb_1 = df.iloc[-1]['low_lt_lb']
        # low_lt_lb_2 = df.iloc[-2]['low_lt_lb']
        # low_lt_lb_3 = df.iloc[-3]['low_lt_lb']
        #
        # flag = low_lt_lb_1 and (low_lt_lb_2 or low_lt_lb_3)
        # print((low_lt_lb_1, low_lt_lb_2, low_lt_lb_3))
        # print("flag")
        # print(flag)
        # print(df)
        return df, 0

    # return flag, df

def get_sell_msg(df):
    row = df.iloc[-1]
    row1 = df.iloc[-2]
    row2 = df.iloc[-3]
    flag = any((row['sell_flag'], row1['sell_flag'], row2['sell_flag']))
    if not flag:
        return ""
    return f"{row['name']}{row['klt']}分钟BOLL连续下限"

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)  # 显示所有列
    # res = get_stock_minutes_rt("872190") # 北证0
    # df = get_stock_minutes_rt("002409", klt=1)
    df = get_stock_minutes_rt("002409", klt=101, end='20250830')
    # df = get_stock_minutes_rt("563180", klt=1)
    print(df.shape)
    print(df)
    print(get_sell_msg(df))
