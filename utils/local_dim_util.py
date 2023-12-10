# coding: utf-8

import pandas as pd

from utils.now import Now


class LocalDimUtil(object):

    @classmethod
    def get_stock_df(cls):
        df = pd.read_csv('../ts/data/stock_basic.csv', sep='\u0001', header=0)
        print(df.head())
        return df

    @classmethod
    def get_date_df(cls, is_open=True):
        df = pd.read_csv('../ts/data/trade_cal.csv', sep='\u0001', header=0)
        df1 = df[
            (df['cal_date'] >= 20000101)
            & (df['cal_date'] <= int(Now().datekey))
        ]
        if is_open:
            df1 = df1[df1['is_open'] == 1]
        df1.set_index(df1['cal_date'], inplace=True)
        print(df1.head())
        return df1


if __name__ == '__main__':
    LocalDimUtil.get_date_df()
    # LocalDimUtil.get_stock_df()
