# coding: utf-8

import pandas as pd
from base_task import BaseTask
from utils.local_dim_util import LocalDimUtil
from utils.now import Now


class HkHold(BaseTask):
    """沪深港股通持股明细 https://tushare.pro/document/2?doc_id=188"""

    DATA_FILE = 'hk_hold.csv'
    SQL_FILE = 'hk_hold.sql'
    SLEEP_SECONDS = 1

    @classmethod
    def run(cls):
        return cls.run_by_dt()

    @classmethod
    def get_date_df(cls):
        df = LocalDimUtil.get_date_df()
        df = df[df['cal_date'] >= 20160101]
        return df

    @classmethod
    def get_df(cls, *args, **kwargs):
        dt = kwargs['dt']
        df1 = cls.pro.hk_hold(**{
            "code": "",
            "ts_code": "",
            "trade_date": dt,
            "start_date": "",
            "end_date": "",
            "exchange": "SH",
            "limit": "",
            "offset": ""
        }, fields=[
            "code",
            "trade_date",
            "ts_code",
            "name",
            "vol",
            "ratio",
            "exchange"
        ])
        df2 = cls.pro.hk_hold(**{
            "code": "",
            "ts_code": "",
            "trade_date": dt,
            "start_date": "",
            "end_date": "",
            "exchange": "SZ",
            "limit": "",
            "offset": ""
        }, fields=[
            "code",
            "trade_date",
            "ts_code",
            "name",
            "vol",
            "ratio",
            "exchange"
        ])

        return pd.concat([df1, df2])


if __name__ == '__main__':
    HkHold.run()
