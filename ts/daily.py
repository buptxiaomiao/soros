# coding: utf-8

from base_task import BaseTask


class Daily(BaseTask):

    DATA_FILE = 'daily.csv'
    SQL_FILE = 'daily.sql'

    @classmethod
    def run(cls):
        return cls.run_by_dt()

    @classmethod
    def get_df(cls, *args, **kwargs):
        dt = kwargs['dt']
        return cls.pro.daily(**{
            "ts_code": "",
            "trade_date": dt,
            "start_date": "",
            "end_date": "",
            "offset": "",
            "limit": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount"
        ])


if __name__ == '__main__':
    Daily.run()
