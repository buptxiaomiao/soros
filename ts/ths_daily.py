# coding: utf-8

from base_task import BaseTask


class ThsDaily(BaseTask):

    DATA_FILE = 'ths_daily.csv'
    SQL_FILE = 'ths_daily.sql'

    @classmethod
    def run(cls):
        return cls.run_by_dt()

    @classmethod
    def get_df(cls, *args, **kwargs):
        dt = kwargs['dt']
        return cls.pro.ths_daily(**{
            "ts_code": "",
            "trade_date": dt,
            "start_date": "",
            "end_date": "",
            "limit": 5000,
            "offset": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "close",
            "open",
            "high",
            "low",
            "pre_close",
            "avg_price",
            "change",
            "pct_change",
            "vol",
            "turnover_rate",
            "total_mv",
            "float_mv",
            "pe_ttm",
            "pb_mrq"
        ])


if __name__ == '__main__':
    ThsDaily.run()
