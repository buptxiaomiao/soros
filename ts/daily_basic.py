# coding: utf-8

from base_task import BaseTask


class DailyBasic(BaseTask):
    """日线行情 https://tushare.pro/document/2?doc_id=27"""

    DATA_FILE = 'daily_basic.csv'
    SQL_FILE = 'daily_basic.sql'

    @classmethod
    def run(cls):
        cls.run_by_dt()

    @classmethod
    def get_df(cls, *args, **kwargs):
        dt = kwargs['dt']
        return cls.pro.daily_basic(**{
            "ts_code": "",
            "trade_date": dt,
            "start_date": "",
            "end_date": "",
            "offset": "",
            "limit": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "close",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "dv_ratio",
            "dv_ttm",
            "total_share",
            "float_share",
            "free_share",
            "total_mv",
            "circ_mv",
            "limit_status"
        ])


if __name__ == '__main__':
    DailyBasic.run()
