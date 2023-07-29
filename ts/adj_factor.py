# coding: utf-8

from base_task import BaseTask


class AdjFactor(BaseTask):
    """复权因子 https://tushare.pro/document/2?doc_id=28"""

    DATA_FILE = 'adj_factor.csv'
    SQL_FILE = 'adj_factor.sql'
    SLEEP_SECONDS = 0.2

    @classmethod
    def run(cls):
        return cls.run_by_dt()

    @classmethod
    def get_df(cls, *args, **kwargs):
        dt = kwargs['dt']
        return cls.pro.adj_factor(**{
                "ts_code": "",
                "trade_date": dt,
                "start_date": "",
                "end_date": "",
                "limit": "",
                "offset": ""
            }, fields=[
                "ts_code",
                "trade_date",
                "adj_factor"
            ])


if __name__ == '__main__':
    AdjFactor.run()
