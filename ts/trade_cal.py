# coding: utf-8

from base_task import BaseTask


class TradeCal(BaseTask):
    """交易日历 https://tushare.pro/document/2?doc_id=26"""

    DATA_FILE = 'trade_cal.csv'
    SQL_FILE = 'trade_cal.sql'

    @classmethod
    def run(cls):
        return cls.run_no_dt()

    @classmethod
    def get_df(cls):
        df = cls.pro.trade_cal(**{
            "exchange": "",
            "cal_date": "",
            "start_date": "",
            "end_date": "",
            "is_open": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "exchange",
            "cal_date",
            "is_open",
            "pretrade_date"
        ])
        df.to_csv('./data/'+cls.DATA_FILE, sep='\u0001', index=False)
        return df


if __name__ == '__main__':
    TradeCal.run()
