# coding: utf-8

from base_task import BaseTask


class MoneyFlowHsgt(BaseTask):
    """沪深港通资金流向总量  https://tushare.pro/document/2?doc_id=47"""

    DATA_FILE = 'money_flow_hsgt.csv'
    SQL_FILE = 'money_flow_hsgt.sql'
    SLEEP_SECONDS = 0.2

    @classmethod
    def run(cls):
        return cls.run_by_dt()

    @classmethod
    def get_df(cls, *args, **kwargs):
        dt = kwargs['dt']
        return cls.pro.moneyflow_hsgt(**{
            "trade_date": dt,
            "start_date": "",
            "end_date": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "trade_date",
            "ggt_ss",
            "ggt_sz",
            "hgt",
            "sgt",
            "north_money",
            "south_money"
        ])


if __name__ == '__main__':
    MoneyFlowHsgt.run()
