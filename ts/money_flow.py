# coding: utf-8

from base_task import BaseTask


class MoneyFlow(BaseTask):
    """个股资金流向 https://tushare.pro/document/2?doc_id=170"""

    DATA_FILE = 'money_flow.csv'
    SQL_FILE = 'money_flow.sql'
    SLEEP_SECONDS = 0.2

    @classmethod
    def run(cls):
        return cls.run_by_dt()

    @classmethod
    def get_df(cls, *args, **kwargs):
        dt = kwargs['dt']
        return cls.pro.moneyflow(**{
                "ts_code": "",
                "trade_date": dt,
                "start_date": "",
                "end_date": "",
                "limit": "",
                "offset": ""
            }, fields=[
                "ts_code",
                "trade_date",
                "buy_sm_vol",
                "buy_sm_amount",
                "sell_sm_vol",
                "sell_sm_amount",
                "buy_md_vol",
                "buy_md_amount",
                "sell_md_vol",
                "sell_md_amount",
                "buy_lg_vol",
                "buy_lg_amount",
                "sell_lg_vol",
                "sell_lg_amount",
                "buy_elg_vol",
                "buy_elg_amount",
                "sell_elg_vol",
                "sell_elg_amount",
                "net_mf_vol",
                "net_mf_amount",
                "trade_count"
            ])


if __name__ == '__main__':
    MoneyFlow.run()
