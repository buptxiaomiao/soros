# coding: utf-8

from base_task import BaseTask


class StockHolderNum(BaseTask):
    """获取上市公司股东户数数据，数据不定期公布 https://tushare.pro/document/2?doc_id=166"""

    DATA_FILE = 'stock_holder_number.csv'
    SQL_FILE = 'stock_holder_number.sql'
    SLEEP_SECONDS = 0.2

    @classmethod
    def run(cls):
        return cls.run_by_dt(check_cache=False)

    @classmethod
    def get_df(cls, *args, **kwargs):
        dt = kwargs['dt']
        return cls.pro.stk_holdernumber(**{
            "ts_code": "",
            "enddate": dt,
            "start_date": "",
            "end_date": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "ann_date",
            "end_date",
            "holder_num",
            "holder_nums"
        ])


if __name__ == '__main__':
    StockHolderNum.run()
