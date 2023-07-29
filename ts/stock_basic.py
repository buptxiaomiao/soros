# coding: utf-8

from base_task import BaseTask


class StockBasic(BaseTask):
    """基础信息 https://tushare.pro/document/2?doc_id=25"""

    DATA_FILE = 'stock_basic.csv'
    SQL_FILE = 'stock_basic.sql'

    @classmethod
    def run(cls):
        return cls.run_no_dt()

    @classmethod
    def get_df(cls, *args, **kwargs):
        df = cls.pro.stock_basic(**{
            "ts_code": "",
            "name": "",
            "exchange": "",
            "market": "",
            "is_hs": "",
            "list_status": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "symbol",
            "name",
            "area",
            "industry",
            "market",
            "exchange",
            "list_status",
            "list_date",
            "delist_date",
            "is_hs"
        ])
        df.to_csv('./data/'+cls.DATA_FILE, sep='\u0001', index=False)
        return df


if __name__ == '__main__':
    StockBasic.run()
