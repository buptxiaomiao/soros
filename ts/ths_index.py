# coding: utf-8

from base_task import BaseTask


class ThsIndex(BaseTask):
    """同花顺概念和行业 https://tushare.pro/document/2?doc_id=259"""

    DATA_FILE = 'ths_index.csv'
    SQL_FILE = 'ths_index.sql'

    @classmethod
    def run(cls):
        return cls.run_no_dt()

    @classmethod
    def get_df(cls, *args, **kwargs):
        df = cls.pro.ths_index(**{
            "ts_code": "",
            "exchange": "",
            "type": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "name",
            "count",
            "exchange",
            "list_date",
            "type"
        ])
        df.to_csv('./data/'+cls.DATA_FILE, sep='\u0001', index=False)
        return df


if __name__ == '__main__':
    ThsIndex.run()
