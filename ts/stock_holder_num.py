# coding: utf-8

from base_task import BaseTask
from utils.now import Now
from utils.local_dim_util import LocalDimUtil


class StockHolderNum(BaseTask):
    """获取上市公司股东户数数据，数据不定期公布 https://tushare.pro/document/2?doc_id=166"""

    DATA_FILE = 'stock_holder_num.csv'
    SQL_FILE = 'stock_holder_num.sql'
    SLEEP_SECONDS = 0.2

    @classmethod
    def run(cls):
        return cls.run_by_dt(check_cache=False)

    @classmethod
    def get_date_df(cls):
        """初始化后，增量每次只跑近200天"""
        df = LocalDimUtil.get_date_df(is_open=False)
        df = df[df['cal_date'] >= int(Now().delta(200).datekey)]
        return df

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
