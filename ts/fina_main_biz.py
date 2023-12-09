# coding: utf-8

from base_task import BaseTask


class FinaMainBiz(BaseTask):
    """获得上市公司主营业务构成，分地区和产品两种方式 https://tushare.pro/document/2?doc_id=81"""

    DATA_FILE = 'fina_main_biz.csv'
    SQL_FILE = 'fina_main_biz.sql'
    SLEEP_SECONDS = 0.2

    @classmethod
    def run(cls):
        # by stock
        # 写文件、上传文件，都需check
        pass

    @classmethod
    def get_df(cls, *args, **kwargs):
        ts_code = kwargs['ts_code']
        return cls.pro.fina_mainbz(**{
            "ts_code": ts_code,
            "period": "",
            "type": "",
            "start_date": "",
            "end_date": "",
            "is_publish": "",
            "limit": 1000,
            "offset": ''
        }, fields=[
            "ts_code",
            "end_date",
            "bz_item",
            "bz_sales",
            "bz_profit",
            "bz_cost",
            "curr_type"
        ])


if __name__ == '__main__':
    FinaMainBiz.run()
