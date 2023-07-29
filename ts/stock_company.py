# coding: utf-8

from base_task import BaseTask


class StockCompany(BaseTask):
    """上市公司基本信息 https://tushare.pro/document/2?doc_id=112"""

    DATA_FILE = 'stock_company.csv'
    SQL_FILE = 'stock_company.sql'

    @classmethod
    def run(cls):
        return cls.run_no_dt()

    @classmethod
    def get_df(cls):
        return cls.pro.stock_company(**{
            "ts_code": "",
            "exchange": "",
            "status": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "exchange",
            "chairman",
            "manager",
            "secretary",
            "reg_capital",
            "setup_date",
            "province",
            "city",
            "introduction",
            "website",
            "business_scope",
            "employees",
            "main_business"
        ])


if __name__ == '__main__':
    StockCompany.run()
