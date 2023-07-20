# coding: utf-8

import os
import sys

sys.path.append('..')
from utils.ts_util import pro
from utils.path_util import PathUtil
from utils.template_util import TemplateUtil


class StockCompany(object):

    DATA_FILE = 'stock_company.csv'
    SQL_FILE = 'stock_company.sql'

    @classmethod
    def run(cls):
        df = pro.stock_company(**{
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

        df.to_csv(PathUtil.get_data_file_name(cls.DATA_FILE), sep='\u0001', index=False, header=False)

        # 渲染sql
        t = TemplateUtil(cls.SQL_FILE,
                         search_list={'data_file_path': PathUtil.get_data_file_ambiguous_name(cls.DATA_FILE)},
                         cata='ods')
        print(t.sql)
        sql_file = t.write_and_get_result_sql_path()

        print(f"sudo -u hive hive -f {sql_file}")
        os.system(f"sudo -u hive hive -f {sql_file}")


if __name__ == '__main__':
    StockCompany.run()
