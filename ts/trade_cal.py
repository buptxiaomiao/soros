# coding: utf-8

import os
import sys


sys.path.append('..')
from utils.ts_util import pro
from utils.path_util import PathUtil
from utils.template_util import TemplateUtil


class TradeCal(object):

    DATA_FILE = 'trade_cal.csv'
    SQL_FILE = 'trade_cal.sql'

    @classmethod
    def run(cls):
        df = pro.trade_cal(**{
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
        print(df)
        df.to_csv('./data/'+cls.DATA_FILE, sep='\u0001', index=False)
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
    TradeCal.run()
