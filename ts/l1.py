# coding: utf-8

import os

from base_task import BaseTask
from utils.template_util import TemplateUtil


class L1Task(BaseTask):

    @classmethod
    def run(cls):
        conf_list = [
            'dim_stock.sql',
            'dim_open_date.sql',
            'fact_stock_daily.sql',
            'fact_stock_future_change.sql',
            'fact_stock_tag_price_prev.sql',
            'fact_stock_money_flow.sql',
            'fact_market_amount.sql',
            'fact_stock_holder_log.sql',
            'dim_rela_ths_stock.sql'
        ]
        for name in conf_list:
            cls.render_and_exec_l1(name)

    @classmethod
    def render_and_exec_l1(cls, sql_file_name):
        # 渲染sql
        t = TemplateUtil(sql_file_name,
                         cata='l1')
        print(t.sql)
        sql_file = t.write_and_get_result_sql_path()

        print(f"sudo -u hive hive -f {sql_file}")
        exit_code = os.system(f"sudo -u hive hive -f {sql_file}")
        return exit_code


if __name__ == '__main__':

    L1Task.run()
