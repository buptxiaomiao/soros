# coding: utf-8

import os
import sys
import time


sys.path.append('..')
from utils.ts_util import pro
from utils.path_util import PathUtil
from utils.template_util import TemplateUtil
from utils.local_dim_util import LocalDimUtil
from utils.now import Now
from functools import reduce
from utils.local_pick_util import LocalPickleUtil
import pandas as pd


class MoneyFlow(object):

    DATA_FILE = 'money_flow.csv'
    SQL_FILE = 'money_flow.sql'

    @classmethod
    def save_to_csv(cls, data_df_list, dt_list, cache):
        res = reduce(lambda x, y: pd.concat([x, y]) if x is not None else y, data_df_list, None)
        suf = str(dt_list[-1]) + '_' + str(dt_list[0])
        file_name = PathUtil.get_data_file_name(cls.DATA_FILE, suf)
        res.to_csv(file_name, sep='\u0001', index=False)
        print(f'{cls.__name__} dt:{suf} save to {file_name}.')
        del res
        cache.commit()

    @classmethod
    def run(cls):

        cache = LocalPickleUtil(cls)
        date_df = LocalDimUtil.get_date_df()
        date_list = date_df['cal_date'].to_list()

        df_list = []
        dt_list = []
        num = 0
        for dt in date_list:
            if cache.check_pickle_exist(dt):
                continue
            cache.add_pickle(dt)

            df = pro.moneyflow(**{
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
            time.sleep(0.1)
            df_list.append(df)
            dt_list.append(dt)
            num += df.shape[0]
            print(f'{cls.__name__} dt:{dt}, cache_num:{num}')

            if num >= 200000:
                cls.save_to_csv(data_df_list=df_list, dt_list=dt_list, cache=cache)
                del df_list
                del dt_list
                df_list = []
                dt_list = []
                num = 0
        if df_list:
            cls.save_to_csv(data_df_list=df_list, dt_list=dt_list, cache=cache)

        # 渲染sql
        t = TemplateUtil(cls.SQL_FILE,
                         search_list={'data_file_path': PathUtil.get_data_file_ambiguous_name(cls.DATA_FILE)},
                         cata='ods')
        print(t.sql)
        sql_file = t.write_and_get_result_sql_path()

        print(f"sudo -u hive hive -f {sql_file}")
        exit_code = os.system(f"sudo -u hive hive -f {sql_file}")


if __name__ == '__main__':
    MoneyFlow.run()
