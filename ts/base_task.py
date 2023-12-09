# coding: utf-8

import os
import sys
import time

sys.path.append('..')
from utils.ts_util import pro
from utils.path_util import PathUtil
from utils.template_util import TemplateUtil
from utils.local_dim_util import LocalDimUtil
from functools import reduce
from utils.local_pick_util import LocalPickleUtil
import pandas as pd


class BaseTask(object):

    DATA_FILE = 'base'
    SQL_FILE = 'base'
    SLEEP_SECONDS = 0.1
    pro = pro

    @classmethod
    def run(cls):
        raise NotImplementedError

    @classmethod
    def run_by_dt(cls, check_cache=True):
        total_num = cls.save_data_by_dt(check_cache)
        if total_num:
            cls.render_and_exec()

    @classmethod
    def run_no_dt(cls):
        df = cls.get_df()
        cls.save_to_csv(df)
        cls.render_and_exec()

    @classmethod
    def get_df(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def get_date_df(cls):
        return LocalDimUtil.get_date_df()

    @classmethod
    def save_data_by_dt(cls, check_cache=True):

        cache = LocalPickleUtil(cls)
        date_df = cls.get_date_df()

        date_list = date_df['cal_date'].to_list()

        df_list = []
        dt_list = []
        num = 0
        total_num = 0
        for dt in date_list:
            if check_cache:
                if cache.check_pickle_exist(dt):
                    continue

            df = cls.get_df(dt=dt)
            if cls.SLEEP_SECONDS:
                time.sleep(cls.SLEEP_SECONDS)
            df_list.append(df)
            dt_list.append(dt)
            if df.shape[0] > 0:
                cache.add_pickle(dt)
            else:
                continue

            num += df.shape[0]
            total_num += df.shape[0]
            print(f'{cls.__name__} dt:{dt}, cache_num:{num}')

            if num >= 200000:
                cls.slice_save_to_csv(data_df_list=df_list, dt_list=dt_list, cache=cache)
                del df_list
                del dt_list
                df_list = []
                dt_list = []
                num = 0
        if df_list:
            cls.slice_save_to_csv(data_df_list=df_list, dt_list=dt_list, cache=cache)

        return total_num

    @classmethod
    def save_to_csv(cls, df, suffix=None):
        file_name = PathUtil.get_data_file_name(cls.DATA_FILE, suffix)
        df.to_csv(file_name, sep='\u0001', index=False, header=False)
        print(f'{cls.__name__} suffix:{suffix} save to {file_name}.')

    @classmethod
    def slice_save_to_csv(cls, data_df_list, dt_list, cache=None):
        res = reduce(lambda x, y: pd.concat([x, y]) if x is not None else y, data_df_list, None)
        suf = str(dt_list[-1]) + '_' + str(dt_list[0])

        cls.save_to_csv(res, suf)
        del res
        if cache:
            cache.commit()

    @classmethod
    def render_and_exec(cls):
        # 渲染sql
        t = TemplateUtil(cls.SQL_FILE,
                         search_list={'data_file_path': PathUtil.get_data_file_ambiguous_name(cls.DATA_FILE)},
                         cata='ods')
        print(t.sql)
        sql_file = t.write_and_get_result_sql_path()

        print(f"sudo -u hive hive -f {sql_file}")
        exit_code = os.system(f"sudo -u hive hive -f {sql_file}")
        return exit_code

