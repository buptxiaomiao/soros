# coding: utf-8
import time

import pandas as pd

from base_task import BaseTask


class ThsMember(BaseTask):

    DATA_FILE = 'ths_member.csv'
    SQL_FILE = 'ths_member.sql'

    SLEEP_SECONDS = 0.2

    @classmethod
    def run(cls):
        return cls.run_no_dt()

    @classmethod
    def get_df(cls, *args, **kwargs):
        limit = kwargs.get('limit', 5000)
        offset = kwargs.get('offset', 0)
        df = cls.pro.ths_member(**{
            "ts_code": "",
            "code": "",
            "limit": 5000,
            "offset": 0
        }, fields=[
            "ts_code",
            "code",
            "name",
            "weight",
            "in_date",
            "out_date",
            "is_new"
        ])

        if df.shape[0] < 5000:
            return df

        print(f'{cls.__name__} limit:{limit}, offset:{offset}')

        if cls.SLEEP_SECONDS:
            time.sleep(cls.SLEEP_SECONDS)
        new_offset = limit + offset
        return pd.concat([df, cls.get_df(limit=limit, offset=new_offset)])


if __name__ == '__main__':
    ThsMember.run()
