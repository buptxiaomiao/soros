#coding: utf-8
import time

from utils.ts_util import pro
import pandas as pd

class Daily:

    @classmethod
    def get_df(cls, start_date, end_date, code=None):
        # 生成日期范围
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        # 将日期格式化为 YYYYMMDD 的形式，并转换为 list
        date_list = [date.strftime('%Y%m%d') for date in date_range]
        if code:
            return cls.__get(**{
                'code': code,
                'start_date': min(date_list),
                'end_date': max(date_list)
            })

        df_list = []
        for dt in date_list:
            df = cls.__get(**{'trade_date': dt})
            if df.shape[0] > 0:
                print(f"get-daily: {dt} finish. cnt={df.shape[0]}")
            time.sleep(0.05)
            df_list.append(df)
        res = pd.concat(df_list)
        return res

    @classmethod
    def __get(cls, **kwargs):
        code = kwargs.get("code", "")
        trade_date = kwargs.get("trade_date", "")
        start_date = kwargs.get("start_date", "")
        end_date = kwargs.get("end_date", "")
        df = pro.daily(**{
            "ts_code": code,
            "trade_date": trade_date,
            "start_date": start_date,
            "end_date": end_date,
            "offset": "",
            "limit": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount"
        ])
        return df

if __name__ == '__main__':
    df = Daily.get_df('2025-05-01', '2025-05-23')
    print(df.shape)
    df.to_csv('ddd.csv')
