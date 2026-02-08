# coding: utf-8
"""
集思录-日历数据接口
https://www.jisilu.cn/data/calendar/

支持类型：
- diva: 股票分红
- OTHER: 节假日特殊日期
"""
import time
from enum import Enum

import pandas as pd
from retrying import retry

from rt.api.thread_pool_executor import ThreadPoolExecutorBase


class CalendarType(Enum):
    """日历数据类型"""
    DIVIDEND = "diva"          # 股票分红
    HOLIDAY = "OTHER"          # 节假日特殊日期


def get_stock_dividend_data(start_date=None, end_date=None):
    """
    获取股票分红日历数据

    :param start_date: 开始日期，格式 'YYYY-MM-DD'，默认为30天前
    :param end_date: 结束日期，格式 'YYYY-MM-DD'，默认为30天后
    :return: pandas.DataFrame
    """
    return JslCalendar.get_dividend_df(start_date=start_date, end_date=end_date)


def get_holiday_calendar_data(start_date=None, end_date=None):
    """
    获取节假日和特殊日期日历数据

    :param start_date: 开始日期，格式 'YYYY-MM-DD'，默认为30天前
    :param end_date: 结束日期，格式 'YYYY-MM-DD'，默认为90天后
    :return: pandas.DataFrame
    """
    return JslCalendar.get_holiday_df(start_date=start_date, end_date=end_date)


class JslCalendar(ThreadPoolExecutorBase):
    """
    集思录-日历数据接口
    """

    BASE_URL = "https://www.jisilu.cn/data/calendar/get_calendar_data/"

    # 默认日期范围配置
    DEFAULT_DAY_RANGES = {
        CalendarType.DIVIDEND: (30, 30),    # (过去天数, 未来天数)
        CalendarType.HOLIDAY: (30, 90),
    }

    # 字段映射配置
    FIELD_MAPPINGS = {
        CalendarType.DIVIDEND: {
            'title': '标题',
            'start': '日期',
            'stock_cd': '股票代码',
            'stock_nm': '股票名称',
            'dividend': '分红方案',
            'dividend_dt': '除权除息日',
            'progress': '进度',
            'url': '链接',
            'className': '样式类名'
        },
        CalendarType.HOLIDAY: {
            'title': '节假日名称',
            'start': '日期',
            'type': '类型',
            'description': '描述',
            'url': '链接',
            'className': '样式类名',
            'editable': '可编辑',
            'startStr': '日期字符串'
        }
    }

    @classmethod
    def get_dividend_df(cls, start_date=None, end_date=None):
        """
        获取股票分红数据

        :param start_date: 开始日期，格式 'YYYY-MM-DD'
        :param end_date: 结束日期，格式 'YYYY-MM-DD'
        :return: pandas.DataFrame
        """
        return cls.get_df(calendar_type=CalendarType.DIVIDEND, start_date=start_date, end_date=end_date)

    @classmethod
    def get_holiday_df(cls, start_date=None, end_date=None):
        """
        获取节假日特殊日期数据

        :param start_date: 开始日期，格式 'YYYY-MM-DD'
        :param end_date: 结束日期，格式 'YYYY-MM-DD'
        :return: pandas.DataFrame
        """
        return cls.get_df(calendar_type=CalendarType.HOLIDAY, start_date=start_date, end_date=end_date)

    @classmethod
    def get_df(cls, calendar_type, start_date=None, end_date=None):
        """
        获取日历数据

        :param calendar_type: 日历类型，CalendarType.DIVIDEND 或 CalendarType.HOLIDAY
        :param start_date: 开始日期，格式 'YYYY-MM-DD'
        :param end_date: 结束日期，格式 'YYYY-MM-DD'
        :return: pandas.DataFrame
        """
        start_ts, end_ts = cls._date_to_timestamp(calendar_type, start_date, end_date)
        df = cls._fetch_data(calendar_type, start_ts, end_ts)
        return df

    @classmethod
    def _date_to_timestamp(cls, calendar_type, start_date=None, end_date=None):
        """将日期字符串转换为时间戳"""
        past_days, future_days = cls.DEFAULT_DAY_RANGES.get(calendar_type, (30, 30))

        if start_date:
            start_ts = int(pd.Timestamp(start_date).timestamp())
        else:
            start_ts = int(time.time()) - past_days * 24 * 60 * 60

        if end_date:
            end_ts = int(pd.Timestamp(end_date).timestamp())
        else:
            end_ts = int(time.time()) + future_days * 24 * 60 * 60

        return start_ts, end_ts

    @classmethod
    @retry(stop_max_attempt_number=3, wait_fixed=1000)
    def _fetch_data(cls, calendar_type, start_ts, end_ts):
        """
        从集思录获取日历数据

        :param calendar_type: 日历类型
        :param start_ts: 开始时间戳
        :param end_ts: 结束时间戳
        :return: pandas.DataFrame
        """
        params = {
            "qtype": calendar_type.value,
            "start": str(start_ts),
            "end": str(end_ts),
            "_": str(int(time.time() * 1000))
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.jisilu.cn/data/calendar/",
            "X-Requested-With": "XMLHttpRequest"
        }

        r = cls.session.get(cls.BASE_URL, params=params, headers=headers, proxies=cls.get_proxy_conf())
        r.raise_for_status()

        data = r.json()

        if not data or not isinstance(data, list):
            return pd.DataFrame()

        df = pd.DataFrame(data)

        if df.empty:
            return df

        # 根据类型处理字段
        df = cls._process_fields(calendar_type, df)

        return df

    @classmethod
    def _process_fields(cls, calendar_type, df):
        """处理数据字段"""
        column_mapping = cls.FIELD_MAPPINGS.get(calendar_type, {})

        # 重命名存在的列
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        if rename_dict:
            df = df.rename(columns=rename_dict)

        # 转换日期字段
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期']).dt.date

        # 类型特定处理
        if calendar_type == CalendarType.DIVIDEND:
            # 转换除权除息日
            if '除权除息日' in df.columns and df['除权除息日'].notna().any():
                df['除权除息日'] = pd.to_datetime(df['除权除息日'], errors='coerce').dt.date

        elif calendar_type == CalendarType.HOLIDAY:
            # 类型映射
            if '类型' in df.columns:
                type_mapping = {
                    'holiday': '节假日',
                    'workday': '工作日',
                    'trading': '交易日',
                    'settlement': '结算日',
                    'other': '其他'
                }
                df['类型'] = df['类型'].map(type_mapping).fillna(df['类型'])

        # 按日期排序
        if '日期' in df.columns:
            df = df.sort_values(by='日期', ascending=True).reset_index(drop=True)

        return df


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    # 测试股票分红数据
    print("=" * 50)
    print("股票分红数据")
    print("=" * 50)
    df_dividend = get_stock_dividend_data()
    print(f"数据条数: {len(df_dividend)}")
    print(df_dividend.head(10))

    # 测试节假日数据
    print("\n" + "=" * 50)
    print("节假日数据")
    print("=" * 50)
    df_holiday = get_holiday_calendar_data()
    print(f"数据条数: {len(df_holiday)}")
    print(df_holiday.head(20))

    # 测试统一接口
    print("\n" + "=" * 50)
    print("使用统一接口获取数据")
    print("=" * 50)
    df = JslCalendar.get_df(CalendarType.DIVIDEND)
    print(f"分红数据条数: {len(df)}")
