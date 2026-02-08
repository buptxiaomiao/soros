# coding: utf-8
"""
A股交易时间判断工具类
基于集思录节假日数据和A股交易规则

交易时间规则：
- 周一到周五 9:20-11:30, 13:00-15:00 为开盘时间
- 11:30-13:00 午间休市
- 周六日休市
- 节假日休市（优先从scraper_task/calendar_task落库的数据库获取，备选调用jsl_calendar.py）
"""
import datetime
import logging
import os
import random
import time
from typing import Optional, Set

from rt.store.SqliteUtil import SqliteHelper, DBType
from rt.api.jsl_calendar import get_holiday_calendar_data

# 配置日志
logger = logging.getLogger(__name__)


class TradingTimeUtil:
    """
    A股交易时间判断工具类
    """

    # 交易时间段配置
    TRADING_START_HOUR = 9
    TRADING_START_MINUTE = 20
    TRADING_MORNING_END_HOUR = 11
    TRADING_MORNING_END_MINUTE = 30
    TRADING_AFTERNOON_START_HOUR = 13
    TRADING_AFTERNOON_START_MINUTE = 0
    TRADING_END_HOUR = 15
    TRADING_END_MINUTE = 0

    # 缓存节假日数据
    _holiday_dates: Set[datetime.date] = set()

    @classmethod
    def _refresh_holiday_cache(cls):
        """
        刷新节假日缓存
        优先从scraper_task/calendar_task落库的数据库中获取，title='【A股休市】'
        数据库不存在或无数据时，备选调用jsl_calendar.py获取
        """
        # 多线程时防止重复加载
        time.sleep(random.random() / 1000)
        # 已有缓存数据则直接使用
        if cls._holiday_dates:
            return

        logger.info("开始加载节假日数据")
        holiday_dates = set()

        # 尝试从数据库读取
        try:
            sqlite_helper = SqliteHelper(db_type=DBType.JSL_CALENDAR)
            conn = sqlite_helper.get_connection()
            cursor = conn.cursor()

            # 查询title为'【A股休市】'的记录
            cursor.execute(
                "SELECT date FROM jsl_calendar WHERE title = ? AND type = ?",
                ('【A股休市】', 'holiday')
            )
            rows = cursor.fetchall()

            if rows:
                # 解析日期
                for row in rows:
                    date_str = row[0]
                    if date_str:
                        try:
                            date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
                            holiday_dates.add(date_obj)
                        except ValueError:
                            continue
                logger.info(f"从数据库加载节假日数据 {len(holiday_dates)} 条")
            else:
                logger.warning("数据库中无节假日数据(title='【A股休市】')，将尝试从API获取")
                # 备选：调用API获取
                holiday_dates = cls._fetch_holiday_from_api()

            conn.close()

        except FileNotFoundError as e:
            logger.warning(f"数据库文件不存在: {e}，将尝试从API获取节假日数据")
            holiday_dates = cls._fetch_holiday_from_api()
            try:
                from rt.scraper_task.calendar_task import HolidayCalendarTask
                HolidayCalendarTask().main()
            except Exception as _:
                pass
        except Exception as e:
            logger.error(f"从数据库获取节假日数据失败: {e}，将尝试从API获取")
            holiday_dates = cls._fetch_holiday_from_api()

        cls._holiday_dates = holiday_dates

    @classmethod
    def _fetch_holiday_from_api(cls) -> Set[datetime.date]:
        """
        从API获取节假日数据（备选方案）
        获取前后一年的数据，筛选title包含'A股休市'的记录
        """
        holiday_dates = set()
        try:
            now = datetime.date.today()
            start_date = (now - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
            end_date = (now + datetime.timedelta(days=365)).strftime('%Y-%m-%d')

            logger.info(f"从API获取节假日数据，范围: {start_date} ~ {end_date}")
            df = get_holiday_calendar_data(start_date=start_date, end_date=end_date)

            if df is not None and not df.empty and '日期' in df.columns:
                # 筛选title包含'A股休市'的节假日
                if '节假日名称' in df.columns:
                    holiday_df = df[df['节假日名称'].str.contains('A股休市', na=False)]
                    for date_val in holiday_df['日期'].dropna():
                        if hasattr(date_val, 'date'):
                            holiday_dates.add(date_val.date())
                        elif isinstance(date_val, datetime.date):
                            holiday_dates.add(date_val)
                logger.info(f"从API加载节假日数据 {len(holiday_dates)} 条")
            else:
                logger.warning("API返回数据为空或无日期字段")

        except Exception as e:
            logger.error(f"从API获取节假日数据失败: {e}")

        return holiday_dates

    @classmethod
    def is_trading_time(cls, dt: Optional[datetime.datetime] = None) -> bool:
        """
        判断给定时间是否为开市时间（上午或下午的开市时段）
        排除午间休市时间

        :param dt: 时间，默认为当前时间
        :return: True-是开市时间，False-不是开市时间
        """
        if dt is None:
            dt = datetime.datetime.now()

        # 转换为日期和时间
        date = dt.date()
        hour = dt.hour
        minute = dt.minute

        # 1. 判断是否在交易日（非周六日、非节假日）
        if not cls.is_trading_day(date):
            return False

        # 2. 判断是否在交易时段（考虑午间休市）
        current_minutes = hour * 60 + minute

        # 上午时段：9:20-11:30
        morning_start = cls.TRADING_START_HOUR * 60 + cls.TRADING_START_MINUTE
        morning_end = cls.TRADING_MORNING_END_HOUR * 60 + cls.TRADING_MORNING_END_MINUTE

        # 下午时段：13:00-15:00
        afternoon_start = cls.TRADING_AFTERNOON_START_HOUR * 60 + cls.TRADING_AFTERNOON_START_MINUTE
        afternoon_end = cls.TRADING_END_HOUR * 60 + cls.TRADING_END_MINUTE

        # 在上午时段或下午时段
        if morning_start <= current_minutes < morning_end:
            return True
        if afternoon_start <= current_minutes < afternoon_end:
            return True

        return False

    @classmethod
    def get_time_status(cls, dt: Optional[datetime.datetime] = None) -> str:
        """
        获取给定时间的交易状态类型

        :param dt: 时间，默认为当前时间
        :return: 时间状态类型
            - 'morning_session': 上午开市（9:20-11:30）
            - 'afternoon_session': 下午开市（13:00-15:00）
            - 'noon_break': 午间休市（11:30-13:00）
            - 'before_open': 早晨未开市（<9:20）
            - 'after_close': 已闭市（>=15:00）
            - 'weekend': 周六日闭市
            - 'holiday': 节假日闭市
        """
        if dt is None:
            dt = datetime.datetime.now()

        date = dt.date()
        hour = dt.hour
        minute = dt.minute
        current_minutes = hour * 60 + minute

        # 判断是否为节假日
        cls._refresh_holiday_cache()
        if date in cls._holiday_dates:
            return 'holiday'

        # 判断是否为周六日
        weekday = date.weekday()
        if weekday >= 5:  # 5=周六, 6=周日
            return 'weekend'

        # 交易日内的时段判断
        morning_start = cls.TRADING_START_HOUR * 60 + cls.TRADING_START_MINUTE
        morning_end = cls.TRADING_MORNING_END_HOUR * 60 + cls.TRADING_MORNING_END_MINUTE
        afternoon_start = cls.TRADING_AFTERNOON_START_HOUR * 60 + cls.TRADING_AFTERNOON_START_MINUTE
        afternoon_end = cls.TRADING_END_HOUR * 60 + cls.TRADING_END_MINUTE

        if current_minutes < morning_start:
            return 'before_open'
        elif morning_start <= current_minutes < morning_end:
            return 'morning_session'
        elif morning_end <= current_minutes < afternoon_start:
            return 'noon_break'
        elif afternoon_start <= current_minutes < afternoon_end:
            return 'afternoon_session'
        else:
            return 'after_close'

    @classmethod
    def is_trading_day(cls, date: Optional[datetime.date] = None) -> bool:
        """
        判断给定日期是否为交易日

        :param date: 日期，默认为今天
        :return: True-是交易日，False-不是交易日
        """
        if date is None:
            date = datetime.date.today()

        # 1. 判断是否为周六日
        weekday = date.weekday()
        if weekday >= 5:  # 5=周六, 6=周日
            return False

        # 2. 判断是否为节假日
        cls._refresh_holiday_cache()
        if date in cls._holiday_dates:
            return False

        return True

    @classmethod
    def get_last_trading_end_time(cls, dt: Optional[datetime.datetime] = None) -> datetime.datetime:
        """
        获取最相近的前一个交易时段的结束时间（考虑午间休市）
        如果当前时间是开市时间，直接返回该时间

        逻辑：
        - 如果当前时间是开市时间（9:20-11:30 或 13:00-15:00），直接返回该时间
        - 如果当前时间是上午开市时段（9:20-11:30），返回当天11:30
        - 如果当前时间是午间休市或下午开市时段，返回当天15:00
        - 如果当前时间已经过了当天的收盘时间，返回当天15:00
        - 如果当前时间还没到当天的开盘时间，返回前一个交易日的15:00
        - 如果当天不是交易日，返回前一个交易日的15:00

        :param dt: 时间，默认为当前时间
        :return: 前一个交易时段结束时间，或当前时间（如果在开市时段）
        """
        if dt is None:
            dt = datetime.datetime.now()

        date = dt.date()
        hour = dt.hour
        minute = dt.minute
        current_minutes = hour * 60 + minute

        # 时间段定义（分钟数）
        morning_start = cls.TRADING_START_HOUR * 60 + cls.TRADING_START_MINUTE
        morning_end = cls.TRADING_MORNING_END_HOUR * 60 + cls.TRADING_MORNING_END_MINUTE
        afternoon_start = cls.TRADING_AFTERNOON_START_HOUR * 60 + cls.TRADING_AFTERNOON_START_MINUTE
        afternoon_end = cls.TRADING_END_HOUR * 60 + cls.TRADING_END_MINUTE

        # 判断当前日期是否是交易日
        if cls.is_trading_day(date):
            # 当前是交易日
            if morning_start <= current_minutes < morning_end:
                # 在上午开市时段，直接返回当前时间
                return dt
            elif morning_end <= current_minutes < afternoon_start:
                # 在午间休市时段，返回上午收盘时间11:30
                return datetime.datetime.combine(date, datetime.time(cls.TRADING_MORNING_END_HOUR, cls.TRADING_MORNING_END_MINUTE))
            elif afternoon_start <= current_minutes < afternoon_end:
                # 在下午开市时段，直接返回当前时间
                return dt
            elif current_minutes >= afternoon_end:
                # 已经过了下午收盘时间，返回当天15:00
                return datetime.datetime.combine(date, datetime.time(cls.TRADING_END_HOUR, cls.TRADING_END_MINUTE))
            else:
                # 还没到开盘时间（<9:20），返回前一个交易日的15:00
                prev_trading_day = cls._get_previous_trading_day(date)
                return datetime.datetime.combine(prev_trading_day, datetime.time(cls.TRADING_END_HOUR, cls.TRADING_END_MINUTE))
        else:
            # 当前不是交易日，返回前一个交易日的15:00
            prev_trading_day = cls._get_previous_trading_day(date)
            return datetime.datetime.combine(prev_trading_day, datetime.time(cls.TRADING_END_HOUR, cls.TRADING_END_MINUTE))

    @classmethod
    def _get_previous_trading_day(cls, date: datetime.date) -> datetime.date:
        """
        获取给定日期之前最近的一个交易日

        :param date: 日期
        :return: 前一个交易日
        """
        # 从日期前一天开始往前找
        check_date = date - datetime.timedelta(days=1)

        # 最多往前找30天（避免节假日连休过多）
        for _ in range(30):
            if cls.is_trading_day(check_date):
                return check_date
            check_date -= datetime.timedelta(days=1)

        # 如果30天内没找到，返回date-1（兜底）
        return date - datetime.timedelta(days=1)

    @classmethod
    def get_next_trading_day(cls, date: Optional[datetime.date] = None) -> datetime.date:
        """
        获取给定日期之后最近的一个交易日

        :param date: 日期，默认为今天
        :return: 下一个交易日
        """
        if date is None:
            date = datetime.date.today()

        # 从日期后一天开始往后找
        check_date = date + datetime.timedelta(days=1)

        # 最多往后找30天
        for _ in range(30):
            if cls.is_trading_day(check_date):
                return check_date
            check_date += datetime.timedelta(days=1)

        # 兜底
        return date + datetime.timedelta(days=1)


# ============ 便捷函数 ============

def is_trading_time(dt: Optional[datetime.datetime] = None) -> bool:
    """
    判断给定时间是否为开盘时间

    :param dt: 时间，默认为当前时间
    :return: True-是开盘时间，False-不是开盘时间
    """
    return TradingTimeUtil.is_trading_time(dt)


def is_trading_day(date: Optional[datetime.date] = None) -> bool:
    """
    判断给定日期是否为交易日

    :param date: 日期，默认为今天
    :return: True-是交易日，False-不是交易日
    """
    return TradingTimeUtil.is_trading_day(date)


def get_last_trading_end_time(dt: Optional[datetime.datetime] = None) -> datetime.datetime:
    """
    获取最相近的前一个开盘时间的截止时间

    :param dt: 时间，默认为当前时间
    :return: 前一个收盘时间
    """
    return TradingTimeUtil.get_last_trading_end_time(dt)


def get_next_trading_day(date: Optional[datetime.date] = None) -> datetime.date:
    """
    获取下一个交易日

    :param date: 日期，默认为今天
    :return: 下一个交易日
    """
    return TradingTimeUtil.get_next_trading_day(date)


def get_time_status(dt: Optional[datetime.datetime] = None) -> str:
    """
    获取给定时间的交易状态类型

    :param dt: 时间，默认为当前时间
    :return: 时间状态类型
        - 'morning_session': 上午开市（9:20-11:30）
        - 'afternoon_session': 下午开市（13:00-15:00）
        - 'noon_break': 午间休市（11:30-13:00）
        - 'before_open': 早晨未开市（<9:20）
        - 'after_close': 已闭市（>=15:00）
        - 'weekend': 周六日闭市
        - 'holiday': 节假日闭市
    """
    return TradingTimeUtil.get_time_status(dt)


if __name__ == '__main__':
    import sys

    # 配置日志输出
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )

    # 测试代码
    print("=" * 60)
    print("A股交易时间判断工具测试")
    print("=" * 60)

    # 测试当前时间
    now = datetime.datetime.now()
    print(f"\n当前时间: {now}")
    print(f"时间状态: {get_time_status()}")
    print(f"是否是开市时间: {is_trading_time()}")
    print(f"今天是否是交易日: {is_trading_day()}")
    print(f"前一个交易时段结束时间: {get_last_trading_end_time()}")
    print(f"下一个交易日: {get_next_trading_day()}")

    # 测试特定时间
    test_cases = [
        # 交易日内各时段
        datetime.datetime(2025, 2, 10, 9, 0),     # 周一早晨未开市
        datetime.datetime(2025, 2, 10, 10, 30),   # 周一上午开市
        datetime.datetime(2025, 2, 10, 12, 0),    # 周一午间休市
        datetime.datetime(2025, 2, 10, 14, 0),    # 周一下午开市
        datetime.datetime(2025, 2, 10, 16, 0),    # 周一已闭市
        # 周六日
        datetime.datetime(2025, 2, 8, 10, 0),     # 周六
        datetime.datetime(2025, 2, 9, 10, 0),     # 周日
    ]

    print("\n" + "=" * 60)
    print("特定时间测试")
    print("=" * 60)

    for test_dt in test_cases:
        print(f"\n时间: {test_dt}")
        print(f"  时间状态: {get_time_status(test_dt)}")
        print(f"  是否交易日: {is_trading_day(test_dt.date())}")
        print(f"  是否开市时间: {is_trading_time(test_dt)}")
        print(f"  前一个交易时段结束时间: {get_last_trading_end_time(test_dt)}")
