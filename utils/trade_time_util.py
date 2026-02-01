import datetime
from typing import Optional

class TradeTimeUtil:
    """
    A 股开盘时间判断工具类
    """

    @staticmethod
    def is_trading_hour(dt: Optional[datetime.datetime] = None) -> bool:
        """
        判断指定时间是否是 A 股开盘时间。
        规则：周一至周五，9:30-11:30, 13:00-15:00
        
        :param dt: 待判断的时间，如果不传则使用当前时间
        :return: True 如果在交易时间内，否则 False
        """
        if dt is None:
            dt = datetime.datetime.now()

        # 判断是否为周一至周五 (0=Monday, 6=Sunday)
        if dt.weekday() >= 5:
            return False

        current_time = dt.time()

        # 上午盘：9:30 - 11:30
        morning_start = datetime.time(9, 30, 0)
        morning_end = datetime.time(11, 30, 0)

        # 下午盘：13:00 - 15:00
        afternoon_start = datetime.time(13, 0, 0)
        afternoon_end = datetime.time(15, 0, 0)

        # if (morning_start <= current_time <= morning_end) or \
        #    (afternoon_start <= current_time <= afternoon_end):
        #     return True
        return morning_start <= current_time <= afternoon_end

    @staticmethod
    def is_after_trading_hours(dt: Optional[datetime.datetime] = None) -> bool:
        """
        判断是否已经收盘（15:00 之后）
        """
        if dt is None:
            dt = datetime.datetime.now()
        
        if dt.weekday() >= 5:
            return True
            
        return dt.time() >= datetime.time(15, 0, 0)

    @staticmethod
    def get_last_trading_close_time(dt: datetime.datetime) -> datetime.datetime:
        """
        计算给定时间 dt 之前，最近的一个交易日收盘时间 (15:00)
        """
        # 从 dt 当天开始往回找
        check_date = dt.date()

        # 如果当前是交易日且已经过了 15:00，那么最近的收盘就是今天 15:00
        # 如果当前是交易日但还没到 15:00，或者是非交易日，则需要找上一个交易日
        if check_date.weekday() >= 5 or dt.time() < datetime.time(15, 0):
            # 往前推到上一个周五
            days_to_subtract = 1
            if check_date.weekday() == 0:  # 周一
                days_to_subtract = 3
            elif check_date.weekday() == 6:  # 周日
                days_to_subtract = 2
            else:
                days_to_subtract = 1
            check_date = check_date - datetime.timedelta(days=days_to_subtract)

        # 这里假设没有节假日，只按周六日判断
        # 如果要处理节假日，需配合 chinesecalendar 库循环往前找 is_holiday=False 的日子
        return datetime.datetime.combine(check_date, datetime.time(15, 0))

    @staticmethod
    def should_continue_crawling(db_trade_time_str: str, buffer_minutes: int = 60) -> bool:
        """
        判断是否需要继续爬取
        """
        now = datetime.datetime.now()

        # 1. 如果还在交易时间内，必须继续
        if TradeTimeUtil.is_trading_hour(now):
            print(111)
            return True

        # 2. 解析数据库时间
        try:
            db_dt = datetime.datetime.strptime(db_trade_time_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            db_dt = datetime.datetime.strptime(db_trade_time_str, "%Y-%m-%d %H:%M")

        # 3. 计算“理论上”当前应该拥有的最新收盘时间
        last_close = TradeTimeUtil.get_last_trading_close_time(now)

        # 4. 如果库里的时间已经等于或超过了最近的收盘时间
        if db_dt >= last_close:
            # 特殊情况：如果在收盘后的 buffer 分钟内，依然允许爬取（为了拿最终结算数据）
            buffer_deadline = last_close + datetime.timedelta(minutes=buffer_minutes)
            if last_close <= now <= buffer_deadline:
                print(222)
                return True

            # 已经超过 buffer 时间，且库里数据已最新，停止
            print(333)
            return False

        print(444)
        # 5. 其他情况（库里时间落后于最后收盘时间），继续爬取
        return True

if __name__ == '__main__':
    check_time = "2026-01-30 14:00"
    print(TradeTimeUtil.should_continue_crawling(check_time))

    check_time = "2026-01-30 15:00"
    print(TradeTimeUtil.should_continue_crawling(check_time))