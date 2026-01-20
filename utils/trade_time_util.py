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
