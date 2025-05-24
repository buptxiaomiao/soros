# coding: utf-8
from datetime import datetime
from dateutil.relativedelta import relativedelta


class Now(object):

    def __init__(self, now=None):
        self.now = now or datetime.now()

    def delta(self, days=0, weeks=0, months=0, years=0):
        t = self.now - relativedelta(years=years, months=months, weeks=weeks, days=days)
        return Now(t)

    @property
    def date(self):
        return Now(self.now.date())

    @property
    def datekey(self):
        return self.now.strftime('%Y%m%d')

    @property
    def month_begin_date(self):
        return Now(datetime(self.now.year, self.now.month, 1))

    @property
    def month_end_date(self):
        return self.delta(months=-1).month_begin_date.delta(days=1)

    def __str__(self):
        return str(self.now)[:19]

    @property
    def is_trade_date(self):
        return self.date.now.weekday() <= 5


if __name__ == '__main__':
    a = Now()
    print(a)
    print(a.delta(1).date)
    print(a.delta(1).date.month_begin_date)
    print(a.delta(1).month_begin_date.date)
    print(a.delta(1).month_end_date.date)
    print(a.delta(1).datekey)
