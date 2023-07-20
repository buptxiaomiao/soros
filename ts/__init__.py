# coding: utf-8


class TsTask(object):

    @classmethod
    def stock_basic(cls):
        from stock_basic import StockBasic
        return StockBasic.run()


