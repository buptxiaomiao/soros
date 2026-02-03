import time

from env import proxy_conf
from utils.trade_time_util import TradeTimeUtil
from rt.easyq.astock import get_all_stock_rt
from rt.store.SqliteUtil import DBType
from rt.api.stock_minutes_rt import StockKlineRt
from rt.scraper_task.scraper_task_base import StockScraperBase


class ScraperStockKline(StockScraperBase):

    all_codes = []

    KLT_TO_DB_TYPE = {
        1: DBType.MINUTE_1_DB,
        5: DBType.MINUTE_5_DB,
        15: DBType.MINUTE_15_DB,
        30: DBType.MINUTE_30_DB,
        60: DBType.MINUTE_60_DB,
        120: DBType.MINUTE_120_DB,
        101: DBType.DC_DAY_DB,
    }
    table_name = 'stock_kline'

    def __init__(self, kline_type=30, max_workers=20, use_proxy=True):
        self.kline_type = kline_type
        _proxy_conf = proxy_conf if use_proxy else None
        self.db_type = self.KLT_TO_DB_TYPE[kline_type]
        super().__init__(self.db_type, proxy_conf=_proxy_conf, max_workers=max_workers)

    def main(self):
        stock_list = self.get_all_codes()
        self.run(stock_list)

        # self.run(['000001', '688018', '600809'])

    def get_all_codes(self):
        if self.all_codes:
            return self.all_codes
        data_list = get_all_stock_rt()
        code_list = [i['code'] for i in data_list]
        self.all_codes = code_list
        return self.all_codes

    cols = [
        'trade_time', 'code', 'name', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude',
        'pct_chg', 'chg_amt', 'turnover_rate', 'klt', 'ma', 'std', 'upper_band', 'lower_band',
        'is_low_lt_lb', 'is_low_lt_lb_prev1', 'is_low_lt_lb_prev2', 'sell_flag'
    ]

    def get_create_table_sql_list(self):
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                trade_time TEXT, -- 时间
                code TEXT, -- 股票代码
                name TEXT, -- 股票名称
                open REAL, -- 开盘
                close REAL, -- 收盘
                high REAL, -- 最高
                low REAL, -- 最低
                volume REAL, -- 成交量
                amount REAL, -- 成交额
                amplitude REAL, -- 振幅
                pct_chg REAL, -- 涨跌幅
                chg_amt REAL, -- 涨跌额
                turnover_rate REAL, -- 换手率
                klt INTEGER, -- 周期
                ma REAL, -- 均价
                std REAL, -- 标准差
                upper_band REAL, -- 上轨
                lower_band REAL, -- 下轨
                is_low_lt_lb INTEGER, -- 最低价低于下轨
                is_low_lt_lb_prev1 INTEGER, -- 前一周期最低价低于下轨
                is_low_lt_lb_prev2 INTEGER, -- 前二周期最低价低于下轨
                sell_flag INTEGER, -- 卖出信号
                update_time TEXT DEFAULT (datetime('now', '+8 hours')), -- 更新时间
                PRIMARY KEY (code, trade_time)
            )
        """
        return [ddl]

    def get_insert_sql(self):
        col_str = ','.join(self.cols)
        v_str = ','.join(['?'] * len(self.cols))
        return (f"INSERT OR REPLACE INTO {self.table_name} "
                f"({col_str}) VALUES ({v_str})")

    def fetch_logic(self, code):
        time.sleep(0.05)
        # 1. 查库判断是否需要更新 (复用线程连接)
        conn = self._get_read_conn()
        cursor = conn.cursor()
        cursor.execute(f""
                       f"SELECT code, max(trade_time) as trade_time "
                       f"FROM {self.table_name} WHERE code=? ", (code,))
        row = cursor.fetchone()
        max_trade_time = row[-1] if row else None
        self.logger.info(f"{self.log_prefix} {code} db.trade_time={max_trade_time}")

        # 检验库里的时间是否为最新，判断是否需要继续爬取
        if max_trade_time:
            cursor.execute(f""
                           f"SELECT code, trade_time, update_time "
                           f"FROM {self.table_name} WHERE code=? and trade_time=?", (code, max_trade_time,))
            u_row = cursor.fetchone()
            update_time = u_row[-1] if u_row else None
            if update_time and update_time > max_trade_time:
                # 判断库里的交易时间是否为最新的
                should_continue = TradeTimeUtil.should_continue_crawling(max_trade_time)
                self.logger.info(f"{self.log_prefix} {code} db.trade_time={max_trade_time} "
                                 f"update_time={update_time} should_continue={should_continue}")
                if not should_continue:
                    return []

        # 2. 发起请求
        # 注意：这里不需要写 try-except，基类的 _worker 会统一处理并重试
        res = StockKlineRt.get_stock_minutes_rt(code, self.kline_type, req_tool=self.session)
        df = res[0]
        df = df.iloc[20:].copy()
        data_list = df.to_dict('records')
        return [(
            i['时间'],
            i['code'],
            i['name'],
            i['开盘'],
            i['收盘'],
            i['最高'],
            i['最低'],
            i['成交量'],
            i['成交额'],
            i['振幅'],
            i['涨跌幅'],
            i['涨跌额'],
            i['换手率'],
            i['klt'],
            i['mean'],
            i['std'],
            i['ub'],
            i['lb'],
            i['low_lt_lb'],
            i['low_lt_lb_prev1'],
            i['low_lt_lb_prev2'],
            i['sell_flag'],
        )for i in data_list]


if __name__ == '__main__':

    stock_scraper = ScraperStockKline(30)
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()

    stock_scraper = ScraperStockKline(5)
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()

    stock_scraper = ScraperStockKline(15)
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()

    stock_scraper = ScraperStockKline(60)
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()

    stock_scraper = ScraperStockKline(1)
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()
    stock_scraper.main()

