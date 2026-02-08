import time

from env import proxy_conf
from rt.store.SqliteUtil import DBType
from rt.api.tx_sw_hy_rt import TXSwHyRT
from rt.scraper_task.scraper_task_base import StockScraperBase


class ScraperTxSwHy(StockScraperBase):
    """
    腾讯申万行业板块数据爬取任务
    支持一级行业和二级行业板块数据
    单次请求获取全量数据，无需复杂并发
    """

    table_name = 'tx_sw_hy'

    def __init__(self, hy_level=1, max_workers=1, use_proxy=True):
        """
        :param hy_level: 行业级别，1=申万一级行业，2=申万二级行业
        :param max_workers: 并发数（实际无效，单次请求获取全量数据）
        :param use_proxy: 是否使用代理
        """
        self.hy_level = hy_level
        _proxy_conf = proxy_conf if use_proxy else None
        # 按年粒度分库
        self.db_type = DBType.TX_SW_HY
        super().__init__(self.db_type, proxy_conf=_proxy_conf, max_workers=max_workers)
        # 缓存全量数据
        self._all_data_df = None

    def main(self):
        """主入口：单次请求获取全量数据，直接落库"""
        self.logger.info(f">>> 开始获取 {self.log_prefix} 数据")

        # 获取全量数据（只请求一次）
        df = self.fetch_all_data()
        if df is None or df.empty:
            self.logger.warning(f"未获取到 {self.log_prefix} 数据")
            return

        self.logger.info(f"获取到 {self.log_prefix} 数据 {len(df)} 条")

        # 转换为入库格式并落库
        data_list = self._convert_to_db_format(df)
        self._save_to_db(data_list)

        self.logger.info(f">>> 完成 {self.log_prefix} 数据入库，共 {len(data_list)} 条")

    def fetch_all_data(self):
        """
        获取全量板块数据
        腾讯接口一次性返回所有板块数据
        """
        self.logger.info(f"{self.log_prefix} 获取申万{self.hy_level}级行业板块数据...")
        df, total_num = TXSwHyRT.get_tx_sw_hy_list(self.hy_level, req=self.session)
        if df is None or df.empty:
            self.logger.warning(f"{self.log_prefix} 未获取到数据")
            return None

        # 缓存数据供后续使用
        self._all_data_df = df
        return df

    def get_all_codes(self):
        """
        获取板块代码列表（从缓存数据中提取）
        """
        if self._all_data_df is None:
            self.fetch_all_data()

        if self._all_data_df is None or self._all_data_df.empty:
            return []

        # 从缓存数据中提取板块代码列表
        code_list = self._all_data_df['板块代码'].dropna().astype(str).tolist()
        self.logger.info(f"{self.log_prefix} 获取到 {len(code_list)} 个板块")
        return code_list

    cols = [
        'trade_time', 'pt_bk_code', 'bk_code', 'bk_name', 'level', 'latest_price', 'change_amt', 'pct_chg',
        'rise_speed', 'pct_chg_5d', 'pct_chg_20d', 'pct_chg_60d', 'pct_chg_ytd', 'pct_chg_52w',
        'volume', 'turnover', 'turnover_rate', 'volume_ratio', 'float_market_cap', 'total_market_cap',
        'main_net_inflow', 'main_net_inflow_5d', 'main_net_inflow_20d', 'main_outflow', 'main_inflow',
        'stock_count', 'rise_count', 'fall_count', 'rise_fall_ratio',
        'main_inflow_outflow_ratio', 'leader_stock_mcode', 'leader_stock_code', 'leader_stock_name',
        'leader_stock_change', 'leader_stock_pct_chg', 'leader_stock_price', 'crawl_time'
    ]

    def get_create_table_sql_list(self):
        """创建表SQL"""
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                trade_time TEXT, -- 交易时间
                pt_bk_code TEXT, -- 板块代码（含市场前缀）
                bk_code TEXT, -- 板块代码（纯代码）
                bk_name TEXT, -- 板块名称
                level INTEGER, -- 行业级别（1=一级行业，2=二级行业）
                latest_price REAL, -- 板块指数最新价格
                change_amt REAL, -- 板块指数涨跌金额
                pct_chg REAL, -- 当日涨跌幅(%)
                rise_speed REAL, -- 上涨速度
                pct_chg_5d REAL, -- 近5个交易日累计涨跌幅(%)
                pct_chg_20d REAL, -- 近20个交易日累计涨跌幅(%)
                pct_chg_60d REAL, -- 近60个交易日累计涨跌幅(%)
                pct_chg_ytd REAL, -- 当年累计涨跌幅(%)
                pct_chg_52w REAL, -- 52周涨跌幅(%)
                volume REAL, -- 当日成交股数（单位：万股）
                turnover REAL, -- 当日成交金额（单位：亿元）
                turnover_rate REAL, -- 板块整体换手率(%)
                volume_ratio REAL, -- 当日成交量/近5日平均成交量
                float_market_cap REAL, -- 板块流通市值合计（单位：亿元）
                total_market_cap REAL, -- 板块总市值合计（单位：亿元）
                main_net_inflow REAL, -- 主力资金净流入金额（单位：亿元）
                main_net_inflow_5d REAL, -- 5日主力净流入（单位：亿元）
                main_net_inflow_20d REAL, -- 20日主力净流入（单位：亿元）
                main_outflow REAL, -- 主力流出金额（单位：亿元）
                main_inflow REAL, -- 主力流入金额（单位：亿元）
                stock_count TEXT, -- 板块内股票总数（格式：上涨数/下跌数）
                rise_count REAL, -- 上涨个股数量
                fall_count REAL, -- 下跌个股数量
                rise_fall_ratio REAL, -- 上涨数量/下跌数量
                main_inflow_outflow_ratio REAL, -- 主力流入/主力流出
                leader_stock_mcode TEXT, -- 领涨股代码（含市场前缀）
                leader_stock_code TEXT, -- 领涨股代码（纯代码）
                leader_stock_name TEXT, -- 领涨股名称
                leader_stock_change REAL, -- 领涨股涨跌额
                leader_stock_pct_chg REAL, -- 领涨股涨跌幅(%)
                leader_stock_price REAL, -- 领涨股最新价
                crawl_time TEXT, -- 爬取时间
                update_time TEXT DEFAULT (datetime('now', '+8 hours')), -- 更新时间
                PRIMARY KEY (trade_time, bk_code)
            )
        """
        return [ddl]

    def get_insert_sql(self):
        """插入SQL"""
        col_str = ','.join(self.cols)
        v_str = ','.join(['?'] * len(self.cols))
        return (f"INSERT OR REPLACE INTO {self.table_name} "
                f"({col_str}) VALUES ({v_str})")

    # 中文字段名到英文字段名的映射
    COLS_MAP = {
        'trade_time': 'trade_time',
        'pt板块代码': 'pt_bk_code',
        '板块代码': 'bk_code',
        '板块名称': 'bk_name',
        '最新价': 'latest_price',
        '涨跌额': 'change_amt',
        '涨跌幅': 'pct_chg',
        '涨速': 'rise_speed',
        '5日涨跌幅': 'pct_chg_5d',
        '20日涨跌幅': 'pct_chg_20d',
        '60日涨跌幅': 'pct_chg_60d',
        '年初至今涨跌幅': 'pct_chg_ytd',
        'zdf_w52': 'pct_chg_52w',
        '成交量': 'volume',
        '成交额': 'turnover',
        '换手率': 'turnover_rate',
        '量比': 'volume_ratio',
        '流通市值': 'float_market_cap',
        '总市值': 'total_market_cap',
        '主力净流入': 'main_net_inflow',
        '主力5日净流入': 'main_net_inflow_5d',
        '主力20日净流入': 'main_net_inflow_20d',
        '主力流出': 'main_outflow',
        '主力流入': 'main_inflow',
        '个股数量': 'stock_count',
        '上涨数量': 'rise_count',
        '下跌数量': 'fall_count',
        '上涨下跌个数比': 'rise_fall_ratio',
        '主力流入流出比': 'main_inflow_outflow_ratio',
        '领涨股_mcode': 'leader_stock_mcode',
        '领涨股_code': 'leader_stock_code',
        '领涨股_name': 'leader_stock_name',
        '领涨股_zd': 'leader_stock_change',
        '领涨股_zdf': 'leader_stock_pct_chg',
        '领涨股_zxj': 'leader_stock_price',
        'crawl_time': 'crawl_time',
    }

    def _convert_to_db_format(self, df):
        """
        将DataFrame转换为入库格式
        字段重命名、类型转换
        """
        # 字段重命名：中文 -> 英文
        df = df.rename(columns=self.COLS_MAP).copy()

        # 增加行业级别字段
        df['level'] = self.hy_level

        # 转换 Timestamp 类型为字符串，避免 sqlite3 绑定错误
        for col in df.columns:
            if df[col].dtype.kind == 'M':  # datetime64 类型
                df[col] = df[col].astype(str)

        # 按cols顺序提取字段
        data_list = []
        for _, row in df.iterrows():
            row_data = []
            for col in self.cols:
                val = row.get(col, None)
                # 处理任何可能剩余的 Timestamp 或 datetime 对象
                if hasattr(val, 'strftime'):
                    val = val.strftime('%Y-%m-%d %H:%M:%S')
                row_data.append(val)
            data_list.append(tuple(row_data))

        return data_list

    def _save_to_db(self, data_list):
        """保存数据到数据库"""
        conn = self._get_read_conn()

        # 创建表
        for sql in self.get_create_table_sql_list():
            conn.execute(sql)

        # 插入数据
        sql = self.get_insert_sql()
        conn.executemany(sql, data_list)
        conn.commit()
        conn.close()

    def fetch_logic(self, code):
        """兼容基类接口，实际不使用（通过main直接处理全量数据）"""
        pass

    def run(self, stock_list=None):
        """重写run方法，直接使用main逻辑"""
        self.main()


if __name__ == '__main__':
    # 爬取申万一级行业板块数据
    scraper_hy1 = ScraperTxSwHy(hy_level=1, max_workers=5)
    scraper_hy1.main()

    # 爬取申万二级行业板块数据
    scraper_hy2 = ScraperTxSwHy(hy_level=2, max_workers=5)
    scraper_hy2.main()
