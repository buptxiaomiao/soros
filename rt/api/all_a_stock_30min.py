# coding: utf-8

import os
import sys

import pandas as pd

# 保证项目根目录在 sys.path 中，方便导入 utils / rt 等模块
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)


from rt.api.stock_minutes_rt import StockKlineRt
from utils.ts_util import pro
from rt.store.SqliteUtil import SqliteHelper, DBType
from utils.trade_time_util import TradeTimeUtil
from datetime import datetime
import pandas as pd


KLT_TO_DB_TYPE = {
    1: DBType.MINUTE_1_DB,
    5: DBType.MINUTE_5_DB,
    15: DBType.MINUTE_15_DB,
    30: DBType.MINUTE_30_DB,
    60: DBType.MINUTE_60_DB,
    120: DBType.MINUTE_120_DB,
}


def get_all_a_stock_codes(only_listed: bool = True, exchanges=None):
    from rt.easyq.astock import get_all_stock_rt
    data = get_all_stock_rt()
    return ['000001']
    return [item['code'] for item in data][:500]



class StockKlineRtWithCode(StockKlineRt):
    """
    在原始 30 分钟 K 线结果上补充 code 列，方便区分不同股票。
    """

    @classmethod
    def get_stock_minutes_rt(cls, code, klt=30, window=20, num_std=2, end=""):
        # 使用父类逻辑获取单个标的的 K 线数据（原方法返回 (df, 0)）
        df, _ = super().get_stock_minutes_rt(
            code=code,
            klt=klt,
            window=window,
            num_std=num_std,
            end=end,
        )
        print(f'get_stock_minutes_rt code: {code} df.shape: {df.shape}')
        return df, df.shape[0]

def fetch_all_a_kline(
    klt: int = 30,
    end: str = "",
    only_listed: bool = True,
    exchanges=None,
    batch_size: int = 100,
):
    """
    批量爬取所有 A 股 K 线数据，存储到 SQLite。

    :param klt:         周期（1, 5, 15, 30, 60, 120）
    :param end:         结束日期（YYYYMMDD）
    :param only_listed: 是否只保留在市股票
    :param exchanges:   限定交易所列表
    :param batch_size:  每批处理的股票数量
    :return: pandas.DataFrame (包含本次运行获取的所有数据)
    """
    db_type = KLT_TO_DB_TYPE.get(klt)
    if not db_type:
        raise ValueError(f"不支持的 klt: {klt}")

    # 字段映射：中文 -> 英文
    col_mapping = {
        '时间': 'trade_time',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount',
        '振幅': 'amplitude',
        '涨跌幅': 'pct_chg',
        '涨跌额': 'chg_amt',
        '换手率': 'turnover_rate',
        'code': 'code',
        'name': 'name',
        'klt': 'klt',
        'mean': 'ma',
        'std': 'std',
        'ub': 'upper_band',
        'lb': 'lower_band',
        'low_lt_lb': 'is_low_lt_lb',
        'low_lt_lb_prev1': 'is_low_lt_lb_p1',
        'low_lt_lb_prev2': 'is_low_lt_lb_p2',
        'sell_flag': 'sell_flag',
        'update_time': 'update_time'
    }

    codes = get_all_a_stock_codes(only_listed=only_listed, exchanges=exchanges)
    print(f"\n[{klt}m] 共获取到总股票数: {len(codes)}")

    # 初始化 SQLite 助手
    now = datetime.now()
    db_helper = SqliteHelper(db_type, now.strftime('%Y-%m-%d'))
    table_name = "stock_kline"

    # 建表
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        trade_time TEXT, -- 时间
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
        code TEXT, -- 股票代码
        name TEXT, -- 股票名称
        klt INTEGER, -- 周期
        ma REAL, -- 均价
        std REAL, -- 标准差
        upper_band REAL, -- 上轨
        lower_band REAL, -- 下轨
        is_low_lt_lb INTEGER, -- 最低价低于下轨
        is_low_lt_lb_p1 INTEGER, -- 前一周期最低价低于下轨
        is_low_lt_lb_p2 INTEGER, -- 前二周期最低价低于下轨
        sell_flag INTEGER, -- 卖出信号
        update_time TEXT -- 更新时间
    )
    """
    db_helper.execute(create_table_sql)
    
    is_trading = TradeTimeUtil.is_trading_hour(now)
    print(f"当前时间: {now}, 是否开盘时间: {is_trading}")

    # 幂等检查
    processed_codes = set()
    if db_helper.table_exists(table_name) and not is_trading:
        try:
            # 获取数据库中每只股票的最新交易时间和最后更新时间
            sql = f"SELECT code, MAX(trade_time) as latest_trade, MAX(update_time) as latest_update FROM {table_name} GROUP BY code"
            existing_data = db_helper.execute(sql)
            for row in existing_data:
                code, last_trade, last_update = row[0], row[1], row[2]
                
                # 如果最后更新时间是在非交易时间（且是收盘后），且目前也是非交易时间
                # 我们可以认为已经完成过盘后同步
                if last_update:
                    try:
                        update_dt = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
                        # 如果最后更新时间是在某个交易日的 15:00 之后（收盘后）
                        if TradeTimeUtil.is_after_trading_hours(update_dt):
                            # 这里进一步判断：如果 last_trade 的日期和 update_dt 的日期一致，
                            # 或者 update_dt 是在最新的交易日之后，则认为已处理
                            processed_codes.add(code)
                    except:
                        pass
            
            print(f"检测到数据库已有 {len(processed_codes)} 只股票已完成盘后同步。将跳过。")
        except Exception as e:
            print(f"读取数据库幂等信息失败: {e}")

    codes_to_fetch = [c for c in codes if c not in processed_codes] if not is_trading else codes
    print(f"待爬取股票数: {len(codes_to_fetch)}")

    if not codes_to_fetch:
        print("所有股票已爬取完毕。")
        return pd.DataFrame()

    all_results = []
    
    # 分批处理
    for i in range(0, len(codes_to_fetch), batch_size):
        batch = codes_to_fetch[i : i + batch_size]
        print(f"--- 正在处理 {klt}m 批次 {i // batch_size + 1}, 包含 {len(batch)} 只股票 ---")
        
        func = StockKlineRtWithCode.get_stock_minutes_rt
        args = [[code, klt, 20, 2, end] for code in batch]

        batch_raw_results = StockKlineRtWithCode.run_by_pool_pro(
            fetch_func=func,
            args=args,
        )

        batch_dfs = []
        codes_in_batch = []
        for res in batch_raw_results:
            if isinstance(res, tuple) and isinstance(res[0], pd.DataFrame) and not res[0].empty:
                df = res[0]
                # 数据清洗：去掉窗口计算初期的 20 条无效数据
                if len(df) > 20:
                    df = df.iloc[20:].copy()
                else:
                    continue
                
                # 转换为英文列名
                df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                df = df.rename(columns=col_mapping)
                batch_dfs.append(df)
                codes_in_batch.append(df['code'].iloc[0])
            elif isinstance(res, pd.DataFrame) and not res.empty:
                df = res
                if len(df) > 20:
                    df = df.iloc[20:].copy()
                else:
                    continue
                df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                df = df.rename(columns=col_mapping)
                batch_dfs.append(df)
                codes_in_batch.append(df['code'].iloc[0])

        if batch_dfs:
            final_batch_df = pd.concat(batch_dfs, ignore_index=True)
            
            if not is_trading:
                # 获取这批股票在 DB 中的最新时间
                placeholders = ', '.join(['?'] * len(codes_in_batch))
                sql = f"SELECT code, MAX(trade_time) FROM {table_name} WHERE code IN ({placeholders}) GROUP BY code"
                db_latest = dict(db_helper.execute(sql, codes_in_batch))
                
                filtered_dfs = []
                for df in batch_dfs:
                    code = df['code'].iloc[0]
                    fetched_latest = df['trade_time'].max()
                    if code not in db_latest or fetched_latest > db_latest[code]:
                        filtered_dfs.append(df)
                
                if not filtered_dfs:
                    print(f"批次 {i // batch_size + 1} 的所有股票数据均已是最新，跳过写入。")
                    continue
                
                final_batch_df = pd.concat(filtered_dfs, ignore_index=True)
                codes_to_delete = final_batch_df['code'].unique().tolist()
            else:
                codes_to_delete = codes_in_batch

            with db_helper.get_connection() as conn:
                placeholders = ', '.join(['?'] * len(codes_to_delete))
                conn.execute(f"DELETE FROM {table_name} WHERE code IN ({placeholders})", codes_to_delete)
                final_batch_df.to_sql(table_name, conn, if_exists='append', index=False)
                conn.commit()
            
            all_results.append(final_batch_df)
            print(f"批次 {i // batch_size + 1} 已成功写入数据库: {db_helper.db_name}")

    if not all_results:
        return pd.DataFrame()

    return pd.concat(all_results, ignore_index=True)


def main():
    pd.set_option("display.max_columns", None)

    # 周期列表
    # klts = [1, 5, 15, 30, 60, 120]
    klts = [1]
    exchanges = ["SSE", "SZSE"]

    for klt in klts:
        print(f"\n{'='*20} 开始处理 {klt} 分钟 K 线 {'='*20}")
        try:
            fetch_all_a_kline(
                klt=klt,
                only_listed=True,
                exchanges=exchanges,
                batch_size=10,
            )
        except Exception as e:
            print(f"处理 {klt}m 时发生错误: {e}")
            import traceback
            traceback.print_exc()

    print("\n所有任务执行完毕。")


if __name__ == "__main__":
    main()
