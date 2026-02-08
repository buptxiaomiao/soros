# coding: utf-8
"""
集思录日历数据落库任务
- 股票分红数据
- 节假日特殊日期数据
"""
import datetime
import json
from typing import List

import pandas as pd

from rt.scraper_task.scraper_task_base import StockScraperBase
from rt.store.SqliteUtil import DBType
from rt.api.jsl_calendar import get_stock_dividend_data, get_holiday_calendar_data


class CalendarTaskBase(StockScraperBase):
    """
    日历数据落库基类
    单次请求获取全量数据，无需复杂并发
    """

    table_name = 'jsl_calendar'

    cols = [
        'date', 'code', 'title', 'description', 'type', 'extra_info', 'update_time'
    ]

    def __init__(self, proxy_conf=None):
        super().__init__(DBType.JSL_CALENDAR, proxy_conf=proxy_conf, max_workers=1, batch_size=100)

    def main(self):
        """主入口"""
        self.logger.info(f">>> 开始获取 {self.log_prefix} 数据")

        # 获取数据
        df = self.fetch_data()
        if df is None or df.empty:
            self.logger.warning(f"未获取到 {self.log_prefix} 数据")
            return

        self.logger.info(f"获取到 {self.log_prefix} 数据 {len(df)} 条")

        # 转换为入库格式
        data_list = self._convert_to_db_format(df)

        # 落库
        self._save_to_db(data_list)

        self.logger.info(f">>> 完成 {self.log_prefix} 数据入库，共 {len(data_list)} 条")

    def fetch_data(self):
        """获取数据，子类实现"""
        raise NotImplementedError

    def _convert_to_db_format(self, df) -> List[tuple]:
        """将DataFrame转换为入库格式"""
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_list = []

        # 调试：打印实际的列名
        self.logger.info(f"DataFrame columns: {list(df.columns)}")

        for _, row in df.iterrows():
            # 获取日期字段（可能是'日期'或'start'）
            date_val = row.get('日期', row.get('start', ''))
            if hasattr(date_val, 'strftime'):
                date_str = date_val.strftime('%Y-%m-%d')
            else:
                date_str = str(date_val)[:10] if date_val else ''

            # 获取股票代码（分红数据：尝试所有可能的字段名）
            code = ''
            for code_field in ['股票代码', 'stock_cd', 'code', 'stock_code', 'symbol']:
                if code_field in row and row[code_field]:
                    code = row[code_field]
                    break

            # 获取标题
            title = row.get('标题', row.get('节假日名称', row.get('title', '')))

            # 获取描述
            description = row.get('描述', row.get('description', ''))

            # 获取类型
            data_type = row.get('类型', row.get('type', self.data_type))

            # 构建额外信息（标准JSON格式）
            exclude_fields = ['日期', 'start', '股票代码', 'stock_cd', 'code', 'stock_code', 'symbol',
                              '标题', '节假日名称', 'title',
                              '描述', 'description',
                              '类型', 'type']
            extra_fields = {k: v for k, v in row.items() if k not in exclude_fields}
            # 将值转换为可JSON序列化的格式
            extra_fields = self._make_json_serializable(extra_fields)
            extra_info = json.dumps(extra_fields, ensure_ascii=False) if extra_fields else ''

            data_list.append((
                date_str,
                str(code) if code else '',
                str(title) if title else '',
                str(description) if description else '',
                str(data_type) if data_type else '',
                extra_info[:2000],  # 限制长度
                now
            ))

        return data_list

    @classmethod
    def _make_json_serializable(cls, data: dict) -> dict:
        """将字典中的值转换为JSON可序列化的格式"""
        result = {}
        for k, v in data.items():
            if pd.isna(v):
                result[k] = None
            elif hasattr(v, 'strftime'):
                # 日期时间类型转为字符串
                result[k] = v.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(v, (int, float, str, bool, type(None))):
                result[k] = v
            else:
                result[k] = str(v)
        return result

    def _save_to_db(self, data_list: List[tuple]):
        """保存数据到数据库"""
        conn = self._get_read_conn()

        # 创建表
        for sql in self.get_create_table_sql_list():
            conn.execute(sql)

        # 插入新数据（INSERT OR REPLACE 自动处理重复）
        sql = self.get_insert_sql()
        conn.executemany(sql, data_list)
        conn.commit()

    def get_create_table_sql_list(self) -> List[str]:
        """创建表SQL"""
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                date TEXT, -- 日期
                code TEXT, -- 股票代码（分红数据有，节假日为空）
                title TEXT, -- 标题/名称
                description TEXT, -- 描述
                type TEXT, -- 数据类型(dividend/holiday)
                extra_info TEXT, -- 额外信息(JSON格式)
                update_time TEXT DEFAULT (datetime('now', '+8 hours')), -- 更新时间
                PRIMARY KEY (date, code, title, type)
            )
        """
        return [ddl]

    def get_insert_sql(self) -> str:
        """插入SQL"""
        col_str = ','.join(self.cols)
        v_str = ','.join(['?'] * len(self.cols))
        return f"INSERT OR REPLACE INTO {self.table_name} ({col_str}) VALUES ({v_str})"

    def fetch_logic(self, code):
        """兼容基类接口，实际不使用"""
        pass

    def run(self, stock_list=None):
        """重写run方法，直接使用main逻辑"""
        self.main()


class DividendCalendarTask(CalendarTaskBase):
    """
    股票分红数据落库任务
    """

    data_type = 'dividend'

    def __init__(self, proxy_conf=None, start_date=None, end_date=None):
        self.start_date = start_date
        self.end_date = end_date
        super().__init__(proxy_conf=proxy_conf)
        self.subclass_name = self.__class__.__name__
        self.log_prefix = f"{self.subclass_name}"

    def fetch_data(self):
        """获取股票分红数据，过滤掉title包含XD的行"""
        df = get_stock_dividend_data(start_date=self.start_date, end_date=self.end_date)
        if df is not None and not df.empty:
            # 过滤掉标题包含XD的行
            title_col = None
            for col in ['标题', 'title']:
                if col in df.columns:
                    title_col = col
                    break
            if title_col:
                before_count = len(df)
                df = df[~df[title_col].astype(str).str.contains('XD', na=False)]
                after_count = len(df)
                if before_count != after_count:
                    self.logger.info(f"过滤掉 {before_count - after_count} 条包含XD的记录")
        return df


class HolidayCalendarTask(CalendarTaskBase):
    """
    节假日数据落库任务
    """

    data_type = 'holiday'

    def __init__(self, proxy_conf=None, start_date=None, end_date=None):
        self.start_date = start_date
        self.end_date = end_date
        super().__init__(proxy_conf=proxy_conf)
        self.subclass_name = self.__class__.__name__
        self.log_prefix = f"{self.subclass_name}"

    def fetch_data(self):
        """获取节假日数据"""
        return get_holiday_calendar_data(start_date=self.start_date, end_date=self.end_date)


class CalendarTaskRunner:
    """
    日历数据任务运行器
    统一执行分红和节假日数据落库
    """

    @classmethod
    def run_all(cls, proxy_conf=None):
        """执行所有日历数据任务"""
        # 获取当年数据范围
        now = datetime.datetime.now()
        start_date = f"{now.year}-01-01"
        end_date = f"{now.year + 1}-12-31"

        print(f"=" * 60)
        print(f"开始执行日历数据落库任务")
        print(f"数据范围: {start_date} ~ {end_date}")
        print(f"=" * 60)

        # 股票分红数据
        print("\n>>> 获取股票分红数据...")
        dividend_task = DividendCalendarTask(proxy_conf=proxy_conf, start_date=start_date, end_date=end_date)
        dividend_task.main()

        # 节假日数据
        print("\n>>> 获取节假日数据...")
        holiday_task = HolidayCalendarTask(proxy_conf=proxy_conf, start_date=start_date, end_date=end_date)
        holiday_task.main()

        print("\n" + "=" * 60)
        print("日历数据落库任务完成")
        print("=" * 60)


if __name__ == '__main__':
    # 单独执行分红数据任务
    # task = DividendCalendarTask()
    # task.main()

    # 单独执行节假日数据任务
    # task = HolidayCalendarTask()
    # task.main()

    # 执行所有日历任务
    CalendarTaskRunner.run_all()
