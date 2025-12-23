import logging
import os
import sqlite3
import sys
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Any

import dotenv

sys.path.append('..')
sys.path.append('../..')
dotenv.load_dotenv()
env = os.getenv("env")


class DBType(Enum):
    """
    数据库类型枚举，用于标识不同类型的数据库并计算其名称。
    每个成员的值是一个元组：(基础名称格式, 日期格式)
    """
    ETF_PCF_DB = ('etf_pcf_%s', '%Y%m')  # 例如：etf_pcf_db_202512

    # demo
    LOG_DB = ('log_archive_%s', '%Y%m%d')  # 例如：log_archive_202512
    CONFIG_DB = ('config_master', None)  # 例如：config_master (无日期后缀)
    REPORT_DB = ('report_%s', '%Y')  # 例如：report_2025

    def get_db_name(self, target_date=None):
        """
        根据枚举类型和传入的日期，计算并返回最终的数据库名称。

        Args:
            target_date (datetime, optional): 目标日期。默认为None，表示使用当前日期。

        Returns:
            str: 计算出的数据库名称。
        """
        base_format, date_format = self.value
        # 如果未提供日期，则使用当前日期
        if target_date is None or target_date == '':
            if not date_format:
                return base_format
            else:
                raise Exception(f"DBType {self.value} 传入日期为空.")

        elif isinstance(target_date, str):
            # 尝试解析 YYYY-MM-DD 格式
            if len(target_date) == 10 and target_date[4] == '-' and target_date[7] == '-':
                target_date = datetime.strptime(target_date, '%Y-%m-%d')
            # 尝试解析 YYYYMMDD 格式
            elif len(target_date) == 8 and target_date.isdigit():
                target_date = datetime.strptime(target_date, '%Y%m%d')

        if not hasattr(target_date, 'strftime'):
            # 如果target_date既不是None也不是字符串，也没有strftime方法，报错
            raise TypeError(f"DBType target_date:{target_date} type={type(target_date)} 必须是datetime对象、日期字符串或None")

        # 如果该DB类型不需要日期后缀（date_format为None），则直接返回基础名称
        if date_format is None:
            return base_format

        # 将日期格式化为字符串，并插入到基础名称格式中
        date_str = target_date.strftime(date_format)
        return base_format % date_str


# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SqliteHelper:
    """
    简化版 SQLite 数据库辅助类

    功能：
    1. 环境判断
    2. 按环境名创建文件夹
    3. 根据DB类型+日期获取DB名称
    4. 只保留一个执行 SQL 的函数
    """

    def __init__(self,
                 db_type: DBType,
                 trade_date: str):
        """
        初始化数据库辅助类

        Args:
            db_type: 数据库类型，使用 SqliteHelper.TYPE1 或 SqliteHelper.TYPE2
            trade_date: 日期 yyyy-MM-dd
        """
        self.db_type = db_type
        self.env = os.getenv('env')
        assert self.env is not None

        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.db_dir = os.path.join(self.base_path, self.env)
        logger.info(f'初始化DB文件夹: {self.db_dir}')
        os.makedirs(self.db_dir, exist_ok=True)

        self.db_name = db_type.get_db_name(target_date=trade_date) + '.db'
        self.db_path = os.path.join(self.db_dir, self.db_name)
        logger.info(f'DB名称: {self.db_name} 类初始化完成(类型:{self.db_type}, 日期:{trade_date}). 路径: {self.db_path}.')

    def get_connection(self) -> sqlite3.Connection:
        """
        获取数据库连接

        Returns:
            sqlite3.Connection: SQLite 数据库连接对象
        """
        return sqlite3.connect(self.db_path)

    def execute(self, sql: str, parameters: Any = None) -> Any:
        """
        执行 SQL 语句（唯一保留的执行函数）

        功能：
        1. 自动判断是否为查询语句
        2. 支持单条和批量执行
        3. 自动提交事务

        Args:
            sql: SQL 语句
            parameters: 参数，可以是 None、元组、列表或列表的列表

        Returns:
            如果是查询语句，返回结果列表
            如果是修改语句，返回影响的行数
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 处理不同类型的参数
            if parameters is None:
                cursor.execute(sql)
            elif isinstance(parameters, list) and parameters and isinstance(parameters[0], (list, tuple)):
                # 批量执行
                cursor.executemany(sql, parameters)
            else:
                # 单条执行
                cursor.execute(sql, parameters)

            conn.commit()

            # 判断是否为查询语句
            if cursor.description is not None:
                return cursor.fetchall()
            else:
                return cursor.rowcount

    def get_table_info(self, table_name: str) -> List[Tuple]:
        """
        获取表结构信息（保留，因为实用）

        Args:
            table_name: 表名

        Returns:
            表结构信息列表
        """
        return self.execute(f"PRAGMA table_info({table_name})")

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在（保留，因为实用）

        Args:
            table_name: 表名

        Returns:
            bool: 表是否存在
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = self.execute(sql, (table_name,))
        return len(result) > 0


def test_case_win():
    # 1. 测试初始化
    print("1. 测试初始化...")
    helper = SqliteHelper(DBType.LOG_DB, '2024-01-15')
    print(f"   数据库路径: {helper.db_path}")
    print(f"   数据库名称: {helper.db_name}")
    print("   ✓ 初始化成功")

    # 2. 测试表创建和基础操作
    print("2. 测试基础SQL操作...")

    # 创建测试表
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        value REAL,
        created_time DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """
    result = helper.execute(create_table_sql)
    print("   ✓ 表创建成功")

    # 插入测试数据
    insert_sql = "INSERT INTO test_table (name, value) VALUES (?, ?)"
    test_data = [('测试数据1', 100.5), ('测试数据2', 200.3)]

    rows_affected = helper.execute(insert_sql, test_data)
    print(f"   ✓ 插入数据成功，影响行数: {rows_affected}")

    # 查询数据
    select_sql = "SELECT * FROM test_table WHERE value > ?"
    results = helper.execute(select_sql, (150,))
    print(f"   ✓ 查询成功，返回 {len(results)} 条记录")

    # 3. 测试表存在性检查
    print("3. 测试工具方法...")
    table_exists = helper.table_exists('test_table')
    print(f"   表存在性检查: {table_exists}")

    # 4. 测试表结构信息
    table_info = helper.get_table_info('test_table')
    print(f"   表结构信息: {len(table_info)} 个字段")

    # 5. 测试不同DBType
    print("4. 测试不同数据库类型...")
    for db_type in [DBType.ETF_PCF_DB, DBType.LOG_DB, DBType.CONFIG_DB]:
        test_helper = SqliteHelper(db_type, '2024-01-15')
        print(f"   {db_type.name}: {test_helper.db_name}")
        test_helper.get_connection().close()

    print("\n🎉 所有基础功能测试通过！")
    return True


if __name__ == '__main__':
    test_case_win()

