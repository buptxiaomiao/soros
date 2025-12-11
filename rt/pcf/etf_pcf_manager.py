import sqlite3
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json

# 导入您现有的类
import pandas as pd

from shang_etf_pcf_manager import ShangETFPcfManager
from shen_raw import ShenETFPcfParser

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class UnifiedETFPcfManager:
    """
    统一的ETF PCF数据管理器
    支持上证和深证ETF，自动判断市场并选择相应的处理器
    """

    def __init__(self, db_path: str = "etf_pcf_data.db"):
        """
        初始化统一管理器

        Args:
            db_path: SQLite数据库文件路径
        """
        self.db_path = db_path
        self.shang_manager = ShangETFPcfManager()
        self.shen_parser = ShenETFPcfParser()

        # 初始化数据库
        self._init_database()

    def _init_database(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # 创建上证ETF PCF数据表
        cursor.execute('''
            -- 创建ETF PCF基本信息表
            CREATE TABLE IF NOT EXISTS etf_pcf_basic_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,                      -- 基金代码
                trade_date TEXT NOT NULL,                    -- 公告日期
                data_source TEXT NOT NULL,                   -- 数据源标识 (如: 'SZ', 'SH')
                fund_name TEXT,                              -- 基金名称
                fund_company TEXT,                           -- 基金管理公司
                target_index_code TEXT,                      -- 目标指数代码
                fund_type TEXT,                              -- 基金类型
                nav_per_unit REAL,                           -- 基金份额净值
                nav_per_min_unit REAL,                       -- 最小申购、赎回单位资产净值
                estimated_cash REAL,                         -- 预估现金差额
                min_sub_redeem_unit INTEGER,                 -- 最小申购、赎回单位(份)
                cash_substitute_ratio REAL,                  -- 现金替代比例上限
                publish_iopv INTEGER,                        -- 是否需要公布IOPV (1:是, 0:否)
                allow_sub_redeem TEXT,                       -- 申购赎回允许情况
                max_total_sub INTEGER,                       -- 当天累计可申购的基金份额上限
                max_total_redeem INTEGER,                    -- 当天累计可赎回的基金份额上限
                create_time TIMESTAMP DEFAULT (datetime('now','localtime')), -- 记录创建时间
                UNIQUE (fund_code, trade_date)
            )
        ''')
        # 创建索引（在表创建后单独执行）
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_basic_date_fund ON etf_pcf_basic_info(trade_date, fund_code)')

        # 创建深证ETF原始数据表
        cursor.execute('''
            -- 创建ETF PCF成分股信息表
            CREATE TABLE IF NOT EXISTS etf_pcf_component_info (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,                     -- 基金代码
                trade_date TEXT NOT NULL,                    -- 公告日期
                data_source TEXT NOT NULL,                   -- 数据源标识 (如: 'SZ', 'SH')
                stock_code TEXT NOT NULL,                    -- 证券代码
                stock_name TEXT,                             -- 证券简称
                quantity INTEGER,                            -- 股份数量
                cash_sub_flag TEXT,                          -- 现金替代标志 
                 /* 允许：可选择交付实物股票或现金，结算价按申购/赎回日市价浮动计算。允许情况下，sub_cash_amount 19601元为预估值
                    必须：强制使用现金结算数量
                        =0股：成分股因停牌、流通限制等被临时移出实物篮子，投资者无需操作。
                        数量>0股（如德赛西威）：成分股无法实物交割（如长期停牌），需按固定金额现金替代,"必须"模式下，非0金额（如11057元）为固定支付额。
                    禁止现金替代 必须实物交割，无现金选项。 
                        数量>0股,流动性良好
                  */
                sub_premium_ratio REAL,                      -- 申购现金替代溢价比例/保证金率 
                 /* 10%：可选交1700股或现金（按申购日收盘价×110%）
                    高溢价比例（如73%）通常因成分股流动性差或停牌风险，补偿管理人操作成本。
                 */
                redeem_discount_ratio REAL,                  -- 赎回现金替代折价比例/保证金率 10%：通常收1700股，特殊情况下收现金（按赎回日收盘价×90%）
                sub_cash_amount REAL,                        -- 申购替代金额 19601元为预估值，实际按市价浮动
                redeem_cash_amount REAL,                     -- 赎回替代金额 --上证etf没有此列
                market TEXT,                                 -- 挂牌市场  
                mapping_code TEXT,                           -- 映射代码
                create_time TIMESTAMP DEFAULT (datetime('now','localtime')), -- 记录创建时间
                UNIQUE (fund_code, trade_date, stock_code)
                
            )
        ''')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_component_date_fund '
                       'ON etf_pcf_component_info(trade_date, fund_code)')

        conn.commit()
        conn.close()
        logger.info(f"数据库初始化完成: {self.db_path}")

    def _get_market_type(self, etf_code: str) -> str:
        """
        根据ETF代码判断市场类型

        Args:
            etf_code: ETF代码

        Returns:
            'shang' - 上证 | 'shen' - 深证
        """
        # 上证ETF代码通常以5开头，深证以1开头
        if etf_code.startswith('5'):
            return '上证'
        elif etf_code.startswith('1'):
            return '深证'
        else:
            # 如果无法判断，默认使用上证
            logger.warning(f"无法判断ETF代码{etf_code}的市场类型，默认使用上证")
            return 'shang'

    def check_db_exist(self, etf_code: str, date_str: str):
        try:
            data = self.query_etf_data(etf_code, date_str)
            return data.get('basic_info', None) and data.get('components', None)
        except Exception as e:
            logger.error(e, exc_info=True)
        return False

    def get_etf_pcf_data(self, etf_code: str, date_str: str, save_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        获取ETF PCF数据的主要接口

        Args:
            etf_code: ETF代码
            date_str: 日期字符串(YYYYMMDD)
            save_dir: 文件保存目录

        Returns:
            包含数据和操作结果的字典
        """
        market_type = self._get_market_type(etf_code)

        if self.check_db_exist(etf_code, date_str):
            logger.info(f'etf_code:{etf_code}, date_str:{date_str} exist in db.')
            return {
                "success": True,
                "etf_code": etf_code,
                "date": date_str,
                "market": market_type,
                "msg": 'db_exists.'
            }

        try:
            if market_type == '上证':
                return self._get_shang_pcf_data(etf_code, date_str, save_dir)
            else:
                return self._get_shen_pcf_data(etf_code, date_str, save_dir)
        except Exception as e:
            error_msg = f"获取ETF {etf_code} 数据失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            # raise e
            return {
                "success": False,
                "error": error_msg,
                "etf_code": etf_code,
                "date": date_str,
                "market": market_type
            }

    def _get_shang_pcf_data(self, etf_code: str, date_str: str, save_dir: Optional[str] = None) -> Dict[str, Any]:
        """获取上证ETF PCF数据"""
        # 下载并解析数据
        result = self.shang_manager.get_etf_pcf_data(etf_code, date_str, save_dir)

        print(f"_get_shang_pcf_data.success={result['success']}")
        if not result['success']:
            return {
                "success": False,
                "error": "获取数据失败",
                "etf_code": etf_code,
                "date": date_str,
                "market": "shang"
            }

        # 存储到数据库
        storage_result = self._store_shang_data_to_db(result, etf_code, date_str)

        return {
            "success": True,
            "etf_code": etf_code,
            "date": date_str,
            "market": "shang",
            "data": result,
            "storage_result": storage_result
        }

    def _get_shen_pcf_data(self, etf_code: str, date_str: str, save_dir: Optional[str] = None) -> Dict[str, Any]:
        """获取深证ETF PCF数据"""
        # 构建保存路径
        if save_dir:
            save_path = f"{save_dir}/pcf_{etf_code}_{date_str}.xml"
        else:
            save_path = f"shen_pcf/pcf_{etf_code}_{date_str}.xml"

        # 下载并解析数据
        result = self.shen_parser.get_etf_pcf_data(etf_code, date_str, save_path)

        if not result.get("success", False):
            return {
                "success": False,
                "error": result.get("error", "获取数据失败"),
                "etf_code": etf_code,
                "date": date_str,
                "market": "shen"
            }

        # 存储到数据库
        storage_result = self._store_shen_data_to_db(result, etf_code, date_str)

        return {
            "success": True,
            "etf_code": etf_code,
            "date": date_str,
            "market": "shen",
            "data": result.get("data", []),
            "storage_result": storage_result
        }

    def _store_shang_data_to_db(self, data: Dict, etf_code: str, date_str: str) -> Dict[str, Any]:
        """
        存储上证ETF数据到数据库

        Args:
            data: 上证ETF数据
            etf_code: ETF代码
            date_str: 日期字符串

        Returns:
            存储结果
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            basic_info = data.get('basic_info', {})
            components = data.get('data', [])
            print(basic_info)
            for i in components[:5]:
                print(i)

            # 存储基本信息
            cursor.execute('''
                INSERT OR REPLACE INTO etf_pcf_basic_info (
                    fund_code, trade_date, data_source, fund_name, fund_company,
                    target_index_code, fund_type, nav_per_unit, nav_per_min_unit,
                    estimated_cash, min_sub_redeem_unit, cash_substitute_ratio,
                    publish_iopv, allow_sub_redeem, max_total_sub, max_total_redeem
                ) VALUES (?, ?, 'SH', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                etf_code,
                date_str,
                f"上证ETF{etf_code}",  # 基金名称
                "未知基金公司",  # 需要从数据中获取
                "未知指数",  # 目标指数代码
                "ETF",  # 基金类型
                basic_info.get('nav'),  # 基金份额净值
                basic_info.get('nav_per_cu'),  # 最小申购赎回单位资产净值
                basic_info.get('estimated_cash_component'),  # 预估现金差额
                basic_info.get('creation_redemption_unit'),  # 最小申购赎回单位
                basic_info.get('max_cash_ratio'),  # 现金替代比例上限
                basic_info.get('publish_iopv_flag'),  # 公布IOPV标志
                "允许",  # 申购赎回允许情况
                basic_info.get('redemption_limit'),  # 最大申购份额
                basic_info.get('redemption_limit')  # 最大赎回份额
            ))

            # 存储成分股信息
            for component in components:
                cursor.execute('''
                    INSERT OR REPLACE INTO etf_pcf_component_info (
                        fund_code, trade_date, data_source, stock_code, stock_name,
                        quantity, cash_sub_flag, sub_premium_ratio, redeem_discount_ratio,
                        sub_cash_amount, redeem_cash_amount, market, mapping_code
                    ) VALUES (?, ?, 'SH', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    etf_code,
                    date_str,
                    component.get('instrument_id'),  # 证券代码
                    component.get('instrument_name'),  # 证券简称
                    component.get('quantity'),  # 股份数量
                    self._get_shang_cash_sub_flag(component),  # 现金替代标志
                    component.get('creation_premium_rate'),  # 申购溢价比例
                    component.get('redemption_discount_rate'),  # 赎回折价比例
                    component.get('substitution_cash_amount'),  # 申购替代金额
                    0,  # 赎回替代金额（上证没有此字段）
                    "SH",  # 挂牌市场
                    component.get('underlying_security_id')  # 映射代码
                ))

            conn.commit()
            logger.info(f"成功存储上证ETF {etf_code} 数据，成分股数量: {len(components)}")

            return {
                "success": True,
                "basic_info_rows": 1,
                "component_rows": len(components)
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"存储上证ETF数据失败: {str(e)}")
            raise e
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            conn.close()

    def _get_shang_cash_sub_flag(self, component: Dict) -> str:
        """根据上证ETF成分股数据判断现金替代标志"""
        try:
            sub_flag = component.get('substitution_flag')
            cash_amount = component.get('substitution_cash_amount')

            # 安全处理None值
            if sub_flag is None:
                sub_flag = 0
            if cash_amount is None:
                cash_amount = 0

            # 转换为适当的类型
            try:
                sub_flag = int(sub_flag)
            except (ValueError, TypeError):
                print(f'sub_flag={sub_flag}, error.')
                sub_flag = 0

            try:
                cash_amount = float(cash_amount)
            except (ValueError, TypeError):
                cash_amount = 0

            if sub_flag == 0:
                return "禁止现金替代"
            elif sub_flag == 1 and cash_amount == 0:
                return "允许现金替代"
            elif sub_flag == 1 and cash_amount > 0:
                return "必须现金替代"
            else:
                return "未知"
        except Exception as e:
            logger.error(f"判断现金替代标志时出错: {e}")
            return "未知"

    def _store_shen_data_to_db(self, data: Dict, etf_code: str, date_str: str) -> Dict[str, Any]:
        """
        存储深证ETF数据到数据库

        Args:
            data: 深证ETF数据
            etf_code: ETF代码
            date_str: 日期字符串

        Returns:
            存储结果
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            pcf_data = data.get('data', {})
            fund_info = pcf_data.get('fund_info', {})
            components = pcf_data.get('components', [])

            # 存储基本信息
            cursor.execute('''
                INSERT OR REPLACE INTO etf_pcf_basic_info (
                    fund_code, trade_date, data_source, fund_name, fund_company,
                    target_index_code, fund_type, nav_per_unit, nav_per_min_unit,
                    estimated_cash, min_sub_redeem_unit, cash_substitute_ratio,
                    publish_iopv, allow_sub_redeem, max_total_sub, max_total_redeem
                ) VALUES (?, ?, 'SZ', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                etf_code,
                date_str,
                fund_info.get('symbol', f"深证ETF{etf_code}"),  # 基金名称
                fund_info.get('fund_management_company', '未知基金公司'),  # 基金管理公司
                "未知指数",  # 目标指数代码
                "ETF",  # 基金类型
                fund_info.get('nav'),  # 基金份额净值
                fund_info.get('nav_per_cu'),  # 最小申购赎回单位资产净值
                fund_info.get('estimate_cash_component'),  # 预估现金差额
                fund_info.get('creation_redemption_unit'),  # 最小申购赎回单位
                fund_info.get('max_cash_ratio'),  # 现金替代比例上限
                1,  # 公布IOPV标志（深证默认公布）
                "允许",  # 申购赎回允许情况
                fund_info.get('creation_limit'),  # 最大申购份额
                fund_info.get('redemption_limit')  # 最大赎回份额
            ))

            # 存储成分股信息
            for component in components:
                cursor.execute('''
                    INSERT OR REPLACE INTO etf_pcf_component_info (
                        fund_code, trade_date, data_source, stock_code, stock_name,
                        quantity, cash_sub_flag, sub_premium_ratio, redeem_discount_ratio,
                        sub_cash_amount, redeem_cash_amount, market, mapping_code
                    ) VALUES (?, ?, 'SZ', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    etf_code,
                    date_str,
                    component.get('underlying_security_id'),  # 证券代码
                    component.get('underlying_symbol'),  # 证券简称
                    component.get('component_share'),  # 股份数量
                    self._get_shen_cash_sub_flag(component),  # 现金替代标志
                    component.get('premium_ratio'),  # 申购溢价比例
                    component.get('discount_ratio'),  # 赎回折价比例
                    component.get('creation_cash_substitute'),  # 申购替代金额
                    component.get('redemption_cash_substitute'),  # 赎回替代金额
                    "SZ",  # 挂牌市场
                    component.get('underlying_security_id')  # 映射代码
                ))

            conn.commit()
            logger.info(f"成功存储深证ETF {etf_code} 数据，成分股数量: {len(components)}")

            return {
                "success": True,
                "basic_info_rows": 1,
                "component_rows": len(components)
            }

        except Exception as e:
            conn.rollback()
            logger.error(f"存储深证ETF数据失败: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            conn.close()

    def _get_shen_cash_sub_flag(self, component: Dict) -> str:
        """根据深证ETF成分股数据判断现金替代标志"""
        try:
            sub_flag = component.get('substitute_flag', '')
            cash_amount = component.get('creation_cash_substitute')

            # 安全处理None值
            if cash_amount is None:
                cash_amount = 0

            # 转换为适当的类型
            try:
                cash_amount = float(cash_amount)
            except (ValueError, TypeError):
                cash_amount = 0

            if sub_flag == '0':
                return "禁止现金替代"
            elif sub_flag == '1' and cash_amount == 0:
                return "允许现金替代"
            elif sub_flag == '1' and cash_amount > 0:
                return "必须现金替代"
            else:
                return "未知"
        except Exception as e:
            logger.error(f"判断现金替代标志时出错: {e}")
            return "未知"

    def batch_update_etf_data(self, etf_codes: List[str], start_date: str, end_date: str,
                              save_dir: Optional[str] = None) -> Dict[str, Any]:
        """
        批量更新ETF数据

        Args:
            etf_codes: ETF代码列表
            start_date: 开始日期(YYYYMMDD)
            end_date: 结束日期(YYYYMMDD)
            save_dir: 文件保存目录

        Returns:
            批量更新结果统计
        """
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')

        results = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "failed_details": [],
            "start_date": start_date,
            "end_date": end_date,
            "etf_codes": etf_codes
        }

        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y%m%d')

            for etf_code in etf_codes:
                results["total_requests"] += 1

                try:
                    logger.info(f"处理ETF {etf_code} 在 {date_str} 的数据")
                    result = self.get_etf_pcf_data(etf_code, date_str, save_dir)
                    time.sleep(0.1)

                    if result.get("success", False):
                        results["successful_requests"] += 1
                    else:
                        results["failed_requests"] += 1
                        results["failed_details"].append({
                            "etf_code": etf_code,
                            "date": date_str,
                            "error": result.get("error", "未知错误")
                        })

                except Exception as e:
                    results["failed_requests"] += 1
                    results["failed_details"].append({
                        "etf_code": etf_code,
                        "date": date_str,
                        "error": str(e)
                    })

            current_date += timedelta(days=1)

        logger.info(f"批量更新完成: 成功{results['successful_requests']}/失败{results['failed_requests']}")
        return results

    def query_etf_data(self, etf_code: str, date_str: str = None) -> Dict[str, Any]:
        """
        查询ETF数据

        Args:
            etf_code: ETF代码
            date_str: 日期(YYYYMMDD)，如果为None则查询最新数据

        Returns:
            包含ETF基本信息和成分股的字典
        """
        conn = sqlite3.connect(self.db_path)

        try:
            if date_str:
                # 查询特定日期的数据
                basic_info_sql = '''
                    SELECT * FROM etf_pcf_basic_info 
                    WHERE fund_code = ? AND trade_date = ?
                '''
                components_sql = '''
                    SELECT * FROM etf_pcf_component_info 
                    WHERE fund_code = ? AND trade_date = ?
                    ORDER BY quantity DESC
                '''
                params = (etf_code, date_str)
            else:
                # 查询最新数据
                basic_info_sql = '''
                    SELECT * FROM etf_pcf_basic_info 
                    WHERE fund_code = ? 
                    ORDER BY trade_date DESC 
                    LIMIT 1
                '''
                components_sql = '''
                    SELECT c.* FROM etf_pcf_component_info c
                    JOIN etf_pcf_basic_info b ON c.fund_code = b.fund_code AND c.trade_date = b.trade_date
                    WHERE c.fund_code = ? 
                    ORDER BY b.trade_date DESC, c.quantity DESC
                '''
                params = (etf_code,)

            # 获取基本信息
            basic_info = conn.execute(basic_info_sql, params).fetchall()

            # 获取成分股信息
            components = conn.execute(components_sql, params).fetchall()

            return {
                "basic_info": basic_info,
                "components": components
            }

        finally:
            conn.close()

    def get_etf_list(self) -> List[str]:
        """获取数据库中所有的ETF代码列表"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT fund_code FROM etf_pcf_basic_info ORDER BY fund_code')
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_date_range(self, etf_code: str) -> tuple:
        """获取指定ETF的日期范围"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT MIN(trade_date), MAX(trade_date) 
                FROM etf_pcf_basic_info 
                WHERE fund_code = ?
            ''', (etf_code,))
            result = cursor.fetchone()
            return result
        finally:
            conn.close()


# 测试用例
class TestUnifiedETFPcfManager:
    """测试用例类"""

    @staticmethod
    def _print_database_data(manager, etf_code, date_str, market_type):
        """从数据库读取并打印ETF数据"""
        print(f"\n=== {market_type} ETF {etf_code} 数据库数据 ===")

        # 查询数据库
        data = manager.query_etf_data(etf_code, date_str)

        # 打印基本信息
        if data['basic_info']:
            print("\n--- 基本信息 ---")
            for row in data['basic_info']:
                # 正确处理数据库行对象
                row_dict = {}
                if hasattr(row, '_fields'):  # 如果是sqlite3.Row对象
                    for field in row._fields:
                        row_dict[field] = row[field]
                else:  # 如果是元组
                    # 获取列名
                    conn = sqlite3.connect(manager.db_path)
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA table_info(etf_pcf_basic_info)")
                    columns = [col[1] for col in cursor.fetchall()]
                    conn.close()

                    # 将元组转换为字典
                    for i, value in enumerate(row):
                        if i < len(columns):
                            row_dict[columns[i]] = value

                # 打印基本信息
                for key, value in row_dict.items():
                    if key not in ['id', 'create_time']:  # 跳过内部字段
                        print(f"{key}: {value}")
                print("-" * 30)
        else:
            print("未找到基本信息")

        # 打印成分股信息
        if data['components']:
            print(f"\n--- 成分股信息 (共{len(data['components'])}个) ---")

            # 处理成分股数据
            component_data = []
            for row in data['components']:
                row_dict = {}
                if hasattr(row, '_fields'):  # 如果是sqlite3.Row对象
                    for field in row._fields:
                        row_dict[field] = row[field]
                else:  # 如果是元组
                    # 获取列名
                    conn = sqlite3.connect(manager.db_path)
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA table_info(etf_pcf_component_info)")
                    columns = [col[1] for col in cursor.fetchall()]
                    conn.close()

                    # 将元组转换为字典
                    for i, value in enumerate(row):
                        if i < len(columns):
                            row_dict[columns[i]] = value

                component_data.append(row_dict)

            # 创建DataFrame以便更好地显示
            if component_data:
                df_components = pd.DataFrame(component_data)

                # 选择要显示的列
                display_columns = ['stock_code', 'stock_name', 'quantity', 'cash_sub_flag']
                if 'sub_premium_ratio' in df_components.columns:
                    display_columns.append('sub_premium_ratio')
                if 'redeem_discount_ratio' in df_components.columns:
                    display_columns.append('redeem_discount_ratio')
                if 'sub_cash_amount' in df_components.columns:
                    display_columns.append('sub_cash_amount')

                # 只显示存在的列
                display_columns = [col for col in display_columns if col in df_components.columns]

                if display_columns:
                    display_df = df_components[display_columns].head(20)  # 显示前20行
                    print(display_df.to_string(index=False))

                    if len(component_data) > 20:
                        print(f"... 还有 {len(component_data) - 20} 个成分股未显示")
                else:
                    print("无有效列可显示")
            else:
                print("无成分股数据")
        else:
            print("未找到成分股信息")

        return len(data['basic_info']) > 0 and len(data['components']) > 0

    @staticmethod
    def test_shang_etf():
        """测试上交所ETF数据获取和存储，从数据库读取并打印信息"""
        print("=== 测试上交所ETF ===")
        manager = UnifiedETFPcfManager("test_etf.db")

        # 测试上证50ETF
        etf_code = "510300"
        date_str = "20251205"

        # 先获取数据
        result = manager.get_etf_pcf_data(etf_code, date_str)
        print(f"数据获取结果: {result.get('success', False)}")

        if result.get("success"):
            print(f"数据存储结果: {result.get('storage_result', {}).get('success', False)}")

        # 从数据库读取并打印信息
        has_data = TestUnifiedETFPcfManager._print_database_data(manager, etf_code, date_str, "上证")

        return result.get("success", False) and has_data

    @staticmethod
    def test_shen_etf():
        """测试深交所ETF数据获取和存储，从数据库读取并打印信息"""
        print("\n=== 测试深交所ETF ===")
        manager = UnifiedETFPcfManager("test_etf.db")

        # 测试深证ETF
        etf_code = "159919"
        date_str = "20251205"

        # 先获取数据
        result = manager.get_etf_pcf_data(etf_code, date_str)
        print(f"数据获取结果: {result.get('success', False)}")

        if result.get("success"):
            print(f"数据存储结果: {result.get('storage_result', {}).get('success', False)}")

        # 从数据库读取并打印信息
        has_data = TestUnifiedETFPcfManager._print_database_data(manager, etf_code, date_str, "深证")

        return result.get("success", False) and has_data

    @staticmethod
    def test_batch_update():
        """测试批量更新，从数据库验证结果"""
        print("\n=== 测试批量更新 ===")
        manager = UnifiedETFPcfManager("test_etf.db")
        import easyquotation
        quotation = easyquotation.use('jsl')  # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
        # res = quotation.market_snapshot(prefix=True)
        res = quotation.etfindex(index_id="", min_volume=0, max_discount=None, min_discount=None)
        #
        etf_codes = res.keys()
        etf_codes = [i for i in etf_codes if i[0] == '5']
        # etf_codes = ["510010"]
        start_date = "20251211"
        end_date = "20251211"

        # 执行批量更新
        results = manager.batch_update_etf_data(etf_codes, start_date, end_date)

        print(f"批量更新结果:")
        print(f"总请求数: {results['total_requests']}")
        print(f"成功请求: {results['successful_requests']}")
        print(f"失败请求: {results['failed_requests']}")


if __name__ == '__main__':
    TestUnifiedETFPcfManager.test_batch_update()
## {'instrument_id': '600018', 'instrument_name': '上港集团', 'quantity': 500, 'substitution_flag': 1, 'creation_premium_rate': 0.34, 'redemption_discount_rate': 0.0, 'substitution_cash_amount': None, 'underlying_security_id': '101'}
