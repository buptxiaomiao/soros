import xml.etree.ElementTree as ET
import pandas as pd
from typing import Dict, List, Optional, Any


class ShangETFPcfParser:
    """
    上证ETF投资组合文件(PCF)解析器
    """

    def __init__(self, xml_content: str = None, xml_file_path: str = None):
        """
        初始化解析器

        Args:
            xml_content: XML字符串内容
            xml_file_path: XML文件路径
        """
        if xml_content:
            self.root = ET.fromstring(xml_content)
        elif xml_file_path:
            tree = ET.parse(xml_file_path)
            self.root = tree.getroot()
        else:
            raise ValueError("必须提供xml_content或xml_file_path参数")

        self._parse_basic_info()
        self._parse_components()

    def _parse_basic_info(self):
        """解析基本信息"""
        self.basic_info = {
            'fund_instrument_id': self.root.findtext('FundInstrumentID'),
            'creation_redemption_unit': int(self.root.findtext('CreationRedemptionUnit')),
            'trading_day': self.root.findtext('TradingDay'),
            'pre_trading_day': self.root.findtext('PreTradingDay'),
            'nav_per_cu': float(self.root.findtext('NAVperCU')),
            'nav': float(self.root.findtext('NAV')),
            'pre_cash_component': float(self.root.findtext('PreCashComponent')),
            'estimated_cash_component': float(self.root.findtext('EstimatedCashComponent')),
            'max_cash_ratio': float(self.root.findtext('MaxCashRatio')),
            'redemption_limit': int(self.root.findtext('RedemptionLimit')),
            'publish_iopv_flag': int(self.root.findtext('PublishIOPVFlag')),
            'creation_redemption_switch': int(self.root.findtext('CreationRedemptionSwitch')),
            'creation_redemption_mechanism': int(self.root.findtext('CreationRedemptionMechanism')),
            'record_number': int(self.root.findtext('RecordNumber'))
        }

    def _parse_components(self):
        """解析成分股列表"""
        self.components = []
        component_list = self.root.find('ComponentList')

        if component_list is not None:
            for component in component_list.findall('Component'):
                component_data = {
                    'instrument_id': component.findtext('InstrumentID'),
                    'instrument_name': component.findtext('InstrumentName'),
                    'quantity': self._safe_int(component.findtext('Quantity')),
                    'substitution_flag': self._safe_int(component.findtext('SubstitutionFlag')),
                    'creation_premium_rate': self._safe_float(component.findtext('CreationPremiumRate')),
                    'redemption_discount_rate': self._safe_float(component.findtext('RedemptionDiscountRate')),
                    'substitution_cash_amount': self._safe_float(component.findtext('SubstitutionCashAmount')),
                    'underlying_security_id': component.findtext('UnderlyingSecurityID')
                }
                self.components.append(component_data)

    def _safe_int(self, value: Optional[str]) -> Optional[int]:
        """安全转换为整数"""
        if value is None or value == '':
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value: Optional[str]) -> Optional[float]:
        """安全转换为浮点数"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def get_basic_info(self) -> Dict[str, Any]:
        """
        获取基本信息

        Returns:
            包含基本信息的字典
        """
        return self.basic_info.copy()

    def get_components(self) -> List[Dict[str, Any]]:
        """
        获取成分股列表

        Returns:
            成分股字典列表
        """
        return self.components.copy()

    def get_components_dataframe(self) -> pd.DataFrame:
        """
        获取成分股的DataFrame

        Returns:
            pandas DataFrame包含所有成分股信息
        """
        return pd.DataFrame(self.components)

    def get_component_by_id(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        """
        根据证券代码获取特定成分股信息

        Args:
            instrument_id: 证券代码

        Returns:
            成分股信息字典，如果未找到返回None
        """
        for component in self.components:
            if component['instrument_id'] == instrument_id:
                return component.copy()
        return None

    def get_active_components(self) -> List[Dict[str, Any]]:
        """
        获取活跃成分股（数量大于0的）

        Returns:
            活跃成分股列表
        """
        return [comp for comp in self.components if comp.get('quantity', 0) > 0]

    def get_substitution_components(self) -> List[Dict[str, Any]]:
        """
        获取可替代成分股（SubstitutionFlag为1的）

        Returns:
            可替代成分股列表
        """
        return [comp for comp in self.components if comp.get('substitution_flag') == 1]

    def get_component_summary(self) -> Dict[str, Any]:
        """
        获取成分股统计摘要

        Returns:
            统计摘要字典
        """
        active_components = self.get_active_components()
        substitution_components = self.get_substitution_components()

        return {
            'total_components': len(self.components),
            'active_components': len(active_components),
            'substitution_components': len(substitution_components),
            'total_quantity': sum(comp.get('quantity', 0) for comp in active_components),
            'market_distribution': self._get_market_distribution()
        }

    def _get_market_distribution(self) -> Dict[str, int]:
        """
        获取市场分布（根据UnderlyingSecurityID）

        Returns:
            市场分布字典
        """
        distribution = {}
        for component in self.get_active_components():
            market_id = component.get('underlying_security_id', 'Unknown')
            distribution[market_id] = distribution.get(market_id, 0) + 1
        return distribution

    def print_summary(self):
        """打印摘要信息"""
        basic_info = self.get_basic_info()
        summary = self.get_component_summary()

        print("=" * 60)
        print("ETF投资组合文件(PCF)解析结果")
        print("=" * 60)
        print(f"基金代码: {basic_info['fund_instrument_id']}")
        print(f"交易日期: {basic_info['trading_day']}")
        print(f"单位净值: {basic_info['nav']:.4f}")
        print(f"每组合单位资产净值: {basic_info['nav_per_cu']:,.2f}")
        print(f"现金替代比例上限: {basic_info['max_cash_ratio']:.1%}")
        print(f"成分股总数: {summary['total_components']}")
        print(f"活跃成分股: {summary['active_components']}")
        print(f"可替代成分股: {summary['substitution_components']}")
        print(f"市场分布: {summary['market_distribution']}")
        print("=" * 60)


# 使用示例
if __name__ == "__main__":
    # 使用方法1: 直接传入XML字符串
    with open('pcf_510330_20251205.xml', 'r', encoding='utf-8') as f:
        xml_content = f.read()

    # 使用方法2: 传入文件路径
    # parser = ShangETFPcfParser(xml_file_path='your_file.xml')

    parser = ShangETFPcfParser(xml_content=xml_content)

    # 打印摘要信息
    parser.print_summary()

    # 获取基本信息
    basic_info = parser.get_basic_info()
    print("\n基本信息:")
    for key, value in basic_info.items():
        print(f"{key}: {value}")

    # 获取成分股DataFrame
    df = parser.get_components_dataframe()
    print(f"\n成分股DataFrame形状: {df.shape}")
    print("\n前5个成分股:")
    print(df.columns)
    print(df.head(5))

    for i in parser.components[:300]:
        print(i)

    # 获取特定股票信息
    stock_info = parser.get_component_by_id('600519')  # 贵州茅台
    if stock_info:
        print(f"\n贵州茅台信息: {stock_info}")

    # 获取统计信息
    summary = parser.get_component_summary()
    print(f"\n统计摘要: {summary}")