import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime

import chardet
import pandas as pd
import requests

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ShenETFPcfParser:
    """
    https://reportdocs.static.szse.cn/files/text/etf/ETF15991920251208.txt?random=0.08979976806617573
    https://www.szse.cn/disclosure/fund/currency/index.html
    深证-ETF PCF文件下载和解析器（修复证券名称乱码问题）
    """

    def __init__(self):
        self.base_url = "https://reportdocs.static.szse.cn/files/text/ETFDown/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/xml, text/xml, */*; q=0.01'
        })

    def build_pcf_url(self, etf_code, date_str):
        """构建PCF文件下载URL
        https://reportdocs.static.szse.cn/files/text/ETFDown/pcf_159562_20251205.xml
        """
        filename = f"pcf_{etf_code}_{date_str}.xml"
        url = f"{self.base_url}{filename}"
        return url

    def detect_encoding(self, file_path):
        """检测文件编码"""
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            logger.info(f"检测到文件编码: {encoding} (置信度: {confidence:.2f})")
            return encoding

    def fix_encoding(self, text):
        """修复乱码文本"""
        if not text or not isinstance(text, str):
            return text

        # 常见的乱码情况处理
        # 情况1: UTF-8编码被错误地以其他编码解码
        try:
            # 尝试将文本重新编码为字节，然后用正确编码解码
            # 先尝试常见的编码错误情况
            if 'é' in text or '' in text or '' in text:
                # 可能是UTF-8被错误地以Latin-1或类似编码解码
                try:
                    # 重新编码为字节，然后用UTF-8解码
                    fixed = text.encode('latin-1').decode('utf-8')
                    logger.debug(f"修复乱码: {text} -> {fixed}")
                    return fixed
                except (UnicodeEncodeError, UnicodeDecodeError):
                    pass

            # 情况2: 尝试GBK编码
            try:
                fixed = text.encode('latin-1').decode('gbk')
                logger.debug(f"修复乱码(GBK): {text} -> {fixed}")
                return fixed
            except (UnicodeEncodeError, UnicodeDecodeError):
                pass

            # 情况3: 尝试GB2312编码
            try:
                fixed = text.encode('latin-1').decode('gb2312')
                logger.debug(f"修复乱码(GB2312): {text} -> {fixed}")
                return fixed
            except (UnicodeEncodeError, UnicodeDecodeError):
                pass

        except Exception as e:
            logger.warning(f"修复编码时出错: {e}")

        return text  # 如果无法修复，返回原文本

    def download_pcf_file(self, etf_code, date_str, save_path=None):
        """下载PCF文件到本地"""
        try:
            datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            return {"success": False, "error": "日期格式错误，应为YYYYMMDD格式"}

        url = self.build_pcf_url(etf_code, date_str)
        logger.info(f"正在下载PCF文件: {url}")

        if save_path is None:
            save_path = f"shen_pcf/pcf_{etf_code}_{date_str}.xml"

        try:
            response = self.session.get(url, timeout=30)

            if response.status_code == 200:
                # 尝试多种编码方式处理响应内容
                encodings_to_try = ['utf-8', 'gbk', 'gb2312', 'latin-1']
                content = None

                for encoding in encodings_to_try:
                    try:
                        content = response.content.decode(encoding)
                        # 检查是否包含有效的中文字符
                        if re.search(r'[\u4e00-\u9fff]', content) or '<?xml' in content:
                            logger.info(f"成功使用编码 {encoding} 解码")
                            break
                    except UnicodeDecodeError:
                        continue

                if content is None:
                    # 如果所有编码都失败，使用chardet检测
                    encoding = chardet.detect(response.content)['encoding']
                    if encoding:
                        try:
                            content = response.content.decode(encoding)
                            logger.info(f"使用chardet检测的编码 {encoding} 解码")
                        except UnicodeDecodeError:
                            return {"success": False, "error": "无法解码下载内容"}
                    else:
                        return {"success": False, "error": "无法确定内容编码"}

                # 确保目录存在
                os.makedirs(os.path.dirname(save_path) if os.path.dirname(save_path) else ".", exist_ok=True)

                # 保存为UTF-8编码的文件
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(content)

                logger.info(f"PCF文件已保存至: {save_path}")
                return {
                    "success": True,
                    "file_path": save_path,
                    "url": url
                }
            elif response.status_code == 404:
                return {"success": False, "error": "未找到对应的PCF文件 (404错误)"}
            else:
                return {"success": False, "error": f"HTTP错误: {response.status_code}"}

        except requests.exceptions.RequestException as e:
            logger.error(f"网络请求失败: {str(e)}")
            return {"success": False, "error": f"网络请求失败: {str(e)}"}
        except Exception as e:
            logger.error(f"下载过程中发生错误: {str(e)}")
            return {"success": False, "error": f"下载过程中发生错误: {str(e)}"}

    def parse_pcf_file(self, file_path):
        """解析本地PCF XML文件（修复编码问题）"""
        try:
            if not os.path.exists(file_path):
                return {"success": False, "error": f"文件不存在: {file_path}"}

            # 检测文件编码
            file_encoding = self.detect_encoding(file_path)

            # 尝试多种编码读取文件
            encodings_to_try = [file_encoding, 'utf-8', 'gbk', 'gb2312', 'latin-1']
            xml_content = None

            for encoding in encodings_to_try:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        xml_content = f.read()

                    # 检查是否包含有效的中文字符或XML声明
                    if re.search(r'[\u4e00-\u9fff]', xml_content) or '<?xml' in xml_content:
                        logger.info(f"成功使用编码 {encoding} 读取文件")
                        break
                except UnicodeDecodeError:
                    continue

            if xml_content is None:
                # 如果所有编码都失败，尝试二进制读取并使用chardet
                with open(file_path, 'rb') as f:
                    raw_content = f.read()
                    encoding = chardet.detect(raw_content)['encoding']
                    if encoding:
                        try:
                            xml_content = raw_content.decode(encoding)
                        except UnicodeDecodeError:
                            return {"success": False, "error": "无法解码文件内容"}
                    else:
                        return {"success": False, "error": "无法确定文件编码"}

            # 解析XML内容
            namespace = {'ns': 'http://ts.szse.cn/Fund'}

            # 尝试修复XML内容中的编码问题
            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError as e:
                logger.warning(f"XML解析错误: {e}，尝试修复编码问题")
                # 尝试修复编码后重新解析
                fixed_content = self.fix_encoding(xml_content)
                try:
                    root = ET.fromstring(fixed_content)
                except ET.ParseError:
                    return {"success": False, "error": f"无法解析XML文件: {e}"}

            # 初始化结果字典
            result = {"fund_info": {
                "version": root.findtext('ns:Version', namespaces=namespace),
                "security_id": root.findtext('ns:SecurityID', namespaces=namespace),
                "security_id_source": root.findtext('ns:SecurityIDSource', namespaces=namespace),
                "symbol": self.fix_encoding(root.findtext('ns:Symbol', namespaces=namespace)),
                "fund_management_company": self.fix_encoding(
                    root.findtext('ns:FundManagementCompany', namespaces=namespace)),
                "underlying_security_id": root.findtext('ns:UnderlyingSecurityID', namespaces=namespace),
                "underlying_security_id_source": root.findtext('ns:UnderlyingSecurityIDSource', namespaces=namespace),
                "creation_redemption_unit": self._safe_float(
                    root.findtext('ns:CreationRedemptionUnit', namespaces=namespace)),
                "estimate_cash_component": self._safe_float(
                    root.findtext('ns:EstimateCashComponent', namespaces=namespace)),
                "max_cash_ratio": self._safe_float(root.findtext('ns:MaxCashRatio', namespaces=namespace)),
                "publish": root.findtext('ns:Publish', namespaces=namespace),
                "creation": root.findtext('ns:Creation', namespaces=namespace),
                "redemption": root.findtext('ns:Redemption', namespaces=namespace),
                "record_num": self._safe_int(root.findtext('ns:RecordNum', namespaces=namespace)),
                "total_record_num": self._safe_int(root.findtext('ns:TotalRecordNum', namespaces=namespace)),
                "trading_day": root.findtext('ns:TradingDay', namespaces=namespace),
                "pre_trading_day": root.findtext('ns:PreTradingDay', namespaces=namespace),
                "cash_component": self._safe_float(root.findtext('ns:CashComponent', namespaces=namespace)),
                "nav_per_cu": self._safe_float(root.findtext('ns:NAVperCU', namespaces=namespace)),
                "nav": self._safe_float(root.findtext('ns:NAV', namespaces=namespace)),
                "dividend_per_cu": self._safe_float(root.findtext('ns:DividendPerCU', namespaces=namespace)),
                "creation_limit": self._safe_float(root.findtext('ns:CreationLimit', namespaces=namespace)),
                "redemption_limit": self._safe_float(root.findtext('ns:RedemptionLimit', namespaces=namespace)),
                "creation_limit_per_user": self._safe_float(
                    root.findtext('ns:CreationLimitPerUser', namespaces=namespace)),
                "redemption_limit_per_user": self._safe_float(
                    root.findtext('ns:RedemptionLimitPerUser', namespaces=namespace)),
                "net_creation_limit": self._safe_float(root.findtext('ns:NetCreationLimit', namespaces=namespace)),
                "net_redemption_limit": self._safe_float(root.findtext('ns:NetRedemptionLimit', namespaces=namespace)),
                "net_creation_limit_per_user": self._safe_float(
                    root.findtext('ns:NetCreationLimitPerUser', namespaces=namespace)),
                "net_redemption_limit_per_user": self._safe_float(
                    root.findtext('ns:NetRedemptionLimitPerUser', namespaces=namespace))
            }, "components": [], "file_path": file_path}

            # 解析基金基本信息（应用编码修复）

            # 解析成分股信息（应用编码修复）
            components = root.find('ns:Components', namespaces=namespace)
            if components is not None:
                for component in components.findall('ns:Component', namespaces=namespace):
                    comp_data = {
                        "underlying_security_id": component.findtext('ns:UnderlyingSecurityID', namespaces=namespace),
                        "underlying_security_id_source": component.findtext('ns:UnderlyingSecurityIDSource',
                                                                            namespaces=namespace),
                        "underlying_symbol": self.fix_encoding(
                            component.findtext('ns:UnderlyingSymbol', namespaces=namespace)),
                        "component_share": self._safe_float(
                            component.findtext('ns:ComponentShare', namespaces=namespace)),
                        "substitute_flag": component.findtext('ns:SubstituteFlag', namespaces=namespace),
                        "premium_ratio": self._safe_float(component.findtext('ns:PremiumRatio', namespaces=namespace)),
                        "discount_ratio": self._safe_float(
                            component.findtext('ns:DiscountRatio', namespaces=namespace)),
                        "creation_cash_substitute": self._safe_float(
                            component.findtext('ns:CreationCashSubstitute', namespaces=namespace)),
                        "redemption_cash_substitute": self._safe_float(
                            component.findtext('ns:RedemptionCashSubstitute', namespaces=namespace))
                    }
                    result["components"].append(comp_data)

            return {"success": True, "data": result}

        except ET.ParseError as e:
            logger.error(f"XML解析错误: {str(e)}")
            return {"success": False, "error": f"XML解析错误: {str(e)}"}
        except Exception as e:
            logger.error(f"解析PCF文件时发生错误: {str(e)}")
            return {"success": False, "error": f"解析PCF文件时发生错误: {str(e)}"}

    def _safe_float(self, value):
        """安全转换为浮点数"""
        if value is None or value == '':
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _safe_int(self, value):
        """安全转换为整数"""
        if value is None or value == '':
            return 0
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 0

    def get_etf_pcf_data(self, etf_code, date_str, save_path=None):
        """获取ETF PCF数据的主要接口函数"""
        download_result = self.download_pcf_file(etf_code, date_str, save_path)

        if not download_result["success"]:
            return download_result

        parse_result = self.parse_pcf_file(download_result["file_path"])

        result = {
            "success": parse_result["success"],
            "etf_code": etf_code,
            "date": date_str,
            "download_url": download_result.get("url", ""),
            "file_path": download_result["file_path"]
        }

        if parse_result["success"]:
            result["data"] = parse_result["data"]
        else:
            result["error"] = parse_result.get("error", "解析失败")

        return result


# 使用示例和工具函数
def print_pcf_summary(data):
    """打印PCF数据摘要"""
    if not data.get("success", False):
        print(f"错误: {data.get('error', '未知错误')}")
        return

    pcf_data = data["data"]

    # 打印基金基本信息
    fund_info = pcf_data["fund_info"]
    print("\n=== 基金基本信息 ===")
    print(f"ETF代码: {fund_info['security_id']}")
    print(f"ETF名称: {fund_info['symbol']}")
    print(f"基金管理公司: {fund_info['fund_management_company']}")
    print(f"交易日期: {fund_info['trading_day']}")
    print(f"单位净值(NAV): {fund_info['nav']}")
    print(f"每单位净值(NAV per CU): {fund_info['nav_per_cu']}")
    print(f"最小申赎单位: {fund_info['creation_redemption_unit']}")
    print(f"现金差额: {fund_info['cash_component']}")
    print(f"预估现金差额: {fund_info['estimate_cash_component']}")

    # 打印成分股信息
    components = pcf_data["components"]
    print(f"\n=== 成分股信息 (共{len(components)}只) ===")
    for i in components:
        print(i)

    # 创建DataFrame以便更好地显示
    df_components = pd.DataFrame(components)

    # 重命名列名以便更好理解
    column_mapping = {
        'underlying_security_id': '证券代码',
        'underlying_security_id_source': '代码源',
        'underlying_symbol': '证券名称',
        'component_share': '份额数量',
        'substitute_flag': '替代标志',
        'premium_ratio': '溢价比例',
        'discount_ratio': '折价比例',
        'creation_cash_substitute': '申购现金替代',
        'redemption_cash_substitute': '赎回现金替代'
    }
    df_components = df_components.rename(columns=column_mapping)

    # 显示前10行
    print(df_components.head(10).to_string(index=False))

    if len(components) > 10:
        print(f"... 还有 {len(components) - 10} 只成分股未显示")

    return df_components


def test_encoding_fix():
    """测试编码修复功能"""
    parser = ShenETFPcfParser()

    # 测试您提供的乱码示例
    test_cases = [
        "é»éè¡ETF",  # 应该是"黄金ETF"
        "åå¤åºéç®¡çæéå¬å¸",  # 基金管理公司
        "ç³èµç°é",  # 成分股名称
    ]

    print("=== 编码修复测试 ===")
    for test_case in test_cases:
        fixed = parser.fix_encoding(test_case)
        print(f"原始: {test_case}")
        print(f"修复: {fixed}")
        print("-" * 50)


def main():
    """主函数 - 使用示例"""
    # 先测试编码修复
    test_encoding_fix()

    parser = ShenETFPcfParser()

    # 示例: 下载并解析用户提供的ETF示例
    etf_code = "159562"  # 根据文档中的SecurityID
    date_str = "20251205"

    print(f"\n正在获取ETF代码{etf_code}在日期{date_str}的PCF数据...")

    # 下载并解析PCF文件
    result = parser.get_etf_pcf_data(etf_code, date_str, save_path=f"pcf_{etf_code}_{date_str}.xml")

    if result["success"]:
        print("✓ PCF数据获取成功！")
        print(f"下载URL: {result['download_url']}")
        print(f"文件路径: {result['file_path']}")

        # 打印摘要信息
        df_components = print_pcf_summary(result)

        # 可选：保存成分股数据到CSV
        csv_path = f"pcf_components_{etf_code}_{date_str}.csv"
        df_components.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"\n成分股数据已保存至: {csv_path}")

    else:
        print("✗ PCF数据获取失败!")
        print(f"错误信息: {result.get('error', '未知错误')}")


# 单独使用解析功能的示例
def parse_existing_pcf_file():
    """解析已存在的PCF文件示例"""
    parser = ShenETFPcfParser()

    # 假设已经有一个PCF文件
    file_path = "shen_pcf/pcf_159919_20251205.xml"  # 替换为实际文件路径

    if os.path.exists(file_path):
        print(f"正在解析PCF文件: {file_path}")
        result = parser.parse_pcf_file(file_path)

        if result["success"]:
            print("✓ PCF文件解析成功！")
            print_pcf_summary({"success": True, "data": result["data"]})
        else:
            print(f"✗ 解析失败: {result.get('error', '未知错误')}")
    else:
        print(f"文件不存在: {file_path}")


if __name__ == "__main__":
    # 直接运行时的示例
    # main()

    # 如果要解析已存在的文件，取消下面的注释
    parse_existing_pcf_file()