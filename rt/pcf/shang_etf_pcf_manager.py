import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import os
import re
import logging
from urllib.parse import quote, unquote
import chardet
from shang_etf_pcf_parser import ShangETFPcfParser
from shang_etf_pcf_downloader import ShangETFPcfDownloader

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ShangETFPcfManager:
    """
    https://www.sse.com.cn/disclosure/fund/etflist/detail.shtml?type=009&fundid=510300
    上交所ETF PCF文件管理器
    整合下载和解析功能，提供统一接口
    """

    def __init__(self):
        self.downloader = ShangETFPcfDownloader()
        self.parser = None

    def get_etf_pcf_data(self, etf_code, date_str=None, save_dir=None):
        """
        获取ETF PCF数据的主要接口函数
        先下载后解析

        Args:
            etf_code (str): ETF代码
            date_str (str, optional): 日期字符串(YYYYMMDD)
            save_dir (str, optional): 文件保存目录

        Returns:
            dict: 包含下载和解析结果的字典
        """
        # 下载PCF文件
        download_result = self.downloader.download_pcf_file(etf_code, date_str, save_dir)

        if not download_result["success"]:
            return download_result

        self.parser = ShangETFPcfParser(xml_file_path=download_result['file_path'])
        # 解析PCF文件
        print("parse download_result")

        result = {
            'etf_code': etf_code,
            'date': download_result['date'],
            'download_url': download_result['url'],
            'file_path': download_result['file_path'],
            'data': self.parser.components,
            'basic_info': self.parser.basic_info
        }

        if self.parser.components and self.parser.basic_info:
            result['success'] = True
        else:
            result['success'] = False
            result['error'] = '解析失败了...'
            print(f"basic_info={self.parser.basic_info}")
            print(f"components={self.parser.components}")

        return result

    def parse_local_pcf_file(self, file_path):
        """
        解析本地PCF文件

        Args:
            file_path (str): 本地PCF文件路径

        Returns:
            dict: 解析结果
        """
        self.parser = ShangETFPcfParser(xml_file_path=file_path)
        # 解析PCF文件
        parse_result = self.parser.components
        return parse_result


# 使用示例和工具函数
def print_shang_pcf_summary(data):
    """打印上交所PCF数据摘要"""
    # if not data.get("success", False):
    #     print(f"错误: {data.get('error', '未知错误')}")
    #     return
    #
    # pcf_data = data["data"]
    #
    # # 打印基金基本信息
    # fund_info = pcf_data["fund_info"]
    # print("\n=== 基金基本信息 ===")
    # for key, value in fund_info.items():
    #     print(f"{key}: {value}")
    #
    # # 打印成分股信息
    # components = pcf_data["components"]
    # print(f"\n=== 成分股信息 (共{len(components)}只) ===")
    #
    # if components:
    #     # 创建DataFrame以便更好地显示
    #     df_components = pd.DataFrame(components)
    #
    #     # 确保有正确的列名
    #     if 'security_code' in df_components.columns and 'quantity' in df_components.columns:
    #         # 只显示关键列
    #         display_columns = ['security_code', 'security_name',
    #                            'quantity'] if 'security_name' in df_components.columns else ['security_code',
    #                                                                                          'quantity']
    #         display_df = df_components[display_columns].head(10)
    #
    #         print(display_df.to_string(index=False))
    #
    #         if len(components) > 10:
    #             print(f"... 还有 {len(components) - 10} 只成分股未显示")
    #
    #         return df_components
    #     else:
    #         print("成分股数据结构异常，显示原始数据:")
    #         print(df_components.head(10).to_string(index=False))
    #         return df_components


if __name__ == '__main__':
    # manager = ShangETFPcfManager()
    # result = manager.get_etf_pcf_data('510300')
    # print(result)
    # for i in result['data']:
    #     print(i)

    # data = ShangETFPcfManager().parse_local_pcf_file('./shang_pcf_data/pcf_5100040_20251211.xml')
    data = ShangETFPcfManager().parse_local_pcf_file('./shang_pcf/pcf_510040_20251211.xml')
    print_shang_pcf_summary(data)
    for i in data:
        print(i)



