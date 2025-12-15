import requests
import pandas as pd
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import os
import re
import logging
from urllib.parse import quote, unquote
import chardet
import sys
sys.path.append('..')
sys.path.append('../..')
from rt.api.thread_pool_executor import ThreadPoolExecutorBase

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ShangETFPcfDownloader:
    """
    上交所ETF PCF文件下载器
    只负责下载功能，不包含解析逻辑
    """

    def __init__(self):
        self.base_url = "https://query.sse.com.cn/etfDownload/downloadETF2Bulletin.do"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.sse.com.cn/',
            'Accept': 'application/xml, text/xml, */*; q=0.01'
        })

    def download_pcf_file(self, etf_code, date_str=None, save_dir=None):
        """
        下载指定日期的PCF文件

        Args:
            etf_code (str): ETF代码，如"510030"
            date_str (str, optional): 日期字符串(YYYYMMDD)，如果为None则下载最新
            save_dir (str, optional): 文件保存目录

        Returns:
            dict: 包含下载状态和文件路径的字典
        """
        params = {
            'fundCode': etf_code
        }

        if date_str:
            try:
                datetime.strptime(date_str, '%Y%m%d')
                params['date'] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            except ValueError:
                return {"success": False, "error": "日期格式错误，应为YYYYMMDD格式"}

        try:
            if date_str:
                logger.info(f"正在下载ETF代码{etf_code}在日期{date_str}的PCF文件...")
            else:
                logger.info(f"正在下载ETF代码{etf_code}的最新PCF文件...")

            logger.info(f'开始get请求:{self.base_url}, params={params}, '
                        f'use proxies={True if ThreadPoolExecutorBase.get_proxy_conf() else False}')
            response = self.session.get(self.base_url, params=params, timeout=30,
                                        proxies=ThreadPoolExecutorBase.get_proxy_conf())
            logger.info(f'请求res: status_code={response.status_code}, len(content)={len(response.content)}')

            if response.status_code == 200:
                # 尝试多种编码方式处理响应内容
                encodings_to_try = ['utf-8', 'gbk', 'gb2312', 'latin-1']
                content = None

                for encoding in encodings_to_try:
                    try:
                        content = response.content.decode(encoding)
                        if '<?xml' in content or '<PCFData>' in content or '<FundInfo>' in content:
                            logger.info(f"成功使用编码 {encoding} 解码")
                            break
                    except UnicodeDecodeError:
                        continue

                if content is None:
                    encoding = chardet.detect(response.content)['encoding'] or 'utf-8'
                    try:
                        content = response.content.decode(encoding)
                        logger.info(f"使用chardet检测的编码 {encoding} 解码")
                    except UnicodeDecodeError:
                        return {"success": False, "error": "无法解码下载内容"}

                # 从内容中提取日期
                extracted_date = self._extract_date_from_content(content)
                date_str = extracted_date or date_str or datetime.now().strftime("%Y%m%d")

                # 确定文件名和保存路径
                filename = f"shang_pcf/pcf_{etf_code}_{date_str}.xml"

                if save_dir:
                    os.makedirs(save_dir, exist_ok=True)
                    file_path = os.path.join(save_dir, filename)
                else:
                    file_path = filename

                # 保存文件
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)

                logger.info(f"PCF文件已保存至: {file_path}")
                return {
                    "success": True,
                    "file_path": file_path,
                    "etf_code": etf_code,
                    "date": date_str,
                    "url": response.url
                }
            else:
                error_msg = f"HTTP错误: {response.status_code}"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}

        except requests.exceptions.RequestException as e:
            error_msg = f"网络请求失败: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"下载过程中发生错误: {str(e)}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

    def _extract_date_from_content(self, content):
        """从XML内容中提取日期"""
        try:
            # 尝试解析XML
            root = ET.fromstring(content)

            # 查找可能的日期字段
            date_fields = [
                'PublishDate', 'CreationDate', 'RedemptionDate',
                'TradingDay', 'PreTradingDay', 'Date', 'publishDate'
            ]

            for field in date_fields:
                element = root.find(field)
                if element is not None and element.text:
                    date_str = element.text.strip()
                    try:
                        if ' ' in date_str:
                            date_str = date_str.split(' ')[0]
                        if 'T' in date_str:
                            date_str = date_str.split('T')[0]

                        for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y%m%d', '%Y.%m.%d']:
                            try:
                                dt = datetime.strptime(date_str, fmt)
                                return dt.strftime('%Y%m%d')
                            except ValueError:
                                continue
                    except Exception:
                        continue

            # 如果标准字段找不到，尝试在内容中搜索日期模式
            date_patterns = [
                r'<PublishDate>(\d{4}-\d{2}-\d{2})</PublishDate>',
                r'<TradingDay>(\d{8})</TradingDay>',
                r'(\d{4})[-/]?(\d{2})[-/]?(\d{2})',
                r'(\d{4})(\d{2})(\d{2})'
            ]

            for pattern in date_patterns:
                match = re.search(pattern, content)
                if match:
                    groups = match.groups()
                    if len(groups) == 1 and len(groups[0]) == 8:
                        return groups[0]
                    elif len(groups) == 3:
                        year, month, day = groups
                        month = month.zfill(2)
                        day = day.zfill(2)
                        return f"{year}{month}{day}"

            return None
        except ET.ParseError:
            return self._extract_date_with_regex(content)
        except Exception as e:
            logger.warning(f"提取日期时发生错误: {str(e)}")
            return None

    def _extract_date_with_regex(self, content):
        """使用正则表达式从内容中提取日期"""
        patterns = [
            r'<PublishDate>(\d{4}-\d{2}-\d{2})</PublishDate>',
            r'<CreationDate>(\d{4}-\d{2}-\d{2})</CreationDate>',
            r'<TradingDay>(\d{8})</TradingDay>',
            r'<Date>(\d{8})</Date>',
            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'
        ]

        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                groups = match.groups()
                if len(groups) == 1 and len(groups[0]) == 8:
                    return groups[0]
                elif len(groups) == 3:
                    year, month, day = groups
                    month = month.zfill(2)
                    day = day.zfill(2)
                    return f"{year}{month}{day}"

        return None

    # def download_historical_pcf(self, etf_code, start_date, end_date, save_dir=None):
    #     """
    #     下载历史PCF数据
    #
    #     Args:
    #         etf_code (str): ETF代码
    #         start_date (str): 开始日期(YYYYMMDD)
    #         end_date (str): 结束日期(YYYYMMDD)
    #         save_dir (str, optional): 文件保存目录
    #
    #     Returns:
    #         list: 下载结果列表
    #     """
    #     start = datetime.strptime(start_date, '%Y%m%d')
    #     end = datetime.strptime(end_date, '%Y%m%d')
    #
    #     results = []
    #     current_date = start
    #
    #     while current_date <= end:
    #         date_str = current_date.strftime('%Y%m%d')
    #         logger.info(f"下载{etf_code}在{date_str}的PCF数据...")
    #
    #         result = self.download_pcf_file(etf_code, date_str, save_dir)
    #         results.append(result)
    #
    #         current_date += timedelta(days=1)
    #
    #     return results


if __name__ == '__main__':
    ShangETFPcfDownloader().download_pcf_file('510330')
