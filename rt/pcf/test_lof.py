import requests
import json
import re


def get_fund_limit_api(fund_code='161226'):
    """
    通过天天基金API获取基金限额信息
    """
    url = f'http://fund.eastmoney.com/pingzhongdata/{fund_code}.js'

    headers = {
        'Referer': 'http://fund.eastmoney.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        print(response.text)

        # 在JS文件中查找限额信息
        # 可能需要根据实际返回内容调整正则表达式
        limit_match = re.search(r'日累计申购限额["\']?:\s*["\']?(\d+\.?\d*)', response.text)
        if limit_match:
            return float(limit_match.group(1))

        return None

    except Exception as e:
        print(f"API请求失败: {e}")
        return None

ress = get_fund_limit_api()
print(ress)