import time
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


class FundDataCrawler:
    def __init__(self, headless=True):
        """初始化浏览器配置"""
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-images')
        self.driver = None

    def setup_driver(self):
        """设置WebDriver"""
        try:
            self.driver = webdriver.Chrome(options=self.chrome_options)
            self.driver.implicitly_wait(10)
            return True
        except Exception as e:
            print(f"WebDriver初始化失败: {e}")
            return False

    def crawl_fund_data(self, fund_code):
        """爬取基金交易数据"""
        url = f"https://fundf10.eastmoney.com/jjfl_{fund_code}.html"

        try:
            self.driver.get(url)
            # 等待页面加载完成
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "box"))
            )

            # 获取交易信息数据
            fund_data = self.extract_transaction_info()
            return fund_data

        except Exception as e:
            print(f"爬取过程中出现错误: {e}")
            return None

    def extract_transaction_info(self):
        """提取交易相关信息"""
        transaction_data = {}

        try:
            # 定位交易确认表格
            table = WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "txt_in"))
            )
            if len(table) == 0:
                raise Exception('not found.')
            text = table[0].text

            patterns = {
                "申购状态": r'申购状态\s+(\S+)',
                "赎回状态": r'赎回状态\s+(\S+)',
                "日累计申购限额": r'日累计申购限额\s+(\S+)',
                "持仓上限": r'持仓上限\s+(\S+)',
                "买入确认日": r'买入确认日\s+(T\+\d+)',
                "卖出确认日": r'卖出确认日\s+(T\+\d+)'
            }

            # 创建一个字典来存储提取结果
            results = {}

            # 遍历每个模式，在文本中搜索并提取结果[2,5](@ref)
            for field, pattern in patterns.items():
                match = re.search(pattern, text)
                if match:
                    # 如果匹配成功，将捕获组（group(1)）的内容存入结果字典
                    results[field] = match.group(1)
                else:
                    # 如果未匹配到，标记为未找到，便于调试
                    results[field] = "未找到"

            # 打印提取结果
            for field, value in results.items():
                print(f"{field}: {value}")

            return results

        except Exception as e:
            print(f"提取交易信息时出错: {e}")
            return {}

    def close(self):
        """关闭浏览器"""
        if self.driver:
            self.driver.quit()


def main():
    """主函数"""
    crawler = FundDataCrawler(headless=True)  # 设置为False可以看到浏览器操作

    if not crawler.setup_driver():
        print("无法启动浏览器，请检查ChromeDriver配置")
        return

    try:
        fund_code = "161226"
        fund_code = "161715"
        fund_code = "161225"
        print(f"开始爬取基金 {fund_code} 的交易信息...")

        data = crawler.crawl_fund_data(fund_code)

        if data:
            print("\n=== 基金交易信息 ===")
            for key, value in data.items():
                print(f"{key}: {value}")

            # 保存到CSV文件
            df = pd.DataFrame([data])
            df.to_csv(f"fund_{fund_code}_transaction_info.csv", index=False, encoding='utf-8-sig')
            print(f"\n数据已保存到 fund_{fund_code}_transaction_info.csv")
        else:
            print("未能获取到有效数据")

    finally:
        crawler.close()


if __name__ == "__main__":
    main()
