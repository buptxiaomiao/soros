from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from pandas.core.interchange.dataframe_protocol import DataFrame
from typing import List

import os
import dotenv
import sys
import requests

sys.path.append('..')
sys.path.append('./..')
dotenv.load_dotenv()
server = os.getenv("proxy_server")
key = os.getenv("proxy_key")
sec = os.getenv("proxy_secret")


class ThreadPoolExecutorBase:

    session = requests.Session()

    # 调整连接池大小
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=20,  # 缓存的连接池数量
        pool_maxsize=20  # 每个 host 的最大连接数
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    proxy_conf = {}

    @classmethod
    def run_by_pool(cls, fetch_func, total_page) -> List[DataFrame]:
        results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            # 提交任务到线程池
            futures = [executor.submit(fetch_func, page_no)
                       for page_no in range(1, total_page + 1)]

            # 收集结果
            for future in as_completed(futures):
                try:
                    res_tuple = future.result()
                    results.append(res_tuple[0])
                except Exception as e:
                    print(f"Error fetching page: {e}")
        return results

    @classmethod
    def run_by_pool_pro(cls, fetch_func, args) -> List[DataFrame]:
        results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            # 提交任务到线程池
            futures = [executor.submit(fetch_func, *arg)
                       for arg in args]
            # 收集结果
            for future in as_completed(futures):
                try:
                    res_tuple = future.result()
                    results.append(res_tuple[0])
                except Exception as e:
                    print(f"Error fetching page: {e}")
                    raise e
        return results

    @classmethod
    def get_proxy_conf(cls):

        if cls.proxy_conf:
            return cls.proxy_conf

        if server and key and sec:
            proxy_url = "http://%(user)s:%(password)s@%(server)s" % {
                "user": key,
                "password": sec,
                "server": server,
            }
            cls.proxy_conf = {
                'http': proxy_url,
                'https': proxy_url
            }
        return cls.proxy_conf
