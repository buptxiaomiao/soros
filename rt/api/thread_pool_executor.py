from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from pandas.core.interchange.dataframe_protocol import DataFrame
from typing import List


class ThreadPoolExecutorBase:

    @classmethod
    def run_by_pool(cls, fetch_func, total_page) -> List[DataFrame]:
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
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
        with ThreadPoolExecutor(max_workers=10) as executor:
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
        import os
        server = os.getenv("proxy_server")
        key = os.getenv("proxy_key")
        sec = os.getenv("proxy_secret")

        if server and key and sec:
            proxyUrl = "http://%(user)s:%(password)s@%(server)s" % {
                "user": key,
                "password": sec,
                "server": server,
            }
            return {
                'http': proxyUrl,
                'https': proxyUrl
            }
        return {}