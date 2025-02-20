from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from lark_oapi.api.corehr.v1 import Object
from pandas.core.interchange.dataframe_protocol import DataFrame
from typing import List


class ThreadPoolExecutorBase(Object):

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
