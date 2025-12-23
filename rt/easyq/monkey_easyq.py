
import sys
import logging
import multiprocessing
from functools import partial

from easyquotation.basequotation import BaseQuotation

sys.path.append('..')
sys.path.append('../..')
from rt.api.thread_pool_executor import ThreadPoolExecutorBase

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _fetch_stock_data(self, stock_list, pool_size: int = 50):
    """获取股票信息"""
    pool = multiprocessing.pool.ThreadPool(pool_size)
    logger.info(f'using threadPool size={pool_size}. len(stock_list)={len(stock_list)}')
    try:
        res = pool.map(self.get_stocks_by_range, stock_list)
    finally:
        pool.close()
    return [d for d in res if d is not None]


def monkey_easyq_wrapper(quo: BaseQuotation, use_proxy: bool = True, pool_size: int = 50) -> BaseQuotation:
    proxies = ThreadPoolExecutorBase.get_proxy_conf()

    # 增加proxy
    new_get = partial(quo._session.get, proxies=proxies if use_proxy and proxies else {})
    setattr(quo._session, 'get', new_get)

    logger.info(f'wrap easyquotation {type(quo)} get function  use_proxy={use_proxy and proxies}')
    # 修改线程池
    quo._fetch_stock_data = partial(_fetch_stock_data, quo, pool_size=pool_size)

    return quo

