
import time
import json
import requests


class MyJslETF:
    """
    感谢集思录的数据支持
    https://www.jisilu.cn/data/qdii/#qdiie

    """

    # 普通指数ETF
    INDEX_ETF_URL = "https://www.jisilu.cn/data/etf/etf_list/?___jsl=LST___t={ctime:d}&volume=&unit_total=&rp=25"
    # 黄金ETF
    GOLD_ETF_URL = "https://www.jisilu.cn/data/etf/gold_list/?___jsl=LST___t={ctime:d}&rp=25&page=1"

    # 指数LOF
    INDEX_LOF_URL = "https://www.jisilu.cn/data/lof/index_lof_list/?___jsl=LST___t={ctime:d}&rp=25"
    # 股票LOF
    STOCK_LOF_URL = "https://www.jisilu.cn/data/lof/stock_lof_list/?___jsl=LST___t={ctime:d}&rp=25"

    # 商品QDII
    COMMODITY_LOF_URL = "https://www.jisilu.cn/data/qdii/qdii_list/C?___jsl=LST___t={ctime:d}&rp=22"
    # 亚太QDII
    QDII_AS_URL = "https://www.jisilu.cn/data/qdii/qdii_list/A?___jsl=LST___t={ctime:d}&rp=22&page=1"
    # 欧美QDII
    QDII_EU_URL = "https://www.jisilu.cn/data/qdii/qdii_list/E?___jsl=LST___t={ctime:d}&only_lof=y&only_etf=y&rp=22"

    def __init__(self):
        self._session = requests.session()
        pass

    def index_etf(self):
        return self._base_get_data(self.INDEX_ETF_URL)

    def gold_etf(self):
        return self._base_get_data(self.GOLD_ETF_URL)

    def index_lof(self):
        return self._base_get_data(self.INDEX_LOF_URL)

    def stock_lof(self):
        return self._base_get_data(self.GOLD_ETF_URL)

    def commodity_lof(self):
        return self._base_get_data(self.COMMODITY_LOF_URL)

    def qdii_as(self):
        return self._base_get_data(self.QDII_AS_URL)

    def qdii_eu(self):
        return self._base_get_data(self.QDII_EU_URL)

    def _base_get_data(self, base_url):
        url = base_url.format(ctime=int(time.time()))
        print(f''
              f''
              f'request url: {url}')
        rep = self._session.get(url)
        # 获取返回的json字符串
        index_lof_json = json.loads(rep.text)
        # {
        #     'page': 1,
        #     'rows': [
        #         {
        #             'id': '160119',
        #             'cell': {
        #                 'fund_id': '160119',
        #                 'fund_nm': '500ETF联接LOF',
        #                 'idx_price_dt': '2025-12-24',
        #                 'amount_incr_tips': '最新份额：4083万份；增长：-0.15%',
        #                 'turnover_rt': '1.05'
        #             }
        #         } ] }

        rows = index_lof_json.get('rows', [])
        res_list = [row.get('cell', {}) for row in rows if row.get('cell', {})]
        columns = list(res_list[0].keys()) if len(res_list) > 0 else []
        print(f"total_size={len(res_list)}")
        print(f"columns.size={len(columns)} columns={columns}")
        for i in res_list[:3]:
            print(i)

        return res_list


if __name__ == '__main__':
    jsl = MyJslETF()
    index_etf = jsl.index_etf()
    gold_etf = jsl.gold_etf()
    index_lof = jsl.index_lof()
    stock_lof = jsl.stock_lof()
    commodity_lof = jsl.commodity_lof()
    qdii_as = jsl.qdii_as()
    qdii_eu = jsl.qdii_eu()

