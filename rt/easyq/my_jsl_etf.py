
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
        fix_list = [i for i in res_list
                    if i.get('apply_status', '') not in ('暂停申购', '')
                    and '-' not in i.get('discount_rt', '-')
                    and float(str(i.get('discount_rt')).replace('%', '')) > 2
                    ]
        # fix_list = res_list
        for i in fix_list:
            discount_rt = str(i.get('discount_rt')).replace('%', '')
            print(f"溢价：{discount_rt} "
                  f" {i.get('fund_id')} "
                  f" {i.get('fund_nm')}"
                  f"  价格:{i.get('price')}"
                  f"  涨幅:{i.get('increase_rt')}"
                  # f"  预估净值:{i.get('estimate_value')}"
                  f"  成交额{round(float(i.get('volume'))/10000, 2)}亿"
                  f"  份额{round(float(i.get('amount'))/10000, 2)}亿"
                  f"  份额增加{round(float(i.get('amount_incr'))/10000, 2)}亿"
                  f"  最新净值:{i.get('fund_nav')}"
                  f"  {i.get('apply_status')}"
                  f"  申购费率{i.get('apply_fee')}"
                  f"  换手{i.get('turnover_rt')}"
                  # f"{i}"
                  f"")
            # print(i.get('discount_rt'), i)

        return res_list


if __name__ == '__main__':
    jsl = MyJslETF()
    # index_etf = jsl.index_etf()
    # gold_etf = jsl.gold_etf()
    index_lof = jsl.index_lof()
    stock_lof = jsl.stock_lof()
    commodity_lof = jsl.commodity_lof()
    qdii_as = jsl.qdii_as()
    qdii_eu = jsl.qdii_eu()

