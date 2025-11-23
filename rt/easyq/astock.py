import easyquotation
import pandas as pd


def get_all_stock_rt():

    quotation = easyquotation.use('tencent') # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
    res = quotation.market_snapshot(prefix=False)
    # 将字典值转换为列表，然后创建DataFrame
    data_list = [value for value in res.values()]
    df = pd.DataFrame(data_list)
    print(df.head(1))
    print(df.columns)
    # Index(['name', 'code', 'now', 'close', 'open', 'volume', 'bid_volume',
    #        'ask_volume', 'bid1', 'bid1_volume', 'bid2', 'bid2_volume', 'bid3',
    #        'bid3_volume', 'bid4', 'bid4_volume', 'bid5', 'bid5_volume', 'ask1',
    #        'ask1_volume', 'ask2', 'ask2_volume', 'ask3', 'ask3_volume', 'ask4',
    #        'ask4_volume', 'ask5', 'ask5_volume', '最近逐笔成交', 'datetime', '涨跌',
    #        '涨跌(%)', 'high', 'low', '价格/成交量(手)/成交额', '成交量(手)', '成交额(万)', 'turnover',
    #        'PE', 'unknown', 'high_2', 'low_2', '振幅', '流通市值', '总市值', 'PB', '涨停价',
    #        '跌停价', '量比', '委差', '均价', '市盈(动)', '市盈(静)'],
    #       dtype='object')

    df.columns = [
        'TS_CODE', 'NAME', 'price', 'PCT_CHANGE', 'amount', 'high', 'low', 'vol_ratio', 'pre_close', 'total_mv',
        'float_mv',
        'pe_ttm', 'turnover_rate', 'change', 'volume', 'swing', 'open', 'pb', 'change_pct_60', 'change_pct_this_year'
    ]
    return df

if __name__ == '__main__':
    get_all_stock_rt()
