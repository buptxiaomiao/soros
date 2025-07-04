# coding: utf-8
import time
import uuid
from random import randint

import pandas as pd
import requests
from retrying import retry

from rt.api.thread_pool_executor import ThreadPoolExecutorBase


def get_etf_list_rt():
    return ETFListRT.run()

class ETFListRT(ThreadPoolExecutorBase):

    @classmethod
    def run(cls):
        func = cls.get_rt_etf_all_a_dc
        total_page = cls.get_total_page()
        result_df_list = cls.run_by_pool(fetch_func=func, total_page=total_page)

        # 拼接所有分页数据
        final_df = pd.concat(result_df_list, ignore_index=True)
        final_df = final_df.sort_values(by='PCT_CHANGE', ascending=False).reset_index(drop=True)
        return final_df


    @classmethod
    def get_total_page(cls):
        if hasattr(cls, 'total_page'):
            return getattr(cls, 'total_page')
        (df, total_cnt) = cls.get_rt_etf_all_a_dc()
        page_size = df.shape[0]
        total_page = int(total_cnt / page_size + 1)
        setattr(cls, 'total_page', total_page)
        return total_page


    @classmethod
    @retry(stop_max_attempt_number=3, wait_fixed=200)
    def get_rt_etf_all_a_dc(cls, page_no = None) -> (pd.DataFrame, int):
        """
        东方财富网- ETF-实时行情
        https://quote.eastmoney.com/center/gridlist.html#hs_a_board
        :return: 实时行情
        :rtype: pandas.DataFrame
            1、序号:RANK
            2、代码:TS_CODE
            3、名称:NAME
            4、最新价:PRICE
            实时净值: RT_VALUE
            溢价百分比: RT_VALUE_GAP
            5、涨跌幅:PCT_CHANGE
            6、涨跌额:CHANGE
            7、成交量:VOLUME
            8、成交额:AMOUNT
            9、振幅:SWING
            10、最高:HIGH
            11、最低:LOW
            12、今开:OPEN
            13、昨收:CLOSE
            14、量比:VOL_RATIO
            15、换手率:TURNOVER_RATE
            16、市盈率-动态:PE
            17、市净率:PB
            18、总市值:TOTAL_MV
            19、流通市值:FLOAT_MV
            20、涨速:RISE
            21、5分钟涨跌:5MIN
            22、60日涨跌幅:60DAY
            23、年初至今涨跌幅:1YEAR
        """

        ut = str(uuid.uuid4()).replace('-', '')

        url = f"http://{randint(1, 100)}.push2.eastmoney.com/api/qt/clist/get"
        a = "https://1.push2.eastmoney.com/api/qt/clist/get?" \
            "pn=1&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2" \
            "&wbp2u=7433395868590096|0|1|0|web&fid=f3&fs=b:MK0021,b:MK0022,b:MK0023,b:MK0024" \
            "&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1706634482365"
        params = {
            "pn":  "1" if not page_no else str(page_no), # 页码
            "pz": "200",
            "po": "1",
            "np": "1",
            "ut": ut,
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            # "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
            "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024",
            "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f145,f115,f152",
            "_": str(time.time()),
        }
        r = requests.get(url, params=params)
        data_json = r.json()
        if ((data_json is None)
                or (not data_json["data"])
                or (not data_json["data"]["diff"])):
            return pd.DataFrame(), 0

        total_num = data_json['data']['total']
        temp_df = pd.DataFrame(data_json["data"]["diff"])
        temp_df.columns = [
            "_",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "成交量",
            "成交额",
            "振幅",
            "换手率",
            "市盈率-动态",
            "量比",
            "5分钟涨跌",
            "代码",
            "_",
            "名称",
            "最高",
            "最低",
            "今开",
            "昨收",
            "总市值",
            "流通市值",
            "涨速",
            "市净率",
            "60日涨跌幅",
            "年初至今涨跌幅",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "实时净值",
            "-",
        ]
        temp_df.reset_index(inplace=True)
        # temp_df["index"] = temp_df.index + 1
        # temp_df.rename(columns={"index": "序号"}, inplace=True)
        temp_df = temp_df[
            [
                # "序号",
                "代码",
                "名称",
                "最新价",
                "实时净值",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "振幅",
                "最高",
                "最低",
                "今开",
                "昨收",
                "量比",
                "换手率",
                "市盈率-动态",
                "市净率",
                "总市值",
                "流通市值",
                "涨速",
                "5分钟涨跌",
                "60日涨跌幅",
                "年初至今涨跌幅",
            ]
        ]

        temp_df["代码"] = temp_df["代码"]
        temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
        temp_df["实时净值"] = pd.to_numeric(temp_df["实时净值"], errors="coerce")
        temp_df.insert(4, '溢价百分比', temp_df.apply(
            lambda x: round(100.0 * (x['最新价']-x['实时净值']) / x['实时净值'], 2), axis=1)
        )

        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
        temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
        temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
        temp_df["今开"] = pd.to_numeric(temp_df["今开"], errors="coerce")
        temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce")
        temp_df["量比"] = pd.to_numeric(temp_df["量比"], errors="coerce")
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
        temp_df["市盈率-动态"] = pd.to_numeric(temp_df["市盈率-动态"], errors="coerce")
        temp_df["市净率"] = pd.to_numeric(temp_df["市净率"], errors="coerce")
        temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
        temp_df["流通市值"] = pd.to_numeric(temp_df["流通市值"], errors="coerce")
        temp_df["涨速"] = pd.to_numeric(temp_df["涨速"], errors="coerce")
        temp_df["5分钟涨跌"] = pd.to_numeric(temp_df["5分钟涨跌"], errors="coerce")
        temp_df["60日涨跌幅"] = pd.to_numeric(temp_df["60日涨跌幅"], errors="coerce")
        temp_df["年初至今涨跌幅"] = pd.to_numeric(temp_df["年初至今涨跌幅"], errors="coerce")
        temp_df.columns = [
            # "RANK",
            "TS_CODE",
            "NAME",
            "PRICE",
            "RT_VALUE",
            "RT_VALUE_GAP",
            "PCT_CHANGE",
            "CHANGE",
            "VOLUME",
            "AMOUNT",
            "SWING",
            "HIGH",
            "LOW",
            "OPEN",
            "CLOSE",
            "VOL_RATIO",
            "TURNOVER_RATE",
            "PE",
            "PB",
            "TOTAL_MV",
            "FLOAT_MV",
            "RISE",
            "5MIN",
            "60DAY",
            "1YEAR",
        ]
        df_sorted = temp_df.sort_values(by='PCT_CHANGE', ascending=False).reset_index(drop=True)
        return df_sorted, total_num


if __name__ == '__main__':
    df = get_etf_list_rt()
    pd.set_option('display.max_columns', None)  # 显示所有列
    print(df.shape)
    print(df.head(3))
