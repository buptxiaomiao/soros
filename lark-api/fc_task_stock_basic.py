# coding: utf-8
from typing import List, Dict, Any, Optional

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import AppTableRecord, AppTableRecordBuilder, BatchCreateAppTableRecordResponseBody, \
    BatchUpdateAppTableRecordResponseBody
import pandas as pd
from pandas import DataFrame
from lark_util import lark_util


class FsTaskStockBasic:
    """
    每日执行 0 20 * * *
    同步股票基础信息到飞书多维表格文档.
    """

    def __init__(self):
        self.lark_util = lark_util
        self.logger = lark.logger

    def exec(self):
        all_df = self.get_data_local()
        print(all_df['delist_date'].dtype)
        all_df['delist_date'].astype('str')
        print(all_df.head(5))
        print(all_df.shape)
        df = all_df[
            (~all_df['name'].str.contains('ST', na=False)) & (all_df['delist_date'].isna())
        ]
        print(df.shape)
        df['is_hs'] = df.apply(lambda x: x['is_hs'] in ('H', 'S'), axis=1)
        df['symbol'] = df.apply(lambda x: x['ts_code'].split('.')[0], axis=1)

        def market_func(x) -> str:
            ts_code = x['ts_code']
            prefix1 = ts_code[:1]
            prefix2 = ts_code[:2]
            # 按代码特异性从高到低判断
            if prefix2 == '68':
                return '科创'
            elif prefix2 in ['30']:
                return '创业'
            elif prefix2 == '60':  # 注意：在688判断之后，60开头才归为主板
                return '上证'
            elif prefix2 == '00':
                return '深证'
            elif prefix2 in ['43', '83', '87', '88'] or prefix1 == '9':
                return '北证'
            else:
                return '未知'
        df['market'] = df.apply(market_func, axis=1)
        df['act_ent_type'] = df['act_ent_type'].fillna('')
        # df['act_ent_type'] = df.apply(lambda x: x['act_ent_type'] if x['act_ent_type'] != '无' else '')

        df = df[
            ['ts_code', 'name', 'symbol', 'area', 'industry', 'act_ent_type', 'is_hs', 'market']
        ]
        # self.upsert_fc_table(df)

        # df = df[
        #     df['ts_code'].apply(lambda x: x[:2] == '00')
        # ]

        print(df[:6])
        print(df.shape)
        print(df.dtypes)
        print(df.columns)
        # self.upsert_fc_table(df[:5])
        self.upsert_fc_table(df)

    def get_data(self) -> DataFrame:
        pass

    def get_data_local(self) -> DataFrame:
        """
        Index(['ts_code', 'symbol', 'name', 'area', 'industry', 'cnspell', 'market',
               'list_date', 'act_name', 'act_ent_type', 'exchange', 'fullname',
               'list_status', 'delist_date', 'is_hs', 'enname', 'curr_type'],
              dtype='object')
        """
        file_path = 'C:/Users/wjh/Downloads/202512/tushare_stock_basic_20251206222832.csv'
        print(file_path)
        df = pd.read_csv(file_path)
        print(df.head())
        print(df.shape)
        print(df.columns)
        print(df.info)
        return df

    def upsert_fc_table(self, df):
        data_dict_list = df.to_dict(orient="records")
        # ['ts_code', 'name', 'symbol', 'area', 'industry', 'act_ent_type', 'is_hs', 'market']

        # 文档中已经存在的记录
        exist_records = self.lark_util.search_all_records()
        exist_records_map = {i.fields.get('TS_CODE')[0].get('text', ''): i.record_id for i in exist_records
                             if i.fields.get('TS_CODE', []) and i.fields.get('TS_CODE')[0].get('text', '')}
        # 创建/更新列表
        insert_dict_list: List[Dict[str, Any]] = []
        update_app_record_list: List[AppTableRecord] = []

        for data_dict in data_dict_list:
            ts_code = data_dict['ts_code']
            fields = {
                '名称': data_dict['name'],
                'CODE': data_dict['symbol'],
                '市场': data_dict['market'],
                '地区': data_dict['area'],
                'TS行业': data_dict['industry'],
                '企业性质': data_dict['act_ent_type'],
                '沪深股通': '是' if data_dict['is_hs'] else None,
            }
            record_id = exist_records_map.get(ts_code, '')
            if not record_id:
                fields['TS_CODE'] = ts_code
                insert_dict_list.append(fields)
                print(fields)
            else:
                update_app_record = AppTableRecordBuilder()\
                    .record_id(record_id)\
                    .fields(fields)\
                    .build()
                update_app_record_list.append(update_app_record)

        self.logger.info(f'批量创建飞书文档记录: {len(insert_dict_list)}条')
        batch_insert_res: List[Optional[BatchCreateAppTableRecordResponseBody]] = self.lark_util\
            .batch_create_records(records=insert_dict_list)
        self.logger.info(f'批量创建飞书文档记录res: {sum([len(i.records) for i in batch_insert_res if i])} 条.')

        self.logger.info(f'批量更新飞书文档记录: {len(update_app_record_list)}条')
        batch_update_res: List[Optional[BatchUpdateAppTableRecordResponseBody]] = self.lark_util.\
            batch_update_records(records=update_app_record_list)
        self.logger.info(f'批量更新飞书文档记录res: {sum([len(i.records) for i in batch_update_res if i])} 条.')
        pass


if __name__ == '__main__':
    # df1 = FsStockBasic().get_data_local()
    # print(df1)
    # print(df1.shape)
    # print(df1.columns)

    FsTaskStockBasic().exec()

    pass
