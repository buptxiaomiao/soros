# coding: utf-8

import os
import time
import dotenv
import lark_oapi as lark

from typing import List, Optional, Dict, Any

from uuid import uuid4
from lark_oapi.api.bitable.v1 import (
    CopyAppRequest, CopyAppRequestBody, SearchAppTableRecordRequest, SearchAppTableRecordRequestBody,
    AppTableRecord, SearchAppTableRecordResponseBody, BatchUpdateAppTableRecordRequest,
    BatchUpdateAppTableRecordRequestBody, BatchCreateAppTableRecordRequest, BatchCreateAppTableRecordRequestBody,
    BatchCreateAppTableRecordResponseBody, BatchUpdateAppTableRecordResponseBody
)
from lark_oapi.api.drive.v1 import (
    ListFileRequest, File
)

dotenv.load_dotenv()

APP_ID = os.getenv("FS_APP_ID", "")
APP_SECRET = os.getenv("FS_APP_SECRET", "")
FOLDER_TOKEN = os.getenv("FS_FOLDER_TOKEN", "")
APP_TOKEN = os.getenv("FS_APP_TOKEN", "")
TABLE_ID = os.getenv("FS_TABLE_ID", "")


class LarkUtil:
    """
    简化版飞书文档操作工具类
    仅保留根据文件夹token获取文件列表的接口
    """

    def __init__(self, app_id: str = APP_ID, app_secret: str = APP_SECRET):
        """
        初始化LarkUtil
        
        Args:
            app_id: 飞书应用ID
            app_secret: 飞书应用密钥
        """
        self.client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .build()

    def list_files(self, folder_token: str = FOLDER_TOKEN) -> Optional[List[File]]:
        """
        根据文件夹token获取文件列表
        https://open.feishu.cn/document/server-docs/docs/drive-v1/folder/list

        Args:
            folder_token: 文件夹token
            
        Returns:
            List[File]: 文件列表
        """
        request = ListFileRequest.builder() \
            .folder_token(folder_token) \
            .page_size(200) \
            .build()

        response = self.client.drive.v1.file.list(request)

        if not response.success():
            lark.logger.error(
                f"获取文件列表失败, "
                f"code: {response.code}, "
                f"msg: {response.msg}, "
                f"log_id: {response.get_log_id()}")
            return None

        return response.data.files

    def copy_app(self, name: str, app_token: str = APP_TOKEN, folder_token: str = FOLDER_TOKEN,
                 without_content: bool = True) -> Optional[str]:
        """
        复制多维表格
        https://open.feishu.cn/document/server-docs/docs/bitable-v1/app/copy

        Args:
            app_token: 源多维表格token
            name: 新多维表格名称
            folder_token: 目标文件夹token
            without_content: 是否仅复制结构不复制数据，默认为True
            
        Returns:
            str: 新多维表格token
        """
        # 构建请求体
        request_body = CopyAppRequestBody.builder() \
            .name(name) \
            .folder_token(folder_token) \
            .without_content(without_content) \
            .build()

        # 构建请求
        request = CopyAppRequest.builder() \
            .app_token(app_token) \
            .request_body(request_body) \
            .build()

        # 发送请求
        response = self.client.bitable.v1.app.copy(request)

        if not response.success():
            lark.logger.error(
                f"复制多维表格失败, "
                f"code: {response.code}, "
                f"msg: {response.msg}, "
                f"log_id: {response.get_log_id()}")
            return None
        print(response.data.app)

        return response.data.app.app_token

    def _search_records(self, app_token: str = APP_TOKEN, table_id: str = TABLE_ID,
                        page_size: int = 500, page_token: str = "") -> Optional[SearchAppTableRecordResponseBody]:
        """
        搜索表数据
        https://open.feishu.cn/document/docs/bitable-v1/app-table-record/search
        Args:
            app_token: 多维表格token
            table_id: 数据表ID
            page_size: 分页大小，默认500
            
        Returns:
            List[AppTableRecord]: 记录列表
        """
        # 构建请求体
        request_body_builder = SearchAppTableRecordRequestBody.builder()

        request_body = request_body_builder.build()

        # 构建请求
        request = SearchAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .page_size(page_size) \
            .page_token(page_token) \
            .request_body(request_body) \
            .build()

        # 发送请求
        response = self.client.bitable.v1.app_table_record.search(request)

        if not response.success():
            lark.logger.error(
                f"搜索表数据失败, "
                f"code: {response.code}, "
                f"msg: {response.msg}, "
                f"log_id: {response.get_log_id()}")
            return None

        return response.data

    def search_all_records(self, app_token: str = APP_TOKEN, table_id: str = TABLE_ID) -> List[AppTableRecord]:
        """分页获取所有表记录"""
        all_records = []
        page_token = ""
        while True:
            response = self._search_records(app_token, table_id, page_token=page_token)
            if not response:
                raise Exception(f"搜索失败: {response}")

            data = response
            all_records.extend(data.items)

            if not data.has_more:
                break
            page_token = data.page_token

        return all_records

    def _batch_create_records_once(self,
                                   records: List[Dict[str, Any]] = None,
                                   app_token: str = APP_TOKEN,
                                   table_id: str = TABLE_ID) -> Optional[BatchCreateAppTableRecordResponseBody]:
        """
        https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-record/batch_create
        单次新增多条记录（最多1000条）
        Args:
            app_token: 多维表格token
            table_id: 数据表ID
            records: 记录列表，每个记录是字段名到字段值的映射字典

        Returns:
            BatchCreateAppTableRecordResponseBody: 批量创建记录的响应数据
        """
        if records is None:
            records = []

        # 检查记录数量限制（单次最多1000条）
        if len(records) > 1000:
            lark.logger.error(f"单次新增记录数量超过限制: {len(records)} > 1000")
            return None

        if len(records) == 0:
            lark.logger.warning("记录列表为空，跳过新增操作")
            return None

        try:
            # 构建记录对象列表
            record_objects = []
            for record_data in records:
                # 构建字段映射
                fields = {field_name: field_value for field_name, field_value in record_data.items()}

                # 创建AppTableRecord对象
                record_builder = AppTableRecord.builder()
                record_builder.fields(fields)
                record_objects.append(record_builder.build())

            # 构建请求体
            request_body_builder = BatchCreateAppTableRecordRequestBody.builder()
            request_body_builder.records(record_objects)
            request_body = request_body_builder.build()

            uuid = uuid4()
                # .client_token(str(uuid)) \
            # 构建请求
            request = BatchCreateAppTableRecordRequest.builder() \
                .app_token(app_token) \
                .table_id(table_id) \
                .request_body(request_body) \
                .build()

            # 发送请求
            response = self.client.bitable.v1.app_table_record.batch_create(request)

            if not response.success():
                lark.logger.error(
                    f"新增记录失败, "
                    f"code: {response.code}, "
                    f"msg: {response.msg}, "
                    f"log_id: {response.get_log_id()}")
                return None

            lark.logger.info(f"成功新增 {len(records)} 条记录")
            return response.data

        except Exception as e:
            lark.logger.error(f"新增记录时发生异常: {str(e)}")
            return None

    def batch_create_records(self,
                             records: List[Dict[str, Any]] = None,
                             app_token: str = APP_TOKEN,
                             table_id: str = TABLE_ID,
                             batch_size: int = 1000,
                             sleep_secs: float = 0.5) -> List[Optional[BatchCreateAppTableRecordResponseBody]]:
        """
        批量新增多条记录（支持超过1000条的分批处理）

        Args:
            records: 记录列表
            app_token: 多维表格token
            table_id: 数据表ID
            batch_size: 批量插入条数
            sleep_secs: 批量插入频率控制secs

        Returns:
            List[BatchCreateAppTableRecordResponseBody]: 各批次新增结果的列表
        """
        if records is None:
            records = []

        if len(records) == 0:
            lark.logger.warning("记录列表为空，跳过批量新增操作")
            return []

        results = []
        total_records = len(records)

        # 分批处理
        for i in range(0, total_records, batch_size):
            batch_records = records[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_records + batch_size - 1) // batch_size

            lark.logger.info(f"正在处理第 {batch_num}/{total_batches} 批次，本批次 {len(batch_records)} 条记录")

            # 调用单次新增函数
            result = self._batch_create_records_once(batch_records, app_token, table_id)
            results.append(result)

            # 如果不是最后一批，可以添加短暂延迟避免频率限制
            if batch_num < total_batches:
                time.sleep(sleep_secs)

        lark.logger.info(f"批量新增完成，共处理 {total_records} 条记录，分 {len(results)} 批次")

        return results

    def _batch_update_records_once(self,
                                   records: List[AppTableRecord] = None,
                                   app_token: str = APP_TOKEN,
                                   table_id: str = TABLE_ID) -> Optional[BatchUpdateAppTableRecordResponseBody]:
        """
        批量更新表记录
        https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-record/batch_update
        Args:
            app_token: 多维表格token
            table_id: 数据表ID
            records: 记录列表

        Returns:
            bool: 是否更新成功
        """

        if records is None:
            records = []

        # 检查记录数量限制（单次最多1000条）
        if len(records) > 1000:
            lark.logger.error(f"单次新增记录数量超过限制: {len(records)} > 1000")
            return None

        if len(records) == 0:
            lark.logger.warning("记录列表为空，跳过新增操作")
            return None

        request_body = BatchUpdateAppTableRecordRequestBody.builder() \
            .records(records) \
            .build()

        request = BatchUpdateAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .request_body(request_body) \
            .build()
        response = self.client.bitable.v1.app_table_record.batch_update(request)
        print(response)
        if not response.success():
            lark.logger.error(
                f"更新表记录失败, "
                f"code: {response.code}, "
                f"msg: {response.msg}, "
                f"log_id: {response.get_log_id()}")
            return None
        print(response.data)
        return response.data

    def batch_update_records(self,
                             records: List[AppTableRecord] = None,
                             app_token: str = APP_TOKEN,
                             table_id: str = TABLE_ID,
                             batch_size: int = 1000,
                             sleep_secs: float = 0.5) -> List[Optional[BatchUpdateAppTableRecordResponseBody]]:
        """
        批量更新多条记录（支持超过1000条的分批处理）

        Args:
            records: 记录列表
            app_token: 多维表格token
            table_id: 数据表ID
            batch_size: 批量插入条数
            sleep_secs: 批量插入频率控制secs

        Returns:
            List[BatchCreateAppTableRecordResponseBody]: 各批次新增结果的列表
        """
        if records is None:
            records = []

        if len(records) == 0:
            lark.logger.warning("记录列表为空，跳过批量更新操作")
            return []

        results = []
        total_records = len(records)
        # 分批处理
        for i in range(0, total_records, batch_size):
            batch_records = records[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_records + batch_size - 1) // batch_size

            lark.logger.info(f"正在处理第 {batch_num}/{total_batches} 批次更新，本批次 {len(batch_records)} 条记录")

            # 调用单次新增函数
            result = self._batch_update_records_once(batch_records, app_token, table_id)
            results.append(result)

            # 如果不是最后一批，可以添加短暂延迟避免频率限制
            if batch_num < total_batches:
                time.sleep(sleep_secs)  # 延迟1秒

        lark.logger.info(f"批量更新完成，共处理 {total_records} 条记录，分 {len(results)} 批次")

        return results


lark_util = LarkUtil()


def list_files_test():

    # 获取文件列表
    print("获取文件列表...")
    # 替换为有效的文件夹token
    files = lark_util.list_files()
    print(files)

    if files is None:
        print("获取文件列表失败")
        exit(1)

    print(f"共找到 {len(files)} 个文件:")
    for file in files:
        print(f"  - 文件名: {file.name}, 文件类型: {file.type}, 文件token: {file.token}")


def copy_app_test():

    # 获取文件列表
    print("复制多维表格文档...")
    # 替换为有效的文件夹token
    files = lark_util.copy_app("测试多维表格-copy")
    print(files)
    if files is None:
        print("复制多维表格失败")
    print(files)


def search_record_test():

    # 获取文件列表
    print("获取多维表格数据...")
    # 替换为有效的文件夹token
    response = lark_util._search_records()
    if response is None:
        print("获取多维表格数据失败")
    print(response)

    print(f"共 {response.total} 条记录, 本次查询返回 {len(response.items)} 条记录,"
          f" has_more={response.has_more} next_page_token={response.page_token}")
    for record in response.items:
        print(f"  - {record}")
        print(f"  - {record.record_id}, {record.fields}, {record.record_url}")


def search_all_records_test():

    # 获取所有记录
    records = lark_util.search_all_records()
    print(f"共 {len(records)} 条记录")
    for record in records[:15]:
        print(f"  - {record}")
        print(f"  - {record.record_id}, {record.fields}, {record.record_url}")


def test_update_record_batch():
    records = lark_util.search_all_records()
    req_records = []
    for record in records:
        new_record = AppTableRecord.builder() \
            .record_id(record.record_id) \
            .fields({}) \
            .build()
        new_record.fields["涨跌"] = 11
        new_record.fields["地区"] = '北京'
        new_record.fields["CODE"] = '123'
        print(new_record.fields)
        req_records.append(new_record)

    lark_util.batch_update_records(req_records)
    print(f"共 {len(records)} 条记录")


def test_records_batch_create():
    records = [{
        '价格': 99.88 + i
    } for i in range(11)]
    res = lark_util.batch_create_records(records)
    print(res)
    for r in res:
        print(r.records)
        for c in r.records:
            print(c.record_id, c.fields)


# 测试用例
if __name__ == "__main__":
    # search_record_test()
    # search_all_records_test()
    # test_update_record_batch()
    test_records_batch_create()

"""
2. 解析记录为dataframe（？主要获取key）
3. dataframe转记录（字段维护）
4. 通过dataframe创建/更新数据，指定key
"""
