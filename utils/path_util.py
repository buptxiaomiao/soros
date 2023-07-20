# coding: utf-8

import os
import time
from utils.now import Now


class PathUtil(object):

    @classmethod
    def get_root_abs_path(cls):
        cls_path = os.path.abspath(__file__)
        util_path = os.path.dirname(cls_path)
        soros_path = os.path.dirname(util_path)
        return soros_path
        # cls_path_list = os.path.split(cls_path)
        # cls_dir = cls_path_list[0]
        # return cls_dir.rsplit('/', 1)[0]

    @classmethod
    def get_sql_template_dir(cls, cata=None):
        root_path = cls.get_root_abs_path()
        path = os.path.join(root_path, 'sql_template', cata or '')
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    @classmethod
    def get_sql_file_dir(cls):
        root_path = cls.get_root_abs_path()
        path = os.path.join(root_path, 'sql_files')
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    @classmethod
    def get_data_file_dir(cls):
        root_path = cls.get_root_abs_path()
        path = os.path.join(root_path, 'data_files')
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    @classmethod
    def get_sql_template_file_name(cls, file_name, cata=None):
        """
        模板文件名
        :param file_name: stock_basic.sql
        :param cata: 默认None, FOR ods文件夹
        :return: xxxx/sql_template/stock_basic.sql
        """
        dir_path = cls.get_sql_template_dir(cata)
        file_path = os.path.join(dir_path, file_name)
        return file_path

    @classmethod
    def get_data_file_name(cls, file_name, suffix=None):
        """
        数据文件名
        :param suffix:
        :param file_name: stock_basic.csv
        :return: xxx/data_files/stock_basic_20230713_0001.SZ_xxxxx.csv
        """
        dir_path = cls.get_data_file_dir()
        f_list = file_name.split('.')
        suf = '' if suffix is None else suffix + '_'
        f_name = f_list[0] + '_' + Now().datekey + '_' + suf + str(int(time.time())) + '.' + f_list[1]
        file_path = os.path.join(dir_path, f_name)
        return file_path

    @classmethod
    def get_data_file_ambiguous_name(cls, file_name):
        """
        数据文件模糊名
        :param file_name: stock_basic.sql
        :return: xxx/data_files/stock_basic_20230713*.sql
        """
        dir_path = cls.get_data_file_dir()
        f_list = file_name.split('.')
        f_name = f_list[0] + '_' + Now().datekey + "*." + f_list[1]
        return os.path.join(dir_path, f_name)

    @classmethod
    def get_sql_file_name(cls, file_name):
        """
        sql文件名
        :param file_name: stock_basic.sql
        :return: xxx/sql_files/stock_basic_20230713.sql
        """
        dir_path = cls.get_sql_file_dir()
        f_list = file_name.split('.')
        f_name = f_list[0] + '_' + Now().datekey + '.' + f_list[1]
        return os.path.join(dir_path, f_name)


if __name__ == '__main__':
    p = PathUtil
    print(p.get_root_abs_path())
    print(p.get_sql_template_dir())
    print(p.get_sql_template_file_name('stock_basic.sql'))
    print(p.get_sql_file_dir())
    print(p.get_sql_file_name('stock_basic.sql'))
    print(p.get_data_file_dir())
    print(p.get_data_file_name('stock_basic.csv'))
    print(p.get_data_file_ambiguous_name('stock_basic.csv'))
