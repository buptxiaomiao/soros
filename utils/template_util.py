# coding: utf-8
# https://cheetahtemplate.org/users_guide/index.html

from Cheetah.Template import Template
from utils.now import Now
from utils.path_util import PathUtil


class TemplateUtil(object):

    def __init__(self, file_name, search_list=None, cata=None):
        self.file_name = file_name
        self.file_path = PathUtil.get_sql_template_file_name(file_name, cata)
        self.output_path = PathUtil.get_sql_file_name(file_name)
        self.search_list = search_list or dict()
        if 'now' not in self.search_list:
            self.search_list['now'] = Now()

    @property
    def _tpl_str(self):
        with open(file=self.file_path, encoding="utf-8") as f:
            return f.read()

    def update_search_list(self, k=None, v=None, **kwargs):
        if k and v:
            self.search_list[k] = v
        if kwargs:
            self.search_list.update(kwargs)

    @property
    def sql(self):
        t = Template(self._tpl_str, searchList=self.search_list)
        return str(t)

    def write_and_get_result_sql_path(self):
        with open(self.output_path, 'w') as f:
            f.write(self.sql)
        return self.output_path


if __name__ == '__main__':
    t = TemplateUtil('stock_basic.sql', cata='ods', search_list={'now': Now(), 'data_file_path': 'fffff.abc'})
    # t.update_search_list(**{'now': Now().delta(10)})
    print(t.sql)

