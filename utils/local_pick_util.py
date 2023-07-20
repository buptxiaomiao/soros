# coding: utf-8

import pickle
import os


class LocalPickleUtil(object):

    def __init__(self, cls):
        if isinstance(cls, type(type)):
            self.name = cls.__name__
        else:
            self.name = str(cls)
        self.file_name = f'../ts/data/{self.name}.pickle'
        self.data = self._init_data()

    def _init_data(self):
        if not os.path.exists(self.file_name):
            return {}
        else:
            with open(self.file_name, 'rb') as f:
                return pickle.load(f)

    def reload(self):
        self.data = self._init_data()

    def add_pickle(self, dt, code=''):
        if code in self.data:
            self.data[code].add(dt)
        else:
            s = set()
            s.add(dt)
            self.data[code] = s

    def check_pickle_exist(self, dt, code=''):
        if code not in self.data:
            return False
        if dt not in self.data[code]:
            return False
        return True

    def commit(self):
        with open(self.file_name, 'wb') as f:
            pickle.dump(self.data, f)

    @classmethod
    def remove(cls, name, code, dt_list):
        a = cls(name)
        if code in a.data:
            for i in dt_list:
                a.data[code].remove(i)
        a.commit()
        del a

    def __del__(self):
        print("触发del")


if __name__ == '__main__':
    a = LocalPickleUtil(LocalPickleUtil)
    dtt = 20230101
    print(a.check_pickle_exist(dtt))
    a.add_pickle(dtt)
    print(a.check_pickle_exist(dtt))
    a.commit()

    LocalPickleUtil.remove(LocalPickleUtil, '', [dtt])
    # a.reload()
    # print(a.check_pickle(dtt))
