import pickle
import os

class CacheManager:

    def __init__(self, file_path):
        self.file_path = file_path
        self.data = self.read_cache()

    def write_cache(self):
        """
        将当前self.data写入缓存文件。
        """
        try:
            with open(self.file_path, 'wb') as f:
                pickle.dump(self.data, f)
            print("数据已写入缓存。")
        except Exception as e:
            print(f"写入缓存时发生错误：{e}")

    def read_cache(self):
        """
        从缓存文件读取数据。

        :return: 缓存的数据，如果文件不存在或读取失败，则返回一个空字典。
        """
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"读取缓存时发生错误：{e}")
        else:
            print("缓存文件不存在。")
        return {}

    def get(self, key, default=None):
        """
        获取缓存中的值。

        :param key: 要获取的键。
        :param default: 如果键不存在，返回的默认值。
        :return: 键对应的值或默认值。
        """
        return self.data.get(key, default)

    def update(self, key, value):
        """
        更新缓存中的值。

        :param key: 要更新的键。
        :param value: 新的值。
        """
        self.data[key] = value
        self.write_cache()

    def __getitem__(self, key):
        """
        通过字典键值访问的方式来获取缓存数据。

        :param key: 要获取的键。
        :return: 键对应的值。
        """
        return self.get(key)

    def __setitem__(self, key, value):
        """
        通过字典键值访问的方式来设置缓存数据。

        :param key: 要设置的键。
        :param value: 新的值。
        """
        self.update(key, value)


# 使用示例
if __name__ == "__main__":
    # 假设我们有一个本地文件路径
    file_path = 'cache.pkl'

    # 创建CacheManager实例
    cache_manager = CacheManager(file_path)

    # 写入缓存
    cache_manager['key1'] = 'value1'
    cache_manager.update('key2', 'value2')

    # 读取缓存
    print(cache_manager['key1'])  # 应该打印出：value1
    print(cache_manager.get('key2'))  # 应该打印出：value2
    print(cache_manager.get('key3', 'default'))  # 应该打印出：default