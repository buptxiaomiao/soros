import os
import time
import queue
import logging
import sqlite3
import threading
import random
import requests
import datetime
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler


class TestThreadPool:
    def __init__(self, max_workers=60, batch_size=500):
        self.db_path = 'test.db'
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.db_queue = queue.Queue(maxsize=1000)
        self._thread_local = threading.local()
        self.subclass_name = self.__class__.__name__

        self.logger = self._setup_logging()

    def _setup_logging(self):
        logger = logging.getLogger(self.subclass_name)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            # 自动生成日志文件名：Min5Scraper.log
            handler = RotatingFileHandler(f"{self.subclass_name}.log", maxBytes=5 * 1024 * 1024, backupCount=2)
            handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
            logger.addHandler(handler)
        return logger

    def _get_read_conn(self):
        """获取线程复用的只读连接，增加超时配置"""
        if not hasattr(self._thread_local, "conn"):
            # timeout=60 极大程度缓解 database is locked
            conn = sqlite3.connect(self.db_path, timeout=60)
            conn.execute("PRAGMA journal_mode=WAL;")
            self._thread_local.conn = conn
        return self._thread_local.conn

    def _safe_commit(self, conn, sql, batch):
        """带重试机制的数据库提交"""
        for i in range(5):
            try:
                conn.executemany(sql, batch)
                conn.commit()
                return True
            except sqlite3.OperationalError as e:
                if "locked" in str(e):
                    # 遭遇锁定，随机避让后重试
                    wait_time = (i + 1) * random.uniform(0.1, 0.3)
                    time.sleep(wait_time)
                    continue
                self.logger.error(f"SQL执行错误: {e}")
                break
        return False

    def _sqlite_saver(self):
        """独立写入线程"""
        conn = sqlite3.connect(self.db_path, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(self.get_create_table_sql())

        sql = self.get_insert_sql()
        batch = []

        while True:
            data = self.db_queue.get()
            if data is None: break

            if isinstance(data, list):
                batch.extend(data)
            else:
                batch.append(data)

            if len(batch) >= self.batch_size:
                self._safe_commit(conn, sql, batch)
                batch = []
            self.db_queue.task_done()

        if batch:
            self._safe_commit(conn, sql, batch)
        conn.close()

    def _worker(self, code):
        """任务执行器：带 API 重试逻辑"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.fetch_logic(code)
                if result:
                    self.db_queue.put(result)
                return True, code
            except Exception as e:
                return False, f"{code} 程序异常: {str(e)}"

    def run(self, stock_list):
        self.logger.info(f">>> 启动任务:")
        t = threading.Thread(target=self._sqlite_saver, daemon=True)
        t.start()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._worker, code): code for code in stock_list}
            for i, future in enumerate(as_completed(futures)):
                success, msg = future.result()
                if not success:
                    self.logger.warning(msg)
                if (i + 1) % self.batch_size == 0:
                    self.logger.info(f"进度: {i + 1}/{len(stock_list)}")

        self.db_queue.put(None)
        t.join()
        self.logger.info(f">>> 任务结束:")

    @abstractmethod
    def get_create_table_sql(self):
        return """CREATE TABLE IF NOT EXISTS `my_table` (code text, utime TEXT);"""

    @abstractmethod
    def get_insert_sql(self):
        return """INSERT INTO `my_table` (code, utime) VALUES (?, ?)"""

    @abstractmethod
    def fetch_logic(self, code):
        time.sleep(1)
        return code, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == '__main__':
    cls = TestThreadPool(max_workers=100, batch_size=50)
    l = [i for i in range(200)]
    cls.run(l)
