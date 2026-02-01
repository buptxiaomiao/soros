import time
import sys
import queue
import logging
import sqlite3
import threading
import random
import datetime
import requests
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler
from rt.store.SqliteUtil import SqliteHelper, DBType


class StockScraperBase(ABC):

    def __init__(self, db_type: DBType, proxy_conf=None, max_workers=30, batch_size=100):
        # DB类型
        self.db_type = db_type
        self.trade_date = datetime.datetime.now().strftime("%Y-%m-%d")
        # 并发配置
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.db_queue = queue.Queue(maxsize=10000)
        self._thread_local = threading.local()
        self.subclass_name = self.__class__.__name__

        self.log_prefix = f"{self.subclass_name}.{self.db_type.name}"

        self.logger = self._setup_logging()
        self.session = self._init_session(proxy_conf)

    def _init_session(self, proxy_conf):
        s = requests.Session()
        if proxy_conf:
            s.proxies = proxy_conf
        adapter = requests.adapters.HTTPAdapter(pool_connections=self.max_workers, pool_maxsize=self.max_workers)
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        return s

    def _setup_logging(self):
        logger = logging.getLogger(self.subclass_name)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            handler = RotatingFileHandler(f"{self.subclass_name}.log", maxBytes=1 * 1024 * 1024, backupCount=2)
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            # 2. 配置标准输出 Handler (输出到控制台)
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        return logger

    def __get_new_db_conn(self):
        sqlite_helper = SqliteHelper(db_type=self.db_type, trade_date=self.trade_date)
        conn = sqlite_helper.get_connection()
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _get_read_conn(self):
        """获取线程复用的只读连接，增加超时配置"""
        if not hasattr(self._thread_local, "conn"):
            # timeout=60 极大程度缓解 database is locked
            conn = self.__get_new_db_conn()
            self._thread_local.conn = conn
        return self._thread_local.conn

    def _safe_commit(self, conn, sql, batch):
        """带重试机制的数据库提交"""
        for i in range(5):
            try:
                conn.executemany(sql, batch)
                conn.commit()
                self.logger.info(f"save success. num={len(batch)}")
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
        conn = self._get_read_conn()
        sql_list = self.get_create_table_sql_list()
        for sql in sql_list:
            conn.execute(sql)

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
            except requests.exceptions.RequestException as e:
                # 针对网络错误的指数退避
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 2 + random.random()
                    time.sleep(wait)
                    continue
                return False, f"{self.log_prefix} {code} API失败: {str(e)}"
            except Exception as e:
                return False, f"{self.log_prefix} {code} 程序异常: {str(e)}"

    def run(self, stock_list):
        self.logger.info(f">>> 启动任务: {self.log_prefix} total_size={len(stock_list)}")
        t = threading.Thread(target=self._sqlite_saver, daemon=True)
        t.start()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._worker, code): code for code in stock_list}
            for i, future in enumerate(as_completed(futures)):
                success, msg = future.result()
                if not success:
                    self.logger.warning(msg)
                if (i + 1) % 100 == 0:
                    self.logger.info(f"任务: {self.log_prefix} 进度: {i + 1}/{len(stock_list)}")

        self.db_queue.put(None)
        t.join()
        self.logger.info(f">>> 任务结束: {self.log_prefix} total_size={len(stock_list)}")

    @abstractmethod
    def get_create_table_sql_list(self):
        raise NotImplementedError

    @abstractmethod
    def get_insert_sql(self):
        raise NotImplementedError

    @abstractmethod
    def fetch_logic(self, code):
        raise NotImplementedError