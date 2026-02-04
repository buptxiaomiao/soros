import duckdb
from socketserver import ThreadingMixIn
from buenavista.backends.duckdb import DuckDBConnection
from buenavista.postgres import BuenaVistaServer


# 开启多线程模式，防止网络 IO 层面排队
class ThreadedBVServer(BuenaVistaServer, ThreadingMixIn):
    daemon_threads = True


def start_server():
    # 你的默认数据库文件
    db_path = ":memory:"

    # 建议：手动创建一个原生连接，并开启一些性能参数
    # DuckDB 0.9.x+ 之后并发能力有所提升
    native_conn = duckdb.connect(db_path)

    # 2. 核心步骤：挂载其他数据库文件
    # 假设你有名为 db1.duckdb 和 db2.duckdb 的文件
    # 执行后，你可以在 SQL 中通过 db1.table_name 访问
    try:
        native_conn.execute("ATTACH '~/test/test.db' AS db1")
        print("✅ 已成功挂载外部数据库: test.db")
    except Exception as e:
        print(f"⚠️ 挂载失败（请检查路径或文件锁）: {e}")

    # 3. 初始化 Adapter
    # 在 0.5.0 中，DuckDBConnection 接受一个 duckdb.DuckDBPyConnection 对象
    db_adapter = DuckDBConnection(native_conn)

    # 启动服务
    # 127.0.0.1 仅限本地访问，如果需要局域网访问请改 0.0.0.0
    server = ThreadedBVServer(("127.0.0.1", 5433), db_adapter)

    print(f"✅ Buena Vista 0.5.0 已启动")
    print(f"📍 地址: 127.0.0.1:5433")
    print(f"文件: {db_path}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        print("\n服务已停止")


if __name__ == "__main__":
    start_server()