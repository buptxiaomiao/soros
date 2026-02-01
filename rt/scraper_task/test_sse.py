
from env import proxy_conf
import requests

url = "https://81.push2his.eastmoney.com/api/qt/stock/trends2/sse?fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f17&fields2=f51,f52,f53,f54,f55,f56,f57,f58&mpi=1000&ut=fa5fd1943c7b386f172d6893dbfba10b&secid=0.920088&ndays=2&iscr=0&iscca=0&wbp2u=1849325530509956|0|1|0|web"
proxies = proxy_conf


def fetch_with_proxy():
    try:
        # 使用 Session 保持连接环境
        with requests.Session() as session:
            # 必须设置 timeout，防止代理死掉后程序无限挂起
            # 这里的 timeout 是指“多久没收到新数据就断开”
            with session.get(url, proxies=proxies, stream=True, timeout=60) as r:
                for line in r.iter_lines():
                    if line:
                        print(line.decode('utf-8'))
    except (requests.exceptions.ProxyError, requests.exceptions.ConnectionError):
        print("代理失效或 IP 变更导致连接中断，准备触发重连...")
        # 在这里触发重连逻辑


if __name__ == "__main__":
    fetch_with_proxy()


