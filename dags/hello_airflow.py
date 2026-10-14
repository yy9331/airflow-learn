"""第一个 DAG:用于第 1 阶段 UI 练习(手动 Trigger,观察 running -> success)。

放在 dags/ 目录下,Airflow 的 dag-processor 会自动发现它。
"""

import time
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hello_airflow",
    schedule=None,  # 不自动调度,只能手动触发 —— 最适合练习
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["learn"],
)
def hello_airflow():

    @task
    def prepare() -> list:
        print("准备 ETF 代码:['SPY', 'QQQ', '510300']")
        return ["SPY", "QQQ", "510300"]

    @task
    def fetch(symbols: list) -> dict:
        time.sleep(3)  # 故意等 3 秒,方便在 UI 上抓到 running 状态
        return {s: 100.0 for s in symbols}

    @task
    def report(prices: dict):
        for symbol, price in prices.items():
            print(f"{symbol} = {price}")

    @task
    def oops(prices: dict):
        x = len(prices)
        return prices

    report(oops(fetch(prepare())))  # 函数嵌套调用 = 任务依赖关系


hello_airflow()
