# Apache Airflow 学习路线图

围绕"每日 ETF 行情管道"实战项目分阶段练习:抓行情(前 4 阶段用模拟数据,不依赖外网)→ 算指标 → 写入 PostgreSQL → 数据感知调度。每个知识点配一个可直接跑的 DAG 或命令,每完成一个阶段把踩过的坑补进对应"易错点"。

> 环境用 Docker Compose 一键启动(官方 quick-start)。注意:DAG 文件是 **Python** 写的——Airflow 以 Python 为一等公民,本项目脚本语言固定为 Python。

## 当前进度

| 阶段 | 状态 | 备注 |
|------|------|------|
| 第 1 阶段:核心概念与环境搭建 | 🔄 进行中 | UI 四步 + CLI 练习已完成;小作业(并行触发 / 失败排错)进行中 |
| 第 2 阶段:第一个 DAG 与 TaskFlow API | ⏳ 未开始 | @dag/@task、依赖、触发 |
| 第 3 阶段:调度、回填与重试 | ⏳ 未开始 | schedule、catchup、backfill、retries |
| 第 4 阶段:数据传递与控制流 | ⏳ 未开始 | XCom、分支、TaskGroup、动态映射 |
| 第 5 阶段:连接外部系统 | ⏳ 未开始 | Connections、Hooks、Sensor、写 PostgreSQL |
| 第 6 阶段:生产实践(选学) | ⏳ 未开始 | Executor 选型、监控告警、备份、最佳实践 |

---

## 第 1 阶段:核心概念与环境搭建

### 1.1 概念地图(先背下来,后面全部围绕它)

| 概念 | 一句话解释 | 类比 |
|------|-----------|------|
| **DAG** | 一个工作流:任务 + 依赖关系组成的有向无环图 | 一张流程图 |
| **Task / Operator** | DAG 里的一个节点;Operator 是任务的"模板" | 流程图里的一个框 |
| **DAG Run** | DAG 的一次执行实例 | 按了一次"开始" |
| **Task Instance** | Task 在某次 DAG Run 里的一次执行 | 某一次跑到的那个框 |
| **Scheduler** | 常驻进程:解析 DAG 文件、按计划生成 DAG Run | 闹钟 + 排班员 |
| **Executor** | 决定任务"在哪跑":Local / Celery / Kubernetes | 车间主任 |
| **Worker** | 真正干活执行任务的进程 | 工人 |
| **API Server + UI** | Web 界面:看状态、手动触发、看日志(3.0 起替代 webserver) | 控制台 |
| **Metadata DB** | 存所有状态/历史(默认 PostgreSQL) | 账本 |
| **Triggerer** | 支持 deferrable 任务挂起等待的常驻进程 | 传呼机 |

### 1.2 知识点清单

| 知识点 | 干什么 | 状态 |
|--------|--------|------|
| Docker Compose 部署 | 一键起全套服务 | ✅ |
| UI 四大视图 | DAGs / Grid(3.x 叫 Overview) / Graph / Gantt | ✅ |
| 手动触发 DAG Run | UI 的 Trigger 按钮 | ✅ |
| 看任务日志 | 概述页点任务方块 → Logs | ✅ |
| CLI 基本命令 | `airflow dags list`、`airflow tasks list`、`airflow dags trigger` | ✅ |
| 健康检查 | `curl localhost:8080/api/v2/monitor/health` | ✅ |

### 1.3 动手步骤

> **项目结构(2026-10-14 已收拢,全部在本目录 `airflow-learn/` 内):**
> ```
> airflow-learn/
> ├── docker-compose.yaml   # 编排文件(官方 3.3.2 + 健康检查 CPU 优化,见 1.4 踩坑记录)
> ├── .env                  # AIRFLOW_UID=501
> ├── dags/                 # 你的 DAG 放这里,挂载到容器 /opt/airflow/dags
> │   └── hello_airflow.py  # 第一个 DAG,手动触发练习用
> ├── logs/  config/  plugins/   # 容器挂载目录,自动生成
> └── LEARNING_PATH.md
> ```
> compose 里用 `${AIRFLOW_PROJ_DIR:-.}/dags` 相对路径挂载,所以**所有 `docker compose` 命令都要在本目录下执行**;项目名 = 目录名 = `airflow-learn`,容器名形如 `airflow-learn-airflow-scheduler-1`。
> DAG 运行状态/历史存在 compose 自带 postgres 容器的命名卷 `airflow-learn_postgres-db-volume` 里,删容器不丢,`docker compose down -v` 才会清空。

```bash
# 1) 下载官方 docker-compose.yaml(版本号以官网 Getting Started 页面给出的最新 3.x 为准)
#    https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/<最新3.x版本>/docker-compose.yaml'

# 2) 设置 Linux/Mac 必需的环境变量,生成 .env
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 3) 初始化(建 metadata 库、建管理员账号)
docker compose up airflow-init

# 4) 启动全部服务
docker compose up -d

# 5) 浏览器打开 http://localhost:8080 (账号 airflow / airflow)
```

UI 操作练习(示例 DAG 已关闭,改用自建的 hello_airflow):
1. DAGs 页面:找到 hello_airflow → 手动 Trigger → 观察状态从 running 到 success
2. 点 Dag ID 进详情页 → **概览(Overview)** 标签:看每次 DAG Run 里各任务的状态方块(3.x 把 2.x 的 "Grid 视图" 改名成了 Overview)
3. **图表(Graph)**:不在 DAG 页顶部标签栏,要点进 **执行记录** 里某一次具体运行,运行内部才有"图表/甘特图/详情/代码"标签,看任务依赖连线
4. 概述页点某次 Run 里的任务方块 → Logs:看任务日志

```bash
# CLI 练习(scheduler 容器里有完整 CLI)
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow tasks list <dag_id>
curl -s localhost:8080/api/v2/monitor/health
```

### 1.4 经典坑预警(练习时重点观察,踩过的坑补在这里)

- `AIRFLOW_UID` 没设置 → 容器写日志目录报 permission denied
- 本机 Docker 内存给太少(< 4GB)→ 服务集体重启
- example DAG 太多刷屏 → compose 里设 `AIRFLOW__CORE__LOAD_EXAMPLES=False`
- DAG 文件夹没挂载对 → UI 一直显示 "No DAGs found"

#### 已踩坑记录(2026-10-13):CPU 飙到 760%/800%,所有服务 unhealthy

**现象:** 启动后 Docker Desktop 总 CPU 700%+,scheduler 心跳一次要 30~84 秒,健康检查全部 10 秒超时。

**两层原因叠加:**

1. **无关项目抢占 CPU**:一个 15 个月前的 Supabase 本地栈(`*_edge-functions`,10+ 容器)同时在跑,独占 ~440%。Airflow 全家桶再要 ~660%,总需求 ~1100% > 8 核 VM 的 800% 上限 → 互相饿死。排查第一步永远是 `docker stats --no-stream` 看**全部容器**,不要只盯 Airflow。
2. **健康检查周期性冷启 Python**:官方 compose 给 scheduler/dag-processor/triggerer 的健康检查是 `airflow jobs check ...`,每 30 秒一次;worker 是 `celery inspect ping`。这俩命令每次都要冷启动完整 Python+Airflow 栈,单核 100% 烧约 10 秒,4 个容器同步触发就是 ~400% 尖峰。CPU 一旦被抢占,检查超时 → unhealthy → 进程排队 → 持续饱和(雪崩)。另外 `LOAD_EXAMPLES=true` 时 DAG processor 要反复解析 74 个示例 DAG,进一步放大。

**修复(已执行):**

- 停掉无关 Supabase 栈(容器和数据卷保留):`docker stop $(docker ps -q --filter "name=edge-functions")`
- compose 里 `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`
- scheduler 健康检查改成内置 HTTP 端口(瞬时返回,零开销):`curl --fail http://localhost:8974/health`
- dag-processor/triggerer/worker 没有可用 HTTP 健康端点,只能保留 CLI/ping 检查,间隔 30s → **60s**,timeout 10s → 30s,start_period → 60s
- 结果:稳态每服务 0.2~5%;每分钟一次约 10 秒的检查尖峰(峰值 ~400%),7 个服务全部 healthy

**排查手法:** `docker stats` 看总览 → `docker logs` 找 "Heartbeat recovered after N seconds" / "exceeding the timeout" → 容器内没 top/ps 时,用纯 Python 读 `/proc/[pid]/stat` 两次采样定位忙进程(记得 `docker exec -i`,stdin 才能传进去)。

#### 已踩坑记录(2026-10-14):UI 显示 "0 Dags / 找不到 Dags"

**现象:** 关掉示例 DAG(`LOAD_EXAMPLES=false`)后,UI 里一个 DAG 都看不到;`airflow dags list` 先返回空、再报 `_PriorityWeightStrategyNotRegistered` 反序列化错误。

**三层原因:**

1. **示例 DAG 关了,自己的 DAG 还没写**:`dags/` 目录是空的 → 自然 0 个 DAG。这不是 bug,是预期行为。
2. **元数据库残留 116 条示例 DAG**:之前 `LOAD_EXAMPLES=true` 时示例已被序列化进 DB;改成 false 后这些记录既不刷新也不清理,其中一条引用了示例插件 `decreasing_priority_weight_strategy`,导致 `dags list` 反序列化崩溃。UI 也因此列不出东西。
3. **新文件发现要等 5 分钟**:`AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` 默认 **300 秒**,写了新 DAG 文件后要等下一轮扫描才出现。

**修复(已执行):**

- 清掉残留示例数据(当前表里全是示例,直接清空即可):
  ```sql
  DELETE FROM task_instance; DELETE FROM dag_run; DELETE FROM dag;   -- dag 的子表都是 ON DELETE CASCADE
  ```
- 写第一个 DAG `dags/hello_airflow.py`(`schedule=None` 只能手动触发,`fetch` 里 `sleep(3)` 方便抓 running 状态)
- 重启 dag-processor 强制立即扫描(`docker restart airflow-learn-airflow-dag-processor-1`),否则要等满 300 秒
- `airflow dags unpause hello_airflow`(因为 `DAGS_ARE_PAUSED_AT_CREATION=true`,新建的 DAG 默认暂停)
- 验证:CLI 触发后 `dag_run=success`,`prepare`/`fetch`/`report` 三个 task 全 `success`

**排查手法:** `airflow dags list` + `airflow dags list-import-errors` 看解析是否通过 → `psql -U airflow -d airflow` 直接查 `dag` / `serialized_dag` / `dag_run` / `task_instance` 四张表核对真实状态,比只看日志快。

#### 作业记录(2026-10-14):并行触发 + 失败排错

- 连续触发 5 次 → 多条 Run 同时 running:CeleryExecutor 多 worker 并行,DAG 默认 `max_active_runs=16`,不排队
- 故意 `1/0` 炸掉 `oops` 后:`prepare`/`fetch` 照常 success,`oops`=failed,**`report`=upstream_failed(根本没被执行,不是 failed)**。排查口诀:看到 upstream_failed 别点它自己的日志(是空的),直接去看它的上游为什么挂
- **Clear(清除)入口(3.x UI)**:DAG Run 详情页"任务实例"表 → 右侧"任务操作"列的图标(悬停看提示)里有清除;或点蓝色任务 ID 进任务实例详情页,顶部操作区也有;概述页点任务方块 → 侧边面板里同样有
- **Clear vs 重新 Trigger**:Clear 只把本 Run 内的任务状态重置为 None 并重新调度(默认连带下游),还是同一次 Run、同一历史;Trigger 是全新 Run 从头跑。修好代码后重跑失败部分 → 用 Clear
- 小观察:run_id 里是 UTC(`14:33`)而界面显示本地 `22:33` —— Airflow 内部全 UTC,第 3 阶段玩调度时会再碰到

---

## 第 2 阶段:第一个 DAG 与 TaskFlow API

### 2.1 知识点清单

| 知识点 | 干什么 | 状态 |
|--------|--------|------|
| `@dag` / `@task` 装饰器 | TaskFlow 写法(现代标准,函数即任务) | ⏳ |
| 传统 Operator 写法 | `PythonOperator(python_callable=...)` 对照理解 | ⏳ |
| 依赖 `>>` | 定义执行顺序 | ⏳ |
| `schedule` / `start_date` / `catchup` | DAG 三大调度参数 | ⏳ |
| `airflow dags test` | 本地单跑一次 DAG,不过调度器(调试神器) | ⏳ |
| UI 手动触发 vs 计划触发 | 两者的区别 | ⏳ |

### 2.2 示例 DAG(直接跑)

`dags/market_pipeline.py`:

```python
from datetime import datetime
from airflow.sdk import dag, task   # Airflow 3.x;2.x 写法: from airflow.decorators import dag, task

@dag(
    dag_id="market_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,           # 不补历史,后面第 3 阶段专门玩这个
    tags=["learn"],
)
def market_pipeline():

    @task
    def fetch_prices() -> dict:
        # 前 4 阶段用模拟数据,不依赖外网
        return {"ETF510300": 4.52, "SPY": 590.10}

    @task
    def compute_ma(prices: dict) -> dict:
        # 假装这是 5 日均线
        return {k: round(v * 0.99, 2) for k, v in prices.items()}

    @task
    def report(ma: dict):
        for k, v in ma.items():
            print(f"{k} 均线 = {v}")

    report(compute_ma(fetch_prices()))   # 函数调用顺序 = 依赖关系

market_pipeline()
```

验证:

```bash
# 不过调度器,本地立刻跑一遍(学习期最快验证方式)
docker compose exec airflow-scheduler airflow dags test market_pipeline 2026-10-08

# 或在 UI 里 trigger 后去 Graph/Log 看
```

### 2.3 对照:传统 Operator 写法

```python
from airflow.providers.standard.operators.python import PythonOperator

def _fetch():
    return {"SPY": 590.10}

fetch = PythonOperator(task_id="fetch", python_callable=_fetch, dag=dag)
# 依赖:fetch >> compute
```

> TaskFlow 的函数返回值自动通过 XCom 传给下游,传统写法要手动 `provide_context` 和 `ti.xcom_pull`——这是 TaskFlow 最大的省心点。

### 2.4 经典坑预警

- `start_date` 是过去日期 + `catchup` 默认 True → 一保存就触发补跑风暴(第 3 阶段细讲,但第 2 阶段就会踩)
- DAG 文件顶层代码**每次调度器解析都会执行** → 顶层别放重逻辑(建连接、大循环)
- `dag_id` 不能中文、不能空格
- 改了 DAG 文件 UI 没刷新 → 调度器解析有间隔(默认 30s),看 UI 右上角 "Last parsed"

### 2.5 综合小作业

给 market_pipeline 加一个任务:比较最新价与均线,打印 "金叉/死叉" 提示(纯 print 即可)。

---

## 第 3 阶段:调度、回填与重试

### 3.1 知识点清单

| 知识点 | 干什么 | 状态 |
|--------|--------|------|
| `schedule` 三种写法 | cron 串 / `@daily` / `timedelta` | ⏳ |
| data_interval 与 logical date | "这次跑的是哪段数据" 的核心概念 | ⏳ |
| `catchup` | 补不补历史区间 | ⏳ |
| `airflow dags backfill` | 手动回填指定日期段 | ⏳ |
| `retries` / `retry_delay` | 失败自动重试 | ⏳ |
| `execution_timeout` | 单任务超时上限 | ⏳ |
| Task 状态机 | pending→running→success/failed/up_for_retry/skipped | ⏳ |

### 3.2 示例与练习

schedule 写法速查:

| 写法 | 含义 |
|------|------|
| `"@daily"` / `"0 0 * * *"` | 每天 UTC 0 点 |
| `"0 9 * * 1-5"` | 工作日 9 点(cron 5 段) |
| `timedelta(minutes=30)` | 每 30 分钟 |
| `None` | 只能手动/外部触发 |

实验(观察 catchup 补跑):

```python
# 实验 A:catchup=False → 只跑最近一次
# 实验 B:catchup=True → start_date 到现在,每天补一个 DAG Run
@dag(dag_id="catchup_demo", schedule="@daily",
     start_date=datetime(2026, 10, 1), catchup=True)
```

手动回填:

```bash
docker compose exec airflow-scheduler airflow dags backfill catchup_demo -s 2026-10-01 -e 2026-10-03
```

重试实验:

```python
import random

@task(retries=3, retry_delay=timedelta(seconds=10), execution_timeout=timedelta(minutes=5))
def flaky():
    if random.random() < 0.7:
        raise ValueError("假装行情接口挂了")
    return "ok"
```

### 3.3 核心概念:logical date 不是"运行时刻"

DAG Run 的 logical date 是**数据区间的左端点**。`@daily` 在 10 月 9 日 0 点触发的 run,logical date 是 10 月 8 日——语义是"处理 8 日的数据"。任务模板里用 `{{ data_interval_start }}` / `{{ data_interval_end }}` 取区间。**先想数据区间,再看运行时间**,这是 Airflow 和 crontab 最大的思维差异。

### 3.4 经典坑预警

- 时区:`start_date` 默认 UTC,想要北京时间用 `pendulum.datetime(2026, 10, 1, tz="Asia/Shanghai")`
- `retries` 写在 `@task` 参数或 DAG 的 `default_args`,两层都写时 task 级覆盖
- backfill 在 3.x 走新的 backfill 引擎,参数和 2.x 有差异,以 `airflow dags backfill --help` 为准
- 旧版 SLA 机制在 3.x 已移除,超时告警用 `execution_timeout` + 回调/on_failure_callback 实现

---

## 第 4 阶段:数据传递与控制流

### 4.1 知识点清单

| 知识点 | 干什么 | 状态 |
|--------|--------|------|
| XCom | 任务间传小数据(元数据,不是大数据通道) | ⏳ |
| 多返回值 | TaskFlow 返回 dict/list 自动拆分 | ⏳ |
| `@task.branch` | 条件分支,选路执行 | ⏳ |
| TriggerRule | 下游在什么条件触发(all_success / one_failed / none_failed...) | ⏳ |
| TaskGroup | 任务分组成视觉/逻辑单元 | ⏳ |
| 动态映射 `.expand()` | 运行时按数据量生成 N 个并行任务 | ⏳ |
| `@task.short_circuit` | 条件为假直接短路跳过整段下游 | ⏳ |

### 4.2 示例(接着 market_pipeline 玩)

分支:涨了发提醒 A,跌了发提醒 B:

```python
from airflow.sdk import task

@task.branch
def pick_branch(price: float, ma: float):
    return "notify_rise" if price > ma else "notify_fall"

@task
def notify_rise():
    print("涨了,考虑定投")

@task
def notify_fall():
    print("跌了,按计划定投")

# pick_branch(price, ma) >> [notify_rise(), notify_fall()]
```

动态映射:一次并行处理 N 只 ETF:

```python
SYMBOLS = ["SPY", "QQQ", "510300", "IWM"]

@task
def fetch_one(symbol: str) -> dict:
    return {"symbol": symbol, "price": len(symbol) * 1.0}

prices = fetch_one.expand(symbol=SYMBOLS)   # 运行时生成 4 个并行任务实例
```

### 4.3 TriggerRule 速查

| 规则 | 触发条件 |
|------|---------|
| `all_success`(默认) | 所有上游成功 |
| `one_failed` | 任一上游失败(做告警) |
| `none_failed_min_one_success` | 没失败且至少一个成功(**分支下游必配**) |
| `all_done` | 所有上游跑完(不管成败,做清理) |

### 4.4 经典坑预警

- **XCom 只适合小对象**(存进 metadata 库),传 DataFrame/大文件要写外部存储(本地文件/S3)只传路径
- 分支任务后面下游默认 `all_success`,被跳过的分支会导致下游 skipped → 分支下游 trigger rule 要改成 `none_failed_min_one_success`
- `.expand()` 的参数必须是列表(可以是上游 XCom 动态产生),`>>` 一个映射任务 = 依赖它的所有映射实例

### 4.5 综合小作业

用 `.expand()` 对 4 只 ETF 并行 fetch,再用 TaskGroup 把 "fetch → compute" 分组,最后按涨跌分支输出提醒。

---

## 第 5 阶段:连接外部系统(Operators / Hooks / Connections / Variables)

### 5.1 知识点清单

| 知识点 | 干什么 | 状态 |
|--------|--------|------|
| Connection | 存外部系统地址+凭据(UI 或环境变量配置) | ⏳ |
| Variable | 存全局配置(API key、开关) | ⏳ |
| Hook | 连接外部系统的客户端封装 | ⏳ |
| `SQLExecuteQueryOperator` | 通用 SQL 执行(含 PostgreSQL) | ⏳ |
| Sensor | 等待外部条件满足(文件、外部任务) | ⏳ |
| Secrets 管理 | 为什么不要把密码写进 DAG | ⏳ |

### 5.2 实战:把行情写进 PostgreSQL(和你 postgre-learn 项目闭环)

compose 自带的 postgres 就是现成目标。配 Connection 有两种方式:

```bash
# 方式一:UI → Admin → Connections → 新建 conn_id=market_pg
# 方式二:环境变量(推荐进 compose 配置)
AIRFLOW_CONN_MARKET_PG=postgresql+psycopg2://airflow:airflow@postgres/airflow
```

DAG 里建表 + 写入:

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import task

create_table = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="market_pg",
    sql="""
        CREATE TABLE IF NOT EXISTS prices (
            symbol TEXT, trade_date DATE, price NUMERIC,
            PRIMARY KEY (symbol, trade_date));
    """,
)

@task
def upsert_prices(prices: dict, ds=None):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id="market_pg")
    rows = [(k, ds, v) for k, v in prices.items()]
    hook.insert_rows("prices", rows, replace=True, replace_index=["symbol", "trade_date"])
```

Sensor 示例:等文件到齐再跑:

```python
from airflow.providers.standard.sensors.filesystem import FileSensor

wait_file = FileSensor(task_id="wait_file", filepath="/opt/airflow/data/ready.flag",
                       poke_interval=30, mode="reschedule")
```

### 5.3 经典坑预警

- 密码明文写 DAG 里 = 事故预定;一律 Connection + Variable,进阶接 secrets backend
- Sensor 默认 `mode="poke"` 会**一直占着一个 worker 槽位**,长时间等待用 `mode="reschedule"` 或 deferrable
- `hook.insert_rows(replace=True)` 的 upsert 语义和 PostgreSQL `ON CONFLICT` 有差异,大批量写入建议直接跑 `INSERT ... ON CONFLICT DO UPDATE` SQL
- Connection 的 conn_id 是字符串引用,拼错要到运行时才报错

---

## 第 6 阶段:生产实践(选学,看方向)

### 6.1 知识点清单

| # | 知识点 | 干什么 | 状态 |
|---|--------|--------|------|
| 1 | Executor 选型 | Local(单机)/ Celery(多机)/ Kubernetes(容器弹性) | ⏳ |
| 2 | DAG 测试与 CI | `dag.test()`、DAG import error 检查、pytest 完整性测试 | ⏳ |
| 3 | 监控告警 | on_failure_callback、健康检查、metrics | ⏳ |
| 4 | deferrable operators | 长等待任务挂起省 worker(配合 Triggerer) | ⏳ |
| 5 | Assets 数据感知调度 | 上游数据就绪才触发(3.x 的 Assets) | ⏳ |
| 6 | 元数据库备份 | `pg_dump` metadata DB | ⏳ |
| 7 | 幂等 DAG 设计 | 重跑不产生重复数据(分区、upsert、先删后写) | ⏳ |
| 8 | 日志治理 | 远程日志(S3/GCS)、日志清理 | ⏳ |

### 6.2 重点提示

- **幂等是 Airflow 最佳实践的核心**:同一个 DAG Run 重跑必须产生同样结果——行情表按 `(symbol, trade_date)` 做主键 + upsert 就是为这个服务的(第 5 阶段已经埋好伏笔)
- LocalExecutor + 单机 docker 就够个人项目用;Celery/K8s 是团队规模的事
- 备份只需备 metadata DB(状态),业务数据在业务库里自己备

---

## 推荐学习资源

| 资源 | 用途 |
|------|------|
| [官方文档](https://airflow.apache.org/docs/) | 权威参考(核心概念、best practices 页面必读) |
| [官方 Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html) | TaskFlow 入门 |
| [Astronomer Guides / Astronaut](https://www.astronomer.io/guides/) | 质量最高的实战文章 |
| 书《Data Pipelines with Apache Airflow》(Manning) | 系统入门 |
| 官方 `docker-compose` 页面 | 部署排错第一站 |

---

## 学习进度跟踪

> 和 neo4j-learn 同一套约定:每完成一个阶段,把"当前进度"表和阶段内知识点状态从 ⏳ 改为 ✅,补上完成日期、练习内容、**踩过的坑**和核心收获。上下文清空后也能无缝续学。
