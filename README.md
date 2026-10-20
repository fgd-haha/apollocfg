# apollocfg

一个**轻量、同步、贴近 Apollo 官方 HTTP API** 的 Python 配置中心客户端。

只依赖 `requests`。长轮询热更新、meta server 服务发现、配置契约（必需 / 冻结）、
访问密钥签名，以及**日志绝不输出配置值**。

```bash
pip install apollocfg
```

## 为什么又写一个

Apollo 官方只维护 Java 和 .NET 客户端（Go 的 [agollo](https://github.com/apolloconfig/agollo)
已被官方收编），Python 侧全是第三方实现。实测几个主流包后发现两类问题：

| 问题 | 实际表现 |
|---|---|
| 客户端读超时 < 服务端挂起时长 | 服务端长轮询挂 60s，客户端设 35s，**第一轮必然 ReadTimeout**；异常未捕获时轮询线程直接死掉，此后配置永不更新且无任何报错 |
| 日志打印整份配置 | `logger.info(..., repr(configs))` 把数据库密码、API token 刷进集中式日志系统，长期留存可检索 |
| 定时拉取冒充长轮询 | 文档写"实时更新"，实现是每 30s 全量拉一次；延迟 0~30s，且不带 `releaseKey` 拿不到 304 |

本库针对这些点做了明确约束：构造函数校验 `poll_timeout > 60`；轮询循环最外层兜住
所有异常，线程永不退出；任何日志级别都只输出 key 名。

## 快速开始

```python
from apollocfg import ApolloClient

client = ApolloClient(
    app_id="your-app-id",
    meta_server="http://apollo-meta.example.com",   # 或 config_url=...
    namespaces=["application"],
)
client.start()

client.get_value("FEATURE_FLAG", default="off")
client.get_configs("application")

@client.register_callback
def on_change(namespace, changes):
    # changes = {key: {"old": ..., "new": ...}}
    print(namespace, sorted(changes))

client.stop()
```

也可用上下文管理器：

```python
with ApolloClient(app_id="app", meta_server="http://meta.example.com") as client:
    client.get_value("KEY")
```

## 三组正交语义

这是本库与其他 Python 客户端的主要差别。三者解决的是不同问题，可以任意组合。

### `required_keys` —— 必须来自 Apollo

声明哪些 key 必须从配置中心解析出非空值。拿不到就是契约违约。

```python
client = ApolloClient(
    app_id="app",
    meta_server="http://meta.example.com",
    required_keys=["MYSQL_DSN", "REDIS_URL"],
    allow_fallback=False,
)
client.start()   # 缺失则抛 RequiredKeysMissing
```

为什么需要它：比"Apollo 整体宕机"概率高得多的失败是**漏发一个 key、或 key 名拼错一个字母**。
前者是全局的、看得见的；后者是局部的、静默的。`required_keys` 把后者变成启动期的硬失败。

值为空白字符串视为未解析出值。支持按 namespace 指定：

```python
required_keys={"application": ["A"], "database": ["DSN", "POOL"]}
```

### `frozen_keys` —— 禁止热更新

声明哪些 key 在进程生命周期内冻结：启动时快照，之后 Apollo 怎么变都不影响读取结果，
变更也不会出现在回调里。

```python
client = ApolloClient(
    app_id="app",
    meta_server="http://meta.example.com",
    frozen_keys=["MYSQL_DSN", "REDIS_URL"],
)
```

为什么需要它：数据库和 Redis 的连接池在启动时按当时的配置建好，之后改配置**不会**让
已有连接重连。此时让 `get_value()` 返回新值是有害的——会出现"配置显示新地址、连接还在
老地址"，更糟的是某些 lazy 初始化的客户端会用新值建连接，于是新旧连接并存。

准确的语义是：**被声明冻结的 key，其可见值在进程生命周期内不变。**

| 启动时 | 运行期 Apollo 发布/修改该 key | `get_value` | `effective_configs` |
|---|---|---|---|
| Apollo 有值 | 改了 | 启动时的 Apollo 值 | 同左 |
| Apollo 无值、`static_fallback` 有值 | 发布了 | `None` | 启动时的静态值 |
| 两边都无值 | 发布了 | `None` | 不含该 key |

第二行的差异不是不一致，而是两个方法的契约不同：`get_value` / `get_configs` 只反映
Apollo 侧，`effective_configs` 才是"最终生效"视图。声明了 `static_fallback` 的
场景应当读 `effective_configs`。

第三行容易被忽略但同样重要：让一个冻结的 key 中途"冒出"一个值，和"值变了"是
同一类不一致——配置显示有值，而资源是按无值建起来的。想保证"必须拿到值"用
`required_keys`，那是另一层。

以上三种情况都不会触发变更回调。启动时拉取失败的 namespace 同样会建立冻结快照
（此时只有静态兜底值可用），所以哨兵断言在 Apollo 不可达时依然有效。

### `allow_fallback` —— 不可达时是否降级

```python
allow_fallback=True    # 默认。Apollo 不可达时告警降级，由调用方静态配置兜底
allow_fallback=False   # Apollo 不可达时抛 ConfigServiceUnavailable，拒绝启动
```

适用场景不同：有些部署环境（比如和配置中心网络不通的旧环境）本来就读不到 Apollo，
需要用代码里的静态配置正常运行；而切换窗口期的新环境必须确保配置来自 Apollo。

### `require_resolved_keys` —— key 缺失时是否拒绝启动

和 `allow_fallback` 正交：后者管"Apollo **连不上**怎么办"，前者管"Apollo 连上了但
**key 没给**怎么办"。这是两个独立的失败模式，概率和处置方式都不同。

```python
ApolloClient(
    required_keys=["MYSQL_DSN"],
    allow_fallback=True,          # 允许 Apollo 不可达时降级
    require_resolved_keys=True,   # 但 key 解析不出来就拒绝启动
)
```

默认 `False`（只告警）。`allow_fallback=False` 隐含 `require_resolved_keys=True`——
既然不允许降级，自然也不接受缺 key。

### `static_fallback` + `on_resolved` —— 启动断言

`allow_fallback=True` 有个隐含风险：**回退到静态配置，可能回退到一份错误的配置**。
比如切换窗口期，Apollo 里是新地址、代码里还是旧地址，Apollo 一旦拉取失败就静默连回旧库。

解法是在静态配置里放哨兵占位符，启动时断言最终生效的值不是哨兵。这需要把静态值
告诉客户端——否则回调只能看到 Apollo 侧的空配置，`configs.get(...)` 返回 `None`
而不是哨兵，断言会错误地通过：

```python
def reject_sentinel(namespace, configs):
    if configs.get("MYSQL_DSN") == "APOLLO_REQUIRED":
        return "MYSQL_DSN 仍是哨兵值，Apollo 未下发真实配置"
    return None   # 返回 None 或 "" 表示通过

client = ApolloClient(
    app_id="app",
    meta_server="http://meta.example.com",
    static_fallback={"application": {"MYSQL_DSN": "APOLLO_REQUIRED"}},
    on_resolved=reject_sentinel,
)
client.start()   # 断言失败抛 StartupAssertionFailed
```

`static_fallback` 只用于两件事：`required_keys` 的"已解析"判定，以及传给
`on_resolved` 的最终生效配置。它不进内部配置表、不参与 diff，所以不会触发变更回调。

两个读取方法的区别：

| 方法 | 返回 |
|---|---|
| `get_configs(ns)` | 只有 Apollo 的值（已套用冻结快照） |
| `effective_configs(ns)` | 静态兜底值被 Apollo 的值覆盖后的结果 |

Apollo 下发空白值不会覆盖掉有效的静态兜底值。

断言在降级**之后**执行，所以能挡住"Apollo 没拉到、回退到哨兵"这条路径。这比依赖
`allow_fallback=False` 更稳：后者要求"开关必须被正确设置"，而哨兵值即使开关丢失也会失败。

## 服务发现

给 `meta_server` 时，客户端先查 `GET {meta}/services/config` 拿到 Config Service
实例列表，随机选一个（避免所有客户端都打到第一个），请求失败时自动轮换到其他实例。

```python
ApolloClient(app_id="app", meta_server="http://apollo-meta.example.com")
```

给 `config_url` 则直连，不做服务发现。两个都给时以 `config_url` 为首选地址，
`meta_server` 作为轮换来源。

## 为什么是长轮询

Apollo 没有服务端主动推送。客户端发起 `GET /notifications/v2` 后，服务端用
Servlet 3.0 异步 + Spring `DeferredResult` 把请求挂起（硬编码 60s）；期间有变更就
立即返回新的 `notificationId`，无变更则超时返回 304，客户端随即发起下一轮。

所以 **60s 不是配置生效延迟，而是无变更时连接保持的上限**，实际变更生效在秒级。

这也解释了为什么 `poll_timeout` 必须大于 60：客户端读超时短于服务端挂起时长，
每一轮都会在服务端应答之前被客户端自己掐断。本库在构造函数里直接校验这一点。

部署注意：60s 的挂起请求经过 Nginx / ALB 时容易被判超时返回 504，反向代理的读超时
需要调到 60s 以上。

## 日志安全

库在**任何日志级别**都不输出配置值，只输出 key 名：

```
INFO  namespace=application 配置变更: ['FEATURE_FLAG', 'MYSQL_DSN']
INFO  namespace=application 已冻结 2 个 key（禁止热更新）: ['MYSQL_DSN', 'REDIS_URL']
WARN  required_keys 未从 Apollo 解析出值，按 allow_fallback=True 继续; missing={'application': ['REDIS_URL']}
```

`access_key_secret` 同样不会出现在日志里。测试套件里有专门的断言覆盖这一点
（`tests/test_logging_redaction.py`）。

## 在 asyncio 应用里使用

长轮询跑在独立线程,`requests` 等 socket 时会释放 GIL,所以**同步实现不会阻塞事件
循环**。实测(10ms 心跳协程量测延迟):

| | p50 | p99 | max | >5ms 尖峰 |
|---|---|---|---|---|
| 基线(无 apollocfg) | 0.13ms | 0.22ms | 0.32ms | 0 |
| 纯 CPU 线程(GIL 争用上界) | 5.41ms | 10.60ms | 11.18ms | 571 |
| apollocfg(秒回 + 300 key,10ms 一轮) | 0.13ms | 0.75ms | 11.72ms | 1 |

第三行是刻意构造的最坏情况。读配置也是免费的:`get_value()` 只读内存快照,
实测 1.34µs/次,不发请求。

**但变更回调跑在长轮询线程**,那里没有运行中的事件循环:

```python
@client.register_callback
def bad(ns, changes):
    asyncio.create_task(rebuild())     # RuntimeError: no running event loop
```

把协程函数传给 `register_callback` 会在**注册时**直接报 `TypeError` —— 协程不被
await 会被静默丢弃,是最难查的那类 bug,所以库不让它通过。

异步场景用 `register_async_callback`,它用 `loop.call_soon_threadsafe` 做跨线程桥接:

```python
from contextlib import asynccontextmanager

async def rebuild_pool(namespace, changes):
    await engine.dispose()             # 真正的异步逻辑，在事件循环里执行

@asynccontextmanager
async def lifespan(app):
    loop = asyncio.get_running_loop()
    client.register_async_callback(rebuild_pool, loop=loop)
    client.start()
    yield
    client.stop()
```

`ConfigManager` 对应的是 `on_namespace_change_async(namespace, loop=...)`。

### 启动阻塞上界

`start()` 是同步的 —— 它要先拉到配置、校验契约,才能让应用带着正确配置启动。
Apollo 正常时实测 34~64ms。但 Apollo 慢或不可达时,阻塞时长是
`request_timeout × namespace 数 × 实例轮换次数`。

这在 FastAPI 的 lifespan 里是个问题:lifespan 卡住意味着端口还没监听,容器平台的
探活会判不健康。用 `startup_timeout` 封顶:

```python
ApolloClient(..., startup_timeout=10.0)    # 超预算就跳过剩余 namespace
```

超时后按 `allow_fallback` 决定抛错还是降级;跳过的 namespace 由长轮询起来后自己补上。

## 定时全量拉取兜底

长轮询可能丢通知 —— 网络抖动、代理吞掉响应、或 notificationId 因某种服务端状态
不再推进。此时配置会**一直停在旧值且没有任何报错**,定时全量拉取是唯一的自愈路径。

默认每 300 秒一次,跑在**独立线程**:

```python
ApolloClient(..., refresh_interval=300.0)   # 设 0 关闭（不建议）
```

为什么是独立线程而不是放在长轮询循环里:兜底要兜的正是"长轮询出问题"。长轮询每轮
被服务端挂住约 60s,放在同一个循环里,兜底的触发间隔就被长轮询绑死 —— 实测
`refresh_interval=8` 时实际只能每 61s 执行一次。安全机制不该依赖它要保护的东西。

兜底拉取会先清掉 `releaseKey` 以强制拿到完整响应,否则服务端回 304 就白跑了。

## 非 properties 格式的 namespace

Apollo 对 json / yaml / xml / txt 格式的 namespace **不拆 key-value**,而是把整段
原文放在 `configurations.content` 里。所以 `get_configs()` 拿到的是 `{"content": "..."}`。

```python
client = ApolloClient(..., namespaces=["database.json"])
databases = client.get_json("database.json")    # 解析 content
raw = client.get_content("database.json")       # 原文
```

namespace 名**必须带后缀**,写成 `database` 会被解析成同名的 properties namespace。

json 格式适合存嵌套结构(如 Django 的 `DATABASES`):类型原生保留(int/bool/嵌套
dict),整块原子发布,不会出现"改了 host 漏了密码"的半生效状态。

代价是 Apollo 的审计、diff、灰度都是 key 粒度的,整块存成一个 `content` 就失去了
这些能力。适合变更频率低、成组变更的配置;单点变更频繁的业务配置用 properties。

## 跨 namespace 合并

```python
client = ApolloClient(..., namespaces=["theta.infra", "application"])
client.merged_configs()        # application 的同名 key 覆盖 theta.infra
```

**后面的覆盖前面的**。典型用法是让项目私有配置覆盖关联的公共 namespace。
json/yaml 的 `content` 不参与合并(合并两段原文没有意义)。

## ConfigManager

按 namespace 组织回调的薄封装：

```python
from apollocfg import ConfigManager

manager = ConfigManager(
    app_id="app",
    meta_server="http://meta.example.com",
    namespaces=["application", "database"],
)
manager.start()

@manager.on_namespace_change("database")
def on_db_change(namespace, changes):
    rebuild_pool()

manager.get_value("KEY", namespace="database")
```

`get_typed` 可以用任意模型类解析某个 namespace（只要提供 `model_validate(dict)`
类方法，pydantic v2 天然满足）。库本身不绑定任何模型库：

```python
cfg = manager.get_typed("database", MyPydanticModel)
```

## 从环境变量构建

```python
from apollocfg import create_config_manager_from_env

manager = create_config_manager_from_env()
```

| 环境变量 | 说明 | 默认 |
|---|---|---|
| `APOLLO_APP_ID` | 应用 AppId | **必填** |
| `APOLLO_META_SERVER` | meta server 地址 | 与下一项二选一 |
| `APOLLO_CONFIG_URL` | Config Service 地址 | 与上一项二选一 |
| `APOLLO_CLUSTER` | 集群名 | `default` |
| `APOLLO_NAMESPACES` | 逗号分隔的 namespace | `application` |
| `APOLLO_ENV` | 环境名（仅用于日志标识） | 空 |
| `APOLLO_CLIENT_IP` | 灰度发布用客户端 IP | 空 |
| `APOLLO_ACCESS_KEY_SECRET` | 访问密钥 | 空（匿名） |
| `APOLLO_REQUIRED_KEYS` | 逗号分隔，必须来自 Apollo 的 key | 空 |
| `APOLLO_FROZEN_KEYS` | 逗号分隔，禁止热更新的 key | 空 |
| `APOLLO_ALLOW_FALLBACK` | 不可达时是否降级 | `true` |
| `APOLLO_REQUIRE_RESOLVED_KEYS` | key 缺失时拒绝启动 | `false` |

空白值视为未设置（容器平台常把未配置的变量注入成空串）。`on_resolved` 是可调用对象，
只能通过参数传入。

装了 `python-dotenv` 会自动加载 `.env`：

```bash
pip install apollocfg[dotenv]
```

## 异常

```
ApolloError
├── ConfigServiceUnavailable   所有实例不可达（allow_fallback=False 时抛）
├── ApolloProtocolError        响应不符合 Apollo 协议（如 2xx 但 body 为空）
├── RequiredKeysMissing        required_keys 未解析出值，.missing 按 namespace 聚合
└── StartupAssertionFailed     on_resolved 拒绝了最终配置，.reason 为原因
```

`ApolloProtocolError` 单独成类是为了区分"网络不可达"和"中间有代理在干预"——
后者通常表现为代理返回了自己的错误页或空响应。

## 构造参数

| 参数 | 说明 | 默认 |
|---|---|---|
| `app_id` | 应用 AppId | 必填 |
| `config_url` / `meta_server` | 地址，至少给一个 | — |
| `cluster` | 集群 | `default` |
| `namespaces` | namespace 列表 | `["application"]` |
| `env` | 环境名标识 | `None` |
| `client_ip` | 灰度 IP | `None` |
| `access_key_secret` | 访问密钥 | `None` |
| `required_keys` | 必需 key | `None` |
| `frozen_keys` | 冻结 key | `None` |
| `allow_fallback` | Apollo 不可达时允许降级 | `True` |
| `require_resolved_keys` | key 缺失时拒绝启动 | `False` |
| `static_fallback` | 调用方静态兜底值 `{ns: {key: value}}` | `None` |
| `on_resolved` | 启动断言 | `None` |
| `poll_timeout` | 长轮询读超时，必须 > 60 | `90` |
| `request_timeout` | 普通请求超时 | `5` |
| `poll_interval` | 异常后首次重试间隔 | `2.0` |
| `max_retry_backoff` | 退避上限 | `30.0` |
| `refresh_interval` | 定时全量拉取间隔，0 关闭 | `300.0` |
| `startup_timeout` | `start()` 阻塞上界，None 不限 | `None` |

## 测试

```bash
pip install -e ".[test]"
pytest                    # 单元测试，全部走 requests_mock，不碰网络
```

端到端测试默认跳过，需要真实 Apollo 实例：

```bash
APOLLOCFG_E2E=1 \
APOLLOCFG_E2E_META_SERVER=http://your-apollo-meta.example.com \
APOLLOCFG_E2E_APP_ID=your-app-id \
pytest tests/test_e2e.py -v -m e2e
```

E2E 只读，不修改 Apollo 上的配置。其中一条会真实等待服务端 60s 挂起以验证长轮询语义，
整套耗时约 2.5 分钟。

## 致谢

本项目基于 [apollo-config-client](https://github.com/Jiafan/apollo-config-client)
（MIT, © Jiafan）改造，保留了其长轮询与签名实现的核心思路。服务发现与实例轮换的设计
参考了 [OuterCloud/pyapollo](https://github.com/OuterCloud/pyapollo)。

相对上游的主要变化：移除本地磁盘缓存（改为纯内存）与 Oracle/LDAP 领域模型，
新增 `required_keys` / `frozen_keys` / `on_resolved` 契约语义、meta server 服务发现、
`poll_timeout` 校验与日志脱敏。

## License

MIT
