"""apollocfg —— 轻量、同步、贴近 Apollo 官方 HTTP API 的 Python 配置中心客户端。

特性：

  - **长轮询热更新**：基于 ``notifications/v2``，变更秒级生效，带 key 级 diff 回调。
  - **服务发现**：支持 meta server，自动发现 Config Service 实例并在失败时轮换。
  - **配置契约**：``required_keys`` 声明必须来自 Apollo 的 key，缺失即违约。
  - **冻结语义**：``frozen_keys`` 声明禁止热更新的 key（如数据库连接串——连接池
    不会因为配置变了就重建，让值"看起来"变了反而危险）。
  - **降级策略**：``allow_fallback`` 控制 Apollo 不可达时是否允许用调用方静态配置。
  - **启动断言**：``on_resolved`` 钩子让调用方校验最终生效的值（例如拒绝哨兵占位符）。
  - **日志安全**：任何级别都不输出配置值，只输出 key 名。
  - **极简依赖**：只依赖 ``requests``。

典型用法::

    from apollocfg import ApolloClient

    client = ApolloClient(
        app_id="your-app-id",
        meta_server="http://apollo-meta.example.com",
        namespaces=["application"],
        required_keys=["MYSQL_DSN", "REDIS_URL"],
        frozen_keys=["MYSQL_DSN", "REDIS_URL"],
        allow_fallback=True,
    )
    client.start()

    client.get_value("MYSQL_DSN")        # 冻结值，返回启动快照
    client.get_value("FEATURE_FLAG")     # 普通值，热更新生效

    @client.register_callback
    def on_change(namespace, changes):   # changes 已剔除 frozen_keys
        ...

公开 API：

  - :class:`ApolloClient`：客户端本体。
  - :class:`ConfigManager`：按 namespace 订阅变更的二次封装。
  - :func:`create_config_manager` / :func:`create_config_manager_from_env`：工厂。
  - 异常：:class:`ApolloError` 及其子类。
"""
from __future__ import annotations

from apollocfg._factory import create_config_manager, create_config_manager_from_env
from apollocfg.client import ApolloClient, ChangeCallback, ResolvedAssertion
from apollocfg.errors import (
    ApolloError,
    ConfigServiceUnavailable,
    RequiredKeysMissing,
    StartupAssertionFailed,
)
from apollocfg.manager import ConfigManager

try:
    from importlib.metadata import PackageNotFoundError, version

    try:
        __version__ = version("apollocfg")
    except PackageNotFoundError:
        # 源码树内直接运行（未安装为发行版）时的兜底值
        __version__ = "0.0.0.dev0"
except ImportError:  # pragma: no cover - Python < 3.8
    __version__ = "0.0.0.dev0"

__all__ = [
    "ApolloClient",
    "ApolloError",
    "ChangeCallback",
    "ConfigManager",
    "ConfigServiceUnavailable",
    "RequiredKeysMissing",
    "ResolvedAssertion",
    "StartupAssertionFailed",
    "__version__",
    "create_config_manager",
    "create_config_manager_from_env",
]
