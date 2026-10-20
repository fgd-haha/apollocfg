"""工厂函数：构建 :class:`~apollocfg.manager.ConfigManager`。

库只提供工厂，不提供全局单例（避免与应用框架的生命周期冲突）。应用侧（如
Django settings / FastAPI lifespan）自行决定是否缓存为单例。
"""
from __future__ import annotations

import os
from typing import Iterable, List, Mapping, Optional, Sequence

from apollocfg.client import ResolvedAssertion
from apollocfg.manager import ConfigManager


def _split_env_list(name: str, default: str = "") -> List[str]:
    """读取逗号分隔的环境变量。

    在函数内读取而不是模块级常量，避免 import 时快照导致后续 ``os.environ``
    修改不生效（测试和 entrypoint 脚本都会在 import 之后设环境变量）。
    """
    return [item.strip() for item in os.getenv(name, default).split(",") if item.strip()]


def _env_float(name: str, default: Optional[float]) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} 不是合法的数字: {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def create_config_manager(
    app_id: str,
    config_url: Optional[str] = None,
    meta_server: Optional[str] = None,
    cluster: str = "default",
    namespaces: Optional[Sequence[str]] = None,
    env: Optional[str] = None,
    client_ip: Optional[str] = None,
    access_key_secret: Optional[str] = None,
    required_keys: Optional[Iterable[str]] = None,
    frozen_keys: Optional[Iterable[str]] = None,
    allow_fallback: bool = True,
    require_resolved_keys: bool = False,
    static_fallback: Optional[Mapping[str, Mapping[str, str]]] = None,
    on_resolved: Optional[ResolvedAssertion] = None,
    auto_start: bool = True,
    poll_timeout: int = 90,
    request_timeout: int = 5,
    poll_interval: float = 2.0,
    max_retry_backoff: float = 30.0,
    refresh_interval: float = 300.0,
    startup_timeout: Optional[float] = None,
) -> ConfigManager:
    """以显式参数创建 ConfigManager。

    ``auto_start=True``（默认）时立即拉取一次全量配置、校验契约并启动长轮询线程；
    契约校验失败时抛异常（见 ``allow_fallback`` / ``require_resolved_keys``）。
    """
    manager = ConfigManager(
        app_id=app_id,
        config_url=config_url,
        meta_server=meta_server,
        cluster=cluster,
        namespaces=namespaces,
        env=env,
        client_ip=client_ip,
        access_key_secret=access_key_secret,
        required_keys=required_keys,
        frozen_keys=frozen_keys,
        allow_fallback=allow_fallback,
        require_resolved_keys=require_resolved_keys,
        static_fallback=static_fallback,
        on_resolved=on_resolved,
        poll_timeout=poll_timeout,
        request_timeout=request_timeout,
        poll_interval=poll_interval,
        max_retry_backoff=max_retry_backoff,
        refresh_interval=refresh_interval,
        startup_timeout=startup_timeout,
    )
    if auto_start:
        manager.start()
    return manager


def create_config_manager_from_env(
    auto_start: bool = True,
    on_resolved: Optional[ResolvedAssertion] = None,
    static_fallback: Optional[Mapping[str, Mapping[str, str]]] = None,
) -> ConfigManager:
    """从环境变量构建 ConfigManager。

    读取以下环境变量::

        APOLLO_APP_ID             应用 AppId（必填）
        APOLLO_META_SERVER        meta server 地址（与 APOLLO_CONFIG_URL 二选一）
        APOLLO_CONFIG_URL         Config Service 地址（与 APOLLO_META_SERVER 二选一）
        APOLLO_CLUSTER            集群名（默认 "default"）
        APOLLO_NAMESPACES         逗号分隔的 namespace 列表（默认 "application"）
        APOLLO_ENV                环境名（可选）
        APOLLO_CLIENT_IP          灰度发布用的客户端 IP（可选）
        APOLLO_ACCESS_KEY_SECRET  访问密钥（可选，留空走匿名）
        APOLLO_REQUIRED_KEYS      逗号分隔，必须解析出值的 key
        APOLLO_FROZEN_KEYS        逗号分隔，禁止热更新的 key
        APOLLO_ALLOW_FALLBACK     Apollo 不可达时是否降级（默认 true）
        APOLLO_REQUIRE_RESOLVED_KEYS
                                  required_keys 缺失时是否拒绝启动（默认 false）
        APOLLO_REFRESH_INTERVAL   定时全量拉取间隔（秒，默认 300，设 0 关闭）
        APOLLO_STARTUP_TIMEOUT    start() 阻塞上界（秒，默认不限制）

    ``on_resolved`` 与 ``static_fallback`` 只能通过参数传入——前者是可调用对象，
    后者是嵌套映射，都没法用环境变量表达。

    若已安装 ``python-dotenv``，会先调用 ``load_dotenv()``（未安装则跳过）。

    Raises:
        ValueError: 未设置 ``APOLLO_APP_ID``，或 meta server 与 config url 都没给。
    """
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    app_id = os.getenv("APOLLO_APP_ID", "").strip()
    if not app_id:
        raise ValueError("APOLLO_APP_ID 未设置")

    meta_server = (os.getenv("APOLLO_META_SERVER") or "").strip() or None
    config_url = (os.getenv("APOLLO_CONFIG_URL") or "").strip() or None
    if not meta_server and not config_url:
        raise ValueError("APOLLO_META_SERVER 与 APOLLO_CONFIG_URL 至少设置一个")

    return create_config_manager(
        app_id=app_id,
        config_url=config_url,
        meta_server=meta_server,
        cluster=os.getenv("APOLLO_CLUSTER", "default"),
        namespaces=_split_env_list("APOLLO_NAMESPACES", "application"),
        env=(os.getenv("APOLLO_ENV") or "").strip() or None,
        client_ip=(os.getenv("APOLLO_CLIENT_IP") or "").strip() or None,
        access_key_secret=(os.getenv("APOLLO_ACCESS_KEY_SECRET") or "").strip() or None,
        required_keys=_split_env_list("APOLLO_REQUIRED_KEYS"),
        frozen_keys=_split_env_list("APOLLO_FROZEN_KEYS"),
        allow_fallback=_env_bool("APOLLO_ALLOW_FALLBACK", True),
        require_resolved_keys=_env_bool("APOLLO_REQUIRE_RESOLVED_KEYS", False),
        static_fallback=static_fallback,
        on_resolved=on_resolved,
        refresh_interval=_env_float("APOLLO_REFRESH_INTERVAL", 300.0),
        startup_timeout=_env_float("APOLLO_STARTUP_TIMEOUT", None),
        auto_start=auto_start,
    )
