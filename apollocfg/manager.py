"""ConfigManager：在 :class:`~apollocfg.client.ApolloClient` 之上的薄封装。

职责：

  - 提供「按 namespace 订阅变更」的装饰器风格回调；
  - 转发配置读取；
  - 通过 :meth:`ConfigManager.get_typed` 接入任意模型类，库本身不绑定 pydantic
    之类的模型库（核心依赖只有 requests）。
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from collections import defaultdict
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

from apollocfg.client import DEFAULT_NAMESPACE, ApolloClient, ResolvedAssertion

logger = logging.getLogger("apollocfg.manager")


def _log_async_result(task: asyncio.Task) -> None:
    """把异步回调里的异常打出来。

    不取 exception() 的 task 在被 GC 时只会留下一句
    "Task exception was never retrieved"，丢掉堆栈。
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("异步 namespace 回调执行失败: %r", exc)

#: 回调签名 ``(namespace, changes)``
NamespaceCallback = Callable[[str, Dict[str, Dict[str, Any]]], None]
T = TypeVar("T")


class ConfigManager:
    """按 namespace 组织变更订阅的封装。

    构造参数与 :class:`~apollocfg.client.ApolloClient` 一致，原样转发。
    """

    def __init__(
        self,
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
        poll_timeout: int = 90,
        request_timeout: int = 5,
        poll_interval: float = 2.0,
        max_retry_backoff: float = 30.0,
        refresh_interval: float = 300.0,
        startup_timeout: Optional[float] = None,
    ) -> None:
        self._client = ApolloClient(
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
        self._ns_callbacks: Dict[str, List[NamespaceCallback]] = defaultdict(list)
        self._client.register_callback(self._dispatch)

    # ------------------------- 生命周期 -------------------------
    @property
    def client(self) -> ApolloClient:
        """底层客户端，供需要直接操作的场景使用。"""
        return self._client

    def start(self) -> None:
        self._client.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._client.stop(timeout=timeout)

    def __enter__(self) -> ConfigManager:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    # ------------------------- 变更订阅 -------------------------
    def _dispatch(self, namespace: str, changes: Dict[str, Dict[str, Any]]) -> None:
        for cb in list(self._ns_callbacks.get(namespace, [])):
            try:
                cb(namespace, changes)
            except Exception:
                logger.exception("namespace=%s 变更回调失败", namespace)

    def on_namespace_change(
        self, namespace: str = DEFAULT_NAMESPACE
    ) -> Callable[[NamespaceCallback], NamespaceCallback]:
        """装饰器 / 注册器：订阅某个 namespace 的变更。

        回调拿到的 changes 已剔除 ``frozen_keys``。
        """

        def decorator(cb: NamespaceCallback) -> NamespaceCallback:
            self._ns_callbacks[namespace].append(cb)
            return cb

        return decorator

    # ------------------------- 读取 -------------------------
    def get_value(
        self,
        key: str,
        default: Optional[str] = None,
        namespace: str = DEFAULT_NAMESPACE,
    ) -> Optional[str]:
        return self._client.get_value(key, default, namespace)

    def get_configs(self, namespace: str = DEFAULT_NAMESPACE) -> Dict[str, str]:
        return self._client.get_configs(namespace)

    def effective_configs(self, namespace: str = DEFAULT_NAMESPACE) -> Dict[str, str]:
        """最终生效的配置：静态兜底值被 Apollo 的值覆盖。"""
        return self._client.effective_configs(namespace)

    def get_content(self, namespace: str) -> Optional[str]:
        """非 properties 格式 namespace 的原文（json / yaml / xml / txt）。"""
        return self._client.get_content(namespace)

    def get_json(self, namespace: str) -> Any:
        """解析 json 格式 namespace 的 content 并返回对象。"""
        return self._client.get_json(namespace)

    def merged_configs(self, namespaces: Optional[Sequence[str]] = None) -> Dict[str, str]:
        """跨 namespace 合并，后面的覆盖前面的。"""
        return self._client.merged_configs(namespaces)

    def on_namespace_change_async(
        self, namespace: str, loop: asyncio.AbstractEventLoop
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """装饰器：订阅某个 namespace 的变更，回调是协程函数。

        自动桥接到事件循环——变更由长轮询线程检测，协程在循环线程执行。
        """

        def decorator(cb: Callable[..., Any]) -> Callable[..., Any]:
            if not inspect.iscoroutinefunction(cb):
                raise TypeError(
                    f"{getattr(cb, '__name__', cb)!r} 不是协程函数，"
                    "请用 on_namespace_change"
                )

            def bridge(ns: str, changes: Dict[str, Dict[str, Any]]) -> None:
                def schedule() -> None:
                    task = loop.create_task(cb(ns, changes))
                    task.add_done_callback(_log_async_result)

                try:
                    loop.call_soon_threadsafe(schedule)
                except RuntimeError as exc:
                    logger.warning("事件循环不可用，跳过异步回调 namespace=%s: %s", ns, exc)

            self._ns_callbacks[namespace].append(bridge)
            return cb

        return decorator

    def get_typed(self, namespace: str, model_cls: Type[T]) -> T:
        """用任意模型类解析某个 namespace 的配置。

        只要 ``model_cls`` 提供 ``model_validate(dict)`` 类方法即可（pydantic v2
        天然满足）。库不绑定任何模型库。
        """
        return model_cls.model_validate(dict(self.get_configs(namespace)))
