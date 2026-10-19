"""apollocfg 的异常类型。

设计意图：把「配置解析失败」和「网络/协议错误」分开。前者是契约违约，
调用方通常应当拒绝启动；后者可能是暂时的，可按 ``allow_fallback`` 策略降级。
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional


class ApolloError(Exception):
    """本库所有异常的基类。"""


class ApolloProtocolError(ApolloError):
    """服务端响应不符合 Apollo 协议（例如 2xx 却没有 body）。

    单独成类是为了和"网络不可达"区分开：这种情况通常意味着中间有代理在干预
    （返回了自己的错误页或空响应），而不是 Config Service 本身的状态。
    """


class ConfigServiceUnavailable(ApolloError):
    """所有 Config Service 实例都不可用（网络不可达 / 全部返回错误）。

    只在 ``allow_fallback=False`` 时由 :meth:`ApolloClient.start` 抛出；
    ``allow_fallback=True`` 时记日志降级，由调用方的静态配置兜底。
    """

    def __init__(self, message: str, attempted: Optional[Iterable[str]] = None) -> None:
        super().__init__(message)
        self.attempted: List[str] = list(attempted or ())


class RequiredKeysMissing(ApolloError):
    """声明在 ``required_keys`` 中的 key 未能从 Apollo 解析出值。

    这是契约违约：调用方明确声明过这些 key 必须来自配置中心，
    拿不到就意味着当前进程会使用一份不完整/过期的配置。

    ``missing`` 按 namespace 聚合，便于调用方打印可读的错误信息。
    """

    def __init__(self, missing: Dict[str, List[str]]) -> None:
        self.missing = {ns: sorted(keys) for ns, keys in missing.items() if keys}
        detail = "; ".join(
            f"{ns}: {', '.join(keys)}" for ns, keys in sorted(self.missing.items())
        )
        super().__init__(f"required keys not resolved from Apollo -> {detail}")


class StartupAssertionFailed(ApolloError):
    """``on_resolved`` 启动断言回调判定配置不可用。

    典型用途：调用方在静态配置里放哨兵值（占位符），启动时断言最终生效的值
    不是哨兵。这样「回退到静态配置」不会变成「静默连到错误的目标」。
    """

    def __init__(self, reason: str) -> None:
        super().__init__(f"startup assertion rejected the resolved config: {reason}")
        self.reason = reason
