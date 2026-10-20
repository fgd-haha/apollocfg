"""Apollo 配置中心 HTTP 客户端（兼容 Apollo 2.x）。

直接基于 Apollo 开放的 HTTP 接口实现，不依赖任何 Apollo 专用 SDK：

  - 服务发现: GET {meta_server}/services/config
  - 配置拉取: GET {config_url}/configs/{appId}/{cluster}/{namespace}
  - 变更通知: GET {config_url}/notifications/v2   （HTTP 长轮询）

官方 Java / .NET 客户端底层用的是同一套接口。

关于「长轮询」：Apollo 没有服务端主动推送。客户端发起 ``notifications/v2`` 请求后，
服务端用 DeferredResult 把请求挂起（默认 60s）；期间有变更就立刻返回新的
notificationId，无变更则超时返回 304，客户端随即发起下一轮。所以 60s 不是生效延迟，
而是「无变更时连接保持的上限」，实际变更生效在秒级。

本客户端的三组正交语义（见 README）：

  - ``required_keys``  —— 必须从 Apollo 解析出值的 key，缺失即契约违约。
  - ``frozen_keys``    —— 禁止热更新的 key，启动时快照，运行期不随 Apollo 变化。
  - ``allow_fallback`` —— Apollo 不可达时是否允许降级（由调用方静态配置兜底）。

安全：本库在任何日志级别都不输出配置值，只输出 key 名。
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import inspect
import json
import logging
import random
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import quote

import requests
from requests import Request

from apollocfg.errors import (
    ApolloError,
    ApolloProtocolError,
    ConfigServiceUnavailable,
    RequiredKeysMissing,
    StartupAssertionFailed,
)

logger = logging.getLogger("apollocfg")

#: 变更回调签名 ``(namespace, changes)``，changes 形如
#: ``{key: {"old": 旧值 | None, "new": 新值 | None}}``。
ChangeCallback = Callable[[str, Dict[str, Dict[str, Any]]], None]

#: 启动断言回调签名 ``(namespace, configs) -> None | str``。
#: 返回非空字符串表示拒绝（内容作为原因），返回 None 或 "" 表示通过。
ResolvedAssertion = Callable[[str, Mapping[str, str]], Optional[str]]

DEFAULT_NAMESPACE = "application"

# Apollo 服务端长轮询默认挂起 60s；客户端读超时必须更长，否则每轮都会在服务端
# 应答之前被客户端自己掐断。这是多个第三方 Python 客户端的共同缺陷。
_SERVER_LONG_POLL_SECONDS = 60

# 连续多少次长轮询超时后判定当前实例有问题并轮换。单次超时多半是网络抖动，
# 立刻换地址会在抖动时造成不必要的实例漂移。
_MAX_CONSECUTIVE_TIMEOUTS = 3


def _normalize_keys(
    value: Optional[Iterable[str]], namespaces: Sequence[str]
) -> Dict[str, set]:
    """把 key 声明归一化成 ``{namespace: {key, ...}}``。

    支持两种写法：

      - 扁平可迭代对象（``["A", "B"]``）—— 套用到所有 namespace；
      - 映射（``{"application": ["A"], "database": ["B"]}``）—— 按 namespace 指定。
    """
    if not value:
        return {ns: set() for ns in namespaces}
    if isinstance(value, Mapping):
        result = {ns: set() for ns in namespaces}
        for ns, keys in value.items():
            # 同样对裸字符串特判，避免 {"ns": "KEY"} 被拆成字符
            result.setdefault(ns, set()).update(
                [keys] if isinstance(keys, str) else (keys or ())
            )
        return result
    # 裸字符串要特判：set("MYSQL_DSN") 会拆成 {'M','Y','S',...}，
    # 调用方写 required_keys="MYSQL_DSN" 是很自然的笔误。
    flat = {value} if isinstance(value, str) else set(value)
    return {ns: set(flat) for ns in namespaces}


class ApolloClient:
    """Apollo Config Service 的同步客户端。

    ``config_url`` 与 ``meta_server`` 二选一（至少给一个）：

      - ``config_url``：直连某个 Config Service，不做服务发现。
      - ``meta_server``：先查 ``/services/config`` 拿到实例列表，失败时自动轮换。

    两个都给时以 ``config_url`` 为首选地址，``meta_server`` 作为轮换来源。
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
        if not config_url and not meta_server:
            raise ValueError("config_url 与 meta_server 至少提供一个")
        if poll_timeout <= _SERVER_LONG_POLL_SECONDS:
            raise ValueError(
                f"poll_timeout({poll_timeout}s) 必须大于服务端长轮询挂起时长"
                f"({_SERVER_LONG_POLL_SECONDS}s)，否则每轮都会被客户端自己超时掐断"
            )
        if refresh_interval is not None and 0 < refresh_interval <= poll_interval:
            raise ValueError(
                f"refresh_interval({refresh_interval}s) 应大于 poll_interval"
                f"({poll_interval}s)，否则定时兜底会挤掉长轮询"
            )
        if startup_timeout is not None and startup_timeout <= 0:
            raise ValueError("startup_timeout 必须为正数或 None")

        self.app_id = app_id
        self.cluster = cluster
        self.env = env
        self.namespaces: List[str] = list(namespaces or [DEFAULT_NAMESPACE])
        self.client_ip = client_ip
        self.access_key_secret = access_key_secret
        self.allow_fallback = allow_fallback
        self.require_resolved_keys = require_resolved_keys
        self.on_resolved = on_resolved
        # 调用方的静态兜底值。只用于两件事：required_keys 的"已解析"判定，
        # 以及传给 on_resolved 的"最终生效配置"。不进 _configs，也不参与 diff，
        # 所以不会触发变更回调。
        self.static_fallback: Dict[str, Dict[str, str]] = {
            ns: dict(values) for ns, values in (static_fallback or {}).items()
        }
        self.poll_timeout = poll_timeout
        self.request_timeout = request_timeout
        self.poll_interval = poll_interval
        self.max_retry_backoff = max_retry_backoff
        # 定时全量拉取兜底。长轮询可能丢通知（网络抖动、代理吞掉响应），
        # 届时这是唯一的自愈路径。设 0 或 None 关闭（不建议）。
        self.refresh_interval = refresh_interval
        # start() 的总阻塞上界。FastAPI 的 lifespan 卡住意味着探活端口不监听，
        # 平台会判不健康；Django 则是 worker 起不来。超时后按 allow_fallback
        # 的设置决定抛错还是降级。None 表示不限制。
        self.startup_timeout = startup_timeout

        self.meta_server = meta_server.rstrip("/") if meta_server else None
        self._config_url = config_url.rstrip("/") if config_url else None
        self._service_pool: List[str] = []
        self._rotations = 0
        # 上次全量拉取的时刻。start() 里会置为当时时间，所以第一次定时兜底
        # 发生在启动后 refresh_interval，而不是立刻又拉一遍。
        self._last_full_refresh = 0.0

        self.required_keys = _normalize_keys(required_keys, self.namespaces)
        self.frozen_keys = _normalize_keys(frozen_keys, self.namespaces)

        # 运行时状态。_lock 同时保护 _configs / _release_keys / _frozen_snapshot。
        self._lock = threading.RLock()
        # 地址状态单独加锁：服务发现要发 HTTP，不能占着 _lock 阻塞配置读取。
        # 主线程（start/属性读取）与轮询线程都会碰 _config_url / _service_pool。
        self._addr_lock = threading.RLock()
        self._configs: Dict[str, Dict[str, str]] = {}
        self._release_keys: Dict[str, str] = {}
        # 冻结快照分两份，避免 static_fallback 泄漏进只应反映 Apollo 的视图：
        #   _frozen_snapshot          -> 启动时 Apollo 侧的冻结值，供 get_configs/get_value
        #   _frozen_effective         -> 启动时最终生效的冻结值（含静态兜底），供 effective_configs
        self._frozen_snapshot: Dict[str, Dict[str, str]] = {ns: {} for ns in self.namespaces}
        self._frozen_effective: Dict[str, Dict[str, str]] = {ns: {} for ns in self.namespaces}
        # 哪些 namespace 已经建过冻结快照。start/stop/start 时不能重建，
        # 否则冻结值会从旧值跳到新值，违反"进程生命周期内不变"。
        self._frozen_captured: set = set()
        self._notification_ids: Dict[str, int] = dict.fromkeys(self.namespaces, -1)

        self._callbacks: List[ChangeCallback] = []
        self._started = False
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._refresh_thread: Optional[threading.Thread] = None
        # 串行化 start/stop，避免并发调用产生两个轮询线程，
        # 或 stop() join 超时后 start() 把尚未退出的旧线程"放行"。
        self._lifecycle_lock = threading.RLock()
        # 复用连接：长轮询每 60s 一轮，新建 TCP+TLS 是无谓开销
        self._session = requests.Session()

    # ------------------------- 对外 API -------------------------
    def register_callback(self, cb: ChangeCallback) -> ChangeCallback:
        """注册**同步**配置变更回调。可作装饰器使用。

        回调拿到的 changes 已剔除 ``frozen_keys``——那些 key 对调用方而言不会变，
        不应该触发任何重建逻辑。

        回调在长轮询线程里执行，**不是**事件循环线程。所以回调内
        ``asyncio.get_running_loop()`` / ``create_task()`` 都会抛
        "no running event loop"。异步场景用 :meth:`register_async_callback`。

        Raises:
            TypeError: 传入了协程函数。协程不会被 await，静默丢弃最难查，
                所以在注册时就拒绝。
        """
        if inspect.iscoroutinefunction(cb):
            raise TypeError(
                f"{getattr(cb, '__name__', cb)!r} 是协程函数，"
                "同步回调不会 await 它（协程会被静默丢弃）。"
                "请改用 register_async_callback(cb, loop=...)"
            )
        self._callbacks.append(cb)
        return cb

    def register_async_callback(
        self,
        cb: Callable[[str, Dict[str, Dict[str, Any]]], Any],
        loop: asyncio.AbstractEventLoop,
    ) -> Callable[[str, Dict[str, Dict[str, Any]]], Any]:
        """注册**异步**配置变更回调，自动桥接到事件循环。

        变更由长轮询线程检测到，而协程必须在事件循环线程里执行。这个方法用
        ``loop.call_soon_threadsafe`` 做跨线程唤醒——这是 asyncio 为此专门提供的
        线程安全入口。

        在 FastAPI 的 lifespan 里这样用::

            @asynccontextmanager
            async def lifespan(app):
                loop = asyncio.get_running_loop()
                client.register_async_callback(rebuild_pool, loop=loop)
                client.start()
                yield
                client.stop()

        Args:
            cb: 协程函数，签名 ``async def cb(namespace, changes)``。
            loop: 目标事件循环。必须在循环运行的线程里用
                ``asyncio.get_running_loop()`` 取得。
        """
        if not inspect.iscoroutinefunction(cb):
            raise TypeError(
                f"{getattr(cb, '__name__', cb)!r} 不是协程函数，直接用 register_callback"
            )

        def bridge(namespace: str, changes: Dict[str, Dict[str, Any]]) -> None:
            # 捕获当前参数值；闭包里不要引用循环变量
            def schedule() -> None:
                task = loop.create_task(cb(namespace, changes))
                # 不保留引用的 task 可能被 GC；挂一个回调把异常打出来，
                # 否则协程里抛异常会变成 "Task exception was never retrieved"
                task.add_done_callback(self._log_async_callback_result)

            try:
                loop.call_soon_threadsafe(schedule)
            except RuntimeError as exc:
                # 事件循环已关闭（进程正在退出）
                logger.warning("事件循环不可用，跳过异步回调 namespace=%s: %s", namespace, exc)

        self._callbacks.append(bridge)
        return cb

    @staticmethod
    def _log_async_callback_result(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("异步配置变更回调执行失败: %r", exc)

    def get_configs(self, namespace: str = DEFAULT_NAMESPACE) -> Dict[str, str]:
        """返回某个 namespace 来自 Apollo 的配置副本（已套用 frozen_keys 快照）。

        不含 ``static_fallback``——那是调用方自己的值，调用方本来就能读到。
        想要合并后的结果用 :meth:`effective_configs`。
        """
        with self._lock:
            declared_frozen = self.frozen_keys.get(namespace, set())
            snapshot = self._frozen_snapshot.get(namespace, {})
            merged = {
                key: value
                for key, value in self._configs.get(namespace, {}).items()
                # 声明冻结但启动时无值的 key：保持"无值"。见 _frozen_snapshot 的说明。
                if key not in declared_frozen
            }
            merged.update(snapshot)
            return merged

    def get_content(self, namespace: str) -> Optional[str]:
        """返回非 properties 格式 namespace 的原文。

        Apollo 对 json / yaml / xml / txt 格式的 namespace 不拆 key-value，而是
        把整段原文放在 ``configurations.content`` 里。这个方法就是取那段原文。

        注意 namespace 名**必须带后缀**（``database.json`` 而非 ``database``），
        否则 Apollo 会解析成同名的 properties namespace。
        """
        return self.get_configs(namespace).get("content")

    def get_json(self, namespace: str) -> Any:
        """解析 json 格式 namespace 的 ``content`` 并返回对象。

        适合存放嵌套结构（如 Django 的 ``DATABASES``）：类型原生保留（int/bool/
        嵌套 dict），且整块原子发布——不会出现"改了 host 漏了密码"的半生效状态。

        代价是 Apollo 的审计、diff、灰度都是 key 粒度的，整块存成一个 ``content``
        就失去了这些。适合变更频率低、成组变更的配置。

        Raises:
            ApolloProtocolError: content 不存在或不是合法 JSON。
        """
        raw = self.get_content(namespace)
        if raw is None:
            raise ApolloProtocolError(
                f"namespace={namespace} 没有 content 字段"
                "（确认 namespace 名带了 .json 后缀，且格式确实是 json）"
            )
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise ApolloProtocolError(
                f"namespace={namespace} 的 content 不是合法 JSON"
            ) from exc

    def merged_configs(self, namespaces: Optional[Sequence[str]] = None) -> Dict[str, str]:
        """跨 namespace 合并，**后面的覆盖前面的**。

        默认按构造时传入的 ``namespaces`` 顺序合并。典型用法是让项目私有配置
        覆盖关联的公共 namespace::

            ApolloClient(namespaces=["theta.infra", "application"])
            client.merged_configs()        # application 的同名 key 胜出

        只合并 properties 形态的 namespace；json/yaml 的 ``content`` 不参与
        （合并两段原文没有意义），用 :meth:`get_json` 单独取。
        """
        order = list(namespaces) if namespaces is not None else self.namespaces
        merged: Dict[str, str] = {}
        for ns in order:
            configs = self.get_configs(ns)
            if set(configs) == {"content"}:
                continue  # 非 properties 格式，跳过
            merged.update(configs)
        return merged

    def effective_configs(self, namespace: str = DEFAULT_NAMESPACE) -> Dict[str, str]:
        """返回「最终生效」的配置：静态兜底值被 Apollo 的值覆盖。

        这是 ``required_keys`` 判定和 ``on_resolved`` 断言所依据的视图——只看
        Apollo 的值会把"回退到静态配置"误判成"什么都没有"。

        冻结快照最后套用，确保声明冻结的 key 在这个视图里也不会被运行期的
        Apollo 值顶掉。
        """
        merged = dict(self.static_fallback.get(namespace, {}))
        for key, value in self.get_configs(namespace).items():
            # Apollo 下发空白值不应覆盖掉调用方的有效兜底值
            if (value or "").strip() or key not in merged:
                merged[key] = value
        with self._lock:
            declared_frozen = self.frozen_keys.get(namespace, set())
            effective_frozen = self._frozen_effective.get(namespace, {})
            # 声明冻结但启动时两边都无值的 key：保持无值
            for key in declared_frozen - set(effective_frozen):
                merged.pop(key, None)
            merged.update(effective_frozen)
        return merged

    def get_value(
        self,
        key: str,
        default: Optional[str] = None,
        namespace: str = DEFAULT_NAMESPACE,
    ) -> Optional[str]:
        """读取单个 key。``frozen_keys`` 返回启动时的快照值。

        被声明冻结但启动时无值的 key 始终返回 ``default``——哪怕 Apollo 后来
        发布了它。冻结语义是"这个 key 的可见值在进程生命周期内不变"，让它中途
        冒出一个值同样会造成"配置显示有值、而资源是按无值建的"的不一致。
        """
        with self._lock:
            frozen = self._frozen_snapshot.get(namespace, {})
            if key in frozen:
                return frozen[key]
            if key in self.frozen_keys.get(namespace, set()):
                return default
            return self._configs.get(namespace, {}).get(key, default)

    @property
    def config_url(self) -> Optional[str]:
        """当前生效的 Config Service 地址。"""
        with self._addr_lock:
            return self._config_url

    @property
    def running(self) -> bool:
        return self._started and not self._stop_event.is_set()

    def start(self) -> None:
        """同步拉取一次全量配置，校验契约，然后启动后台长轮询线程。

        Raises:
            ApolloProtocolError: 响应不符合 Apollo 协议，且 ``allow_fallback=False``。
            ConfigServiceUnavailable: 所有实例不可达且 ``allow_fallback=False``。
            RequiredKeysMissing: ``required_keys`` 未解析出值（见 ``require_resolved_keys``）。
            StartupAssertionFailed: ``on_resolved`` 拒绝了最终配置。
        """
        with self._lifecycle_lock:
            self._start_locked()

    def _start_locked(self) -> None:
        if self._started:
            return
        # 上一轮的线程还没退出（stop() 的 join 超时了）。再起一个会变成两个
        # 线程同时跑、重复派发回调，所以先等它们真正结束。
        for attr, label in (("_thread", "长轮询"), ("_refresh_thread", "定时兜底")):
            thread: Optional[threading.Thread] = getattr(self, attr)
            if thread is None or not thread.is_alive():
                setattr(self, attr, None)
                continue
            thread.join(timeout=self.poll_timeout + 5)
            if thread.is_alive():
                raise ApolloError(f"上一轮{label}线程仍未退出，拒绝重复启动")
            setattr(self, attr, None)

        failures: Dict[str, Exception] = {}
        deadline = (
            time.monotonic() + self.startup_timeout if self.startup_timeout else None
        )
        for ns in self.namespaces:
            if deadline is not None and time.monotonic() >= deadline:
                # 已超出启动预算，剩余 namespace 不再尝试。拖长启动会让探活失败，
                # 代价比"少拉一个 namespace"更大——后者长轮询起来后会自己补上。
                remaining = [n for n in self.namespaces if n not in failures and n != ns]
                failures[ns] = TimeoutError(
                    f"startup_timeout({self.startup_timeout}s) 耗尽，跳过 {[ns, *remaining]}"
                )
                logger.warning(
                    "启动预算 %.1fs 耗尽，跳过未拉取的 namespace: %s",
                    self.startup_timeout,
                    [ns, *remaining],
                )
                for n in remaining:
                    failures[n] = failures[ns]
                break
            try:
                self._fetch_namespace(ns, initial=True)
            except Exception as exc:  # noqa: BLE001 - 汇总后按策略统一处理
                failures[ns] = exc

        if failures and len(failures) == len(self.namespaces):
            # 所有 namespace 都失败，视为 Config Service 整体不可用
            attempted = self._attempted_hosts()
            if not self.allow_fallback:
                # 注意：在这条路径上**不能**补建冻结快照。调用方可能在 Apollo
                # 恢复后复用同一个客户端重试 start()，若此刻已把 namespace 标记
                # 为 captured，重试成功后新拉到的冻结值将永远不可见。
                #
                # 协议错误原样抛出，保留"是代理在干预"这个信息——把它压成
                # ConfigServiceUnavailable 会让调用方以为是网络不可达。
                protocol_errors = [
                    exc for exc in failures.values() if isinstance(exc, ApolloProtocolError)
                ]
                if len(protocol_errors) == len(failures):
                    raise protocol_errors[0]
                raise ConfigServiceUnavailable(
                    f"all namespaces failed to load: {sorted(failures)}", attempted
                ) from next(iter(failures.values()))
            logger.warning(
                "Apollo 不可达，按 allow_fallback=True 降级，调用方静态配置生效; "
                "namespaces=%s tried=%s",
                sorted(failures),
                attempted,
            )
        elif failures:
            logger.warning("部分 namespace 拉取失败: %s", sorted(failures))

        # 为拉取失败的 namespace 补建冻结快照，否则 effective_configs 会把
        # "声明冻结但未捕获"的 key 当成无值删掉——静态兜底值消失，哨兵断言
        # 拿不到哨兵值反而放行。
        self._capture_pending_frozen()

        # 契约校验失败时回滚本次建立的冻结快照。
        #
        # 调用方可以在 Apollo 侧补齐配置后复用同一客户端重试 start()（本方法
        # 支持重入）。若把这次失败启动期间建的快照留下，_capture_frozen 在重试
        # 时会因"已 captured"而跳过，Apollo 新补的冻结值将永远不可见，
        # required_keys 也会永远判定为缺失——进程陷入无法自愈的状态。
        try:
            self._check_required_keys()
            self._run_startup_assertion()
        except Exception:
            self._discard_frozen_snapshots()
            raise

        self._started = True
        self._stop_event.clear()
        # 刚拉过全量，定时兜底从现在开始计时
        self._last_full_refresh = time.monotonic()
        self._thread = threading.Thread(
            target=self._long_poll, name="apollocfg-long-poll", daemon=True
        )
        self._thread.start()

        if self.refresh_interval and self.refresh_interval > 0:
            self._refresh_thread = threading.Thread(
                target=self._refresh_loop, name="apollocfg-refresh", daemon=True
            )
            self._refresh_thread.start()
        logger.info(
            "Apollo 长轮询已启动 appId=%s cluster=%s namespaces=%s frozen=%d required=%d",
            self.app_id,
            self.cluster,
            self.namespaces,
            sum(len(v) for v in self.frozen_keys.values()),
            sum(len(v) for v in self.required_keys.values()),
        )

    def stop(self, timeout: float = 5.0) -> None:
        """停止长轮询线程。可重复调用。

        join 超时时**保留**线程引用——下一次 ``start()`` 需要知道旧线程还活着，
        否则会出现两个线程同时轮询。
        """
        with self._lifecycle_lock:
            self._stop_event.set()
            self._started = False
            # 两个线程都要回收，且不能因为长轮询线程的状态提前返回而漏掉兜底线程
            self._join_worker("_thread", "长轮询", timeout)
            self._join_worker("_refresh_thread", "定时兜底", timeout)

    def _join_worker(self, attr: str, label: str, timeout: float) -> None:
        """join 一个后台线程并按结果决定是否清掉引用。

        join 超时时**保留**引用——下一次 ``start()`` 需要知道旧线程还活着，
        否则会出现两个线程同时跑。
        """
        thread: Optional[threading.Thread] = getattr(self, attr)
        if thread is None or not thread.is_alive():
            setattr(self, attr, None)
            return
        if thread is threading.current_thread():
            # 在变更回调里调 stop() 会 join 自己，直接抛 RuntimeError
            logger.warning("在%s线程内调用 stop()，跳过 join", label)
            return
        thread.join(timeout=timeout)
        if thread.is_alive():
            logger.warning(
                "%s线程在 %.1fs 内未退出，保留引用等待下次 start 时回收", label, timeout
            )
        else:
            setattr(self, attr, None)

    def __enter__(self) -> ApolloClient:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    def close(self) -> None:
        """停止轮询并释放 HTTP 连接池。``stop()`` 之后客户端还可以再 start，
        ``close()`` 之后不应再使用。"""
        self.stop()
        self._session.close()

    # ------------------------- 冻结快照 -------------------------
    def _capture_frozen(self, namespace: str, apollo_configs: Mapping[str, str]) -> None:
        """为一个 namespace 建立冻结快照。只建一次，重复调用无效。

        两份快照分开存，避免 ``static_fallback`` 泄漏进只应反映 Apollo 的视图：

          - ``_frozen_snapshot``  —— Apollo 侧的值，供 ``get_configs`` / ``get_value``
          - ``_frozen_effective`` —— 最终生效值（Apollo 无值时回落静态兜底），
            供 ``effective_configs``
        """
        with self._lock:
            if namespace in self._frozen_captured:
                return
            static = self.static_fallback.get(namespace, {})
            declared = self.frozen_keys.get(namespace, set())
            apollo_frozen = {
                key: apollo_configs[key]
                for key in declared
                if (apollo_configs.get(key) or "").strip()
            }
            effective_frozen = dict(apollo_frozen)
            for key in declared:
                if key not in effective_frozen and key in static:
                    effective_frozen[key] = static[key]

            self._frozen_snapshot[namespace] = apollo_frozen
            self._frozen_effective[namespace] = effective_frozen
            self._frozen_captured.add(namespace)
            if declared:
                logger.info(
                    "namespace=%s 已冻结 %d 个声明 key（禁止热更新），其中 %d 个有值",
                    namespace,
                    len(declared),
                    len(effective_frozen),
                )

    def _capture_pending_frozen(self) -> None:
        """为启动时拉取失败的 namespace 补建冻结快照（只有静态兜底值可用）。

        读未经过滤的 ``_configs`` 而不是 ``get_configs()``：后者会把"声明冻结
        但未捕获"的 key 过滤掉，用它来建快照等于拿已过滤的数据重建，结果恒为空。
        """
        for ns in self.namespaces:
            if ns not in self._frozen_captured:
                with self._lock:
                    raw = dict(self._configs.get(ns, {}))
                self._capture_frozen(ns, raw)

    def _discard_frozen_snapshots(self) -> None:
        """丢弃全部冻结快照，让下一次 ``start()`` 重新建立。

        同时清掉 ``_release_keys``：否则重试时会带上旧的 releaseKey 而收到
        304，``_fetch_namespace`` 直接返回、不会重建快照，冻结值就永久丢了。

        只在启动失败路径上调用——成功启动后冻结值必须保持不变。
        """
        with self._lock:
            self._frozen_captured.clear()
            self._frozen_snapshot = {ns: {} for ns in self.namespaces}
            self._frozen_effective = {ns: {} for ns in self.namespaces}
            # 强制下次拉取拿到完整响应而不是 304
            self._release_keys.clear()

    # ------------------------- 契约校验 -------------------------
    def _check_required_keys(self) -> None:
        """校验 ``required_keys`` 全部解析出了非空值。

        判定依据是 :meth:`effective_configs`——Apollo 的值叠加调用方的静态兜底。
        所以"Apollo 没给但静态配置里有"算解析成功（这正是 fallback 的意义），
        而"两边都没有"才算缺失。

        缺失时的行为由 ``require_resolved_keys`` 决定，**与 ``allow_fallback``
        无关**：前者管"key 解析不出来怎么办"，后者管"Apollo 连不上怎么办"，
        是两个独立的失败模式。
        """
        missing: Dict[str, List[str]] = {}
        for ns, keys in self.required_keys.items():
            if not keys:
                continue
            resolved = self.effective_configs(ns)
            absent = [k for k in keys if not (resolved.get(k) or "").strip()]
            if absent:
                missing[ns] = absent

        if not missing:
            return

        if self.require_resolved_keys or not self.allow_fallback:
            raise RequiredKeysMissing(missing)
        logger.warning(
            "required_keys 未解析出值，按 require_resolved_keys=False 继续; missing=%s",
            {ns: sorted(keys) for ns, keys in missing.items()},
        )

    def _run_startup_assertion(self) -> None:
        """执行调用方注入的启动断言。

        传给回调的是 :meth:`effective_configs`——必须包含静态兜底值，否则哨兵
        断言这个核心用法就是失效的：Apollo 不可达时回调只会看到 ``{}``，
        ``configs.get("MYSQL_DSN")`` 返回 None 而不是哨兵，断言反而通过。
        """
        if self.on_resolved is None:
            return
        for ns in self.namespaces:
            reason = self.on_resolved(ns, self.effective_configs(ns))
            if reason:
                raise StartupAssertionFailed(f"namespace={ns}: {reason}")

    # ------------------------- 服务发现 -------------------------
    def _attempted_hosts(self) -> List[str]:
        with self._addr_lock:
            hosts = []
            if self._config_url:
                hosts.append(self._config_url)
            hosts.extend(h for h in self._service_pool if h != self._config_url)
            return hosts

    def _discover_services(self) -> List[str]:
        """通过 meta server 获取 Config Service 实例列表。

        返回去重后的 homepageUrl 列表；meta server 未配置或查询失败时返回空列表。
        """
        if not self.meta_server:
            return []
        url = f"{self.meta_server}/services/config"
        try:
            resp = requests.get(url, timeout=self.request_timeout)
            resp.raise_for_status()
            items = resp.json() or []
        except (requests.RequestException, ValueError) as exc:
            logger.warning("meta server 查询失败 url=%s: %s", url, exc)
            return []

        if not isinstance(items, list):
            logger.warning("meta server 响应应为数组，实际是 %s", type(items).__name__)
            return []
        seen: List[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            home = item.get("homepageUrl")
            if not isinstance(home, str):
                continue
            home = home.rstrip("/")
            if home and home not in seen:
                seen.append(home)
        if seen:
            # 随机化顺序，避免所有客户端都打到列表里的第一个实例
            random.shuffle(seen)
            logger.info("发现 %d 个 Config Service 实例", len(seen))
        return seen

    def _ensure_config_url(self) -> str:
        """返回可用的 Config Service 地址，必要时触发服务发现。"""
        with self._addr_lock:
            if self._config_url:
                return self._config_url
            if not self._service_pool:
                self._service_pool = self._discover_services()
            if not self._service_pool:
                raise ConfigServiceUnavailable(
                    "no Config Service available from meta server",
                    self._attempted_hosts(),
                )
            self._config_url = self._service_pool[0]
            return self._config_url

    def _rotate_config_url(self) -> bool:
        """当前实例失败后轮换到下一个。返回是否换到了新地址。

        池里没有其他候选时会**重新查一次 meta server**：滚动发布会把整批实例
        换掉，若只认启动时发现的那批地址，长跑进程会永久钉在已下线的实例上，
        热更新静默失效直到进程重启。

        没有配置 meta_server 时无处可换，返回 False 让调用方抛出原始异常，
        而不是静默重试同一个地址。
        """
        with self._addr_lock:
            failed = self._config_url
            candidates = [h for h in self._service_pool if h != failed]

        if not candidates and self.meta_server:
            # 池耗尽，重新发现。注意在锁外发 HTTP——占着 _addr_lock 会把
            # config_url 属性和 _ensure_config_url 的调用方一起阻塞住。
            discovered = self._discover_services()
            with self._addr_lock:
                if discovered:
                    self._service_pool = discovered
                failed = self._config_url
                candidates = [h for h in discovered if h != failed]

        with self._addr_lock:
            if not candidates:
                return False
            self._rotations += 1
            self._config_url = candidates[0]
            # 把失败的实例挪到队尾，下次优先试其他的
            self._service_pool = candidates + ([failed] if failed else [])
            logger.warning("Config Service 轮换: %s -> %s", failed, self._config_url)
            return True

    # ------------------------- HTTP / 签名 -------------------------
    def _auth_headers(self, path_with_query: str) -> Dict[str, str]:
        """Apollo 1.6+ 访问密钥签名。

        明文 = 毫秒时间戳 + "\\n" + pathWithQuery，HMAC-SHA1 后 Base64，
        通过 ``Timestamp`` / ``Authorization`` 两个头传递（没有独立 Signature 头）。
        未配置 ``access_key_secret`` 时返回空 dict（匿名访问）。
        """
        if not self.access_key_secret:
            return {}
        timestamp = str(int(time.time() * 1000))
        plain = f"{timestamp}\n{path_with_query}"
        digest = hmac.new(
            self.access_key_secret.encode("utf-8"), plain.encode("utf-8"), hashlib.sha1
        ).digest()
        return {
            "Timestamp": timestamp,
            "Authorization": f"Apollo {self.app_id}:{base64.b64encode(digest).decode('utf-8')}",
        }

    def _signed_get(
        self, url: str, params: Dict[str, Any], timeout: float
    ) -> requests.Response:
        """发起带签名的 GET。

        用 ``Request.prepare()`` 得到最终 pathWithQuery，确保签名用的 path 与实际
        发出的 URL 完全一致（含查询串编码），否则服务端验签会失败。
        """
        prepared = Request("GET", url, params=params).prepare()
        headers = self._auth_headers(prepared.path_url)
        return self._session.get(prepared.url, headers=headers, timeout=timeout)

    # ------------------------- 配置拉取 -------------------------
    def _fetch_namespace(
        self, namespace: str, initial: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """拉取单个 namespace。返回已过滤 frozen_keys 的变更集。"""
        resp = self._request_with_failover(namespace)

        # releaseKey 与服务端一致时 Apollo 返回 304 + 空 body，表示"无变更"。
        # 必须先判 304 再判状态码：raise_for_status() 不拦 304。
        if resp.status_code == 304:
            return {}

        # 必须在"空 body 短路"之前校验状态码。代理层过载时常返回空 body 的
        # 502/504，若先判空 body 就会把服务端错误误判成"无变更"，于是带着空
        # 配置启动成功——allow_fallback=False 也挡不住。
        resp.raise_for_status()

        if not resp.text.strip():
            # 2xx 却没有 body，不符合 Apollo 协议。当作错误而不是"无变更"，
            # 否则同样会静默地把空配置当成有效结果。
            raise ApolloProtocolError(
                f"namespace={namespace} 返回 HTTP {resp.status_code} 但 body 为空"
            )

        new_configs, new_release = self._parse_config_payload(namespace, resp)

        if initial:
            self._capture_frozen(namespace, new_configs)

        with self._lock:
            old_configs = self._configs.get(namespace, {})
            raw_changes = self._diff(old_configs, new_configs)
            self._configs[namespace] = new_configs
            self._release_keys[namespace] = new_release

            # frozen_keys 对调用方不可见地变化，不该出现在回调里。
            #
            # 用「声明的」冻结 key 集合而不是快照里实际有值的那些：某个 key
            # 启动时两边都没有、后来 Apollo 才发布，它仍然是被声明冻结的，
            # 不该因为快照里没有就放它进回调。
            frozen_names = set(self.frozen_keys.get(namespace, set()))

        changes = {k: v for k, v in raw_changes.items() if k not in frozen_names}
        suppressed = sorted(set(raw_changes) & frozen_names)
        if suppressed:
            logger.info(
                "namespace=%s 以下 key 在 Apollo 侧已变更，但因冻结不生效: %s",
                namespace,
                suppressed,
            )

        if changes and not initial:
            # 只记 key 名，绝不记值
            logger.info("namespace=%s 配置变更: %s", namespace, sorted(changes))
            self._dispatch(namespace, changes)
        return changes

    @staticmethod
    def _parse_config_payload(
        namespace: str, resp: requests.Response
    ) -> tuple[Dict[str, str], str]:
        """严格校验并解析配置响应体。

        不能用 ``data.get("configurations") or {}`` 了事：代理返回的
        ``200 {"error": ...}`` 会被解释成"该 namespace 配置为空"，于是内存配置
        被清空、diff 产出一堆删除变更并派发给调用方——一次代理故障就可能让
        运行中的服务按"配置全删了"去拆资源。
        """
        try:
            data = resp.json()
        except ValueError as exc:
            raise ApolloProtocolError(
                f"namespace={namespace} 响应不是合法 JSON"
            ) from exc

        if not isinstance(data, dict):
            raise ApolloProtocolError(
                f"namespace={namespace} 响应顶层应为对象，实际是 {type(data).__name__}"
            )
        configurations = data.get("configurations")
        if not isinstance(configurations, dict):
            raise ApolloProtocolError(
                f"namespace={namespace} 响应缺少 configurations 对象"
                f"（实际 {type(configurations).__name__}）"
            )
        release_key = data.get("releaseKey")
        if release_key is not None and not isinstance(release_key, str):
            raise ApolloProtocolError(
                f"namespace={namespace} releaseKey 应为字符串"
            )
        # 值统一成字符串：Apollo 的 properties namespace 只会给字符串，
        # 但 json/yaml 格式的 namespace 可能给出别的标量类型。
        # None 和容器类型要拒绝——str(None) == "None" 会伪装成一个正常值。
        coerced: Dict[str, str] = {}
        for key, value in configurations.items():
            if value is None or isinstance(value, (list, dict)):
                raise ApolloProtocolError(
                    f"namespace={namespace} 的 key={key} 值类型非法"
                    f"（{type(value).__name__}）"
                )
            coerced[str(key)] = value if isinstance(value, str) else str(value)
        return coerced, release_key or ""

    def _request_with_failover(self, namespace: str) -> requests.Response:
        """拉取配置，失败时依次尝试服务池里所有未试过的实例。

        只重试一次是不够的：三个实例、前两个都挂的情况下，单次轮换仍会失败，
        而服务发现的意义正是"有健康实例就该找到它"。

        哪些失败该换实例：

          - 连接层异常（超时 / 连接被拒）—— 换。这个实例或链路有问题。
          - **5xx** —— 换。实例过载或内部错误，别的实例可能是健康的；代理层
            过载时返回的空 body 502/504 也落在这里。
          - 4xx —— **不换**。appId / namespace 配错、验签失败都是 4xx，换实例
            结果一样，重试只会放大错误（也会多打几份无效请求）。

        全部实例都失败时抛出最后一次的异常，交给调用方按降级策略处置。
        """
        attempted: List[str] = []
        last_error: Optional[BaseException] = None
        refreshed = False

        while True:
            base = self._ensure_config_url()
            if base in attempted:
                # 已经绕回试过的实例，说明池里的都试完了
                break
            attempted.append(base)

            try:
                resp = self._request_namespace(base, namespace)
            except requests.RequestException as exc:
                last_error = exc
                logger.warning("namespace=%s 从 %s 拉取失败: %s", namespace, base, exc)
            else:
                if resp.status_code < 500:
                    # 含 2xx / 304 / 4xx，交给调用方判断（4xx 由 raise_for_status 抛）
                    return resp
                last_error = requests.HTTPError(
                    f"{resp.status_code} from {base}", response=resp
                )
                logger.warning(
                    "namespace=%s 从 %s 拉取得到 HTTP %s", namespace, base, resp.status_code
                )

            if not self._rotate_config_url():
                break
            if all(host in attempted for host in self._known_hosts()):
                # 池里所有实例都试过且都失败了。整批实例可能已被滚动替换，
                # 强制重新发现一次；拿到新地址就继续试，否则放弃。
                if refreshed or not self._refresh_service_pool(exclude=attempted):
                    break
                refreshed = True

        assert last_error is not None  # 循环至少执行一次，要么 return 要么置 last_error
        raise last_error

    def _refresh_service_pool(self, exclude: Sequence[str]) -> bool:
        """强制重新查询 meta server。返回是否发现了未试过的新实例。"""
        if not self.meta_server:
            return False
        discovered = self._discover_services()
        fresh = [host for host in discovered if host not in exclude]
        if not fresh:
            return False
        with self._addr_lock:
            self._service_pool = fresh + [h for h in discovered if h in exclude]
            self._config_url = fresh[0]
        logger.warning("服务池整批失效，重新发现后换到 %s", fresh[0])
        return True

    def _known_hosts(self) -> List[str]:
        with self._addr_lock:
            return list(self._service_pool)

    def _request_namespace(self, base: str, namespace: str) -> requests.Response:
        # 路径段必须编码：含 / 或空格的 namespace 会改变 URL 结构，
        # 也会让签名用的 path 和服务端解析的不一致。
        url = (
            f"{base}/configs/{quote(self.app_id, safe='')}"
            f"/{quote(self.cluster, safe='')}/{quote(namespace, safe='')}"
        )
        params: Dict[str, Any] = {}
        release_key = self._release_keys.get(namespace)
        if release_key:
            params["releaseKey"] = release_key
        if self.client_ip:
            params["ip"] = self.client_ip
        return self._signed_get(url, params, self.request_timeout)

    def _dispatch(self, namespace: str, changes: Dict[str, Dict[str, Any]]) -> None:
        """派发变更回调。单个回调抛异常不影响其他回调，也不中断轮询。"""
        for cb in list(self._callbacks):
            try:
                # 传副本：前一个回调若改动 changes 不应影响后续回调。
                # 内层 dict 也要复制，否则 changes[k]["new"] 仍是共享的。
                cb(namespace, {k: dict(v) for k, v in changes.items()})
            except Exception:
                logger.exception("配置变更回调执行失败 namespace=%s", namespace)

    @staticmethod
    def _diff(
        old: Mapping[str, str], new: Mapping[str, str]
    ) -> Dict[str, Dict[str, Any]]:
        changes: Dict[str, Dict[str, Any]] = {}
        for key in set(old) | set(new):
            old_value, new_value = old.get(key), new.get(key)
            if old_value != new_value:
                changes[key] = {"old": old_value, "new": new_value}
        return changes

    # ------------------------- 长轮询 -------------------------
    def _long_poll(self) -> None:
        """长轮询主循环。

        这个循环必须永不因异常退出——线程一死，进程会继续用启动时的配置运行，
        而且没有任何报错，是最难排查的故障形态。所以最外层兜一个 Exception。
        """
        backoff = self.poll_interval
        consecutive_timeouts = 0
        while not self._stop_event.is_set():
            try:
                changed = self._poll_once()
                backoff = self.poll_interval
                consecutive_timeouts = 0
                if not changed:
                    continue
            except requests.exceptions.Timeout:
                # 客户端读超时。poll_timeout > 服务端挂起时长，所以单次超时多半是
                # 网络抖动，直接续下一轮即可，不退避也不换地址。
                #
                # 但连续超时就不是抖动了——当前实例或链路有问题。此时必须轮换，
                # 否则会永久钉在这个实例上，热更新静默失效且没有任何报错。
                consecutive_timeouts += 1
                if consecutive_timeouts >= _MAX_CONSECUTIVE_TIMEOUTS:
                    logger.warning(
                        "连续 %d 次长轮询超时，轮换 Config Service", consecutive_timeouts
                    )
                    self._rotate_config_url()
                    consecutive_timeouts = 0
                continue
            except Exception as exc:  # noqa: BLE001 - 见上方说明
                consecutive_timeouts = 0
                logger.warning("长轮询异常，%.1fs 后重试: %s", backoff, exc)
                self._rotate_config_url()
                if self._stop_event.wait(backoff):
                    break
                backoff = min(backoff * 2, self.max_retry_backoff)

    def _refresh_loop(self) -> None:
        """定时全量拉取兜底，跑在**独立线程**里。

        公司对非官方 Apollo 客户端的要求之一（定时主动发起全量拉取），也确实是
        长轮询丢通知时唯一的自愈路径。

        为什么不放在长轮询循环里：兜底要兜的正是"长轮询出问题"。而长轮询每轮会
        被服务端挂住约 60s（代理异常时可能到 poll_timeout），把兜底放在同一个
        循环里，它的触发间隔就被长轮询的节奏绑死了——实测 refresh_interval=8s
        时实际只能每 61s 执行一次。安全机制不该依赖它要保护的东西。
        """
        while not self._stop_event.wait(self.refresh_interval):
            logger.debug("定时全量拉取（间隔 %.0fs）", self.refresh_interval)
            for ns in self.namespaces:
                if self._stop_event.is_set():
                    return
                try:
                    # 清掉 releaseKey 强制拿完整响应，否则服务端会回 304，
                    # 兜底就失去了意义（它要兜的正是"通知丢了但值其实变了"）。
                    with self._lock:
                        self._release_keys.pop(ns, None)
                    self._fetch_namespace(ns)
                except Exception as exc:  # noqa: BLE001 - 兜底失败只告警
                    logger.warning("定时全量拉取失败 namespace=%s: %s", ns, exc)

    def _poll_once(self) -> Optional[List[str]]:
        """执行一轮长轮询。

        Returns:
            None 表示无变更；否则返回本轮更新了的 namespace 列表。

        正常情况下服务端会把请求挂起约 60s 才返回，所以调用方收到结果后立刻
        续下一轮是对的。但服务端或代理可能立刻返回（304、空通知数组、或
        notificationId 与本地相同），此时立刻续下一轮就是忙循环——实测能打出
        800+ 次/0.6 秒。所以本方法在"没有任何更新且服务端秒回"时自己补足
        ``poll_interval``，把节流收口在一处。
        """
        started = time.monotonic()
        result = self._poll_once_inner()
        if not result:
            elapsed = time.monotonic() - started
            remaining = self.poll_interval - elapsed
            if remaining > 0:
                self._stop_event.wait(remaining)
        return result

    def _poll_once_inner(self) -> Optional[List[str]]:
        base = self._ensure_config_url()
        notifications = [
            {"namespaceName": ns, "notificationId": self._notification_ids.get(ns, -1)}
            for ns in self.namespaces
        ]
        resp = self._signed_get(
            f"{base}/notifications/v2",
            {
                "appId": self.app_id,
                "cluster": self.cluster,
                "notifications": json.dumps(notifications),
            },
            self.poll_timeout,
        )
        if resp.status_code == 304:
            return None
        resp.raise_for_status()
        try:
            items = resp.json()
        except ValueError as exc:
            raise ApolloProtocolError("notifications 响应不是合法 JSON") from exc
        # 不能写 `resp.json() or []`：200 {} 这类 falsy JSON 会被吞成"无变更"，
        # 既掩盖了协议问题，又因为立刻返回而打成忙循环。
        if not isinstance(items, list):
            raise ApolloProtocolError(
                f"notifications 响应应为数组，实际是 {type(items).__name__}"
            )
        if not items:
            return None

        updated: List[str] = []
        failed: List[str] = []
        ignored: List[str] = []
        for item in items:
            # 畸形通知项抛协议错误而不是静默跳过：若服务端持续返回同样的畸形
            # 响应，静默跳过会让 _poll_once 返回空列表、被当成成功，于是没有
            # 退避、立刻发起下一轮，变成高频忙循环。
            if not isinstance(item, dict):
                raise ApolloProtocolError(
                    f"notifications 项应为对象，实际是 {type(item).__name__}"
                )
            ns = item.get("namespaceName")
            nid = item.get("notificationId")
            if not isinstance(ns, str) or not ns:
                raise ApolloProtocolError("notifications 项缺少 namespaceName")
            if not isinstance(nid, int) or isinstance(nid, bool):
                raise ApolloProtocolError(
                    f"namespace={ns} 的 notificationId 应为整数"
                )
            if ns not in self._notification_ids:
                # 服务端返回了我们没订阅的 namespace。不是协议错误，忽略即可，
                # 但要记下来——若整轮通知全被忽略，调用方需要据此退避。
                logger.warning("收到未订阅 namespace 的通知: %s", ns)
                ignored.append(ns)
                continue
            # 用"不相等"而不是"更大"：Config Service 重启或轮换到别的实例后，
            # 服务端的 notificationId 可能比客户端持有的更小。要求单调递增会
            # 让这种情况下的热更新永久失效。
            if nid == self._notification_ids.get(ns, -1):
                continue
            try:
                self._fetch_namespace(ns)
                # 只在拉取成功后推进 id。先推进再拉取的话，一次 5xx 就会让
                # 客户端"认为自己已追平"，后续长轮询只收到 304，这次发布
                # 永远不会再被拉取。
                self._notification_ids[ns] = nid
                updated.append(ns)
            except Exception as exc:  # noqa: BLE001
                logger.warning("通知触发后拉取失败 namespace=%s: %s", ns, exc)
                failed.append(ns)

        if failed and not updated:
            # id 没推进，服务端下一轮会立刻再返回同一个通知。此时必须让调用方
            # 走退避路径，否则就是无间隔的忙循环。
            raise ConfigServiceUnavailable(
                f"notification fetch failed for: {sorted(failed)}",
                self._attempted_hosts(),
            )
        if ignored and not updated:
            # 整轮通知全部来自未订阅的 namespace。我们的请求不带这些 namespace，
            # 它们的 id 无从推进，服务端可能一直返回同一份响应。按"无变更"
            # 处理，节流由 _poll_once 统一负责。
            return None
        return updated
