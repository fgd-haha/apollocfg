"""并发安全。

两组共享状态，分别由两把锁保护：

  - ``_lock``      → ``_configs`` / ``_release_keys`` / ``_frozen_snapshot``
  - ``_addr_lock`` → ``_config_url`` / ``_service_pool``

地址状态单独一把锁是必要的：服务发现要发 HTTP，若占用 ``_lock`` 会把所有
``get_value`` 调用方一起阻塞住。

地址状态确实会被两个线程同时访问——主线程（``start()``、``config_url`` 属性）
与轮询线程（失败轮换）都会碰它。
"""
from __future__ import annotations

import threading
import time

import requests
import requests_mock

from apollocfg.client import ApolloClient
from tests.conftest import META_URL, config_payload, services_payload

SERVICES_PATH = f"{META_URL}/services/config"
HOST_A = "http://node-a.test"
HOST_B = "http://node-b.test"


def test_concurrent_reads_during_rotation_are_consistent():
    """轮询线程轮换地址的同时，主线程读 config_url 不应看到撕裂状态。"""
    observed = []
    errors = []

    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=services_payload([HOST_A, HOST_B]))
        for host in (HOST_A, HOST_B):
            m.get(
                f"{host}/configs/unit-app/default/application",
                json=config_payload({"K": "V"}),
            )
            # 长轮询持续失败，迫使轮询线程反复轮换地址
            m.get(f"{host}/notifications/v2", status_code=500)

        client = ApolloClient(
            app_id="unit-app",
            meta_server=META_URL,
            poll_timeout=61,
            request_timeout=1,
            poll_interval=0.01,
            max_retry_backoff=0.02,
        )
        client.start()

        def reader():
            try:
                for _ in range(500):
                    url = client.config_url
                    observed.append(url)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        client.stop()

    assert not errors, f"并发读取抛异常: {errors}"
    assert observed, "应当读到地址"
    # 任何时刻读到的都必须是合法实例之一，不能是 None 或半成品
    assert set(observed) <= {HOST_A, HOST_B}, f"读到非法地址: {set(observed)}"


def test_concurrent_config_reads_during_fetch():
    """拉取更新配置的同时并发读取，不应抛异常或读到混合状态。"""
    errors = []
    snapshots = []

    with requests_mock.Mocker() as m:
        m.get(
            f"{HOST_A}/configs/unit-app/default/application",
            [
                {"json": config_payload({"A": "1", "B": "1"}, "rk1")},
                {"json": config_payload({"A": "2", "B": "2"}, "rk2")},
                {"json": config_payload({"A": "3", "B": "3"}, "rk3")},
            ],
        )
        m.get(f"{HOST_A}/notifications/v2", status_code=304)

        client = ApolloClient(
            app_id="unit-app",
            config_url=HOST_A,
            poll_timeout=61,
            request_timeout=1,
            poll_interval=0.01,
        )
        client.start()

        def reader():
            try:
                for _ in range(300):
                    configs = client.get_configs("application")
                    if configs:
                        snapshots.append((configs.get("A"), configs.get("B")))
            except Exception as exc:
                errors.append(exc)

        def writer():
            try:
                for _ in range(2):
                    client._fetch_namespace("application")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(3)]
        threads.append(threading.Thread(target=writer))
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        client.stop()

    assert not errors, f"并发访问抛异常: {errors}"
    # A 和 B 在每一份发布里都同步变化，读到的快照必须内部一致
    inconsistent = [pair for pair in snapshots if pair[0] != pair[1]]
    assert not inconsistent, f"读到撕裂的配置快照: {inconsistent[:5]}"


def test_get_configs_returns_copy():
    """返回的是副本，调用方改它不能影响客户端内部状态。"""
    with requests_mock.Mocker() as m:
        m.get(
            f"{HOST_A}/configs/unit-app/default/application",
            json=config_payload({"K": "V"}),
        )
        m.get(f"{HOST_A}/notifications/v2", status_code=304)
        client = ApolloClient(
            app_id="unit-app", config_url=HOST_A, poll_timeout=61, poll_interval=0.01
        )
        client.start()
        client.stop()

    configs = client.get_configs("application")
    configs["K"] = "tampered"
    configs["NEW"] = "injected"

    assert client.get_value("K") == "V"
    assert client.get_value("NEW") is None


def test_rotation_without_meta_server_returns_false():
    """没有 meta server 时无处可换，应返回 False 让原始异常抛出。"""
    with requests_mock.Mocker() as m:
        m.get(
            f"{HOST_A}/configs/unit-app/default/application",
            exc=requests.exceptions.ConnectTimeout,
        )
        client = ApolloClient(
            app_id="unit-app",
            config_url=HOST_A,
            allow_fallback=True,
            poll_timeout=61,
            request_timeout=1,
        )
        assert client._rotate_config_url() is False
        assert client.config_url == HOST_A, "无处可换时不应改动原地址"


def test_stop_join_timeout_does_not_spawn_duplicate_thread():
    """stop() 的 join 超时后再 start()，不能出现两个轮询线程。

    线程引用在 join 超时时必须保留——丢掉引用就无从判断旧线程是否还活着，
    下一次 start() 会把尚未退出的旧线程"放行"，形成重复拉取与重复回调。
    """
    with requests_mock.Mocker() as m:
        m.get(
            f"{HOST_A}/configs/unit-app/default/application",
            json=config_payload({"K": "V"}),
        )
        m.get(f"{HOST_A}/notifications/v2", status_code=304)
        client = ApolloClient(
            app_id="unit-app",
            config_url=HOST_A,
            poll_timeout=61,
            request_timeout=1,
            poll_interval=0.01,
        )
        client.start()
        first = client._thread
        client.stop(timeout=0)
        client.start()
        second = client._thread
        alive = [t for t in {first, second} if t is not None and t.is_alive()]
        client.stop()

    assert len(alive) <= 1, f"出现重复轮询线程: {len(alive)}"


def test_concurrent_start_creates_single_thread():
    """并发 start() 只能产生一个轮询线程。"""
    with requests_mock.Mocker() as m:
        m.get(
            f"{HOST_A}/configs/unit-app/default/application",
            json=config_payload({"K": "V"}),
        )
        m.get(f"{HOST_A}/notifications/v2", status_code=304)
        client = ApolloClient(
            app_id="unit-app",
            config_url=HOST_A,
            poll_timeout=61,
            request_timeout=1,
            poll_interval=0.01,
        )
        threads = [threading.Thread(target=client.start) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        poll_threads = [
            t for t in threading.enumerate()
            if t.name == "apollocfg-long-poll" and t is client._thread
        ]
        client.stop()

    assert len(poll_threads) == 1, f"并发 start 产生了 {len(poll_threads)} 个线程"


def test_stop_from_within_callback_does_not_deadlock():
    """在变更回调里调 stop() 不应因 join 自己而抛 RuntimeError。"""
    errors = []

    def stopper(namespace, changes):
        try:
            client.stop(timeout=1)
        except Exception as exc:
            errors.append(exc)

    with requests_mock.Mocker() as m:
        m.get(
            f"{HOST_A}/configs/unit-app/default/application",
            [
                {"json": config_payload({"K": "V1"}, "rk1")},
                {"json": config_payload({"K": "V2"}, "rk2")},
            ],
        )
        m.get(
            f"{HOST_A}/notifications/v2",
            json=[{"namespaceName": "application", "notificationId": 11}],
        )
        client = ApolloClient(
            app_id="unit-app",
            config_url=HOST_A,
            poll_timeout=61,
            request_timeout=1,
            poll_interval=0.01,
        )
        client.register_callback(stopper)
        client.start()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and client.running:
            time.sleep(0.02)
        client.stop(timeout=2)

    assert not errors, f"回调内 stop() 抛异常: {errors}"


def test_concurrent_fetch_does_not_lose_update():
    """并发拉同一个 namespace 时，慢的旧响应不能覆盖快的新响应。

    长轮询线程和定时兜底线程会并发拉取。HTTP 往返若在锁外，先发的请求可能
    后返回 —— 旧响应覆盖新响应，releaseKey 也跟着回退，后续请求带着旧
    releaseKey 可能拿不到更新。实测修复前最终值是 OLD/rk1。
    """

    class FakeResponse:
        def __init__(self, text, delay):
            self.status_code = 200
            self._text = text
            self._delay = delay

        @property
        def text(self):
            time.sleep(self._delay)   # 模拟网络延迟发生在"读响应"阶段
            return self._text

        def raise_for_status(self):
            pass

        def json(self):
            import json as _json

            return _json.loads(self._text)

    client = ApolloClient(
        app_id="unit-app", config_url=HOST_A, poll_timeout=61, refresh_interval=0
    )
    calls = {"n": 0}

    def fake(namespace, timeout_budget=None):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResponse('{"configurations":{"K":"OLD"},"releaseKey":"rk1"}', 0.5)
        return FakeResponse('{"configurations":{"K":"NEW"},"releaseKey":"rk2"}', 0.0)

    client._request_with_failover = fake

    slow = threading.Thread(target=lambda: client._fetch_namespace("application"))
    fast = threading.Thread(target=lambda: client._fetch_namespace("application"))
    slow.start()
    time.sleep(0.05)
    fast.start()
    slow.join(timeout=10)
    fast.join(timeout=10)

    assert client.get_value("K") == "NEW", "旧响应覆盖了新响应"
    assert client._release_keys["application"] == "rk2", "releaseKey 回退了"
