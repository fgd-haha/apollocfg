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
