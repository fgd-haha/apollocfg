"""长轮询循环的行为。

最关键的一条：**轮询线程必须永不因异常退出**。线程一死，进程会继续用启动时的
配置运行，没有任何报错——这是最难排查的故障形态（filamoon/pyapollo 就有这个 bug，
默认 timeout=35 小于服务端挂起的 60s，第一轮长轮询必然抛 ReadTimeout 并杀死线程）。
"""
from __future__ import annotations

import time

import requests
import requests_mock

from apollocfg.client import ApolloClient
from tests.conftest import (
    CONFIG_URL,
    RecordingCallback,
    config_payload,
    notification_payload,
)

CONFIGS_PATH = f"{CONFIG_URL}/configs/unit-app/default/application"
NOTIFY_PATH = f"{CONFIG_URL}/notifications/v2"


def _wait(condition, timeout=2.0, interval=0.02):
    """轮询等待条件成立，避免固定 sleep 造成的脆弱测试。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


def test_start_loads_config_without_firing_callback(client_kwargs):
    """启动时的首次加载不是"变更"，不该触发回调。

    这与上游行为不同（上游会在初始加载时触发一次）。理由：调用方注册回调是为了
    响应"运行期的变化"，启动时值本来就是通过 get_value 读的，再推一次回调会让
    调用方在启动阶段重复执行一遍重建逻辑。
    """
    callback = RecordingCallback()
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**client_kwargs)
        client.register_callback(callback)
        client.start()
        client.stop()

    assert client.get_configs("application") == {"K": "V"}
    assert callback.calls == []


def test_notification_triggers_fetch_and_callback(client_kwargs):
    callback = RecordingCallback()
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "V1"}, "rk1")},
                {"json": config_payload({"K": "V2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=42))
        client = ApolloClient(**client_kwargs)
        client.register_callback(callback)
        client.start()
        got = _wait(lambda: callback.calls)
        client.stop()

    assert got, "长轮询收到新 notificationId 后应拉取并触发回调"
    namespace, changes = callback.calls[0]
    assert namespace == "application"
    assert changes["K"] == {"old": "V1", "new": "V2"}


def test_304_keeps_thread_alive(client_kwargs):
    """无变更返回 304 是常态，循环必须继续而不是退出。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        notify = m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**client_kwargs)
        client.start()
        got = _wait(lambda: notify.call_count >= 3)
        alive = client._thread.is_alive()
        client.stop()

    assert got, "304 之后应立即发起下一轮"
    assert alive, "304 不应终止轮询线程"


def test_read_timeout_keeps_thread_alive(client_kwargs):
    """客户端读超时也不能杀死线程。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        notify = m.get(NOTIFY_PATH, exc=requests.exceptions.ReadTimeout)
        client = ApolloClient(**client_kwargs)
        client.start()
        got = _wait(lambda: notify.call_count >= 3)
        alive = client._thread.is_alive()
        client.stop()

    assert got
    assert alive, "ReadTimeout 不应终止轮询线程"


def test_server_error_backs_off_but_keeps_thread_alive(client_kwargs):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        notify = m.get(NOTIFY_PATH, status_code=500)
        client = ApolloClient(**client_kwargs)
        client.start()
        got = _wait(lambda: notify.call_count >= 2)
        alive = client._thread.is_alive()
        client.stop()

    assert got
    assert alive, "服务端 5xx 应退避重试而不是退出"


def test_malformed_json_keeps_thread_alive(client_kwargs):
    """响应体不是合法 JSON 时也不能崩。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        notify = m.get(NOTIFY_PATH, text="not json at all")
        client = ApolloClient(**client_kwargs)
        client.start()
        got = _wait(lambda: notify.call_count >= 2)
        alive = client._thread.is_alive()
        client.stop()

    assert got
    assert alive


def test_callback_exception_does_not_kill_thread(client_kwargs):
    """调用方的回调抛异常，不能掀翻轮询线程。"""
    bad = RecordingCallback(raises=RuntimeError("业务回调炸了"))
    good = RecordingCallback()
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "V1"}, "rk1")},
                {"json": config_payload({"K": "V2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=7))
        client = ApolloClient(**client_kwargs)
        client.register_callback(bad)
        client.register_callback(good)
        client.start()
        _wait(lambda: good.calls)
        alive = client._thread.is_alive()
        client.stop()

    assert bad.calls, "抛异常的回调应被调用过"
    assert good.calls, "前一个回调抛异常不应阻止后续回调"
    assert alive


def test_stale_notification_id_is_ignored(client_kwargs):
    """notificationId 不前进时不应重复拉取。"""
    with requests_mock.Mocker() as m:
        configs = m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=5))
        client = ApolloClient(**client_kwargs)
        client.start()
        _wait(lambda: configs.call_count >= 2)
        first = configs.call_count
        time.sleep(0.2)
        client.stop()

    # 第一次 start 拉一次 + 通知触发拉一次，之后 id 不变不应继续拉
    assert configs.call_count <= first + 1


def test_stop_is_idempotent(client_kwargs):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**client_kwargs)
        client.start()
        client.stop()
        client.stop()

    assert client.running is False


def test_start_is_idempotent(client_kwargs):
    with requests_mock.Mocker() as m:
        configs = m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**client_kwargs)
        client.start()
        count_after_first = configs.call_count
        client.start()
        client.stop()

    assert configs.call_count == count_after_first, "重复 start 不应重复拉取"


def test_context_manager_starts_and_stops(client_kwargs):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        with ApolloClient(**client_kwargs) as client:
            assert client.running is True
            assert client.get_value("K") == "V"
        assert client.running is False
