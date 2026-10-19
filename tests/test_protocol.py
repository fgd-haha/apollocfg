"""响应体校验与通知语义。

这组测试锁住几个「静默失效」类故障——它们的共同特征是：没有异常、没有告警，
但配置从此不再正确更新。

动机都来自真实的失败形态：
  - 代理返回 ``200 {"error": ...}``，被解释成"配置为空"，派发一堆删除变更；
  - notificationId 先推进再拉取，一次 5xx 就让这次发布永久丢失；
  - 服务端重启后 notificationId 变小，单调递增判断会让热更新永久失效。
"""
from __future__ import annotations

import time

import pytest
import requests_mock

from apollocfg.client import ApolloClient
from apollocfg.errors import ApolloProtocolError
from tests.conftest import (
    CONFIG_URL,
    RecordingCallback,
    config_payload,
    notification_payload,
)

CONFIGS_PATH = f"{CONFIG_URL}/configs/unit-app/default/application"
NOTIFY_PATH = f"{CONFIG_URL}/notifications/v2"


def _wait(condition, timeout=2.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


# ------------------------- 响应体校验 -------------------------
@pytest.mark.parametrize(
    "payload",
    [
        {"error": "proxy error"},           # 代理的错误 JSON
        {"releaseKey": "rk"},               # 缺 configurations
        {"configurations": None},           # configurations 为 null
        {"configurations": []},             # configurations 是数组
        {"configurations": "oops"},         # configurations 是字符串
    ],
)
def test_malformed_2xx_payload_is_protocol_error(client_kwargs, payload):
    """2xx 但结构不对，必须报错而不是当成"配置为空"。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=payload)
        client = ApolloClient(**client_kwargs)
        with pytest.raises(ApolloProtocolError):
            client._fetch_namespace("application", initial=True)


def test_malformed_payload_does_not_wipe_existing_config(client_kwargs):
    """畸形响应不能清空已有配置，也不能派发删除变更。

    一次代理故障就让运行中的服务按"配置全删了"去拆资源，是最坏的情形。
    """
    callback = RecordingCallback()
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"DSN": "mysql://prod"}, "rk1")},
                {"json": {"error": "proxy error"}},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**client_kwargs)
        client.register_callback(callback)
        client.start()

        with pytest.raises(ApolloProtocolError):
            client._fetch_namespace("application")
        client.stop()

    assert client.get_value("DSN") == "mysql://prod", "畸形响应不应清空配置"
    assert callback.calls == [], "不应派发删除变更"


def test_non_json_body_is_protocol_error(client_kwargs):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, text="<html>not json</html>")
        client = ApolloClient(**client_kwargs)
        with pytest.raises(ApolloProtocolError, match="JSON"):
            client._fetch_namespace("application", initial=True)


def test_non_string_values_coerced(client_kwargs):
    """json/yaml 格式的 namespace 可能给出非字符串标量。"""
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            json={"configurations": {"PORT": 3306, "FLAG": True}, "releaseKey": "rk"},
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**client_kwargs)
        client.start()
        client.stop()

    assert client.get_value("PORT") == "3306"
    assert client.get_value("FLAG") == "True"


def test_malformed_notifications_payload_is_protocol_error(client_kwargs):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, json={"not": "a list"})
        client = ApolloClient(**client_kwargs)
        client.start()
        with pytest.raises(ApolloProtocolError, match="数组"):
            client._poll_once()
        client.stop()


# ------------------------- notificationId 语义 -------------------------
def test_notification_id_not_advanced_until_fetch_succeeds(client_kwargs):
    """拉取失败时不能推进 id，否则这次发布永远不会再被拉取。"""
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "V1"}, "rk1")},
                {"status_code": 500, "text": "boom"},
                {"json": config_payload({"K": "V2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=10))
        client = ApolloClient(**client_kwargs)
        client.start()
        # 第一轮通知触发拉取失败 -> id 不推进 -> 下一轮重试 -> 成功拿到 V2
        got = _wait(lambda: client.get_value("K") == "V2", timeout=3.0)
        client.stop()

    assert got, "拉取失败后应当重试并最终拿到新值，而不是永久停在旧值"
    assert client._notification_ids["application"] == 10


def test_smaller_notification_id_is_accepted(client_kwargs):
    """服务端重启或轮换实例后 notificationId 可能变小，不能要求单调递增。

    官方客户端按"id 是否变化"处理。要求递增会让这种情况下热更新永久失效。
    """
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "V1"}, "rk1")},
                {"json": config_payload({"K": "V2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=5))
        client = ApolloClient(**client_kwargs)
        client.start()
        client._notification_ids["application"] = 100

        updated = client._poll_once()
        client.stop()

    assert updated == ["application"], "变小的 id 也应触发拉取"
    assert client._notification_ids["application"] == 5


def test_equal_notification_id_is_skipped(client_kwargs):
    """id 相同才是"无变更"，不应重复拉取。"""
    with requests_mock.Mocker() as m:
        configs = m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=7))
        client = ApolloClient(**client_kwargs)
        client.start()
        client._notification_ids["application"] = 7
        before = configs.call_count

        assert client._poll_once() == []
        client.stop()

    assert configs.call_count == before, "id 未变不应触发拉取"


def test_repeated_fetch_failure_raises_to_trigger_backoff(client_kwargs):
    """拉取持续失败时 _poll_once 必须抛错，否则轮询会变成无退避忙循环。

    因为 id 没推进，服务端下一轮会立刻再返回同一个通知。
    """
    from apollocfg.errors import ConfigServiceUnavailable

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "V1"}, "rk1")},
                {"status_code": 500, "text": "boom"},
            ],
        )
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=9))
        client = ApolloClient(**client_kwargs)
        client.start()
        with pytest.raises(ConfigServiceUnavailable):
            client._poll_once()
        client.stop()


def test_bare_string_key_declaration_not_split(client_kwargs):
    """required_keys="MYSQL_DSN" 不该被拆成单个字符。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"MYSQL_DSN": "x"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            required_keys="MYSQL_DSN", frozen_keys="MYSQL_DSN", **client_kwargs
        )
        client.start()
        client.stop()

    assert client.required_keys["application"] == {"MYSQL_DSN"}
    assert client.frozen_keys["application"] == {"MYSQL_DSN"}


def test_callback_receives_independent_copy(client_kwargs):
    """前一个回调改动 changes 不应影响后一个。"""
    received = []

    def mutating(namespace, changes):
        changes.clear()
        received.append("first")

    def observer(namespace, changes):
        received.append(sorted(changes))

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "V1"}, "rk1")},
                {"json": config_payload({"K": "V2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**client_kwargs)
        client.register_callback(mutating)
        client.register_callback(observer)
        client.start()
        client._fetch_namespace("application")
        client.stop()

    assert received == ["first", ["K"]], "第二个回调仍应看到完整的 changes"
