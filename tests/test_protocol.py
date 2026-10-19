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

    不调 start()——后台轮询线程会抢先消费掉通知，让测试变成竞态。这里直接
    驱动 _poll_once() 以获得确定性。
    """
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V2"}, "rk2"))
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=5))
        client = ApolloClient(**client_kwargs)
        client._notification_ids["application"] = 100

        updated = client._poll_once()

    assert updated == ["application"], "变小的 id 也应触发拉取"
    assert client._notification_ids["application"] == 5
    assert client.get_value("K") == "V2"


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


def test_malformed_notification_item_raises(client_kwargs):
    """畸形通知项应抛协议错误，而不是静默跳过。

    静默跳过会让 _poll_once 返回空列表、被当成成功，于是没有退避、立刻发起
    下一轮——服务端持续返回同样的畸形响应时就是高频忙循环。
    """
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, json=["not-a-dict"])
        client = ApolloClient(**client_kwargs)
        client.start()
        with pytest.raises(ApolloProtocolError, match="应为对象"):
            client._poll_once()
        client.stop()


def test_non_integer_notification_id_raises(client_kwargs):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(
            NOTIFY_PATH,
            json=[{"namespaceName": "application", "notificationId": "abc"}],
        )
        client = ApolloClient(**client_kwargs)
        client.start()
        with pytest.raises(ApolloProtocolError, match="整数"):
            client._poll_once()
        client.stop()


def test_unsubscribed_namespace_notification_is_ignored(client_kwargs):
    """收到未订阅 namespace 的通知只告警，不是协议错误。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(
            NOTIFY_PATH,
            json=[{"namespaceName": "someone-else", "notificationId": 3}],
        )
        client = ApolloClient(**client_kwargs)
        client.start()
        # 返回 None（按"无变更"处理），而不是空列表
        assert client._poll_once() is None
        client.stop()


def test_persistent_unsubscribed_notifications_do_not_busy_loop(client_kwargs):
    """服务端一直只返回未订阅 namespace 时，不能打成高频忙循环。

    我们的请求不带这些 namespace，它们的 id 无从推进，服务端可能一直返回
    同一份响应。若按"成功且无变更"立刻续下一轮，就是无间隔忙循环。
    """
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        notify = m.get(
            NOTIFY_PATH,
            json=[{"namespaceName": "someone-else", "notificationId": 3}],
        )
        kwargs = dict(client_kwargs)
        kwargs["poll_interval"] = 0.2
        client = ApolloClient(**kwargs)
        client.start()
        time.sleep(1.0)
        calls = notify.call_count
        client.stop()

    # 1 秒 / 0.2 秒间隔 -> 个位数请求。没有退避的话会是几百上千次。
    assert calls <= 12, f"疑似忙循环：1 秒内发了 {calls} 次长轮询请求"


def test_frozen_snapshot_not_captured_on_hard_startup_failure(client_kwargs):
    """allow_fallback=False 启动失败时不应固化冻结快照。

    调用方可能在 Apollo 恢复后复用同一客户端重试 start()；若此刻已标记
    captured，重试成功后新拉到的冻结值将永远不可见。
    """
    import requests

    from apollocfg.errors import ConfigServiceUnavailable

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"exc": requests.exceptions.ConnectTimeout},
                {"json": config_payload({"DSN": "mysql://recovered"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            frozen_keys=["DSN"],
            static_fallback={"application": {"DSN": "APOLLO_REQUIRED"}},
            allow_fallback=False,
            **client_kwargs,
        )
        with pytest.raises(ConfigServiceUnavailable):
            client.start()

        # Apollo 恢复后重试
        client.start()
        client.stop()

    assert client.get_value("DSN") == "mysql://recovered", (
        "启动失败后重试成功，应拿到真实的冻结值而不是固化的哨兵值"
    )


@pytest.mark.parametrize(
    ("notify_kwargs", "label"),
    [
        ({"json": [{"namespaceName": "application", "notificationId": 7}]}, "id 与本地相同"),
        ({"json": []}, "空通知数组"),
        ({"status_code": 304}, "304 秒回"),
    ],
)
def test_immediate_server_response_does_not_busy_loop(
    client_kwargs, notify_kwargs, label
):
    """服务端秒回且无更新时，必须按 poll_interval 节流。

    正常 Apollo 会把长轮询挂起约 60s，但代理故障或配置异常时可能立刻返回。
    此时若立刻续下一轮，就是无间隔忙循环——修复前实测 800+ 次/0.6 秒。
    """
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        notify = m.get(NOTIFY_PATH, **notify_kwargs)
        kwargs = dict(client_kwargs)
        kwargs["poll_interval"] = 0.2
        client = ApolloClient(**kwargs)
        client.start()
        client._notification_ids["application"] = 7
        before = notify.call_count
        time.sleep(0.6)
        calls = notify.call_count - before
        client.stop()

    assert calls <= 8, f"{label}: 0.6 秒内发了 {calls} 次长轮询，疑似忙循环"


def test_falsy_notifications_json_is_protocol_error(client_kwargs):
    """200 {} 不能被吞成"无变更"——那会掩盖协议问题。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, json={})
        client = ApolloClient(**client_kwargs)
        client.start()
        with pytest.raises(ApolloProtocolError, match="数组"):
            client._poll_once()
        client.stop()


def test_null_config_value_is_protocol_error(client_kwargs):
    """配置值为 null 要报错，不能 str() 成字符串 "None"。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json={"configurations": {"K": None}, "releaseKey": "rk"})
        client = ApolloClient(**client_kwargs)
        with pytest.raises(ApolloProtocolError, match="值类型非法"):
            client._fetch_namespace("application", initial=True)


def test_frozen_snapshot_rolled_back_on_contract_failure(client_kwargs):
    """契约校验失败时要回滚冻结快照，否则重试永远无法自愈。

    调用方可能在 Apollo 侧补齐配置后复用同一客户端重试 start()。若失败启动
    期间建的快照留下，_capture_frozen 会因"已 captured"跳过，Apollo 新补的
    冻结值永远不可见，required_keys 也永远判定缺失。
    """
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"OTHER": "1"}, "rk1")},
                {"json": config_payload({"OTHER": "1", "DSN": "mysql://real"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            frozen_keys=["DSN"],
            required_keys=["DSN"],
            require_resolved_keys=True,
            **client_kwargs,
        )
        from apollocfg.errors import RequiredKeysMissing

        with pytest.raises(RequiredKeysMissing):
            client.start()

        # Apollo 补上了这个 key，重试应当成功并拿到真实值
        client.start()
        client.stop()

    assert client.get_value("DSN") == "mysql://real"


def test_callback_inner_dict_is_copied(client_kwargs):
    """内层 {"old":..,"new":..} 也要复制，否则前一个回调能改到后一个。"""
    observed = []

    def mutating(namespace, changes):
        changes["K"]["new"] = "TAMPERED"

    def observer(namespace, changes):
        observed.append(changes["K"]["new"])

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

    assert observed == ["V2"], "后一个回调不应看到前一个回调的改动"


@pytest.mark.parametrize(
    "payload",
    [{"not": "a list"}, "a string", 42, [{"homepageUrl": None}], ["not-a-dict"]],
)
def test_malformed_meta_server_response_returns_empty(payload):
    """meta server 响应畸形时返回空列表，不抛未捕获的类型错误。"""
    from tests.conftest import META_URL

    with requests_mock.Mocker() as m:
        m.get(f"{META_URL}/services/config", json=payload)
        client = ApolloClient(app_id="unit-app", meta_server=META_URL, poll_timeout=61)
        assert client._discover_services() == []
