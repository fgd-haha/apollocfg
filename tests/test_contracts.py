"""三组正交语义的测试：required_keys / frozen_keys / allow_fallback，以及 on_resolved。

这些是本 fork 相对上游新增的核心语义，也是最需要测试锁住的部分。
"""
from __future__ import annotations

import pytest
import requests
import requests_mock

from apollocfg.client import ApolloClient
from apollocfg.errors import (
    ConfigServiceUnavailable,
    RequiredKeysMissing,
    StartupAssertionFailed,
)
from tests.conftest import CONFIG_URL, config_payload

CONFIGS_PATH = f"{CONFIG_URL}/configs/unit-app/default/application"
NOTIFY_PATH = f"{CONFIG_URL}/notifications/v2"


# ------------------------- required_keys -------------------------
def test_required_keys_present_passes(client_kwargs):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"A": "1", "B": "2"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(required_keys=["A", "B"], **client_kwargs)
        client.start()
        client.stop()
    assert client.get_value("A") == "1"


def test_required_keys_missing_raises_when_fallback_disabled(client_kwargs):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"A": "1"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            required_keys=["A", "B", "C"], allow_fallback=False, **client_kwargs
        )
        with pytest.raises(RequiredKeysMissing) as excinfo:
            client.start()

    assert excinfo.value.missing == {"application": ["B", "C"]}
    # 异常消息里应包含缺失的 key 名，便于运维直接定位
    assert "B" in str(excinfo.value) and "C" in str(excinfo.value)


def test_required_keys_missing_warns_when_fallback_allowed(client_kwargs, caplog):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"A": "1"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            required_keys=["A", "B"], allow_fallback=True, **client_kwargs
        )
        with caplog.at_level("WARNING", logger="apollocfg"):
            client.start()
        client.stop()

    assert client.running is False
    assert any("required_keys" in r.message for r in caplog.records)


def test_required_keys_empty_string_counts_as_missing(client_kwargs):
    """Apollo 里存在但值为空白的 key，视为未解析出值。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"A": "   "}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            required_keys=["A"], allow_fallback=False, **client_kwargs
        )
        with pytest.raises(RequiredKeysMissing):
            client.start()


def test_required_keys_per_namespace_mapping(client_kwargs):
    """required_keys 支持按 namespace 指定。"""
    kwargs = dict(client_kwargs)
    kwargs["namespaces"] = ["application", "database"]
    db_path = f"{CONFIG_URL}/configs/unit-app/default/database"

    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"A": "1"}))
        m.get(db_path, json=config_payload({"DSN": "x"}, namespace="database"))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            required_keys={"application": ["A"], "database": ["DSN", "POOL"]},
            allow_fallback=False,
            **kwargs,
        )
        with pytest.raises(RequiredKeysMissing) as excinfo:
            client.start()

    assert excinfo.value.missing == {"database": ["POOL"]}


# ------------------------- frozen_keys -------------------------
def test_frozen_key_keeps_startup_snapshot(client_kwargs):
    """frozen_keys 的值在启动时快照，Apollo 后续变更不改变读取结果。"""
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"DSN": "old", "FLAG": "off"}, "rk1")},
                {"json": config_payload({"DSN": "new", "FLAG": "on"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(frozen_keys=["DSN"], **client_kwargs)
        client.start()
        assert client.get_value("DSN") == "old"

        # 模拟长轮询触发的第二次拉取
        client._fetch_namespace("application")
        client.stop()

    assert client.get_value("DSN") == "old", "冻结的 key 不应随 Apollo 变化"
    assert client.get_value("FLAG") == "on", "未冻结的 key 应当热更新"


def test_frozen_key_excluded_from_callback(client_kwargs):
    """冻结的 key 变更不应出现在回调里——调用方不会也不该响应它。"""
    from tests.conftest import RecordingCallback

    callback = RecordingCallback()
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"DSN": "old", "FLAG": "off"}, "rk1")},
                {"json": config_payload({"DSN": "new", "FLAG": "on"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(frozen_keys=["DSN"], **client_kwargs)
        client.register_callback(callback)
        client.start()
        client._fetch_namespace("application")
        client.stop()

    assert callback.changed_keys == ["FLAG"]
    assert "DSN" not in callback.changed_keys


def test_frozen_key_absent_at_startup_is_not_frozen(client_kwargs):
    """启动时 Apollo 里没有这个 key，就没有快照可冻结，后续出现即可见。

    这是有意的：冻结语义是"锁住启动时拿到的值"，没拿到就没有值可锁。
    required_keys 才是用来保证"必须拿到"的那一层。
    """
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"FLAG": "off"}, "rk1")},
                {"json": config_payload({"FLAG": "off", "DSN": "appeared"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(frozen_keys=["DSN"], **client_kwargs)
        client.start()
        assert client.get_value("DSN") is None
        client._fetch_namespace("application")
        client.stop()

    assert client.get_value("DSN") == "appeared"


def test_get_configs_applies_frozen_snapshot(client_kwargs):
    """get_configs 返回的整份配置也要套用冻结快照。"""
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"DSN": "old", "X": "1"}, "rk1")},
                {"json": config_payload({"DSN": "new", "X": "2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(frozen_keys=["DSN"], **client_kwargs)
        client.start()
        client._fetch_namespace("application")
        client.stop()

    assert client.get_configs("application") == {"DSN": "old", "X": "2"}


# ------------------------- allow_fallback -------------------------
def test_unreachable_raises_when_fallback_disabled(client_kwargs):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, exc=requests.exceptions.ConnectTimeout)
        client = ApolloClient(allow_fallback=False, **client_kwargs)
        with pytest.raises(ConfigServiceUnavailable) as excinfo:
            client.start()

    assert CONFIG_URL in excinfo.value.attempted


def test_unreachable_degrades_when_fallback_allowed(client_kwargs, caplog):
    """Apollo 不可达 + allow_fallback=True：启动成功，配置为空，由调用方静态配置兜底。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, exc=requests.exceptions.ConnectTimeout)
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(allow_fallback=True, **client_kwargs)
        with caplog.at_level("WARNING", logger="apollocfg"):
            client.start()
        client.stop()

    assert client.get_configs("application") == {}
    assert any("allow_fallback" in r.message for r in caplog.records)


def test_partial_namespace_failure_does_not_degrade_everything(client_kwargs):
    """只有部分 namespace 失败时，不触发整体降级路径。"""
    kwargs = dict(client_kwargs)
    kwargs["namespaces"] = ["application", "database"]
    db_path = f"{CONFIG_URL}/configs/unit-app/default/database"

    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"A": "1"}))
        m.get(db_path, status_code=500)
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(allow_fallback=False, **kwargs)
        client.start()  # 不应抛异常
        client.stop()

    assert client.get_value("A") == "1"
    assert client.get_configs("database") == {}


# ------------------------- on_resolved 启动断言 -------------------------
def test_on_resolved_rejection_blocks_startup(client_kwargs):
    """哨兵值场景：回退到静态配置后值仍是占位符，断言应拒绝启动。"""

    def reject_sentinel(namespace, configs):
        if configs.get("DSN") == "APOLLO_REQUIRED":
            return "DSN 仍是哨兵占位符，Apollo 未下发真实值"
        return None

    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"DSN": "APOLLO_REQUIRED"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(on_resolved=reject_sentinel, **client_kwargs)
        with pytest.raises(StartupAssertionFailed) as excinfo:
            client.start()

    assert "哨兵占位符" in str(excinfo.value)
    assert client.running is False


def test_on_resolved_pass_allows_startup(client_kwargs):
    def accept(namespace, configs):
        return None

    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"DSN": "mysql://real"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(on_resolved=accept, **client_kwargs)
        client.start()
        client.stop()

    assert client.get_value("DSN") == "mysql://real"


def test_on_resolved_runs_after_fallback(client_kwargs):
    """断言必须在降级之后执行，否则就挡不住"回退到错误值"这种情况。"""
    seen = []

    def record(namespace, configs):
        seen.append((namespace, dict(configs)))
        return None

    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, exc=requests.exceptions.ConnectTimeout)
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(on_resolved=record, allow_fallback=True, **client_kwargs)
        client.start()
        client.stop()

    assert seen == [("application", {})]
