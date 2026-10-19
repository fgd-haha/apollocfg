"""meta server 服务发现与 Config Service 轮换。

上游 apollo-config-client 要求调用方直接给 config_url，没有服务发现能力；
这是本 fork 新增的部分（思路取自 OuterCloud/pyapollo）。
"""
from __future__ import annotations

import pytest
import requests
import requests_mock

from apollocfg.client import ApolloClient
from apollocfg.errors import ConfigServiceUnavailable
from tests.conftest import META_URL, config_payload, services_payload

SERVICES_PATH = f"{META_URL}/services/config"
HOST_A = "http://node-a.test"
HOST_B = "http://node-b.test"


def _configs_path(host: str, namespace: str = "application") -> str:
    return f"{host}/configs/unit-app/default/{namespace}"


def _notify_path(host: str) -> str:
    return f"{host}/notifications/v2"


def test_requires_config_url_or_meta_server():
    with pytest.raises(ValueError, match="至少提供一个"):
        ApolloClient(app_id="app")


def test_poll_timeout_must_exceed_server_hold():
    """服务端挂起 60s，客户端读超时必须更长，否则每轮都被自己掐断。"""
    with pytest.raises(ValueError, match="poll_timeout"):
        ApolloClient(app_id="app", config_url="http://x", poll_timeout=35)


def test_discovers_config_service_from_meta_server():
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=services_payload([HOST_A]))
        m.get(_configs_path(HOST_A), json=config_payload({"K": "V"}))
        m.get(_notify_path(HOST_A), status_code=304)
        client = ApolloClient(
            app_id="unit-app", meta_server=META_URL, poll_timeout=61, poll_interval=0.01
        )
        client.start()
        client.stop()

    assert client.config_url == HOST_A
    assert client.get_value("K") == "V"


def test_trailing_slash_in_homepage_url_is_stripped():
    """Apollo 返回的 homepageUrl 常带结尾斜杠，拼接前必须去掉。"""
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=services_payload([f"{HOST_A}/"]))
        m.get(_configs_path(HOST_A), json=config_payload({"K": "V"}))
        m.get(_notify_path(HOST_A), status_code=304)
        client = ApolloClient(
            app_id="unit-app", meta_server=META_URL, poll_timeout=61, poll_interval=0.01
        )
        client.start()
        client.stop()

    assert client.config_url == HOST_A


def test_rotates_to_next_instance_on_failure():
    """首选实例连不上时，自动轮换到列表里的另一个。"""
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=services_payload([HOST_A, HOST_B]))
        # 两个实例：一个永远超时，一个正常。随机化顺序下无论先选中哪个都应成功。
        m.get(_configs_path(HOST_A), exc=requests.exceptions.ConnectTimeout)
        m.get(_configs_path(HOST_B), json=config_payload({"K": "V"}))
        m.get(_notify_path(HOST_A), status_code=304)
        m.get(_notify_path(HOST_B), status_code=304)
        client = ApolloClient(
            app_id="unit-app",
            meta_server=META_URL,
            allow_fallback=False,
            poll_timeout=61,
            poll_interval=0.01,
        )
        client.start()
        client.stop()

    assert client.get_value("K") == "V"


def test_empty_service_list_is_unavailable():
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=[])
        client = ApolloClient(
            app_id="unit-app",
            meta_server=META_URL,
            allow_fallback=False,
            poll_timeout=61,
        )
        with pytest.raises(ConfigServiceUnavailable):
            client.start()


def test_meta_server_failure_is_unavailable():
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, status_code=500)
        client = ApolloClient(
            app_id="unit-app",
            meta_server=META_URL,
            allow_fallback=False,
            poll_timeout=61,
        )
        with pytest.raises(ConfigServiceUnavailable):
            client.start()


def test_explicit_config_url_skips_discovery():
    """显式给了 config_url 就不该去打 meta server。"""
    with requests_mock.Mocker() as m:
        services = m.get(SERVICES_PATH, json=services_payload([HOST_B]))
        m.get(_configs_path(HOST_A), json=config_payload({"K": "V"}))
        m.get(_notify_path(HOST_A), status_code=304)
        client = ApolloClient(
            app_id="unit-app",
            config_url=HOST_A,
            meta_server=META_URL,
            poll_timeout=61,
            poll_interval=0.01,
        )
        client.start()
        client.stop()

    assert client.config_url == HOST_A
    assert services.call_count == 0, "config_url 已给出时不应触发服务发现"


def test_pool_refreshed_when_exhausted():
    """池里的实例全部下线后，应重新查 meta server 发现新实例。

    滚动发布会把整批实例换掉。只认启动时发现的地址会让长跑进程永久钉在
    已下线的实例上，热更新静默失效直到进程重启。
    """
    with requests_mock.Mocker() as m:
        # 第一次发现只有 old；第二次（池耗尽后）返回换代后的 new
        m.get(
            SERVICES_PATH,
            [
                {"json": services_payload(["http://old"])},
                {"json": services_payload(["http://new"])},
            ],
        )
        m.get(
            "http://old/configs/unit-app/default/application",
            json=config_payload({"K": "V"}),
        )
        m.get("http://old/notifications/v2", status_code=304)
        m.get(
            "http://new/configs/unit-app/default/application",
            json=config_payload({"K": "V2"}, "rk2"),
        )
        m.get("http://new/notifications/v2", status_code=304)

        client = ApolloClient(
            app_id="unit-app", meta_server=META_URL, poll_timeout=61, poll_interval=0.01
        )
        client.start()
        assert client.config_url == "http://old"

        # old 下线，池里没有其他候选 -> 应重新发现并换到 new
        assert client._rotate_config_url() is True
        assert client.config_url == "http://new"
        client.stop()


def test_rotation_returns_false_without_meta_server():
    """没有 meta server 时池耗尽也无处可换。"""
    with requests_mock.Mocker() as m:
        m.get(
            f"{HOST_A}/configs/unit-app/default/application",
            json=config_payload({"K": "V"}),
        )
        m.get(_notify_path(HOST_A), status_code=304)
        client = ApolloClient(
            app_id="unit-app", config_url=HOST_A, poll_timeout=61, poll_interval=0.01
        )
        assert client._rotate_config_url() is False
        assert client.config_url == HOST_A


def test_frozen_snapshot_not_rebuilt_on_restart():
    """start/stop/start 不应重建冻结快照——冻结值必须跨重启保持。"""
    with requests_mock.Mocker() as m:
        m.get(
            f"{HOST_A}/configs/unit-app/default/application",
            [
                {"json": config_payload({"DSN": "v1"}, "rk1")},
                {"json": config_payload({"DSN": "v2"}, "rk2")},
            ],
        )
        m.get(_notify_path(HOST_A), status_code=304)
        client = ApolloClient(
            app_id="unit-app",
            config_url=HOST_A,
            frozen_keys=["DSN"],
            poll_timeout=61,
            poll_interval=0.01,
        )
        client.start()
        first = client.get_value("DSN")
        client.stop()

        client.start()
        second = client.get_value("DSN")
        client.stop()

    assert first == "v1"
    assert second == "v1", "第二次 start 不应把冻结值换成 Apollo 的新值"


def test_static_fallback_does_not_leak_into_get_configs():
    """get_configs 只反映 Apollo 侧，static_fallback 不能从冻结快照漏进来。"""
    with requests_mock.Mocker() as m:
        m.get(
            f"{HOST_A}/configs/unit-app/default/application",
            json=config_payload({"OTHER": "1"}),
        )
        m.get(_notify_path(HOST_A), status_code=304)
        client = ApolloClient(
            app_id="unit-app",
            config_url=HOST_A,
            frozen_keys=["DSN"],
            static_fallback={"application": {"DSN": "from-static"}},
            poll_timeout=61,
            poll_interval=0.01,
        )
        client.start()
        client.stop()

    assert client.get_configs("application") == {"OTHER": "1"}
    assert client.get_value("DSN") is None
    # 但 effective 视图里应当有静态兜底值
    assert client.effective_configs("application")["DSN"] == "from-static"
