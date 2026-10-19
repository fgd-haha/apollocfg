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
