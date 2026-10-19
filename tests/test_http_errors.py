"""HTTP 错误响应的处理，以及多实例故障转移。

这组测试对应一个具体的失败形态：代理层（Nginx / ALB）过载时会返回**空 body 的
5xx**。如果实现先判"body 为空"再判状态码，这类响应就会被误判成 Apollo 的
"无变更"（304 语义），于是服务带着空配置启动成功，且 ``allow_fallback=False``
也挡不住——因为从客户端视角看这次拉取"成功"了。
"""
from __future__ import annotations

import pytest
import requests
import requests_mock

from apollocfg.client import ApolloClient
from apollocfg.errors import ApolloProtocolError, ConfigServiceUnavailable
from tests.conftest import CONFIG_URL, META_URL, config_payload, services_payload

CONFIGS_PATH = f"{CONFIG_URL}/configs/unit-app/default/application"
NOTIFY_PATH = f"{CONFIG_URL}/notifications/v2"
SERVICES_PATH = f"{META_URL}/services/config"


@pytest.mark.parametrize("status", [500, 502, 503, 504, 403, 404])
def test_empty_body_error_status_is_not_treated_as_no_change(client_kwargs, status):
    """空 body 的错误状态码必须当作失败，不能当成"无变更"。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, status_code=status, text="")
        client = ApolloClient(allow_fallback=False, **client_kwargs)
        with pytest.raises(ConfigServiceUnavailable):
            client.start()

    assert client.get_configs("application") == {}


@pytest.mark.parametrize("status", [500, 502, 403])
def test_error_status_with_body_also_fails(client_kwargs, status):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, status_code=status, text="<html>error page</html>")
        client = ApolloClient(allow_fallback=False, **client_kwargs)
        with pytest.raises(ConfigServiceUnavailable):
            client.start()


def test_empty_body_with_200_is_protocol_error(client_kwargs):
    """2xx 却没有 body 不符合 Apollo 协议，应当报错而不是静默接受。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, status_code=200, text="")
        client = ApolloClient(**client_kwargs)
        with pytest.raises(ApolloProtocolError, match="body 为空"):
            client._fetch_namespace("application", initial=True)


def test_304_is_still_treated_as_no_change(client_kwargs):
    """304 是 Apollo 正常的"无变更"语义，不能被上面的修复牵连。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**client_kwargs)
        client.start()

        # 第二次请求返回 304（releaseKey 未变）
        m.get(CONFIGS_PATH, status_code=304, text="")
        assert client._fetch_namespace("application") == {}
        client.stop()

    assert client.get_value("K") == "V", "304 不应清空已有配置"


def test_empty_body_error_degrades_when_fallback_allowed(client_kwargs, caplog):
    """allow_fallback=True 时空 body 的 5xx 应走降级路径（而非假装成功）。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, status_code=502, text="")
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(allow_fallback=True, **client_kwargs)
        with caplog.at_level("WARNING", logger="apollocfg"):
            client.start()
        client.stop()

    assert any("allow_fallback" in r.message for r in caplog.records)


# ------------------------- 多实例故障转移 -------------------------
def test_failover_traverses_all_instances(client_kwargs):
    """三个实例、只有最后一个健康时，也必须找到它。

    只做一次轮换是不够的：随机化顺序下，健康实例排第三就会漏掉。
    """
    hosts = ["http://n1", "http://n2", "http://n3"]
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=services_payload(hosts))
        # 只有 n3 健康
        for host in hosts[:2]:
            m.get(
                f"{host}/configs/unit-app/default/application",
                exc=requests.exceptions.ConnectTimeout,
            )
        m.get(
            "http://n3/configs/unit-app/default/application",
            json=config_payload({"K": "V"}),
        )
        for host in hosts:
            m.get(f"{host}/notifications/v2", status_code=304)

        # 跑多次：实例顺序是随机的，每次都应该成功
        for _ in range(12):
            client = ApolloClient(
                app_id="unit-app",
                meta_server=META_URL,
                allow_fallback=False,
                poll_timeout=61,
                request_timeout=1,
                poll_interval=0.01,
            )
            client.start()
            assert client.get_value("K") == "V"
            client.stop()


def test_failover_gives_up_when_all_instances_fail(client_kwargs):
    hosts = ["http://n1", "http://n2"]
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=services_payload(hosts))
        for host in hosts:
            m.get(
                f"{host}/configs/unit-app/default/application",
                exc=requests.exceptions.ConnectTimeout,
            )
        client = ApolloClient(
            app_id="unit-app",
            meta_server=META_URL,
            allow_fallback=False,
            poll_timeout=61,
            request_timeout=1,
        )
        with pytest.raises(ConfigServiceUnavailable):
            client.start()


def test_failover_does_not_retry_on_http_status(client_kwargs):
    """4xx 多半是 appId/namespace 配错或验签失败，换实例结果一样，不该重试放大错误。"""
    hosts = ["http://n1", "http://n2"]
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=services_payload(hosts))
        counters = {}
        for host in hosts:
            counters[host] = m.get(
                f"{host}/configs/unit-app/default/application",
                status_code=404,
                text="not found",
            )
        client = ApolloClient(
            app_id="unit-app",
            meta_server=META_URL,
            allow_fallback=False,
            poll_timeout=61,
            request_timeout=1,
        )
        with pytest.raises(ConfigServiceUnavailable):
            client.start()

        total = sum(c.call_count for c in counters.values())

    assert total == 1, f"404 不该触发实例轮换，实际请求了 {total} 次"


def test_5xx_triggers_failover_to_healthy_instance():
    """5xx 应当换实例——该实例过载，别的可能是健康的。

    与 4xx 的区别：4xx 是配置或验签问题，换实例结果一样。
    """
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=services_payload(["http://bad", "http://good"]))
        m.get(
            "http://bad/configs/unit-app/default/application",
            status_code=503,
            text="overloaded",
        )
        m.get(
            "http://good/configs/unit-app/default/application",
            json=config_payload({"K": "V"}),
        )
        for host in ("bad", "good"):
            m.get(f"http://{host}/notifications/v2", status_code=304)

        for _ in range(10):
            client = ApolloClient(
                app_id="unit-app",
                meta_server=META_URL,
                allow_fallback=False,
                poll_timeout=61,
                request_timeout=1,
                poll_interval=0.01,
            )
            client.start()
            assert client.get_value("K") == "V"
            client.stop()


def test_empty_body_502_triggers_failover():
    """代理过载返回的空 body 502 也应换实例。"""
    with requests_mock.Mocker() as m:
        m.get(SERVICES_PATH, json=services_payload(["http://bad", "http://good"]))
        m.get("http://bad/configs/unit-app/default/application", status_code=502, text="")
        m.get(
            "http://good/configs/unit-app/default/application",
            json=config_payload({"K": "V"}),
        )
        for host in ("bad", "good"):
            m.get(f"http://{host}/notifications/v2", status_code=304)

        for _ in range(8):
            client = ApolloClient(
                app_id="unit-app",
                meta_server=META_URL,
                allow_fallback=False,
                poll_timeout=61,
                request_timeout=1,
                poll_interval=0.01,
            )
            client.start()
            assert client.get_value("K") == "V"
            client.stop()


def test_protocol_error_propagates_through_start(client_kwargs):
    """协议错误应当原样穿过 start()，而不是被压成 ConfigServiceUnavailable。

    两者语义不同：前者说明中间有代理在干预，后者说明网络不可达。
    """
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, status_code=200, text="")
        client = ApolloClient(allow_fallback=False, **client_kwargs)
        with pytest.raises(ApolloProtocolError):
            client.start()
