"""定时全量兜底、非 properties namespace、跨 namespace 合并、启动超时。

定时兜底是公司对非官方 Apollo 客户端的明文要求之一（「Apollo 接入说明」:
"定时（5 分钟）主动发起一次全量配置拉取"）。它要兜的是"长轮询丢了通知但值
其实变了"——此时 notificationId 不会再推进，长轮询永远拿不到这次变更。
"""
from __future__ import annotations

import json
import time

import pytest
import requests
import requests_mock

from apollocfg.client import ApolloClient
from apollocfg.errors import ApolloProtocolError
from tests.conftest import CONFIG_URL, config_payload

CONFIGS_PATH = f"{CONFIG_URL}/configs/unit-app/default/application"
NOTIFY_PATH = f"{CONFIG_URL}/notifications/v2"


def _wait(cond, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return False


# ------------------------- 定时全量兜底 -------------------------
def test_periodic_refresh_recovers_lost_notification(client_kwargs):
    """长轮询一直收到 304（通知丢了），定时兜底应当把新值拉回来。

    这是兜底机制存在的全部理由：没有它，配置会永久停在旧值且无任何报错。
    """
    kwargs = dict(client_kwargs)
    kwargs["refresh_interval"] = 0.3

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "OLD"}, "rk1")},
                {"json": config_payload({"K": "NEW"}, "rk2")},
            ],
        )
        # 服务端始终说"无变更"——模拟通知丢失
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**kwargs)
        client.start()
        assert client.get_value("K") == "OLD"

        recovered = _wait(lambda: client.get_value("K") == "NEW")
        client.stop()

    assert recovered, "定时兜底未能拉到新值"


def test_periodic_refresh_clears_release_key(client_kwargs):
    """兜底必须清掉 releaseKey，否则服务端回 304，兜底就没有意义。"""
    kwargs = dict(client_kwargs)
    kwargs["refresh_interval"] = 0.3

    with requests_mock.Mocker() as m:
        configs = m.get(CONFIGS_PATH, json=config_payload({"K": "V"}, "rk1"))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**kwargs)
        client.start()
        _wait(lambda: configs.call_count >= 2)
        client.stop()

    # 第一次是 start()，后续是兜底；兜底的请求不应带 releaseKey
    refresh_reqs = [r for r in m.request_history if "/configs/" in r.path][1:]
    assert refresh_reqs, "应当发生过定时兜底拉取"
    assert all("releasekey" not in r.qs for r in refresh_reqs), (
        "兜底请求不该带 releaseKey，否则会收到 304"
    )


def test_refresh_disabled_when_interval_zero(client_kwargs):
    kwargs = dict(client_kwargs)
    kwargs["refresh_interval"] = 0

    with requests_mock.Mocker() as m:
        configs = m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**kwargs)
        client.start()
        before = configs.call_count
        time.sleep(0.4)
        client.stop()

    assert configs.call_count == before, "refresh_interval=0 应关闭兜底"


def test_refresh_failure_does_not_kill_poll_thread(client_kwargs):
    """兜底拉取失败只告警，不能掀翻轮询线程。"""
    kwargs = dict(client_kwargs)
    kwargs["refresh_interval"] = 0.2

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "V"}, "rk1")},
                {"exc": requests.exceptions.ConnectTimeout},
                {"exc": requests.exceptions.ConnectTimeout},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**kwargs)
        client.start()
        time.sleep(0.6)
        alive = client._thread is not None and client._thread.is_alive()
        client.stop()

    assert alive, "兜底失败不应终止轮询线程"


def test_refresh_interval_must_exceed_poll_interval():
    with pytest.raises(ValueError, match="refresh_interval"):
        ApolloClient(
            app_id="a", config_url="http://x", poll_timeout=61,
            poll_interval=5.0, refresh_interval=5.0,
        )


# ------------------------- 非 properties namespace -------------------------
def _json_ns_payload(obj, release="rk1"):
    """Apollo 对 json/yaml 格式 namespace 的真实响应形态：整段原文塞 content。"""
    return {
        "appId": "unit-app",
        "cluster": "default",
        "namespaceName": "database.json",
        "configurations": {"content": json.dumps(obj, ensure_ascii=False)},
        "releaseKey": release,
    }


DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "AICourse",
        "HOST": "mysql.internal",
        "PORT": "3306",
        "CONN_MAX_AGE": 1800,
        "CONN_HEALTH_CHECKS": True,
        "OPTIONS": {"charset": "utf8mb4", "isolation_level": "repeatable read"},
    },
    "osai": {"NAME": "theta_os", "TIME_ZONE": "Asia/Shanghai", "CONN_MAX_AGE": 60},
}


def test_get_json_parses_content():
    """json namespace 的嵌套结构与类型应原样保留。"""
    path = f"{CONFIG_URL}/configs/unit-app/default/database.json"
    with requests_mock.Mocker() as m:
        m.get(path, json=_json_ns_payload(DATABASES))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            app_id="unit-app", config_url=CONFIG_URL, namespaces=["database.json"],
            poll_timeout=61, request_timeout=1, poll_interval=0.01,
        )
        client.start()
        parsed = client.get_json("database.json")
        raw = client.get_content("database.json")
        client.stop()

    assert set(parsed) == {"default", "osai"}
    assert parsed["default"]["CONN_MAX_AGE"] == 1800
    assert isinstance(parsed["default"]["CONN_MAX_AGE"], int), "int 类型应保留"
    assert parsed["default"]["CONN_HEALTH_CHECKS"] is True, "bool 类型应保留"
    assert parsed["default"]["OPTIONS"]["charset"] == "utf8mb4", "嵌套 dict 应保留"
    assert parsed["osai"]["TIME_ZONE"] == "Asia/Shanghai"
    assert isinstance(raw, str)


def test_get_json_rejects_missing_content(client_kwargs):
    """properties namespace 没有 content，调 get_json 要给出可操作的报错。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**client_kwargs)
        client.start()
        with pytest.raises(ApolloProtocolError, match="content"):
            client.get_json("application")
        client.stop()


def test_get_json_rejects_malformed_json():
    path = f"{CONFIG_URL}/configs/unit-app/default/database.json"
    with requests_mock.Mocker() as m:
        m.get(
            path,
            json={
                "configurations": {"content": "{not valid json"},
                "releaseKey": "rk",
            },
        )
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            app_id="unit-app", config_url=CONFIG_URL, namespaces=["database.json"],
            poll_timeout=61, request_timeout=1,
        )
        client.start()
        with pytest.raises(ApolloProtocolError, match="合法 JSON"):
            client.get_json("database.json")
        client.stop()


# ------------------------- 跨 namespace 合并 -------------------------
def test_merged_configs_later_wins():
    """后面的 namespace 覆盖前面的——私有配置应能盖掉关联的公共 namespace。"""
    infra = f"{CONFIG_URL}/configs/unit-app/default/theta.infra"
    with requests_mock.Mocker() as m:
        m.get(
            infra,
            json=config_payload(
                {"OAP": "oap.default", "SHARED": "from-infra"}, namespace="theta.infra"
            ),
        )
        m.get(CONFIGS_PATH, json=config_payload({"SHARED": "from-app", "OWN": "x"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            app_id="unit-app", config_url=CONFIG_URL,
            namespaces=["theta.infra", "application"],
            poll_timeout=61, request_timeout=1, poll_interval=0.01,
        )
        client.start()
        merged = client.merged_configs()
        client.stop()

    assert merged["SHARED"] == "from-app", "后面的 namespace 应覆盖前面的"
    assert merged["OAP"] == "oap.default", "公共 namespace 独有的 key 应保留"
    assert merged["OWN"] == "x"


def test_merged_configs_skips_content_namespaces():
    """json/yaml 的 content 不参与合并——合并两段原文没有意义。"""
    dbpath = f"{CONFIG_URL}/configs/unit-app/default/database.json"
    with requests_mock.Mocker() as m:
        m.get(dbpath, json=_json_ns_payload({"default": {}}))
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            app_id="unit-app", config_url=CONFIG_URL,
            namespaces=["database.json", "application"],
            poll_timeout=61, request_timeout=1, poll_interval=0.01,
        )
        client.start()
        merged = client.merged_configs()
        client.stop()

    assert merged == {"K": "V"}
    assert "content" not in merged


def test_merged_configs_explicit_order():
    infra = f"{CONFIG_URL}/configs/unit-app/default/theta.infra"
    with requests_mock.Mocker() as m:
        m.get(infra, json=config_payload({"S": "infra"}, namespace="theta.infra"))
        m.get(CONFIGS_PATH, json=config_payload({"S": "app"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            app_id="unit-app", config_url=CONFIG_URL,
            namespaces=["theta.infra", "application"],
            poll_timeout=61, request_timeout=1, poll_interval=0.01,
        )
        client.start()
        reversed_order = client.merged_configs(["application", "theta.infra"])
        client.stop()

    assert reversed_order["S"] == "infra", "显式顺序应被尊重"


# ------------------------- 启动超时 -------------------------
def test_startup_timeout_bounds_blocking():
    """startup_timeout 应限制 start() 的总阻塞时长。

    FastAPI 的 lifespan 卡住意味着 28688 端口不监听，轻舟探活会判不健康。
    """
    import threading

    slow = threading.Event()

    def slow_response(request, context):
        slow.wait(2.0)  # 模拟 Apollo 响应很慢
        context.status_code = 200
        return config_payload({"K": "V"})

    namespaces = ["ns1", "ns2", "ns3"]
    with requests_mock.Mocker() as m:
        for ns in namespaces:
            m.get(f"{CONFIG_URL}/configs/unit-app/default/{ns}", json=slow_response)
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(
            app_id="unit-app", config_url=CONFIG_URL, namespaces=namespaces,
            allow_fallback=True, poll_timeout=61, request_timeout=5,
            poll_interval=0.01, startup_timeout=0.5,
        )
        t0 = time.monotonic()
        client.start()
        elapsed = time.monotonic() - t0
        slow.set()
        client.stop()

    # 第一个 namespace 会完整等完（超时检查在每个 namespace 之前），
    # 但后两个应被跳过，所以总时长远小于 3 × 2s
    assert elapsed < 4.0, f"startup_timeout 未生效，start() 阻塞了 {elapsed:.1f}s"


def test_startup_timeout_rejects_non_positive():
    with pytest.raises(ValueError, match="startup_timeout"):
        ApolloClient(app_id="a", config_url="http://x", poll_timeout=61, startup_timeout=0)


def test_refresh_runs_on_dedicated_thread(client_kwargs):
    """兜底必须跑在独立线程，不能被长轮询的节奏绑死。

    兜底要兜的正是"长轮询出问题"。长轮询每轮被服务端挂住约 60s，若把兜底放在
    同一个循环里，它的触发间隔就被长轮询绑死——实测 refresh_interval=8s 时实际
    只能每 61s 执行一次（见 _refresh_loop 的 docstring）。

    这里只断言结构：兜底有自己的线程、且该线程不经过 _poll_once。
    时序上的解耦无法在 requests_mock 下验证——它会把并发请求串行化
    （已实测：5 个请求被一个阻塞请求拖了 0.4s），所以那部分靠真实 Apollo 验证。
    """
    import threading as _th

    kwargs = dict(client_kwargs)
    kwargs["refresh_interval"] = 0.2
    polls: list = []

    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**kwargs)
        orig_poll = client._poll_once
        client._poll_once = lambda: (
            polls.append(_th.current_thread().name), orig_poll()
        )[1]
        client.start()
        names = {t.name for t in _th.enumerate()}
        time.sleep(0.5)
        client.stop()

    assert "apollocfg-refresh" in names, "兜底应有独立线程"
    assert "apollocfg-long-poll" in names
    assert set(polls) <= {"apollocfg-long-poll"}, (
        f"_poll_once 只应由长轮询线程调用，实际: {set(polls)}"
    )


def test_refresh_fetches_without_poll(client_kwargs):
    """兜底自己会拉配置，不依赖长轮询产生的通知。"""
    import threading as _th

    kwargs = dict(client_kwargs)
    kwargs["refresh_interval"] = 0.15
    fetch_threads: list = []

    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**kwargs)
        orig = client._fetch_namespace
        client._fetch_namespace = lambda ns, initial=False: (
            fetch_threads.append(_th.current_thread().name), orig(ns, initial)
        )[1]
        client.start()
        _wait(lambda: "apollocfg-refresh" in fetch_threads, timeout=3.0)
        client.stop()

    assert "apollocfg-refresh" in fetch_threads, (
        f"兜底线程应当自己发起拉取，实际调用方: {set(fetch_threads)}"
    )


def test_refresh_thread_stops_with_client(client_kwargs):
    import threading as _th

    kwargs = dict(client_kwargs)
    kwargs["refresh_interval"] = 0.2
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**kwargs)
        client.start()
        assert any(t.name == "apollocfg-refresh" for t in _th.enumerate())
        client.stop()

    assert not any(t.name == "apollocfg-refresh" for t in _th.enumerate()), (
        "stop() 后兜底线程应已退出"
    )


def test_no_refresh_thread_when_disabled(client_kwargs):
    import threading as _th

    kwargs = dict(client_kwargs)
    kwargs["refresh_interval"] = 0
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(**kwargs)
        client.start()
        names = [t.name for t in _th.enumerate()]
        client.stop()

    assert "apollocfg-refresh" not in names
