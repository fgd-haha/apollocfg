"""端到端测试：连真实 Apollo 实例。

**默认跳过。** 需要同时满足：

  - 设置 ``APOLLOCFG_E2E=1``；
  - 提供 ``APOLLOCFG_E2E_META_SERVER``（或 ``APOLLOCFG_E2E_CONFIG_URL``）与
    ``APOLLOCFG_E2E_APP_ID``。

所有地址都从环境变量读取，仓库里不留任何具体实例信息——这个包是公开发布的，
不应该携带任何部署环境的痕迹。

运行方式::

    APOLLOCFG_E2E=1 \\
    APOLLOCFG_E2E_META_SERVER=http://your-apollo-meta.example.com \\
    APOLLOCFG_E2E_APP_ID=your-app-id \\
    pytest tests/test_e2e.py -v -m e2e

这些测试**只读**，不修改 Apollo 上的任何配置。
"""
from __future__ import annotations

import os
import threading
import time

import pytest

from apollocfg.client import ApolloClient

pytestmark = pytest.mark.e2e

# Apollo 服务端长轮询挂起时长。E2E 里要验证"跨过这个边界线程仍然存活"——
# 这正是 filamoon/pyapollo 失败的地方（客户端 timeout=35 < 服务端 60）。
SERVER_HOLD_SECONDS = 60


def _require(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        pytest.skip(f"E2E 需要环境变量 {name}")
    return value


@pytest.fixture(scope="module", autouse=True)
def _gate() -> None:
    if (os.getenv("APOLLOCFG_E2E") or "").strip() not in {"1", "true", "yes", "on"}:
        pytest.skip("E2E 默认跳过，设 APOLLOCFG_E2E=1 开启")


@pytest.fixture(scope="module")
def e2e_config() -> dict:
    meta = (os.getenv("APOLLOCFG_E2E_META_SERVER") or "").strip()
    direct = (os.getenv("APOLLOCFG_E2E_CONFIG_URL") or "").strip()
    if not meta and not direct:
        pytest.skip(
            "E2E 需要 APOLLOCFG_E2E_META_SERVER 或 APOLLOCFG_E2E_CONFIG_URL"
        )
    return {
        "app_id": _require("APOLLOCFG_E2E_APP_ID"),
        "meta_server": meta or None,
        "config_url": direct or None,
        "cluster": (os.getenv("APOLLOCFG_E2E_CLUSTER") or "default").strip(),
        "access_key_secret": (os.getenv("APOLLOCFG_E2E_ACCESS_KEY_SECRET") or "").strip()
        or None,
    }


def _client(e2e_config: dict, **overrides) -> ApolloClient:
    kwargs = {
        **e2e_config,
        "namespaces": ["application"],
        "poll_timeout": 90,
        "request_timeout": 10,
        "poll_interval": 1.0,
    }
    kwargs.update(overrides)
    return ApolloClient(**kwargs)


def test_meta_server_discovery_returns_instances(e2e_config):
    """meta server 能发现至少一个 Config Service 实例。"""
    if not e2e_config["meta_server"]:
        pytest.skip("未提供 meta server，跳过服务发现测试")

    client = _client(e2e_config, config_url=None)
    instances = client._discover_services()

    assert instances, "meta server 应返回至少一个 Config Service 实例"
    assert all(host.startswith("http") for host in instances)
    assert all(not host.endswith("/") for host in instances), "结尾斜杠应已剥除"


def test_full_fetch_returns_configuration(e2e_config):
    """能从真实 Apollo 拉到配置。"""
    with _client(e2e_config) as client:
        configs = client.get_configs("application")

    assert isinstance(configs, dict)
    assert configs, "application namespace 应当有配置项"
    assert all(isinstance(k, str) for k in configs)
    # 不断言具体 key 名或值——那是部署环境的信息，不该写进公开仓库
    assert client.config_url, "启动后应已确定生效的 Config Service 地址"


def test_long_poll_holds_connection_then_returns_304(e2e_config):
    """验证长轮询语义：服务端把请求挂住约 60s，无变更时返回 304。

    这是"Apollo 不是服务端主动推送"的直接证据。
    """
    client = _client(e2e_config)
    client.start()
    try:
        # 先同步当前 notificationId，下一轮才会真正被挂起
        client._poll_once()

        started = time.monotonic()
        result = client._poll_once()
        elapsed = time.monotonic() - started
    finally:
        client.stop()

    assert result is None, "无变更时 _poll_once 应返回 None（304）"
    assert elapsed >= SERVER_HOLD_SECONDS * 0.8, (
        f"长轮询应被服务端挂住约 {SERVER_HOLD_SECONDS}s，实测 {elapsed:.1f}s；"
        "过短说明没走到长轮询路径"
    )


def test_poll_thread_survives_server_hold_boundary(e2e_config):
    """轮询线程必须跨过服务端 60s 挂起边界仍然存活。

    这是 filamoon/pyapollo 的致命缺陷所在：它默认 timeout=35s，第一轮长轮询
    必然 ReadTimeout，异常没被捕获，线程直接死掉——之后配置永不更新，且无任何报错。
    """
    client = _client(e2e_config)
    client.start()
    try:
        deadline = time.monotonic() + SERVER_HOLD_SECONDS + 15
        while time.monotonic() < deadline:
            assert client._thread is not None and client._thread.is_alive(), (
                "轮询线程在服务端挂起边界附近死亡"
            )
            time.sleep(2)

        names = [t.name for t in threading.enumerate()]
        assert "apollocfg-long-poll" in names
        # 线程存活期间配置应保持可读
        assert client.get_configs("application")
    finally:
        client.stop()

    assert client.running is False


def test_frozen_keys_hold_startup_snapshot(e2e_config):
    """frozen_keys 在真实环境下也只返回启动快照。

    取一个真实存在的 key 做冻结对象，二次拉取后值不应变化。
    """
    with _client(e2e_config) as client:
        configs = client.get_configs("application")
        if not configs:
            pytest.skip("application namespace 为空，无法选取冻结对象")
        probe_key = sorted(configs)[0]

    with _client(e2e_config, frozen_keys=[probe_key]) as client:
        snapshot = client.get_value(probe_key)
        # 强制重新拉取（releaseKey 清掉以拿到全量响应）
        client._release_keys["application"] = ""
        client._fetch_namespace("application")
        assert client.get_value(probe_key) == snapshot, "冻结 key 不应变化"


def test_required_keys_contract_against_real_instance(e2e_config):
    """required_keys 契约：声明一个真实不存在的 key，应当拒绝启动。"""
    from apollocfg.errors import RequiredKeysMissing

    client = _client(
        e2e_config,
        required_keys=["__apollocfg_definitely_absent_key__"],
        allow_fallback=False,
    )
    with pytest.raises(RequiredKeysMissing):
        client.start()
    client.stop()


def test_no_secrets_in_logs_against_real_instance(e2e_config, caplog):
    """真实配置里常含密钥，日志里不能出现任何配置值。"""
    import logging

    with caplog.at_level(logging.DEBUG, logger="apollocfg"):
        with _client(e2e_config) as client:
            configs = client.get_configs("application")

    if not configs:
        pytest.skip("application namespace 为空")

    text = "\n".join(
        [r.getMessage() + str(r.args) for r in caplog.records]
    )
    leaked = [
        key
        for key, value in configs.items()
        # 只检查足够长的值，短值（如 "1"、"on"）可能偶然出现在日志里
        if isinstance(value, str) and len(value) >= 12 and value in text
    ]
    assert not leaked, f"以下 key 的值泄露到日志: {leaked}"
