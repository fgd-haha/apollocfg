"""工厂函数与环境变量解析。

注意 ``on_resolved`` 只能通过参数传入——它是可调用对象，没法用环境变量表达。
"""
from __future__ import annotations

import pytest

from apollocfg._factory import create_config_manager, create_config_manager_from_env
from tests.conftest import CONFIG_URL, META_URL


def test_create_config_manager_explicit():
    manager = create_config_manager(
        app_id="a", config_url=CONFIG_URL, namespaces=["application"], auto_start=False
    )
    assert manager.client.app_id == "a"
    assert manager.client.namespaces == ["application"]
    assert manager.client.running is False


def test_from_env_reads_variables(monkeypatch):
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.setenv("APOLLO_CONFIG_URL", CONFIG_URL)
    monkeypatch.setenv("APOLLO_CLUSTER", "cluster-a")
    monkeypatch.setenv("APOLLO_NAMESPACES", "application,database")

    manager = create_config_manager_from_env(auto_start=False)
    client = manager.client
    assert client.app_id == "env-app"
    assert client.config_url == CONFIG_URL
    assert client.cluster == "cluster-a"
    assert set(client.namespaces) == {"application", "database"}


def test_from_env_reads_meta_server(monkeypatch):
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.delenv("APOLLO_CONFIG_URL", raising=False)
    monkeypatch.setenv("APOLLO_META_SERVER", META_URL)

    manager = create_config_manager_from_env(auto_start=False)
    assert manager.client.meta_server == META_URL
    assert manager.client.config_url is None


def test_from_env_reads_key_contracts(monkeypatch):
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.setenv("APOLLO_CONFIG_URL", CONFIG_URL)
    monkeypatch.setenv("APOLLO_REQUIRED_KEYS", "MYSQL_DSN, REDIS_URL")
    monkeypatch.setenv("APOLLO_FROZEN_KEYS", "MYSQL_DSN")

    client = create_config_manager_from_env(auto_start=False).client
    assert client.required_keys["application"] == {"MYSQL_DSN", "REDIS_URL"}
    assert client.frozen_keys["application"] == {"MYSQL_DSN"}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("false", False),
        ("FALSE", False),
        ("0", False),
        ("no", False),
        ("off", False),
        ("true", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("", True),  # 空值视为未设置，取默认
    ],
)
def test_from_env_allow_fallback_parsing(monkeypatch, raw, expected):
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.setenv("APOLLO_CONFIG_URL", CONFIG_URL)
    monkeypatch.setenv("APOLLO_ALLOW_FALLBACK", raw)

    client = create_config_manager_from_env(auto_start=False).client
    assert client.allow_fallback is expected


def test_from_env_allow_fallback_defaults_true(monkeypatch):
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.setenv("APOLLO_CONFIG_URL", CONFIG_URL)
    monkeypatch.delenv("APOLLO_ALLOW_FALLBACK", raising=False)

    assert create_config_manager_from_env(auto_start=False).client.allow_fallback is True


def test_from_env_anonymous_by_default(monkeypatch):
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.setenv("APOLLO_CONFIG_URL", CONFIG_URL)
    monkeypatch.delenv("APOLLO_ACCESS_KEY_SECRET", raising=False)

    assert create_config_manager_from_env(auto_start=False).client.access_key_secret is None


def test_from_env_requires_app_id(monkeypatch):
    monkeypatch.delenv("APOLLO_APP_ID", raising=False)
    monkeypatch.setenv("APOLLO_CONFIG_URL", CONFIG_URL)

    with pytest.raises(ValueError, match="APOLLO_APP_ID"):
        create_config_manager_from_env(auto_start=False)


def test_from_env_requires_an_address(monkeypatch):
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.delenv("APOLLO_CONFIG_URL", raising=False)
    monkeypatch.delenv("APOLLO_META_SERVER", raising=False)

    with pytest.raises(ValueError, match="APOLLO_META_SERVER"):
        create_config_manager_from_env(auto_start=False)


def test_from_env_blank_values_treated_as_unset(monkeypatch):
    """轻舟/K8s 环境变量常被设成空串，不能当成有效值。"""
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.setenv("APOLLO_CONFIG_URL", CONFIG_URL)
    monkeypatch.setenv("APOLLO_ACCESS_KEY_SECRET", "   ")
    monkeypatch.setenv("APOLLO_ENV", "")

    client = create_config_manager_from_env(auto_start=False).client
    assert client.access_key_secret is None
    assert client.env is None


def test_env_list_ignores_blank_entries(monkeypatch):
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.setenv("APOLLO_CONFIG_URL", CONFIG_URL)
    monkeypatch.setenv("APOLLO_NAMESPACES", "application, ,database,")

    client = create_config_manager_from_env(auto_start=False).client
    assert client.namespaces == ["application", "database"]


def test_namespaces_read_at_call_time(monkeypatch):
    """环境变量必须在调用时读取，不能在 import 时快照。

    entrypoint 脚本和测试都会在 import 之后设置环境变量。
    """
    monkeypatch.setenv("APOLLO_APP_ID", "env-app")
    monkeypatch.setenv("APOLLO_CONFIG_URL", CONFIG_URL)

    monkeypatch.setenv("APOLLO_NAMESPACES", "first")
    assert create_config_manager_from_env(auto_start=False).client.namespaces == ["first"]

    monkeypatch.setenv("APOLLO_NAMESPACES", "second")
    assert create_config_manager_from_env(auto_start=False).client.namespaces == ["second"]
