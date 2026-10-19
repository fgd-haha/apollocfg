"""ConfigManager：按 namespace 组织的变更订阅。"""
from __future__ import annotations

import requests_mock

from apollocfg.manager import ConfigManager
from tests.conftest import APP_ID, CONFIG_URL, config_payload

CONFIGS_PATH = f"{CONFIG_URL}/configs/{APP_ID}/default/application"
DB_CONFIGS_PATH = f"{CONFIG_URL}/configs/{APP_ID}/default/database"
NOTIFY_PATH = f"{CONFIG_URL}/notifications/v2"


def _manager(**overrides) -> ConfigManager:
    kwargs = {
        "app_id": APP_ID,
        "config_url": CONFIG_URL,
        "poll_timeout": 61,
        "request_timeout": 1,
        "poll_interval": 0.01,
        "max_retry_backoff": 0.05,
    }
    kwargs.update(overrides)
    return ConfigManager(**kwargs)


def test_reads_values_through_manager():
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        manager = _manager()
        manager.start()
        manager.stop()

    assert manager.get_value("K") == "V"
    assert manager.get_configs() == {"K": "V"}
    assert manager.get_value("MISSING", "fallback") == "fallback"


def test_namespace_callback_only_fires_for_its_namespace():
    application_hits, database_hits = [], []

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"A": "1"}, "rk1")},
                {"json": config_payload({"A": "2"}, "rk2")},
            ],
        )
        m.get(
            DB_CONFIGS_PATH,
            json=config_payload({"DSN": "x"}, "dbrk1", namespace="database"),
        )
        m.get(NOTIFY_PATH, status_code=304)

        manager = _manager(namespaces=["application", "database"])

        @manager.on_namespace_change("application")
        def _on_app(namespace, changes):
            application_hits.append(changes)

        @manager.on_namespace_change("database")
        def _on_db(namespace, changes):
            database_hits.append(changes)

        manager.start()
        manager.client._fetch_namespace("application")
        manager.stop()

    assert len(application_hits) == 1
    assert application_hits[0]["A"] == {"old": "1", "new": "2"}
    assert database_hits == [], "database 没有变更，不应收到回调"


def test_multiple_callbacks_on_same_namespace_all_fire():
    hits = []

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"A": "1"}, "rk1")},
                {"json": config_payload({"A": "2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        manager = _manager()
        manager.on_namespace_change("application")(lambda ns, ch: hits.append("first"))
        manager.on_namespace_change("application")(lambda ns, ch: hits.append("second"))
        manager.start()
        manager.client._fetch_namespace("application")
        manager.stop()

    assert hits == ["first", "second"]


def test_callback_exception_does_not_block_others():
    hits = []

    def boom(namespace, changes):
        raise RuntimeError("第一个回调炸了")

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"A": "1"}, "rk1")},
                {"json": config_payload({"A": "2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        manager = _manager()
        manager.on_namespace_change("application")(boom)
        manager.on_namespace_change("application")(lambda ns, ch: hits.append("ran"))
        manager.start()
        manager.client._fetch_namespace("application")
        manager.stop()

    assert hits == ["ran"]


def test_decorator_returns_original_callable():
    """装饰器不能吞掉被装饰的函数——调用方可能还要直接调它。"""
    manager = _manager()

    @manager.on_namespace_change("application")
    def handler(namespace, changes):
        return "called"

    assert handler("application", {}) == "called"


def test_frozen_keys_filtered_from_namespace_callback():
    hits = []

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"DSN": "old", "FLAG": "off"}, "rk1")},
                {"json": config_payload({"DSN": "new", "FLAG": "on"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        manager = _manager(frozen_keys=["DSN"])
        manager.on_namespace_change("application")(lambda ns, ch: hits.append(ch))
        manager.start()
        manager.client._fetch_namespace("application")
        manager.stop()

    assert hits and set(hits[0]) == {"FLAG"}


def test_get_typed_uses_model_validate():
    class FakeModel:
        def __init__(self, data):
            self.data = data

        @classmethod
        def model_validate(cls, data):
            return cls(data)

    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"host": "db", "port": "3306"}))
        m.get(NOTIFY_PATH, status_code=304)
        manager = _manager()
        manager.start()
        manager.stop()

    model = manager.get_typed("application", FakeModel)
    assert model.data == {"host": "db", "port": "3306"}


def test_context_manager():
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        with _manager() as manager:
            assert manager.client.running is True
        assert manager.client.running is False
