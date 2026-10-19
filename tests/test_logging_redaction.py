"""日志脱敏：库在任何级别都不得输出配置值。

动机是真实的反面教材：filamoon/pyapollo 与 apollo-client 2.1.2 都在 INFO 级别
``repr`` 整份配置。部署在容器平台上时，这会把数据库密码、API token 直接刷进
集中式日志系统，长期留存且可检索。

这里用一组"长得像密钥"的值做探针，断言它们不出现在任何一条日志记录里。
"""
from __future__ import annotations

import logging

import requests
import requests_mock

from apollocfg.client import ApolloClient
from tests.conftest import CONFIG_URL, config_payload, notification_payload

CONFIGS_PATH = f"{CONFIG_URL}/configs/unit-app/default/application"
NOTIFY_PATH = f"{CONFIG_URL}/notifications/v2"

# 这些值一旦出现在日志里就是泄露
SECRETS = {
    "MYSQL_DSN": "user:sup3rs3cr3t@db.internal:3306/app",
    "API_TOKEN": "sk-abcdef0123456789",
    "PASSWORD": "hunter2",
}


def _all_log_text(records) -> str:
    """把日志记录渲染成一整段文本，含格式化后的消息与参数。"""
    chunks = []
    for record in records:
        chunks.append(record.getMessage())
        chunks.append(str(record.args))
        if record.exc_text:
            chunks.append(record.exc_text)
    return "\n".join(chunks)


def _assert_no_secret_leaked(records) -> None:
    text = _all_log_text(records)
    for key, value in SECRETS.items():
        assert value not in text, f"配置值泄露到日志: {key}"
    # key 名本身是允许出现的——排障需要知道哪个 key 变了
    assert "MYSQL_DSN" in text or not records, "应当记录 key 名以便排障"


def test_no_secret_in_logs_on_startup(client_kwargs, caplog):
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload(SECRETS))
        m.get(NOTIFY_PATH, status_code=304)
        with caplog.at_level(logging.DEBUG, logger="apollocfg"):
            client = ApolloClient(frozen_keys=["MYSQL_DSN"], **client_kwargs)
            client.start()
            client.stop()

    _assert_no_secret_leaked(caplog.records)


def test_no_secret_in_logs_on_change(client_kwargs, caplog):
    rotated = dict(SECRETS, MYSQL_DSN="user:r0tat3d@db.internal:3306/app")
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload(SECRETS, "rk1")},
                {"json": config_payload(rotated, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=9))
        with caplog.at_level(logging.DEBUG, logger="apollocfg"):
            client = ApolloClient(**client_kwargs)
            client.start()
            client._fetch_namespace("application")
            client.stop()

    text = _all_log_text(caplog.records)
    assert "user:r0tat3d@db.internal:3306/app" not in text, "新值泄露"
    assert "user:sup3rs3cr3t@db.internal:3306/app" not in text, "旧值泄露"
    assert "MYSQL_DSN" in text, "应记录变更的 key 名"


def test_no_secret_in_logs_on_frozen_suppression(client_kwargs, caplog):
    """冻结 key 被抑制时的日志也只记 key 名。"""
    rotated = dict(SECRETS, MYSQL_DSN="user:r0tat3d@db.internal:3306/app")
    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload(SECRETS, "rk1")},
                {"json": config_payload(rotated, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)
        with caplog.at_level(logging.DEBUG, logger="apollocfg"):
            client = ApolloClient(frozen_keys=["MYSQL_DSN"], **client_kwargs)
            client.start()
            client._fetch_namespace("application")
            client.stop()

    _assert_no_secret_leaked(caplog.records)


def test_no_secret_in_logs_on_request_failure(client_kwargs, caplog):
    """异常路径同样不能带出值；access_key_secret 也不能出现。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, exc=requests.exceptions.ConnectTimeout)
        m.get(NOTIFY_PATH, status_code=304)
        with caplog.at_level(logging.DEBUG, logger="apollocfg"):
            client = ApolloClient(
                access_key_secret="my-access-key-secret", **client_kwargs
            )
            client.start()
            client.stop()

    text = _all_log_text(caplog.records)
    assert "my-access-key-secret" not in text, "access key 泄露到日志"


def test_required_keys_warning_names_keys_only(client_kwargs, caplog):
    """required_keys 缺失告警要指出是哪些 key（排障需要），但不能带值。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"PRESENT": "value-here"}))
        m.get(NOTIFY_PATH, status_code=304)
        with caplog.at_level(logging.WARNING, logger="apollocfg"):
            client = ApolloClient(
                required_keys=["PRESENT", "ABSENT"], **client_kwargs
            )
            client.start()
            client.stop()

    text = _all_log_text(caplog.records)
    assert "ABSENT" in text, "应指出缺失的 key 名"
    assert "value-here" not in text, "不应带出已有 key 的值"
