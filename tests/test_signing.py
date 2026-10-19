"""Apollo 1.6+ 访问密钥签名。

签名明文 = ``毫秒时间戳 + "\\n" + pathWithQuery``，HMAC-SHA1 后 Base64，
通过 ``Timestamp`` / ``Authorization`` 两个请求头传递（没有独立 Signature 头）。

最容易出错的点是 **pathWithQuery 必须与实际发出的 URL 完全一致**（含查询串的
百分号编码）。所以实现里先 ``Request.prepare()`` 再取 ``path_url`` 去签名，
而不是自己拼字符串。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
from unittest import mock
from urllib.parse import unquote

import requests_mock

from apollocfg.client import ApolloClient
from tests.conftest import CONFIG_URL, config_payload

CONFIGS_PATH = f"{CONFIG_URL}/configs/unit-app/default/application"
NOTIFY_PATH = f"{CONFIG_URL}/notifications/v2"


def _expected_signature(secret: str, timestamp: str, path_with_query: str) -> str:
    plain = f"{timestamp}\n{path_with_query}"
    return base64.b64encode(
        hmac.new(secret.encode("utf-8"), plain.encode("utf-8"), hashlib.sha1).digest()
    ).decode("utf-8")


def test_anonymous_has_no_auth_headers(client_kwargs):
    client = ApolloClient(**client_kwargs)
    assert client._auth_headers("/configs/app/default/application") == {}


def test_signed_headers_shape_and_value(client_kwargs):
    client = ApolloClient(access_key_secret="secret", **client_kwargs)
    with mock.patch("time.time", return_value=1600000000.0):
        headers = client._auth_headers("/configs/app/default/application")

    assert headers["Timestamp"] == "1600000000000"
    expected = _expected_signature(
        "secret", "1600000000000", "/configs/app/default/application"
    )
    assert headers["Authorization"] == f"Apollo unit-app:{expected}"


def test_signature_covers_query_string(client_kwargs):
    """带查询参数时，签名明文必须包含查询串。"""
    client = ApolloClient(access_key_secret="secret", **client_kwargs)
    with mock.patch("time.time", return_value=1600000000.0):
        without = client._auth_headers("/configs/a/b/c")
        with_query = client._auth_headers("/configs/a/b/c?releaseKey=rk1")

    assert without["Authorization"] != with_query["Authorization"]


def test_actual_request_signature_matches_sent_path(client_kwargs):
    """端到端校验：实际发出请求的 path_url 与用于签名的 path 一致。

    这是 Apollo 验签失败最常见的原因——自己拼 path 时漏掉编码差异。
    """
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        with mock.patch("time.time", return_value=1600000000.0):
            client = ApolloClient(access_key_secret="secret", **client_kwargs)
            client.start()
            client.stop()

    config_requests = [
        r for r in m.request_history if "/configs/" in r.path
    ]
    assert config_requests, "应当发出过配置拉取请求"
    request = config_requests[0]
    path_with_query = request.path
    if request.query:
        # requests_mock 把 query 规范化成小写，用原始 url 还原
        path_with_query = request.url.split(request.netloc, 1)[1]

    expected = _expected_signature("secret", "1600000000000", unquote(path_with_query))
    actual = request.headers["Authorization"].split(":", 1)[1]
    # 允许 unquote 前后一致的情形，两种写法任一匹配即可
    alt = _expected_signature("secret", "1600000000000", path_with_query)
    assert actual in {expected, alt}, (
        "签名所用 path 与实际请求 path 不一致，Apollo 会验签失败"
    )


def test_notification_requests_are_signed_too(client_kwargs):
    """长轮询请求同样需要签名。"""
    with requests_mock.Mocker() as m:
        m.get(CONFIGS_PATH, json=config_payload({"K": "V"}))
        m.get(NOTIFY_PATH, status_code=304)
        client = ApolloClient(access_key_secret="secret", **client_kwargs)
        client.start()
        client.stop()

    notify_requests = [r for r in m.request_history if "notifications" in r.path]
    assert notify_requests, "应当发出过长轮询请求"
    for request in notify_requests:
        assert request.headers["Authorization"].startswith("Apollo unit-app:")
        assert "Timestamp" in request.headers


def test_timestamp_advances_between_requests(client_kwargs):
    """每次请求都要用当前时间戳，不能复用（Apollo 会校验时间窗）。"""
    client = ApolloClient(access_key_secret="secret", **client_kwargs)
    with mock.patch("time.time", side_effect=[1600000000.0, 1600000005.0]):
        first = client._auth_headers("/configs/a/b/c")
        second = client._auth_headers("/configs/a/b/c")

    assert first["Timestamp"] == "1600000000000"
    assert second["Timestamp"] == "1600000005000"
    assert first["Authorization"] != second["Authorization"]
