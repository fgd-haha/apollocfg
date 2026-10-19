"""共享测试夹具。

约定：

  - 所有单元测试都用 ``requests_mock`` 拦截 HTTP，不碰真实网络。
  - ``poll_timeout`` 在单测里必须 > 60（构造函数会校验），但由于请求被 mock，
    不会真的等待；长轮询节奏靠 ``poll_interval`` 控制。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

CONFIG_URL = "http://apollo.test"
META_URL = "http://meta.test"
APP_ID = "unit-app"


def config_payload(
    configurations: Dict[str, str],
    release_key: str = "rk1",
    namespace: str = "application",
) -> Dict[str, Any]:
    """构造 Apollo ``/configs/...`` 接口的响应体。"""
    return {
        "appId": APP_ID,
        "cluster": "default",
        "namespaceName": namespace,
        "configurations": configurations,
        "releaseKey": release_key,
    }


def notification_payload(
    namespace: str = "application", notification_id: int = 10
) -> List[Dict[str, Any]]:
    """构造 ``/notifications/v2`` 接口的响应体。"""
    return [{"namespaceName": namespace, "notificationId": notification_id}]


def services_payload(hosts: List[str]) -> List[Dict[str, str]]:
    """构造 meta server ``/services/config`` 接口的响应体。"""
    return [
        {
            "appName": "APOLLO-CONFIGSERVICE",
            "instanceId": f"instance-{i}",
            "homepageUrl": host,
        }
        for i, host in enumerate(hosts)
    ]


@pytest.fixture
def client_kwargs() -> Dict[str, Any]:
    """单测用的基础构造参数：直连 config_url，轮询间隔压到最小。"""
    return {
        "app_id": APP_ID,
        "config_url": CONFIG_URL,
        "poll_timeout": 61,
        "request_timeout": 1,
        "poll_interval": 0.01,
        "max_retry_backoff": 0.05,
    }


class RecordingCallback:
    """记录回调调用的辅助类，替代测试里到处写 lambda + list。"""

    def __init__(self, raises: Optional[Exception] = None) -> None:
        self.calls: List[tuple] = []
        self._raises = raises

    def __call__(self, namespace: str, changes: Dict[str, Dict[str, Any]]) -> None:
        self.calls.append((namespace, changes))
        if self._raises is not None:
            raise self._raises

    @property
    def changed_keys(self) -> List[str]:
        """所有调用中出现过的 key，按首次出现顺序。"""
        seen: List[str] = []
        for _, changes in self.calls:
            for key in changes:
                if key not in seen:
                    seen.append(key)
        return seen
