"""异步回调桥接。

动机：变更由长轮询线程检测到，而协程必须在事件循环线程里执行。实测
（见 README「在 asyncio 应用里使用」）同步回调跑在 ``apollocfg-long-poll``
线程上，那里 ``asyncio.get_running_loop()`` 会抛 "no running event loop"。

所以库要提供桥接，并且在注册时就拦住"把协程函数传给同步回调"这个错误——
协程不被 await 会被静默丢弃，是最难查的那类 bug。
"""
from __future__ import annotations

import asyncio
import threading

import pytest
import requests_mock

from apollocfg.client import ApolloClient
from tests.conftest import CONFIG_URL, config_payload, notification_payload

CONFIGS_PATH = f"{CONFIG_URL}/configs/unit-app/default/application"
NOTIFY_PATH = f"{CONFIG_URL}/notifications/v2"


def test_sync_register_rejects_coroutine_function(client_kwargs):
    """协程函数传给 register_callback 要在注册时报错，不能等到运行时静默丢弃。"""
    client = ApolloClient(**client_kwargs)

    async def handler(namespace, changes):
        pass

    with pytest.raises(TypeError, match="协程函数"):
        client.register_callback(handler)


def test_async_register_rejects_plain_function(client_kwargs):
    """反向也要拦：普通函数传给 register_async_callback 没有意义。"""
    client = ApolloClient(**client_kwargs)
    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(TypeError, match="不是协程函数"):
            client.register_async_callback(lambda ns, ch: None, loop=loop)
    finally:
        loop.close()


def test_sync_callback_runs_on_poll_thread(client_kwargs):
    """记录现状：同步回调在长轮询线程执行，拿不到事件循环。

    这不是 bug，是线程模型的必然结果。锁住它是为了防止有人"顺手"在回调里
    用 asyncio 而不自知。
    """
    seen = {}

    def handler(namespace, changes):
        seen["thread"] = threading.current_thread().name
        try:
            asyncio.get_running_loop()
            seen["loop"] = True
        except RuntimeError:
            seen["loop"] = False

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "V1"}, "rk1")},
                {"json": config_payload({"K": "V2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, json=notification_payload(notification_id=31))
        client = ApolloClient(**client_kwargs)
        client.register_callback(handler)
        client.start()
        for _ in range(200):
            if seen:
                break
            threading.Event().wait(0.02)
        client.stop()

    assert seen.get("thread") == "apollocfg-long-poll"
    assert seen.get("loop") is False, "轮询线程里不应有运行中的事件循环"


def test_async_callback_bridged_to_event_loop(client_kwargs):
    """异步回调应当在事件循环线程执行，并且能 await。"""

    async def scenario():
        loop = asyncio.get_running_loop()
        loop_thread = threading.current_thread().name
        done = asyncio.Event()
        seen = {}

        async def handler(namespace, changes):
            seen["thread"] = threading.current_thread().name
            seen["namespace"] = namespace
            seen["keys"] = sorted(changes)
            await asyncio.sleep(0)  # 证明确实在循环里
            seen["awaited"] = True
            done.set()

        with requests_mock.Mocker() as m:
            m.get(
                CONFIGS_PATH,
                [
                    {"json": config_payload({"K": "V1"}, "rk1")},
                    {"json": config_payload({"K": "V2"}, "rk2")},
                ],
            )
            m.get(NOTIFY_PATH, json=notification_payload(notification_id=42))
            client = ApolloClient(**client_kwargs)
            client.register_async_callback(handler, loop=loop)
            client.start()
            await asyncio.wait_for(done.wait(), timeout=10)
            client.stop()

        return loop_thread, seen

    loop_thread, seen = asyncio.run(scenario())
    assert seen["thread"] == loop_thread, "异步回调应回到事件循环线程"
    assert seen["awaited"] is True
    assert seen["namespace"] == "application"
    assert seen["keys"] == ["K"]


def test_async_callback_exception_does_not_kill_poll_thread(client_kwargs):
    """协程里抛异常不能影响轮询线程，也不该变成未取回的 task 异常。"""

    async def scenario():
        loop = asyncio.get_running_loop()
        fired = asyncio.Event()

        async def boom(namespace, changes):
            fired.set()
            raise RuntimeError("业务重建失败")

        with requests_mock.Mocker() as m:
            m.get(
                CONFIGS_PATH,
                [
                    {"json": config_payload({"K": "V1"}, "rk1")},
                    {"json": config_payload({"K": "V2"}, "rk2")},
                ],
            )
            m.get(NOTIFY_PATH, json=notification_payload(notification_id=55))
            client = ApolloClient(**client_kwargs)
            client.register_async_callback(boom, loop=loop)
            client.start()
            await asyncio.wait_for(fired.wait(), timeout=10)
            await asyncio.sleep(0.1)  # 让 task 跑完并触发 done_callback
            alive = client._thread is not None and client._thread.is_alive()
            client.stop()
        return alive

    assert asyncio.run(scenario()) is True, "异步回调抛异常不应终止轮询线程"


def test_async_callback_survives_closed_loop(client_kwargs):
    """事件循环已关闭时只告警，不能让轮询线程崩。

    进程退出过程中这是正常状态。
    """
    loop = asyncio.new_event_loop()
    loop.close()

    with requests_mock.Mocker() as m:
        m.get(
            CONFIGS_PATH,
            [
                {"json": config_payload({"K": "V1"}, "rk1")},
                {"json": config_payload({"K": "V2"}, "rk2")},
            ],
        )
        m.get(NOTIFY_PATH, status_code=304)

        async def handler(namespace, changes):
            pass

        client = ApolloClient(**client_kwargs)
        client.register_async_callback(handler, loop=loop)
        client.start()
        # 直接驱动一次变更，桥接应当捕获 RuntimeError
        client._fetch_namespace("application")
        alive = client._thread is not None and client._thread.is_alive()
        client.stop()

    assert alive is True
