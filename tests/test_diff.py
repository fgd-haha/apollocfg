"""key 级 diff 计算。

diff 是变更回调的输入，语义必须精确：调用方靠 ``old`` / ``new`` 判断该做什么
（新增、改值、还是删除）。
"""
from __future__ import annotations

from apollocfg.client import ApolloClient


def test_diff_add_update_delete():
    changes = ApolloClient._diff({"a": "1", "b": "2"}, {"b": "20", "c": "3"})

    assert set(changes) == {"a", "b", "c"}
    assert changes["a"] == {"old": "1", "new": None}, "删除：new 为 None"
    assert changes["b"] == {"old": "2", "new": "20"}, "改值：两侧都有"
    assert changes["c"] == {"old": None, "new": "3"}, "新增：old 为 None"


def test_diff_empty_when_equal():
    assert ApolloClient._diff({"a": "1"}, {"a": "1"}) == {}


def test_diff_both_empty():
    assert ApolloClient._diff({}, {}) == {}


def test_diff_from_empty_is_all_additions():
    changes = ApolloClient._diff({}, {"a": "1", "b": "2"})
    assert changes == {
        "a": {"old": None, "new": "1"},
        "b": {"old": None, "new": "2"},
    }


def test_diff_to_empty_is_all_deletions():
    changes = ApolloClient._diff({"a": "1"}, {})
    assert changes == {"a": {"old": "1", "new": None}}


def test_diff_distinguishes_empty_string_from_absent():
    """值改成空字符串和 key 被删除是两件事，不能混。"""
    to_empty = ApolloClient._diff({"a": "1"}, {"a": ""})
    deleted = ApolloClient._diff({"a": "1"}, {})

    assert to_empty == {"a": {"old": "1", "new": ""}}
    assert deleted == {"a": {"old": "1", "new": None}}
    assert to_empty != deleted
