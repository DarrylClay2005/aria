import types

import pytest

from core.database import db
from core import db_helpers


class FakeCursorContext:
    def __init__(self, recorded, cursor_cls=None):
        self.recorded = recorded
        self.cursor_cls = cursor_cls

    async def __aenter__(self):
        self.recorded['cursor_cls'] = self.cursor_cls
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, recorded):
        self.recorded = recorded

    def cursor(self, cursor_cls=None):
        return FakeCursorContext(self.recorded, cursor_cls)


class FakeAcquire:
    def __init__(self, recorded):
        self.recorded = recorded

    async def __aenter__(self):
        self.recorded['acquired'] = True
        return FakeConnection(self.recorded)

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, recorded):
        self.recorded = recorded

    def acquire(self):
        return FakeAcquire(self.recorded)


@pytest.mark.asyncio
async def test_db_cursor_uses_shared_pool_and_default_cursor(monkeypatch):
    recorded = {}
    fake_aiomysql = types.SimpleNamespace(
        DictCursor=object(),
    )
    monkeypatch.setattr(db_helpers, 'aiomysql', fake_aiomysql)
    monkeypatch.setattr(db, '_pool', FakePool(recorded))

    async with db_helpers.db_cursor() as cur:
        assert cur is not None

    assert recorded['acquired'] is True
    assert recorded['cursor_cls'] is None


@pytest.mark.asyncio
async def test_db_cursor_can_request_dict_rows(monkeypatch):
    recorded = {}
    dict_cursor = object()
    fake_aiomysql = types.SimpleNamespace(
        DictCursor=dict_cursor,
    )
    monkeypatch.setattr(db_helpers, 'aiomysql', fake_aiomysql)
    monkeypatch.setattr(db, '_pool', FakePool(recorded))

    async with db_helpers.db_cursor(dict_rows=True) as cur:
        assert cur is not None

    assert recorded['cursor_cls'] is dict_cursor
