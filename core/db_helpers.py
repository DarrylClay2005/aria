from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

try:
    import aiomysql
except ImportError:  # pragma: no cover - optional in lightweight local test shells
    aiomysql = None


@asynccontextmanager
async def db_cursor(*, dict_rows: bool = False) -> AsyncIterator:
    """Yield a cursor from Aria's shared aiomysql pool.

    All legacy cogs already call ``aiomysql.create_pool`` which is patched at
    startup to return the shared global pool. This helper removes repeated pool /
    connection / cursor boilerplate while continuing to work with that shim.
    """
    if aiomysql is None:
        raise RuntimeError("aiomysql is not installed; database features are unavailable.")

    cursor_cls = aiomysql.DictCursor if dict_rows else None
    async with aiomysql.create_pool() as pool:
        async with pool.acquire() as conn:
            if cursor_cls is None:
                async with conn.cursor() as cur:
                    yield cur
            else:
                async with conn.cursor(cursor_cls) as cur:
                    yield cur
