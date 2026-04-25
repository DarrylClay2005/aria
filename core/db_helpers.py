from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

try:
    import aiomysql
except ImportError:  # pragma: no cover
    aiomysql = None


@asynccontextmanager
async def db_cursor(*, dict_rows: bool = False) -> AsyncIterator:
    """Yield a cursor from Aria's shared aiomysql pool."""
    if aiomysql is None:
        raise RuntimeError("aiomysql is not installed; database features are unavailable.")

    from core.database import db

    cursor_cls = aiomysql.DictCursor if dict_rows else None
    pool = await db.pool
    async with pool.acquire() as conn:
        if cursor_cls is None:
            async with conn.cursor() as cur:
                yield cur
        else:
            async with conn.cursor(cursor_cls) as cur:
                yield cur
