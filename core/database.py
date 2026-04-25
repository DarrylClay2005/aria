import asyncio
import logging
import re

from core.settings import DB_CONFIG

try:
    import aiomysql
except ImportError:  # pragma: no cover
    aiomysql = None

logger = logging.getLogger("discord")
_ORIGINAL_CREATE_POOL = aiomysql.create_pool if aiomysql else None


async def ensure_database_exists():
    """Create Aria's MariaDB schema before opening the shared pool."""
    if aiomysql is None:
        return
    db_name = str(DB_CONFIG.get("db") or "").strip()
    if not db_name:
        raise RuntimeError("DB_CONFIG['db'] is empty; cannot create Aria database.")
    if not re.fullmatch(r"[A-Za-z0-9_]+", db_name):
        raise RuntimeError(f"Unsafe Aria database name: {db_name!r}")
    conn = await aiomysql.connect(
        host=DB_CONFIG.get("host", "host.docker.internal"),
        port=int(DB_CONFIG.get("port", 3306)),
        user=DB_CONFIG.get("user", "botuser"),
        password=DB_CONFIG.get("password", ""),
        autocommit=True,
        connect_timeout=10,
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
    finally:
        conn.close()


class _SharedPoolProxy:
    """Awaitable/context-compatible proxy used by legacy cogs."""

    def __init__(self, manager):
        self.manager = manager

    async def _resolve(self):
        if not self.manager._pool:
            await self.manager.connect()
        if not self.manager._pool:
            raise RuntimeError("Global database pool is not initialized.")
        return self.manager._pool

    def __await__(self):
        return self._resolve().__await__()

    async def __aenter__(self):
        return await self._resolve()

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def acquire(self):
        async def _acquire_cm():
            pool = await self._resolve()
            return pool.acquire()
        # Support direct `async with db.pool.acquire() as conn:` usage.
        class _AcquireProxy:
            async def __aenter__(self_inner):
                self_inner._cm = await _acquire_cm()
                return await self_inner._cm.__aenter__()
            async def __aexit__(self_inner, exc_type, exc, tb):
                return await self_inner._cm.__aexit__(exc_type, exc, tb)
        return _AcquireProxy()

    def __bool__(self):
        return bool(self.manager._pool)


class DatabaseManager:
    def __init__(self):
        self._pool = None
        self.pool = _SharedPoolProxy(self)
        self._connect_lock = asyncio.Lock()

    async def connect(self, attempts: int = 12, delay: int = 5):
        """Initialize the global connection pool, retry-safe and legacy-safe."""
        if self._pool:
            return
        async with self._connect_lock:
            if self._pool:
                return
            if aiomysql is None:
                logger.error("aiomysql is not installed; database features are unavailable.")
                return
            for attempt in range(1, attempts + 1):
                try:
                    await ensure_database_exists()
                    pool_config = {**DB_CONFIG, "minsize": 1, "maxsize": 15, "autocommit": True}
                    self._pool = await _ORIGINAL_CREATE_POOL(**pool_config)
                    logger.info("🟢 Global Database Pool initialized.")
                    return
                except Exception as e:
                    logger.exception(
                        "🔴 Failed to initialize database pool on attempt %s/%s: %s",
                        attempt,
                        attempts,
                        e,
                    )
                    if attempt >= attempts:
                        return
                    await asyncio.sleep(delay)

    def patch_legacy_create_pool(self):
        """Route old aiomysql.create_pool usage to the shared pool."""
        if aiomysql is None:
            return
        if getattr(aiomysql.create_pool, "_aria_uses_shared_pool", False):
            return

        def shared_create_pool(*args, **kwargs):
            return _SharedPoolProxy(self)

        shared_create_pool._aria_uses_shared_pool = True
        aiomysql.create_pool = shared_create_pool

    async def close(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            logger.info("🔴 Global Database Pool closed.")


db = DatabaseManager()
