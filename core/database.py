import logging

from core.settings import DB_CONFIG

try:
    import aiomysql
except ImportError:  # pragma: no cover - optional in lightweight local test shells
    aiomysql = None

logger = logging.getLogger("discord")
_ORIGINAL_CREATE_POOL = aiomysql.create_pool if aiomysql else None


class _SharedPoolProxy:
    def __init__(self, manager):
        self.manager = manager

    async def _resolve(self):
        if not self.manager.pool:
            raise RuntimeError("Global database pool is not initialized.")
        return self.manager.pool

    def __await__(self):
        return self._resolve().__await__()

    async def __aenter__(self):
        return await self._resolve()

    async def __aexit__(self, exc_type, exc, tb):
        return False

class DatabaseManager:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Initialize the global connection pool."""
        if not self.pool:
            if aiomysql is None:
                logger.error("aiomysql is not installed; database features are unavailable in this environment.")
                return
            try:
                pool_config = {**DB_CONFIG, "minsize": 1, "maxsize": 15, "autocommit": True}
                self.pool = await _ORIGINAL_CREATE_POOL(**pool_config)
                logger.info("🟢 Global Database Pool initialized.")
            except Exception as e:
                logger.exception("🔴 Failed to initialize database pool: %s", e)

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
        """Close the pool gracefully on shutdown."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("🔴 Global Database Pool closed.")

# Instantiate a single global instance
db = DatabaseManager()
