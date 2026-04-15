import aiomysql
import os
import logging

logger = logging.getLogger("discord")

class DatabaseManager:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Initialize the global connection pool."""
        if not self.pool:
            try:
                self.pool = await aiomysql.create_pool(
                    host=os.getenv('ARIA_DB_HOST'),
                    user=os.getenv('ARIA_DB_USER'),
                    password=os.getenv('ARIA_DB_PASSWORD'),
                    db=os.getenv('ARIA_DB_NAME'),
                    autocommit=True,
                    minsize=1,
                    maxsize=15
                )
                logger.info("🟢 Global Database Pool initialized.")
            except Exception as e:
                logger.error(f"🔴 Failed to initialize database pool: {e}")

    async def close(self):
        """Close the pool gracefully on shutdown."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("🔴 Global Database Pool closed.")

# Instantiate a single global instance
db = DatabaseManager()