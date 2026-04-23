import asyncio
import logging
import os
import sys
import traceback

import aiohttp
import discord

from core.database import db

logger = logging.getLogger("discord")
BOT_NAME = "aria"
_installed = False


def _env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


WEBHOOK_URL = _env_first(
    "ARIA_WEBHOOK_URL",
    "SWARM_WEBHOOK_URL",
    "WEBHOOK_URL",
    "ARIA_SWARM_WEBHOOK_URL",
)
ERROR_WEBHOOK_URL = _env_first(
    "ARIA_ERROR_WEBHOOK_URL",
    "SWARM_ERROR_WEBHOOK_URL",
    "ERROR_WEBHOOK_URL",
    "SWARM_WEBHOOK_ERROR_URL",
)
OPS_WEBHOOK_URL = _env_first(
    "ARIA_OPS_WEBHOOK_URL",
    "SWARM_OPS_WEBHOOK_URL",
    "OPS_WEBHOOK_URL",
    "ARIA_WEBHOOK_URL",
    "SWARM_WEBHOOK_URL",
    "WEBHOOK_URL",
)


class HTTPSessionManager:
    _session = None

    async def __aenter__(self):
        if not HTTPSessionManager._session or HTTPSessionManager._session.closed:
            HTTPSessionManager._session = aiohttp.ClientSession()
        return HTTPSessionManager._session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


def _trim(text, limit):
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


async def _persist_error_event(title, description, traceback_text=None, error_type="runtime", level="error"):
    if not getattr(db, "pool", None):
        return
    try:
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_error_events (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        bot_name VARCHAR(50) NOT NULL,
                        error_level VARCHAR(20) NOT NULL DEFAULT 'error',
                        error_type VARCHAR(50) NOT NULL DEFAULT 'runtime',
                        title VARCHAR(255) NOT NULL,
                        description TEXT NULL,
                        traceback_text MEDIUMTEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                await cur.execute(
                    """
                    INSERT INTO aria_error_events (bot_name, error_level, error_type, title, description, traceback_text)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        BOT_NAME,
                        _trim(level, 20),
                        _trim(error_type, 50),
                        _trim(title, 255),
                        _trim(description, 5000),
                        _trim(traceback_text, 20000) if traceback_text else None,
                    ),
                )
    except Exception as exc:
        print(f"[ARIA] Failed to persist error event: {exc}", file=sys.stderr)


async def _send_embed_to_url(url: str, *, title, description, color, retries=3, fields=None, username="Aria", footer="Aria Matrix", thumbnail_url=None):
    if not url:
        return False
    for attempt in range(retries):
        try:
            async with HTTPSessionManager() as session:
                webhook = discord.Webhook.from_url(url, session=session)
                embed = discord.Embed(
                    title=_trim(title, 256) or "Aria Update",
                    description=_trim(description, 4000),
                    color=color,
                    timestamp=discord.utils.utcnow(),
                )
                embed.set_footer(text=footer)
                if thumbnail_url:
                    embed.set_thumbnail(url=thumbnail_url)
                if fields:
                    for name, value, inline in fields[:20]:
                        embed.add_field(name=_trim(name, 256), value=_trim(value, 1024) or "—", inline=bool(inline))
                await webhook.send(embed=embed, username=_trim(username, 80) or "Aria")
                return True
        except Exception as exc:
            if attempt >= retries - 1:
                logger.warning("Aria webhook dispatch failed: %s", exc)
            else:
                await asyncio.sleep(2 ** attempt)
    return False


async def send_ops_webhook_log(title, description, color=discord.Color.gold(), retries=3, fields=None, username="Ops Node: Aria"):
    await _send_embed_to_url(
        OPS_WEBHOOK_URL,
        title=title,
        description=description,
        color=color,
        retries=retries,
        fields=fields,
        username=username,
        footer="Swarm Ops Feed",
    )


async def send_webhook_log(title, description, color=discord.Color.blurple(), retries=3, fields=None, username="Aria"):
    await _send_embed_to_url(
        WEBHOOK_URL,
        title=title,
        description=description,
        color=color,
        retries=retries,
        fields=fields,
        username=username,
        footer="Aria Matrix",
    )


async def send_error_webhook_log(title, description, color=discord.Color.red(), retries=3, fields=None, traceback_text=None):
    await _persist_error_event(title, description, traceback_text=traceback_text)
    if not ERROR_WEBHOOK_URL or ERROR_WEBHOOK_URL == "PASTE_YOUR_NEW_WEBHOOK_URL_HERE":
        return
    for attempt in range(retries):
        try:
            async with HTTPSessionManager() as session:
                webhook = discord.Webhook.from_url(ERROR_WEBHOOK_URL, session=session)
                embed = discord.Embed(
                    title=_trim(title, 256),
                    description=_trim(description, 3500),
                    color=color,
                    timestamp=discord.utils.utcnow(),
                )
                embed.set_footer(text="Swarm Error Matrix")
                if fields:
                    for name, value, inline in fields:
                        embed.add_field(name=_trim(name, 256), value=_trim(value, 1024), inline=inline)
                if traceback_text:
                    embed.add_field(name="Traceback", value="```py\n{}\n```".format(_trim(traceback_text, 900)), inline=False)
                await webhook.send(embed=embed, username="Error Node: Aria")
                return
        except Exception as exc:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                print(f"[ARIA] Error webhook dispatch failed: {exc}", file=sys.stderr)


def dispatch_runtime_error(title, exc=None, *, description=None, traceback_text=None, error_type="runtime", level="error"):
    message = description or (str(exc) if exc else title)
    trace = traceback_text
    if trace is None and exc is not None:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))

    async def runner():
        await send_error_webhook_log(
            title,
            message,
            fields=[("Type", error_type, True), ("Level", level, True)],
            traceback_text=trace,
        )

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(runner())
    except RuntimeError:
        try:
            asyncio.run(runner())
        except Exception as dispatch_exc:
            print(f"[ARIA] Failed to dispatch runtime error: {dispatch_exc}", file=sys.stderr)


class AriaErrorWebhookHandler(logging.Handler):
    def emit(self, record):
        if record.levelno < logging.ERROR:
            return
        try:
            message = self.format(record)
            dispatch_runtime_error(
                f"Python Log Error [{record.name}]",
                description=message,
                traceback_text=getattr(record, "exc_text", None),
                error_type="python_log",
                level=record.levelname.lower(),
            )
        except Exception:
            pass


def install_error_reporting():
    global _installed
    if _installed:
        return
    handler = AriaErrorWebhookHandler(level=logging.ERROR)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    logging.getLogger().addHandler(handler)

    def excepthook(exc_type, exc, tb):
        dispatch_runtime_error(
            "Uncaught Python Exception",
            exc,
            description=str(exc),
            traceback_text=''.join(traceback.format_exception(exc_type, exc, tb)),
            error_type="uncaught_exception",
            level="critical",
        )
    sys.excepthook = excepthook
    _installed = True


def install_loop_exception_handler(loop=None):
    try:
        current_loop = loop or asyncio.get_running_loop()
    except RuntimeError:
        return

    def handler(active_loop, context):
        exc = context.get("exception")
        message = context.get("message", "Unhandled asyncio exception")
        tb = None
        if exc is not None:
            tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        dispatch_runtime_error(
            "Asyncio Loop Error",
            exc,
            description=message if exc is None else f"{message}: {exc}",
            traceback_text=tb,
            error_type="asyncio",
            level="error",
        )
    current_loop.set_exception_handler(handler)
