from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger("discord")


TelegramHandler = Callable[[dict[str, Any]], Awaitable[str | None]]


@dataclass
class TelegramBridgeStatus:
    enabled: bool
    running: bool
    bot_username: str = ""
    allowed_chat_count: int = 0
    last_error: str = ""
    last_update_at: float = 0.0


class TelegramBridge:
    """Small Telegram Bot API long-polling bridge with no extra dependency."""

    def __init__(
        self,
        *,
        token: str,
        name: str,
        handler: TelegramHandler,
        allowed_chat_ids: set[int] | None = None,
        commands: list[tuple[str, str]] | None = None,
        poll_timeout_seconds: int = 25,
    ) -> None:
        self.token = str(token or "").strip()
        self.name = name
        self.handler = handler
        self.allowed_chat_ids = set(allowed_chat_ids or set())
        self.commands = list(commands or [])
        self.poll_timeout_seconds = max(5, int(poll_timeout_seconds or 25))
        self._task: asyncio.Task[Any] | None = None
        self._closing = asyncio.Event()
        self._offset: int | None = None
        self._commands_registered = False
        self.status = TelegramBridgeStatus(
            enabled=bool(self.token),
            running=False,
            allowed_chat_count=len(self.allowed_chat_ids),
        )

    async def start(self) -> None:
        if not self.token or (self._task and not self._task.done()):
            return
        try:
            info = await self._api("getMe", timeout=12)
            user = info.get("result") or {}
            self.status.bot_username = str(user.get("username") or "")
            await self._api("deleteWebhook", {"drop_pending_updates": "false"}, timeout=12)
            await self._register_commands()
            logger.info("Telegram bridge for %s connected as @%s.", self.name, self.status.bot_username or "unknown")
        except Exception as exc:
            self.status.last_error = str(exc)[:240]
            logger.warning("Telegram bridge for %s could not verify token yet: %s", self.name, exc)
        self._closing.clear()
        self._task = asyncio.create_task(self._poll_loop(), name=f"{self.name}-telegram-bridge")

    async def close(self) -> None:
        self._closing.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self.status.running = False

    async def _register_commands(self) -> None:
        if not self.commands:
            self._commands_registered = True
            return
        await self._api(
            "setMyCommands",
            {"commands": json.dumps([{"command": key, "description": desc} for key, desc in self.commands])},
            timeout=12,
        )
        self._commands_registered = True

    async def send_message(self, chat_id: int | str, text: str) -> None:
        payload = str(text or "").strip()
        if not payload:
            return
        for chunk in self._chunks(payload, 3900):
            await self._api("sendMessage", {"chat_id": str(chat_id), "text": chunk, "disable_web_page_preview": "true"}, timeout=15)

    async def _poll_loop(self) -> None:
        self.status.running = True
        while not self._closing.is_set():
            try:
                # If startup failed after getMe but before setMyCommands, keep retrying the
                # actual command registration instead of treating a username as success.
                if not self._commands_registered:
                    try:
                        info = await self._api("getMe", timeout=12)
                        user = info.get("result") or {}
                        self.status.bot_username = str(user.get("username") or "")
                        await self._register_commands()
                        logger.info("Telegram bridge for %s registered commands.", self.name)
                    except Exception as reg_exc:
                        logger.warning("Telegram bridge for %s command registration failed, will retry: %s", self.name, reg_exc)

                params: dict[str, Any] = {
                    "timeout": self.poll_timeout_seconds,
                    "allowed_updates": json.dumps(["message"]),
                }
                if self._offset is not None:
                    params["offset"] = self._offset
                payload = await self._api("getUpdates", params, timeout=self.poll_timeout_seconds + 10)
                for update in payload.get("result") or []:
                    update_id = int(update.get("update_id") or 0)
                    self._offset = max(self._offset or 0, update_id + 1)
                    await self._handle_update(update)
                self.status.last_error = ""
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.status.last_error = str(exc)[:240]
                logger.warning("Telegram bridge for %s polling error: %s", self.name, exc)
                await asyncio.sleep(5)
        self.status.running = False

    async def _handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        text = str(message.get("text") or "").strip()
        if chat_id is None or not text:
            return
        try:
            normalized_chat_id = int(chat_id)
        except (TypeError, ValueError):
            return
        self.status.last_update_at = time.time()
        if self.allowed_chat_ids and normalized_chat_id not in self.allowed_chat_ids:
            await self.send_message(normalized_chat_id, "This Telegram chat is not allowed to control this bot.")
            return
        try:
            reply = await self.handler({"chat_id": normalized_chat_id, "text": text, "message": message, "update": update})
            if reply:
                await self.send_message(normalized_chat_id, reply)
        except Exception as exc:
            logger.exception("Telegram handler for %s failed: %s", self.name, exc)
            await self.send_message(normalized_chat_id, f"Telegram command failed: {exc}")

    async def _api(self, method: str, params: dict[str, Any] | None = None, *, timeout: int = 20) -> dict[str, Any]:
        return await asyncio.to_thread(self._api_sync, method, params or {}, timeout)

    def _api_sync(self, method: str, params: dict[str, Any], timeout: int) -> dict[str, Any]:
        url = f"https://api.telegram.org/bot{self.token}/{method}"
        data = urllib.parse.urlencode(params).encode("utf-8") if params else None
        request = urllib.request.Request(url, data=data, method="POST" if data else "GET")
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not payload.get("ok"):
            raise RuntimeError(str(payload.get("description") or f"Telegram {method} failed"))
        return payload

    @staticmethod
    def _chunks(text: str, limit: int) -> list[str]:
        remaining = text
        chunks: list[str] = []
        while remaining:
            chunks.append(remaining[:limit])
            remaining = remaining[limit:]
        return chunks[:8]
