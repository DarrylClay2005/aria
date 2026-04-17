from __future__ import annotations

import asyncio
import logging
import os

from core.settings import GEMINI_MODEL_ID

logger = logging.getLogger("discord")


class AIServiceUnavailable(RuntimeError):
    def __init__(self, message: str, public_message: str | None = None):
        super().__init__(message)
        self.public_message = public_message or "My AI backend isn't available right now."


class AIService:
    def __init__(self, *, model_id: str | None = None, api_key: str | None = None):
        self.model_id = (model_id or GEMINI_MODEL_ID).strip() or "gemini-2.5-flash"
        self.api_key = (api_key or os.getenv("GEMINI_API_KEY", "")).strip()
        self._client = None
        self._types = None

    def _ensure_client(self):
        if not self.api_key:
            raise AIServiceUnavailable(
                "GEMINI_API_KEY is not configured.",
                "My neural net isn't configured yet. Add `GEMINI_API_KEY` and try `/aria` again.",
            )

        if self._client is not None and self._types is not None:
            return self._client, self._types

        try:
            from google import genai
            from google.genai import types
        except ImportError as exc:
            raise AIServiceUnavailable(
                "google-genai is not installed.",
                "My AI backend package is missing on this host, so the chat commands can't boot yet.",
            ) from exc

        self._client = genai.Client(api_key=self.api_key)
        self._types = types
        return self._client, self._types

    async def generate(self, prompt: str, *, system_instruction: str | None = None) -> str:
        client, types = self._ensure_client()

        def _run_request() -> str:
            config = None
            if system_instruction:
                config = types.GenerateContentConfig(system_instruction=system_instruction)
            response = client.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=config,
            )
            return (getattr(response, "text", "") or "").strip()

        try:
            return await asyncio.to_thread(_run_request)
        except AIServiceUnavailable:
            raise
        except Exception as exc:
            logger.exception("Gemini request failed: %s", exc)
            raise
