from __future__ import annotations

import asyncio
import logging
import os

from core.settings import GEMINI_MODEL_ID

logger = logging.getLogger("discord")
DEFAULT_TIMEOUT_SECONDS = max(20, int(os.getenv("ARIA_AI_TIMEOUT_SECONDS", "90")))
DEFAULT_PROMPT_LIMIT = max(4096, int(os.getenv("ARIA_AI_MAX_PROMPT_CHARS", "60000")))
DEFAULT_SYSTEM_LIMIT = max(2048, int(os.getenv("ARIA_AI_MAX_SYSTEM_CHARS", "12000")))


class AIServiceUnavailable(RuntimeError):
    def __init__(self, message: str, public_message: str | None = None):
        super().__init__(message)
        self.public_message = public_message or "My AI backend isn't available right now."


class AIService:
    def __init__(
        self,
        *,
        model_id: str | None = None,
        api_key: str | None = None,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        prompt_limit: int = DEFAULT_PROMPT_LIMIT,
        system_limit: int = DEFAULT_SYSTEM_LIMIT,
    ):
        self.model_id = (model_id or GEMINI_MODEL_ID).strip() or "gemini-2.5-flash"
        self.api_key = (
            api_key
            or os.getenv("ARIA_GEMINI_API_KEY", "")
            or os.getenv("GEMINI_API_KEY", "")
        ).strip()
        self.timeout_seconds = max(20, int(timeout_seconds))
        self.prompt_limit = max(2048, int(prompt_limit))
        self.system_limit = max(1024, int(system_limit))
        self._client = None
        self._types = None

    @staticmethod
    def _clip_text(value: str | None, limit: int) -> str:
        cleaned = (value or "").strip()
        if len(cleaned) <= limit:
            return cleaned
        clipped = cleaned[: max(0, limit - 48)].rstrip()
        return f"{clipped}\n\n[Truncated for runtime safety]"

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
        prompt = self._clip_text(prompt, self.prompt_limit)
        system_instruction = self._clip_text(system_instruction, self.system_limit) if system_instruction else None

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
            return await asyncio.wait_for(asyncio.to_thread(_run_request), timeout=self.timeout_seconds)
        except TimeoutError as exc:
            raise AIServiceUnavailable(
                "Gemini request timed out.",
                "My AI backend timed out on that one. Try splitting the request into smaller parts.",
            ) from exc
        except AIServiceUnavailable:
            raise
        except Exception as exc:
            logger.exception("Gemini request failed: %s", exc)
            raise
