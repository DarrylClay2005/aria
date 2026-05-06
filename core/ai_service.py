from __future__ import annotations

import asyncio
import logging
import os
import re
import time

from core.settings import GEMINI_FALLBACK_MODELS, GEMINI_MODEL_ID

logger = logging.getLogger("discord")
DEFAULT_TIMEOUT_SECONDS = max(20, int(os.getenv("ARIA_AI_TIMEOUT_SECONDS", "90")))
DEFAULT_PROMPT_LIMIT = max(4096, int(os.getenv("ARIA_AI_MAX_PROMPT_CHARS", "60000")))
DEFAULT_SYSTEM_LIMIT = max(2048, int(os.getenv("ARIA_AI_MAX_SYSTEM_CHARS", "12000")))
DEFAULT_MAX_RETRY_DELAY_SECONDS = max(3.0, float(os.getenv("ARIA_AI_MAX_RETRY_DELAY_SECONDS", "20")))
DEFAULT_RETRY_ATTEMPTS = max(0, int(os.getenv("ARIA_AI_RETRY_ATTEMPTS", "1")))
RETRY_DELAY_RE = re.compile(r"retry(?: in)?\s+([0-9]+(?:\.[0-9]+)?)s", re.IGNORECASE)
RETRY_INFO_RE = re.compile(r"'retryDelay':\s*'([0-9]+)s'", re.IGNORECASE)


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
        fallback_models: list[str] | None = None,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        max_retry_delay_seconds: float = DEFAULT_MAX_RETRY_DELAY_SECONDS,
    ):
        self.model_id = (model_id or GEMINI_MODEL_ID).strip() or "gemini-2.5-flash"
        configured_fallbacks = fallback_models if fallback_models is not None else GEMINI_FALLBACK_MODELS
        self.fallback_models = [
            value.strip()
            for value in (configured_fallbacks or [])
            if value and value.strip() and value.strip() != self.model_id
        ]
        self.api_key = (
            api_key
            or os.getenv("ARIA_GEMINI_API_KEY", "")
            or os.getenv("GEMINI_API_KEY", "")
        ).strip()
        self.timeout_seconds = max(20, int(timeout_seconds))
        self.prompt_limit = max(2048, int(prompt_limit))
        self.system_limit = max(1024, int(system_limit))
        self.retry_attempts = max(0, int(retry_attempts))
        self.max_retry_delay_seconds = max(1.0, float(max_retry_delay_seconds))
        self._client = None
        self._types = None
        self._rate_limited_until = 0.0

    @staticmethod
    def _extract_retry_delay_seconds(message: str) -> float | None:
        if not message:
            return None
        match = RETRY_DELAY_RE.search(message) or RETRY_INFO_RE.search(message)
        if not match:
            return None
        try:
            return float(match.group(1))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _is_rate_limited_error(message: str) -> bool:
        lowered = (message or "").lower()
        return "resource_exhausted" in lowered or "quota exceeded" in lowered or "429" in lowered

    @staticmethod
    def _is_daily_quota_error(message: str) -> bool:
        lowered = (message or "").lower()
        return "perday" in lowered or "per day" in lowered or "current quota" in lowered

    @staticmethod
    def _format_retry_window(seconds: float | None) -> str | None:
        if seconds is None or seconds <= 0:
            return None
        rounded = max(1, int(seconds + 0.999))
        minutes, secs = divmod(rounded, 60)
        if minutes:
            return f"{minutes}m {secs}s" if secs else f"{minutes}m"
        return f"{secs}s"

    def _rate_limit_public_message(
        self,
        *,
        retry_after: float | None,
        used_fallbacks: bool,
        daily_quota: bool = False,
    ) -> str:
        retry_window = self._format_retry_window(retry_after)
        if retry_after and retry_after <= self.max_retry_delay_seconds and not daily_quota:
            return (
                f"My AI backend hit a temporary Gemini rate limit. Give me about {retry_window} and try again."
            )
        if daily_quota and retry_window:
            prefix = "My Gemini daily quota is tapped out"
            if used_fallbacks:
                prefix = "My Gemini daily quota is tapped out across the configured models"
            return (
                f"{prefix}. The API says to retry in about {retry_window}, but if the free-tier daily cap is exhausted it may stay unavailable until the quota resets."
            )
        if retry_window:
            prefix = "My Gemini quota is tapped out right now"
            if used_fallbacks:
                prefix = "My Gemini quota is tapped out across the configured models right now"
            return f"{prefix}. The current retry window is about {retry_window}."
        if used_fallbacks:
            return (
                "My Gemini quota is tapped out across the configured models right now. Give it a bit and try again, or increase the API quota/billing."
            )
        return (
            "My Gemini quota is tapped out right now. Give it a bit and try again, or increase the API quota/billing."
        )

    def _deduped_models(self) -> list[str]:
        models = [self.model_id, *self.fallback_models]
        seen = set()
        ordered = []
        for model in models:
            clean = str(model or "").strip()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            ordered.append(clean)
        return ordered

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

        now = time.monotonic()
        if self._rate_limited_until > now:
            remaining = self._rate_limited_until - now
            raise AIServiceUnavailable(
                f"Gemini requests temporarily paused for {remaining:.1f}s after recent rate limiting.",
                self._rate_limit_public_message(retry_after=remaining, used_fallbacks=bool(self.fallback_models)),
            )

        def _run_request(model_id: str) -> str:
            config = None
            if system_instruction:
                config = types.GenerateContentConfig(system_instruction=system_instruction)
            response = client.models.generate_content(
                model=model_id,
                contents=prompt,
                config=config,
            )
            return (getattr(response, "text", "") or "").strip()

        try:
            models = self._deduped_models()
            last_exc: Exception | None = None
            used_fallbacks = len(models) > 1

            for model_index, model_id in enumerate(models):
                attempts_remaining = self.retry_attempts + 1
                for _attempt in range(attempts_remaining):
                    try:
                        return await asyncio.wait_for(
                            asyncio.to_thread(_run_request, model_id),
                            timeout=self.timeout_seconds,
                        )
                    except TimeoutError:
                        raise
                    except Exception as exc:
                        text = str(exc)
                        if not self._is_rate_limited_error(text):
                            raise
                        last_exc = exc
                        retry_after = self._extract_retry_delay_seconds(text)
                        is_daily_quota = self._is_daily_quota_error(text)
                        has_next_model = model_index < len(models) - 1

                        if has_next_model:
                            logger.warning(
                                "Gemini model %s hit rate/quota limits; trying fallback model %s.",
                                model_id,
                                models[model_index + 1],
                            )
                            break

                        if (
                            retry_after
                            and retry_after <= self.max_retry_delay_seconds
                            and not is_daily_quota
                            and _attempt < attempts_remaining - 1
                        ):
                            logger.warning(
                                "Gemini model %s rate-limited; retrying after %.1fs.",
                                model_id,
                                retry_after,
                            )
                            await asyncio.sleep(retry_after + 0.5)
                            continue

                        if retry_after:
                            self._rate_limited_until = time.monotonic() + min(retry_after + 0.5, 300.0)
                        raise AIServiceUnavailable(
                            f"Gemini rate/quota limited on model {model_id}: {text}",
                            self._rate_limit_public_message(
                                retry_after=retry_after,
                                used_fallbacks=used_fallbacks,
                                daily_quota=is_daily_quota,
                            ),
                        ) from exc

            if last_exc:
                raise AIServiceUnavailable(
                    f"Gemini exhausted all configured models: {last_exc}",
                    self._rate_limit_public_message(retry_after=None, used_fallbacks=used_fallbacks),
                ) from last_exc
            raise AIServiceUnavailable("Gemini did not return a usable response.")
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
