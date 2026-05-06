from __future__ import annotations

import asyncio
import base64
import logging
import os
import re
import time
from typing import Any

from core.settings import (
    GEMINI_FALLBACK_MODELS,
    GEMINI_MODEL_ID,
    OPENAI_FALLBACK_MODELS,
    OPENAI_MODEL_ID,
)

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
        openai_model_id: str | None = None,
        openai_api_key: str | None = None,
        openai_fallback_models: list[str] | None = None,
        enable_openai_fallback: bool | None = None,
    ):
        self.model_id = (model_id or GEMINI_MODEL_ID).strip() or "gemini-2.5-flash"
        configured_fallbacks = fallback_models if fallback_models is not None else GEMINI_FALLBACK_MODELS
        self.fallback_models = self._dedupe_models(self.model_id, configured_fallbacks)
        self.api_key = (
            api_key
            or os.getenv("ARIA_GEMINI_API_KEY", "")
            or os.getenv("GEMINI_API_KEY", "")
        ).strip()

        self.openai_model_id = (openai_model_id or OPENAI_MODEL_ID).strip() or "gpt-4.1-mini"
        configured_openai_fallbacks = (
            openai_fallback_models if openai_fallback_models is not None else OPENAI_FALLBACK_MODELS
        )
        self.openai_fallback_models = self._dedupe_models(self.openai_model_id, configured_openai_fallbacks)
        self.openai_api_key = (
            openai_api_key
            or os.getenv("ARIA_OPENAI_API_KEY", "")
            or os.getenv("OPENAI_API_KEY", "")
        ).strip()

        if enable_openai_fallback is None:
            env_value = str(os.getenv("ARIA_ENABLE_OPENAI_FALLBACK", "") or "").strip().lower()
            if env_value:
                self.enable_openai_fallback = env_value in {"1", "true", "yes", "on"}
            else:
                self.enable_openai_fallback = bool(self.openai_api_key)
        else:
            self.enable_openai_fallback = bool(enable_openai_fallback)

        self.timeout_seconds = max(20, int(timeout_seconds))
        self.prompt_limit = max(2048, int(prompt_limit))
        self.system_limit = max(1024, int(system_limit))
        self.retry_attempts = max(0, int(retry_attempts))
        self.max_retry_delay_seconds = max(1.0, float(max_retry_delay_seconds))

        self._gemini_client = None
        self._gemini_types = None
        self._openai_client = None
        self._rate_limited_until = 0.0

    @staticmethod
    def _dedupe_models(primary: str, candidates: list[str] | None) -> list[str]:
        seen = {str(primary or "").strip()}
        ordered = []
        for value in candidates or []:
            clean = str(value or "").strip()
            if not clean or clean in seen:
                continue
            seen.add(clean)
            ordered.append(clean)
        return ordered

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
    def _is_service_unavailable_error(message: str) -> bool:
        lowered = (message or "").lower()
        return (
            "503" in lowered
            or "status': 'unavailable'" in lowered
            or 'status": "unavailable"' in lowered
            or "temporarily unavailable" in lowered
            or "experiencing high demand" in lowered
            or "overloaded" in lowered
            or "service unavailable" in lowered
        )

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
                prefix = "My Gemini daily quota is tapped out across the configured Gemini models"
            return (
                f"{prefix}. The API says to retry in about {retry_window}, but if the free-tier daily cap is exhausted it may stay unavailable until the quota resets."
            )
        if retry_window:
            prefix = "My Gemini quota is tapped out right now"
            if used_fallbacks:
                prefix = "My Gemini quota is tapped out across the configured Gemini models right now"
            return f"{prefix}. The current retry window is about {retry_window}."
        if used_fallbacks:
            return (
                "My Gemini quota is tapped out across the configured Gemini models right now. Give it a bit and try again, or increase the API quota/billing."
            )
        return (
            "My Gemini quota is tapped out right now. Give it a bit and try again, or increase the API quota/billing."
        )

    @staticmethod
    def _service_unavailable_public_message(provider_label: str, *, backup_attempted: bool = False) -> str:
        if backup_attempted:
            return (
                f"My {provider_label} backend is under heavy load right now, and the backup route did not come through either. Give it a minute and try again."
            )
        return f"My {provider_label} backend is under heavy load right now. Give it a minute and try again."

    def _fallback_failure_public_message(self) -> str:
        return (
            "My primary AI backend is overloaded right now, and the OpenAI fallback could not take over cleanly. Give it a minute and try again."
        )

    @staticmethod
    def _clip_text(value: str | None, limit: int) -> str:
        cleaned = (value or "").strip()
        if len(cleaned) <= limit:
            return cleaned
        clipped = cleaned[: max(0, limit - 48)].rstrip()
        return f"{clipped}\n\n[Truncated for runtime safety]"

    def _has_openai_fallback(self) -> bool:
        return self.enable_openai_fallback and bool(self.openai_api_key)

    def _ensure_gemini_client(self):
        if not self.api_key:
            raise AIServiceUnavailable(
                "GEMINI_API_KEY is not configured.",
                "My Gemini backend is not configured yet. Add `GEMINI_API_KEY` or enable the OpenAI fallback.",
            )

        if self._gemini_client is not None and self._gemini_types is not None:
            return self._gemini_client, self._gemini_types

        try:
            from google import genai
            from google.genai import types
        except ImportError as exc:
            raise AIServiceUnavailable(
                "google-genai is not installed.",
                "My Gemini backend package is missing on this host, so the chat commands can't boot yet.",
            ) from exc

        self._gemini_client = genai.Client(api_key=self.api_key)
        self._gemini_types = types
        return self._gemini_client, self._gemini_types

    def _ensure_openai_client(self):
        if not self.openai_api_key:
            raise AIServiceUnavailable(
                "OPENAI_API_KEY is not configured.",
                "My OpenAI fallback is not configured yet. Add `OPENAI_API_KEY` or `ARIA_OPENAI_API_KEY` to use it.",
            )

        if self._openai_client is not None:
            return self._openai_client

        try:
            from openai import OpenAI
        except ImportError as exc:
            raise AIServiceUnavailable(
                "openai is not installed.",
                "My OpenAI fallback package is missing on this host, so that backup path cannot boot yet.",
            ) from exc

        self._openai_client = OpenAI(api_key=self.openai_api_key)
        return self._openai_client

    async def _generate_gemini_contents(self, contents: Any, *, system_instruction: str | None = None) -> str:
        client, types = self._ensure_gemini_client()
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
                contents=contents,
                config=config,
            )
            return (getattr(response, "text", "") or "").strip()

        models = [self.model_id, *self.fallback_models]
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
                except TimeoutError as exc:
                    raise AIServiceUnavailable(
                        "Gemini request timed out.",
                        "My Gemini backend timed out on that one. Try splitting the request into smaller parts.",
                    ) from exc
                except Exception as exc:
                    text = str(exc)
                    last_exc = exc
                    if self._is_rate_limited_error(text):
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

                    if self._is_service_unavailable_error(text):
                        has_next_model = model_index < len(models) - 1
                        if has_next_model:
                            logger.warning(
                                "Gemini model %s is temporarily unavailable; trying fallback model %s.",
                                model_id,
                                models[model_index + 1],
                            )
                            break
                        raise AIServiceUnavailable(
                            f"Gemini temporarily unavailable on model {model_id}: {text}",
                            self._service_unavailable_public_message("Gemini"),
                        ) from exc

                    logger.exception("Gemini request failed: %s", exc)
                    raise

        if last_exc:
            if self._is_service_unavailable_error(str(last_exc)):
                raise AIServiceUnavailable(
                    f"Gemini exhausted all configured models due to temporary unavailability: {last_exc}",
                    self._service_unavailable_public_message("Gemini"),
                ) from last_exc
            raise AIServiceUnavailable(
                f"Gemini exhausted all configured models: {last_exc}",
                self._rate_limit_public_message(retry_after=None, used_fallbacks=used_fallbacks),
            ) from last_exc
        raise AIServiceUnavailable("Gemini did not return a usable response.")

    def _build_openai_input(
        self,
        *,
        prompt: str,
        system_instruction: str | None = None,
        attachment_bytes: bytes | None = None,
        attachment_mime_type: str | None = None,
        attachment_name: str | None = None,
    ) -> list[dict[str, Any]]:
        input_items: list[dict[str, Any]] = []
        if system_instruction:
            input_items.append(
                {
                    "role": "system",
                    "content": [{"type": "input_text", "text": self._clip_text(system_instruction, self.system_limit)}],
                }
            )

        user_content: list[dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        if attachment_bytes and attachment_mime_type:
            mime_type = (attachment_mime_type or "application/octet-stream").strip().lower()
            if mime_type.startswith("image/"):
                encoded = base64.b64encode(attachment_bytes).decode("ascii")
                user_content.append(
                    {
                        "type": "input_image",
                        "image_url": f"data:{mime_type};base64,{encoded}",
                        "detail": "auto",
                    }
                )
            else:
                encoded = base64.b64encode(attachment_bytes).decode("ascii")
                user_content.append(
                    {
                        "type": "input_file",
                        "filename": (attachment_name or "attachment.txt")[:255],
                        "file_data": encoded,
                    }
                )

        input_items.append({"role": "user", "content": user_content})
        return input_items

    @staticmethod
    def _extract_openai_output_text(response: Any) -> str:
        text = (getattr(response, "output_text", "") or "").strip()
        if text:
            return text
        output = getattr(response, "output", None) or []
        for item in output:
            if getattr(item, "type", "") != "message":
                continue
            for content in getattr(item, "content", None) or []:
                if getattr(content, "type", "") == "output_text":
                    candidate = (getattr(content, "text", "") or "").strip()
                    if candidate:
                        return candidate
        return ""

    async def _generate_openai_response(
        self,
        *,
        prompt: str,
        system_instruction: str | None = None,
        attachment_bytes: bytes | None = None,
        attachment_mime_type: str | None = None,
        attachment_name: str | None = None,
    ) -> str:
        client = self._ensure_openai_client()
        prompt = self._clip_text(prompt, self.prompt_limit)
        input_payload = self._build_openai_input(
            prompt=prompt,
            system_instruction=system_instruction,
            attachment_bytes=attachment_bytes,
            attachment_mime_type=attachment_mime_type,
            attachment_name=attachment_name,
        )

        def _run_request(model_id: str) -> str:
            response = client.responses.create(
                model=model_id,
                input=input_payload,
            )
            return self._extract_openai_output_text(response)

        models = [self.openai_model_id, *self.openai_fallback_models]
        last_exc: Exception | None = None
        for model_index, model_id in enumerate(models):
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(_run_request, model_id),
                    timeout=self.timeout_seconds,
                )
            except TimeoutError as exc:
                raise AIServiceUnavailable(
                    "OpenAI fallback request timed out.",
                    "My OpenAI fallback timed out on that request. Try again in a moment.",
                ) from exc
            except Exception as exc:
                last_exc = exc
                text = str(exc)
                has_next_model = model_index < len(models) - 1
                if (self._is_rate_limited_error(text) or self._is_service_unavailable_error(text)) and has_next_model:
                    logger.warning(
                        "OpenAI model %s failed with a temporary backend issue; trying fallback model %s.",
                        model_id,
                        models[model_index + 1],
                    )
                    continue
                if self._is_rate_limited_error(text):
                    raise AIServiceUnavailable(
                        f"OpenAI fallback rate/quota limited on model {model_id}: {text}",
                        "My OpenAI fallback is rate-limited right now too. Give it a bit and try again.",
                    ) from exc
                if self._is_service_unavailable_error(text):
                    raise AIServiceUnavailable(
                        f"OpenAI fallback temporarily unavailable on model {model_id}: {text}",
                        self._service_unavailable_public_message("OpenAI fallback"),
                    ) from exc
                logger.exception("OpenAI fallback request failed: %s", exc)
                raise

        raise AIServiceUnavailable(
            f"OpenAI fallback exhausted configured models: {last_exc}",
            self._service_unavailable_public_message("OpenAI fallback"),
        ) from last_exc

    async def _generate_with_fallback(
        self,
        *,
        prompt: str,
        system_instruction: str | None = None,
        attachment_bytes: bytes | None = None,
        attachment_mime_type: str | None = None,
        attachment_name: str | None = None,
    ) -> str:
        gemini_unavailable: AIServiceUnavailable | None = None

        if self.api_key:
            try:
                if attachment_bytes and attachment_mime_type:
                    attachment_label = (attachment_name or "attachment").strip()[:180]
                    mime_type = (attachment_mime_type or "application/octet-stream").strip().lower()
                    client, types = self._ensure_gemini_client()
                    attachment_intro = (
                        f"Attachment provided by the user: `{attachment_label}` ({mime_type}). Use it as direct context for the reply."
                    )
                    contents = [
                        types.Part.from_text(text=f"{attachment_intro}\n\nUser prompt:\n{prompt}"),
                        types.Part.from_bytes(data=attachment_bytes, mime_type=mime_type),
                    ]
                else:
                    contents = self._clip_text(prompt, self.prompt_limit)
                return await self._generate_gemini_contents(contents, system_instruction=system_instruction)
            except AIServiceUnavailable as exc:
                gemini_unavailable = exc
                if not self._has_openai_fallback():
                    raise
                logger.warning("Gemini path unavailable; attempting OpenAI fallback: %s", exc)
        elif not self._has_openai_fallback():
            raise AIServiceUnavailable(
                "No AI provider is configured.",
                "No AI backend is configured right now. Add `GEMINI_API_KEY` or `OPENAI_API_KEY` and try again.",
            )

        try:
            return await self._generate_openai_response(
                prompt=prompt,
                system_instruction=system_instruction,
                attachment_bytes=attachment_bytes,
                attachment_mime_type=attachment_mime_type,
                attachment_name=attachment_name,
            )
        except AIServiceUnavailable as fallback_exc:
            if gemini_unavailable is not None:
                raise AIServiceUnavailable(
                    f"Gemini failed first ({gemini_unavailable}); OpenAI fallback also failed ({fallback_exc}).",
                    self._fallback_failure_public_message(),
                ) from fallback_exc
            raise

    async def generate(self, prompt: str, *, system_instruction: str | None = None) -> str:
        return await self._generate_with_fallback(
            prompt=self._clip_text(prompt, self.prompt_limit),
            system_instruction=system_instruction,
        )

    async def generate_with_attachment(
        self,
        prompt: str,
        *,
        attachment_bytes: bytes,
        attachment_mime_type: str,
        attachment_name: str,
        system_instruction: str | None = None,
    ) -> str:
        return await self._generate_with_fallback(
            prompt=self._clip_text(prompt, self.prompt_limit),
            system_instruction=system_instruction,
            attachment_bytes=attachment_bytes,
            attachment_mime_type=attachment_mime_type,
            attachment_name=attachment_name,
        )
