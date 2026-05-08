from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable

import discord


MAX_CHAT_ATTACHMENT_BYTES = max(1, int(os.getenv("ARIA_CHAT_ATTACHMENT_MAX_BYTES", "500000") or "500000"))
MAX_CHAT_ATTACHMENTS = max(1, int(os.getenv("ARIA_CHAT_MAX_ATTACHMENTS", "5") or "5"))
MAX_CHAT_UPLOAD_CONTEXT_CHARS = max(
    12000,
    int(os.getenv("ARIA_CHAT_UPLOAD_CONTEXT_CHARS", "500000") or "500000"),
)
TEXT_MIME_PREFIXES = ("text/",)
TEXT_MIME_EXACT = {
    "application/json",
    "application/javascript",
    "application/xml",
    "application/yaml",
    "application/x-yaml",
}
TEXT_SUFFIXES = {
    ".txt",
    ".md",
    ".py",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".log",
    ".csv",
    ".html",
    ".css",
    ".xml",
    ".sql",
    ".sh",
    ".env",
}


def _clean_mime_type(value: str | None) -> str:
    return (value or "application/octet-stream").split(";", 1)[0].strip().lower() or "application/octet-stream"


def looks_text_like(filename: str | None, mime_type: str | None) -> bool:
    clean_mime = _clean_mime_type(mime_type)
    if any(clean_mime.startswith(prefix) for prefix in TEXT_MIME_PREFIXES):
        return True
    if clean_mime in TEXT_MIME_EXACT:
        return True
    return Path(filename or "").suffix.lower() in TEXT_SUFFIXES


def _decode_text_payload(payload: bytes, filename: str) -> str:
    for encoding in ("utf-8", "utf-16", "latin-1"):
        try:
            return payload.decode(encoding).replace("\r\n", "\n").strip()
        except UnicodeDecodeError:
            continue
    raise ValueError(f"`{filename}` could not be decoded as readable text.")


async def prepare_chat_uploads(attachments: Iterable[discord.Attachment]) -> list[dict[str, Any]]:
    selected = [attachment for attachment in attachments if attachment is not None]
    if len(selected) > MAX_CHAT_ATTACHMENTS:
        raise ValueError(f"I can hold up to {MAX_CHAT_ATTACHMENTS} files per chat message.")

    uploads: list[dict[str, Any]] = []
    for attachment in selected:
        filename = (attachment.filename or "attachment")[:255]
        size = int(attachment.size or 0)
        if size > MAX_CHAT_ATTACHMENT_BYTES:
            raise ValueError(
                f"`{filename}` is too large for Aria chat context. Keep each file under {MAX_CHAT_ATTACHMENT_BYTES // 1000} KB."
            )

        payload = await attachment.read()
        if not payload:
            raise ValueError(f"`{filename}` was empty.")

        mime_type = _clean_mime_type(attachment.content_type)
        if mime_type.startswith("image/"):
            uploads.append(
                {
                    "filename": filename,
                    "mime_type": mime_type,
                    "size_bytes": len(payload),
                    "content_text": None,
                    "content_bytes": payload,
                }
            )
            continue

        if looks_text_like(filename, mime_type):
            text = _decode_text_payload(payload, filename)
            if not text:
                raise ValueError(f"`{filename}` was empty.")
            uploads.append(
                {
                    "filename": filename,
                    "mime_type": "text/plain" if mime_type == "application/octet-stream" else mime_type,
                    "size_bytes": len(payload),
                    "content_text": text,
                    "content_bytes": None,
                }
            )
            continue

        raise ValueError(
            f"`{filename}` is not a supported Aria chat context file yet. Send images or readable text/code files."
        )

    return uploads


def _bytes_or_none(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return None


def build_chat_upload_prompt(
    prompt: str,
    uploads: list[dict[str, Any]],
) -> tuple[str, str | None, dict[str, Any] | None]:
    if not uploads:
        return prompt, None, None

    remaining = MAX_CHAT_UPLOAD_CONTEXT_CHARS
    sections = [
        "Active uploaded file context follows. These files are temporary and expire about 5 minutes after upload.",
    ]
    direct_attachment: dict[str, Any] | None = None
    names = []

    for upload in uploads:
        filename = str(upload.get("filename") or "attachment")[:255]
        mime_type = _clean_mime_type(str(upload.get("mime_type") or "application/octet-stream"))
        size_bytes = int(upload.get("size_bytes") or 0)
        names.append(filename)

        content_text = str(upload.get("content_text") or "").strip()
        content_bytes = _bytes_or_none(upload.get("content_bytes"))
        header = f"\n\n--- {filename} ({mime_type}, {size_bytes} bytes) ---\n"

        if content_text:
            if remaining <= 0:
                sections.append(header + "[Stored, but omitted from this prompt because the active upload context limit was reached.]")
                continue
            snippet = content_text[:remaining]
            remaining -= len(snippet)
            if len(snippet) < len(content_text):
                snippet = snippet.rstrip() + "\n[Truncated in prompt; full file remains in temporary MariaDB storage.]"
            sections.append(header + snippet)
            continue

        if mime_type.startswith("image/") and content_bytes:
            if direct_attachment is None:
                direct_attachment = {
                    "attachment_bytes": content_bytes,
                    "attachment_mime_type": mime_type,
                    "attachment_name": filename,
                }
                sections.append(header + "[Image attached directly to the model request.]")
            else:
                sections.append(header + "[Image stored temporarily; only the first active image is attached directly.]")
            continue

        sections.append(header + "[Stored temporarily, but this file type has no readable text context.]")

    context_block = "".join(sections)
    contextual_prompt = f"{prompt}\n\n{context_block}" if prompt else context_block
    note = "active uploads: " + ", ".join(names[:MAX_CHAT_ATTACHMENTS])
    return contextual_prompt, note, direct_attachment
