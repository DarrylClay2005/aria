from __future__ import annotations

import re

from core.swarm_control import FILTER_ALIASES, LOOP_ALIASES, DRONE_NAMES, extract_channel_id


ALL_SCOPE_MARKERS = ("all", "swarm", "everyone", "global")
DRONE_PATTERN = "|".join(DRONE_NAMES)
EXPLICIT_SPLIT_RE = re.compile(r"\s*(?:;|\n+|\band then\b|\bthen\b|\balso\b)\s*", re.IGNORECASE)


def _strip_prefix(message: str) -> str:
    cleaned = (message or "").strip()
    cleaned = re.sub(r"^\s*aria[:,]?\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*hey\s+aria[:,]?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*(?:execute|run)\s+", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def _clean_clause(message: str) -> str:
    return (message or "").strip().strip(" ,;.!?")


def _extract_scope_token(command: str) -> str | None:
    lowered = command.lower()
    for marker in ALL_SCOPE_MARKERS:
        if re.search(rf"\b{re.escape(marker)}\b", lowered):
            return marker
    return None


def _split_multi_command(command: str) -> list[str]:
    explicit_segments = [_clean_clause(segment) for segment in EXPLICIT_SPLIT_RE.split(command) if _clean_clause(segment)]
    if len(explicit_segments) > 1:
        return explicit_segments

    if " and " not in command.lower():
        return [_clean_clause(command)]

    conjunction_segments = [_clean_clause(segment) for segment in re.split(r"\band\b", command, flags=re.IGNORECASE) if _clean_clause(segment)]
    return conjunction_segments if len(conjunction_segments) > 1 else [_clean_clause(command)]


class IntentParser:
    async def parse(self, message: str):
        command = _clean_clause(_strip_prefix(message))
        lowered = command.lower().strip()
        if not lowered:
            return {"action": "unknown"}

        if lowered in {"disable auto", "enable auto"}:
            return {"action": "unknown"}

        if re.fullmatch(r"fix|diagnose|self heal|self-heal", lowered):
            return {"action": "self_heal"}

        if re.fullmatch(r"radar", lowered):
            return {"action": "swarm_radar"}

        if re.fullmatch(r"wrapped", lowered):
            return {"action": "swarm_wrapped"}

        match = re.fullmatch(rf"undo\s+({DRONE_PATTERN})", lowered)
        if match:
            return {"action": "swarm_undo", "data": {"drone": match.group(1)}}

        match = re.fullmatch(rf"queue(?:\s+({DRONE_PATTERN}))?", lowered)
        if match:
            return {"action": "swarm_queue", "data": {"drone": match.group(1)}}

        match = re.fullmatch(rf"shuffle(?:\s+({DRONE_PATTERN}|all|swarm))?", lowered)
        if match:
            drone = match.group(1)
            if drone in ALL_SCOPE_MARKERS:
                drone = None
            return {"action": "swarm_shuffle", "data": {"drone": drone}}

        match = re.fullmatch(rf"remove(?:\s+track)?\s+(\d+)\s+(?:from\s+)?({DRONE_PATTERN})", lowered)
        if match:
            return {
                "action": "swarm_remove",
                "data": {"track_number": int(match.group(1)), "drone": match.group(2)},
            }

        match = re.fullmatch(rf"loop\s+([a-z_ ]+?)(?:\s+(?:on|for))?\s*({DRONE_PATTERN}|all|swarm)?", lowered)
        if match:
            mode = LOOP_ALIASES.get(match.group(1).strip())
            drone = match.group(2)
            if mode:
                if drone in ALL_SCOPE_MARKERS:
                    drone = None
                return {"action": "swarm_loop", "data": {"mode": mode, "drone": drone}}

        match = re.fullmatch(rf"filter\s+([a-z0-9_ ]+?)(?:\s+(?:on|for))?\s*({DRONE_PATTERN}|all|swarm)?", lowered)
        if match:
            filter_name = FILTER_ALIASES.get(match.group(1).replace(" ", "").strip())
            drone = match.group(2)
            if filter_name is not None:
                if drone in ALL_SCOPE_MARKERS:
                    drone = None
                return {"action": "swarm_filter", "data": {"filter_type": filter_name, "drone": drone}}

        match = re.fullmatch(rf"(?:execute\s+|run\s+)?(pause|resume|skip|stop)(?:\s+({DRONE_PATTERN}|all|swarm))?", lowered)
        if match:
            drone = match.group(2)
            if drone in ALL_SCOPE_MARKERS:
                drone = None
            return {"action": match.group(1), "data": {"drone": drone}}

        match = re.fullmatch(rf"leave(?:\s+({DRONE_PATTERN}))?", lowered)
        if match:
            return {"action": "leave", "data": {"drone": match.group(1)}}

        match = re.fullmatch(rf"home\s+({DRONE_PATTERN})\s+(.+)", command, flags=re.IGNORECASE)
        if match:
            channel_id = extract_channel_id(match.group(2))
            if channel_id:
                return {"action": "swarm_set_home", "data": {"drone": match.group(1).lower(), "channel_id": channel_id}}

        match = re.fullmatch(rf"set\s+({DRONE_PATTERN})\s+home(?:\s+channel)?\s+(?:to\s+)?(.+)", command, flags=re.IGNORECASE)
        if match:
            channel_id = extract_channel_id(match.group(2))
            if channel_id:
                return {"action": "swarm_set_home", "data": {"drone": match.group(1).lower(), "channel_id": channel_id}}

        match = re.fullmatch(r"broadcast\s+(.+)", command, flags=re.IGNORECASE)
        if match:
            return {"action": "swarm_broadcast", "data": {"query": match.group(1).strip()}}

        match = re.fullmatch(rf"play\s+(.+?)(?:\s+(?:via|using|on)\s+({DRONE_PATTERN}))?", command, flags=re.IGNORECASE)
        if match:
            return {"action": "play", "data": {"query": match.group(1).strip(), "drone": (match.group(2) or "").lower() or None}}

        match = re.fullmatch(
            rf"(?:please\s+)?(?:tell|make|have|ask)\s+(?:the\s+)?(?:bot\s+|node\s+)?({DRONE_PATTERN})\s+(?:to\s+)?play\s+(.+)",
            command,
            flags=re.IGNORECASE,
        )
        if match:
            return {"action": "play", "data": {"drone": match.group(1).lower(), "query": match.group(2).strip()}}

        match = re.fullmatch(
            rf"(?:please\s+)?(?:tell|make|have|ask)\s+(?:the\s+)?(?:bot\s+|node\s+)?({DRONE_PATTERN})\s+(?:to\s+)?(pause|resume|skip|stop|leave)",
            command,
            flags=re.IGNORECASE,
        )
        if match:
            action = match.group(2).lower()
            return {"action": action, "data": {"drone": match.group(1).lower()}}

        scope_marker = _extract_scope_token(lowered)
        if scope_marker and re.search(r"\b(pause|resume|skip|stop|shuffle)\b", lowered):
            for action in ("pause", "resume", "skip", "stop"):
                if re.search(rf"\b{action}\b", lowered):
                    return {"action": action, "data": {"drone": None}}
            if re.search(r"\bshuffle\b", lowered):
                return {"action": "swarm_shuffle", "data": {"drone": None}}

        return {"action": "unknown"}

    async def parse_many(self, message: str) -> list[dict]:
        initial = await self.parse(message)
        if initial.get("action") != "unknown":
            return [initial]

        command = _clean_clause(_strip_prefix(message))
        segments = _split_multi_command(command)
        if len(segments) < 2:
            return []

        intents = []
        for segment in segments:
            parsed = await self.parse(segment)
            if parsed.get("action") == "unknown":
                return []
            intents.append(parsed)
        return intents
