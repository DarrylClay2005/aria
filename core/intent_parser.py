from __future__ import annotations

import re

from core.swarm_control import CHANNEL_MENTION_RE, DRONE_NAMES, FILTER_ALIASES, LOOP_ALIASES, extract_drone_name, extract_channel_id


ALL_SCOPE_MARKERS = ("all", "swarm", "everyone", "global")
DRONE_PATTERN = "|".join(DRONE_NAMES)


def _strip_prefix(message: str) -> str:
    cleaned = (message or "").strip()
    lowered = cleaned.lower()
    if lowered.startswith("aria "):
        return cleaned[5:].strip()
    return cleaned


def _extract_scope_token(command: str) -> str | None:
    lowered = command.lower()
    for marker in ALL_SCOPE_MARKERS:
        if re.search(rf"\b{re.escape(marker)}\b", lowered):
            return marker
    return None


class IntentParser:
    async def parse(self, message: str):
        command = _strip_prefix(message)
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

        match = re.fullmatch(rf"(pause|resume|skip|stop)(?:\s+({DRONE_PATTERN}|all|swarm))?", lowered)
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

        match = re.fullmatch(r"broadcast\s+(.+)", command, flags=re.IGNORECASE)
        if match:
            return {"action": "swarm_broadcast", "data": {"query": match.group(1).strip()}}

        match = re.fullmatch(rf"play\s+(.+?)(?:\s+(?:via|using|on)\s+({DRONE_PATTERN}))?", command, flags=re.IGNORECASE)
        if match:
            return {"action": "play", "data": {"query": match.group(1).strip(), "drone": (match.group(2) or "").lower() or None}}

        return {"action": "unknown"}
