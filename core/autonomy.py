import asyncio
import logging

try:
    import aiomysql
except ImportError:  # pragma: no cover - optional in lightweight local test shells
    aiomysql = None

from core.database import db
from core.swarm_control import DRONE_NAMES

logger = logging.getLogger("Autonomy")


def resolve_bot_from_ctx(ctx):
    bot = getattr(ctx, "bot", None) or getattr(ctx, "client", None)
    if bot is not None:
        return bot
    state = getattr(ctx, "_state", None)
    getter = getattr(state, "_get_client", None)
    if callable(getter):
        return getter()
    return None

class AutonomousEngine:
    def __init__(self, bot):
        self.bot = bot
        self.enabled = True

    async def detect_automation_issues(self):
        issues = []
        if not db.pool:
            return issues

        async with db.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor if aiomysql else None) as cur:
                try:
                    await cur.execute("SELECT id, guild_id, channel_id FROM aria_automations")
                    automations = await cur.fetchall()
                except Exception:
                    automations = []

        for automation in automations:
            channel = self.bot.get_channel(automation["channel_id"])
            if channel is None:
                issues.append(
                    {
                        "type": "missing_automation_channel",
                        "automation_id": automation["id"],
                        "guild_id": automation["guild_id"],
                    }
                )
                continue

            me = channel.guild.me or channel.guild.get_member(self.bot.user.id)
            if me and not channel.permissions_for(me).send_messages:
                issues.append(
                    {
                        "type": "automation_no_permission",
                        "automation_id": automation["id"],
                        "guild_id": automation["guild_id"],
                        "channel_id": automation["channel_id"],
                    }
                )
        return issues

    async def detect_swarm_issues(self):
        issues = []
        if not db.pool:
            return issues

        async with db.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor if aiomysql else None) as cur:
                for drone in DRONE_NAMES:
                    try:
                        await cur.execute(
                            f"SELECT bot_name FROM discord_music_{drone}.swarm_health WHERE last_pulse < NOW() - INTERVAL 3 MINUTE"
                        )
                        for row in await cur.fetchall():
                            issues.append({"type": "stale_swarm_node", "drone": row["bot_name"]})
                    except Exception:
                        continue
        return issues

    async def detect_issues(self):
        issues = []
        for guild in self.bot.guilds:
            vc = guild.voice_client
            if vc and not vc.is_connected():
                issues.append({"type": "voice_disconnect","guild_id": guild.id})
        issues.extend(await self.detect_automation_issues())
        issues.extend(await self.detect_swarm_issues())
        return issues

    async def fix_issue(self, issue):
        try:
            if issue["type"] == "voice_disconnect":
                guild = self.bot.get_guild(issue["guild_id"])
                vc = guild.voice_client
                if vc:
                    await vc.disconnect()
                    await asyncio.sleep(1)
                    return True
            elif issue["type"] == "missing_automation_channel":
                async with db.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("DELETE FROM aria_automations WHERE id = %s", (issue["automation_id"],))
                return True
        except Exception as e:
            logger.error(e)
        return False

    async def run_summary(self):
        findings = await self.detect_issues()
        resolved = 0
        unresolved = []
        for issue in findings:
            fixed = await self.fix_issue(issue)
            if fixed:
                resolved += 1
            else:
                unresolved.append(issue["type"])

        if not findings:
            return "Diagnostics complete. No automation or swarm faults were detected."

        parts = [f"Diagnostics complete. Found {len(findings)} issue(s) and resolved {resolved}."]
        if unresolved:
            parts.append("Still watching: " + ", ".join(sorted(set(unresolved))) + ".")
        return " ".join(parts)

    async def run_once(self):
        if not self.enabled:
            return

        for issue in await self.detect_issues():
            await self.fix_issue(issue)
