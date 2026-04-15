import asyncio, logging

logger = logging.getLogger("Autonomy")

class AutonomousEngine:
    def __init__(self, bot):
        self.bot = bot
        self.enabled = True

    async def detect_issues(self):
        issues = []
        for guild in self.bot.guilds:
            vc = guild.voice_client
            if vc and not vc.is_connected():
                issues.append({"type": "voice_disconnect","guild_id": guild.id})
        return issues

    async def fix_issue(self, issue):
        try:
            if issue["type"] == "voice_disconnect":
                guild = self.bot.get_guild(issue["guild_id"])
                vc = guild.voice_client
                if vc:
                    await vc.disconnect()
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(e)

    async def run(self):
        while True:
            if self.enabled:
                for issue in await self.detect_issues():
                    await self.fix_issue(issue)
            await asyncio.sleep(10)
