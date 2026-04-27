import discord
from discord.ext import commands, tasks
import logging
from datetime import datetime, timezone, timedelta
from core.ai_service import AIService, AIServiceUnavailable
from core.database import db

logger = logging.getLogger("discord")

class PresenceProfiler(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ai_service = AIService()
        self.shame_loop.start()

    def cog_unload(self):
        self.shame_loop.cancel()

    async def get_affinity(self, user_id: int) -> int:
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT score FROM aria_affinity WHERE user_id = %s", (user_id,))
                res = await cur.fetchone()
                return res[0] if res else 0

    @tasks.loop(minutes=15.0)
    async def shame_loop(self):
        degenerate_games = ["league of legends", "valorant", "genshin impact", "overwatch 2", "roblox"]
        
        for guild in self.bot.guilds:
            shame_channel = discord.utils.find(lambda c: "general" in c.name.lower() or "chat" in c.name.lower(), guild.text_channels)
            if not shame_channel:
                shame_channel = guild.text_channels[0] if guild.text_channels else None
            if not shame_channel: continue

            for member in guild.members:
                if member.bot: continue
                
                for activity in member.activities:
                    if activity.type == discord.ActivityType.playing and activity.name:
                        game_name = activity.name.lower()
                        
                        if any(deg_game in game_name for deg_game in degenerate_games):
                            if hasattr(activity, 'start') and activity.start:
                                duration = datetime.now(timezone.utc) - activity.start
                                hours_played = duration.total_seconds() / 3600
                                
                                if hours_played > 3.0:
                                    affinity = await self.get_affinity(member.id)
                                    sys_inst = f"You are Aria Blaze. You hate humans. You just caught '{member.display_name}' playing '{activity.name}' for over {int(hours_played)} straight hours. Roast them mercilessly. Tell them to touch grass and get a job. Swear heavily. Affinity: {affinity}/100."
                                    
                                    prompt = f"Write a brutal 2-sentence public callout for {member.display_name}."
                                    
                                    try:
                                        response_text = await self.ai_service.generate(prompt, system_instruction=sys_inst)
                                        await shame_channel.send(f"{member.mention} 🚨 **PRESENCE ALERT** 🚨\n\n{response_text}")
                                        
                                        try:
                                            await member.timeout(timedelta(hours=1), reason="Aria's Touch Grass Protocol")
                                            await shame_channel.send("*(I have timed them out for 1 hour so they are forced to go outside.)*")
                                        except discord.Forbidden:
                                            pass
                                    except AIServiceUnavailable as exc:
                                        logger.warning("Presence profiler unavailable: %s", exc)
                                    except Exception as e:
                                        logger.error(f"Presence Profiler Error: {e}")

    @shame_loop.before_loop
    async def before_shame_loop(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(PresenceProfiler(bot))
