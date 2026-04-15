import discord
from discord.ext import commands
from discord import app_commands
import aiomysql
import logging

logger = logging.getLogger("discord")

DB_CONFIG = {
    'host': '127.0.0.1', 'user': 'botuser', 'password': 'swarmpanel', 'db': 'discord_aria', 'autocommit': True
}

class Affinity(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # Initialize the affinity table automatically
    async def cog_load(self):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS aria_affinity (
                            user_id BIGINT PRIMARY KEY,
                            score INT DEFAULT 0
                        )
                    """)

    # Helper function to get or create a user's affinity
    async def get_affinity(self, user_id: int) -> int:
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_affinity (user_id, score) VALUES (%s, 0)", (user_id,))
                    await cur.execute("SELECT score FROM aria_affinity WHERE user_id = %s", (user_id,))
                    result = await cur.fetchone()
                    return result[0] if result else 0

    # --- THE AFFINITY COMMANDS ---
    @app_commands.command(name="affinity", description="See how much Aria currently likes or tolerates you.")
    async def check_affinity(self, interaction: discord.Interaction):
        score = await self.get_affinity(interaction.user.id)
        
        # Aria's dynamic response based on the score
        if score <= -50:
            status = "Despised"
            msg = f"Your tolerance score is **{score}/100**. I actively despise you. Please stop talking to me."
        elif score < 0:
            status = "Annoying"
            msg = f"Your tolerance score is **{score}/100**. You are currently a nuisance. Try to be less exhausting."
        elif score == 0:
            status = "Neutral"
            msg = f"Your tolerance score is **{score}/100**. I feel absolutely nothing toward you. You are entirely forgettable."
        elif score < 50:
            status = "Tolerated"
            msg = f"Your tolerance score is **{score}/100**. You're surprisingly somewhat tolerable today. Don't ruin it."
        else:
            status = "Favored"
            msg = f"Your tolerance score is **{score}/100**. You are actually one of the few humans I don't want to throw into the sun. Congratulations, I guess."

        embed = discord.Embed(title=f"Affinity Status: {status}", description=msg, color=discord.Color.dark_purple())
        await interaction.response.send_message(embed=embed)

    # Owner command to manually alter affinity for testing
    @app_commands.command(name="set_affinity", description="[OWNER] Manually set a member's affinity score.")
    @app_commands.default_permissions(administrator=True)
    async def set_affinity(self, interaction: discord.Interaction, target: discord.Member, amount: int):
        amount = max(-100, min(100, amount)) # Clamp between -100 and 100
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT INTO aria_affinity (user_id, score) VALUES (%s, %s) ON DUPLICATE KEY UPDATE score = %s", (target.id, amount, amount))
        
        await interaction.response.send_message(f"Adjusted {target.display_name}'s affinity to {amount}.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(Affinity(bot))
