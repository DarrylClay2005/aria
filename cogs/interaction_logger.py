import discord
from discord.ext import commands
from discord import app_commands
import aiomysql
import logging
import json

logger = logging.getLogger("discord")

DB_CONFIG = {
    'host': '127.0.0.1', 'user': 'botuser', 'password': 'botpassword', 'db': 'discord_aria', 'autocommit': True
}

class InteractionLogger(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS aria_logs (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            user_id BIGINT,
                            command_name VARCHAR(100),
                            arguments TEXT,
                            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    # NEW: Track total messages sent by users
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS aria_message_stats (
                            user_id BIGINT PRIMARY KEY,
                            total_messages INT DEFAULT 0
                        )
                    """)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot: return
        
        # Silently increment their "talkativeness" score
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT INTO aria_message_stats (user_id, total_messages) VALUES (%s, 1) ON DUPLICATE KEY UPDATE total_messages = total_messages + 1", (message.author.id,))

    @commands.Cog.listener()
    async def on_app_command_completion(self, interaction: discord.Interaction, command):
        args = {key: str(value) for key, value in vars(interaction.namespace).items() if not key.startswith('_')} if interaction.namespace else {}
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT INTO aria_logs (user_id, command_name, arguments) VALUES (%s, %s, %s)", (interaction.user.id, command.name, json.dumps(args)))

    # Let admins see who the loudest users are
    @app_commands.command(name="server_activity", description="[ADMIN] See who talks the most in the server")
    @app_commands.default_permissions(administrator=True)
    async def server_activity(self, interaction: discord.Interaction):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT user_id, total_messages FROM aria_message_stats ORDER BY total_messages DESC LIMIT 10")
                    stats = await cur.fetchall()

        if not stats:
            return await interaction.response.send_message("Nobody has said anything since my surveillance went online.")

        desc = "\n".join([f"**<@{uid}>**: {count} messages" for uid, count in stats])
        embed = discord.Embed(title="🔊 Most Talkative Humans", description=desc, color=discord.Color.dark_grey())
        embed.set_footer(text="Aria hears everything.")
        await interaction.response.send_message(embed=embed)

async def setup(bot):
    await bot.add_cog(InteractionLogger(bot))