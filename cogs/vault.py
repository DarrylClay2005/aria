import discord
from discord.ext import commands
from discord import app_commands
import aiomysql
import logging
import random

logger = logging.getLogger("discord")

# Aria's Database Config
DB_CONFIG = {
    'host': '127.0.0.1', 'user': 'botuser', 'password': 'swarmpanel', 'db': 'discord_aria', 'autocommit': True
}

class Vault(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # Initialize the global vault table
    async def cog_load(self):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS aria_vault (
                            guild_id BIGINT PRIMARY KEY,
                            energy_level INT DEFAULT 0
                        )
                    """)

    # Helper function to get the current energy
    async def get_energy(self, guild_id: int) -> int:
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_vault (guild_id, energy_level) VALUES (%s, 0)", (guild_id,))
                    await cur.execute("SELECT energy_level FROM aria_vault WHERE guild_id = %s", (guild_id,))
                    result = await cur.fetchone()
                    return result[0] if result else 0

    # Helper function to add energy and check if it bursts
    async def add_energy(self, guild_id: int, amount: int, channel: discord.TextChannel):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("UPDATE aria_vault SET energy_level = energy_level + %s WHERE guild_id = %s", (amount, guild_id))
                    await cur.execute("SELECT energy_level FROM aria_vault WHERE guild_id = %s", (guild_id,))
                    current_energy = (await cur.fetchone())[0]

        if current_energy >= 100:
            await self.trigger_vault(guild_id, channel)

    # The big payoff event
    async def trigger_vault(self, guild_id: int, channel: discord.TextChannel):
        payout = random.randint(1000, 3000)
        
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Reset energy back to 0
                    await cur.execute("UPDATE aria_vault SET energy_level = 0 WHERE guild_id = %s", (guild_id,))
                    # Give everyone who has an economy profile some coins
                    await cur.execute("UPDATE aria_economy SET balance = balance + %s", (payout,))
                    
        embed = discord.Embed(
            title="🎶 The Siren's Vault Has Reached Critical Mass!", 
            description=f"*Aria absorbs the overwhelming negative energy of the server and smiles.* \n\n\"Delicious. You humans are so delightfully awful to each other. As a reward, I've dispersed **{payout} Aria Coins** to everyone's accounts. Now go fight over what to buy with it.\"", 
            color=discord.Color.green()
        )
        await channel.send(embed=embed)

    # --- THE VAULT COMMAND GROUP ---
    vault_group = app_commands.Group(name="vault", description="Check and feed the server's Negative Energy Vault.")

    @vault_group.command(name="status", description="Check the current negative energy level for this server.")
    async def vault_status(self, interaction: discord.Interaction):
        energy = await self.get_energy(interaction.guild.id)
        
        # Create a visual progress bar
        bar_length = 20
        filled = min(bar_length, int((energy / 100) * bar_length))
        bar = "█" * filled + "░" * (bar_length - filled)
        
        embed = discord.Embed(title="🔮 Negative Energy Vault", description=f"I feed on your suffering and conflict. Keep it coming.\n\n**[{bar}] {min(energy, 100)}%**", color=discord.Color.purple())
        await interaction.response.send_message(embed=embed)

    @vault_group.command(name="sacrifice", description="Sacrifice your own coins to feed energy into the vault.")
    async def vault_sacrifice(self, interaction: discord.Interaction, coins: int):
        if coins < 100:
            return await interaction.response.send_message("Don't waste my time with pocket change. 100 coins minimum.", ephemeral=True)
            
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Check if they have the money
                    await cur.execute("SELECT balance FROM aria_economy WHERE user_id = %s", (interaction.user.id,))
                    res = await cur.fetchone()
                    if not res or res[0] < coins:
                        return await interaction.response.send_message("You can't sacrifice what you don't have. Pathetic.", ephemeral=True)
                        
                    # Deduct the money
                    await cur.execute("UPDATE aria_economy SET balance = balance - %s WHERE user_id = %s", (coins, interaction.user.id))
                    
        # 100 coins = 1% energy
        energy_gained = max(1, coins // 100)
        
        await interaction.response.send_message(f"You sacrificed **{coins} coins**. It fuels the vault by **{energy_gained}%**. I revel in your loss.")
        
        # Add the energy and check if it bursts
        await self.add_energy(interaction.guild.id, energy_gained, interaction.channel)


# This function tells the main bot.py how to load this file
async def setup(bot):
    await bot.add_cog(Vault(bot))
