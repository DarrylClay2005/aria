import discord
from discord.ext import commands
from discord import app_commands
import logging
import random
from core.database import db

logger = logging.getLogger("discord")

class Corruption(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # --- HELPER FUNCTIONS ---
    async def get_balance(self, user_id: int) -> int:
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT IGNORE INTO aria_economy (user_id, balance) VALUES (%s, 0)", (user_id,))
                await cur.execute("SELECT balance FROM aria_economy WHERE user_id = %s", (user_id,))
                res = await cur.fetchone()
                return res[0] if res else 0

    async def update_balance(self, user_id: int, amount: int):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE aria_economy SET balance = balance + %s WHERE user_id = %s", (amount, user_id))

    async def get_affinity(self, user_id: int) -> int:
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT IGNORE INTO aria_affinity (user_id, score) VALUES (%s, 0)", (user_id,))
                await cur.execute("SELECT score FROM aria_affinity WHERE user_id = %s", (user_id,))
                res = await cur.fetchone()
                return res[0] if res else 0

    async def update_affinity(self, user_id: int, amount: int):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE aria_affinity SET score = score + %s WHERE user_id = %s", (amount, user_id))

    # --- THE BRIBE COMMAND ---
    @app_commands.command(name="bribe", description="Offer Aria Coins in exchange for a little more affection.")
    @app_commands.describe(amount="How many coins to offer (Min: 1000)")
    async def bribe(self, interaction: discord.Interaction, amount: int):
        if amount < 1000:
            return await interaction.response.send_message("You think I can be bought for less than 1,000 coins? How insulting. Try again when you aren't broke.", ephemeral=True)
        
        bal = await self.get_balance(interaction.user.id)
        if bal < amount:
            return await interaction.response.send_message(f"You don't even have {amount} coins. Why are you wasting my time?", ephemeral=True)

        # Deduct coins
        await self.update_balance(interaction.user.id, -amount)
        
        # 1000 coins = +5 Affinity. 
        affinity_gain = amount // 200
        await self.update_affinity(interaction.user.id, affinity_gain)
        new_affinity = await self.get_affinity(interaction.user.id)

        await interaction.response.send_message(f"You're offering me **{amount} coins**? Fucking fine. I'll take your money. My tolerance for you has temporarily increased by **{affinity_gain} points**. (Current Affinity: {new_affinity})\n\nDon't let it go to your head. I still think you're annoying, just slightly wealthier than I gave you credit for.")

    # --- THE STEAL COMMAND ---
    @app_commands.command(name="steal", description="Try to steal Aria Coins from another member and hope it works.")
    async def steal(self, interaction: discord.Interaction, target: discord.Member):
        if target.id == interaction.user.id:
            return await interaction.response.send_message("Stealing from yourself? Wow. The bar was on the floor and you brought a shovel.", ephemeral=True)
        if target.bot:
            return await interaction.response.send_message("You can't steal from a bot, you absolute clown.", ephemeral=True)

        target_bal = await self.get_balance(target.id)
        if target_bal < 100:
            return await interaction.response.send_message(f"Don't bother. {target.display_name} is broke. They have less than 100 coins.", ephemeral=True)

        # 40% chance to succeed, 60% chance to fail
        success = random.random() < 0.40
        steal_amount = random.randint(50, min(500, int(target_bal * 0.5)))

        if success:
            await self.update_balance(target.id, -steal_amount)
            await self.update_balance(interaction.user.id, steal_amount)
            
            # Add a little negative energy to the vault organically!
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_vault (guild_id, energy_level) VALUES (%s, 0)", (interaction.guild.id,))
                    await cur.execute("UPDATE aria_vault SET energy_level = energy_level + 5 WHERE guild_id = %s", (interaction.guild.id,))

            await interaction.response.send_message(f"🥷 Well look at that. You managed to steal **{steal_amount} coins** from {target.mention} without tripping over your own feet.\n\n*Aria absorbs 5% Negative Energy from the theft.*")
        else:
            # Penalty! Aria takes the coins from the thief.
            penalty = steal_amount
            user_bal = await self.get_balance(interaction.user.id)
            actual_penalty = min(penalty, user_bal)
            
            if actual_penalty > 0:
                await self.update_balance(interaction.user.id, -actual_penalty)
                msg = f"🚨 **CAUGHT.**\n\nYou tried to steal from {target.mention}, but you were so incredibly loud and clumsy that I had to intervene. As a 'Stupidity Fine', I am seizing **{actual_penalty} coins** from your account for myself. Next time, be a better thief."
            else:
                msg = f"🚨 **CAUGHT.**\n\nYou tried to steal from {target.mention} and failed miserably. I was going to fine you, but you literally have 0 coins. You disgust me."

            await interaction.response.send_message(msg)

# This function tells the main bot.py how to load this file
async def setup(bot):
    await bot.add_cog(Corruption(bot))
