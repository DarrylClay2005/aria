import discord
from discord.ext import commands
from discord import app_commands
import aiomysql
import logging
from google import genai
from google.genai import types

logger = logging.getLogger("discord")

# --- CONFIGURATION ---
DB_CONFIG = {
    'host': '127.0.0.1', 'user': 'botuser', 'password': 'swarmpanel', 'db': 'discord_aria', 'autocommit': True
}

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'AIzaSyBe-PsYYalYB4Tum-vCmqj-N9m6MsfTL2k')
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = 'gemini-2.5-flash'

EVALUATION_INSTRUCTION = """
You are Aria Blaze. A human is submitting a "roast" (an insult) to try and claim a digital bounty on another user's head.
You must evaluate how painful, clever, and devastating the roast is.
Reply with EXACTLY two lines:
Line 1: A score from 1 to 10 (e.g., "Score: 8")
Line 2: A brief, highly sarcastic comment judging their insult, using heavy profanity.
"""

class Bounty(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # Initialize the bounty table
    async def cog_load(self):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS aria_bounties (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            target_id BIGINT,
                            sponsor_id BIGINT,
                            amount INT
                        )
                    """)

    # Helper function to get balance
    async def get_balance(self, user_id: int) -> int:
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_economy (user_id, balance) VALUES (%s, 0)", (user_id,))
                    await cur.execute("SELECT balance FROM aria_economy WHERE user_id = %s", (user_id,))
                    res = await cur.fetchone()
                    return res[0] if res else 0

    # --- THE BOUNTY COMMAND GROUP ---
    bounty_group = app_commands.Group(name="bounty", description="Aria's completely unethical bounty system")

    @bounty_group.command(name="place", description="Place a coin bounty on a user's head")
    async def bounty_place(self, interaction: discord.Interaction, target: discord.Member, amount: int):
        if amount < 500:
            return await interaction.response.send_message("Don't insult me with pocket change. Minimum bounty is 500 coins.", ephemeral=True)
        if target == interaction.user:
            return await interaction.response.send_message("Putting a hit out on yourself? You really are desperate for attention.", ephemeral=True)

        bal = await self.get_balance(interaction.user.id)
        if bal < amount:
            return await interaction.response.send_message(f"You only have {bal} coins. You can't afford this assassination.", ephemeral=True)

        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Deduct the money
                    await cur.execute("UPDATE aria_economy SET balance = balance - %s WHERE user_id = %s", (amount, interaction.user.id))
                    # Check if bounty already exists, if so, add to it
                    await cur.execute("SELECT id, amount FROM aria_bounties WHERE target_id = %s", (target.id,))
                    existing = await cur.fetchone()
                    if existing:
                        await cur.execute("UPDATE aria_bounties SET amount = amount + %s WHERE id = %s", (amount, existing[0]))
                        total = existing[1] + amount
                    else:
                        await cur.execute("INSERT INTO aria_bounties (target_id, sponsor_id, amount) VALUES (%s, %s, %s)", (target.id, interaction.user.id, amount))
                        total = amount

        await interaction.response.send_message(f"🎯 **Bounty Registered.**\n{interaction.user.mention} just put money on {target.mention}'s head. The total bounty is now **{total} Aria Coins**.\n\n*Aria grins.* Let the games begin.")

    @bounty_group.command(name="board", description="View all active targets")
    async def bounty_board(self, interaction: discord.Interaction):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT target_id, amount FROM aria_bounties ORDER BY amount DESC")
                    bounties = await cur.fetchall()

        if not bounties:
            return await interaction.response.send_message("The board is empty. Wow, you people are boring today.", ephemeral=True)

        desc = ""
        for target_id, amount in bounties:
            target = interaction.guild.get_member(target_id)
            name = target.display_name if target else f"User {target_id}"
            desc += f"💀 **{name}** - 💰 {amount} Coins\n"

        embed = discord.Embed(title="📜 The Hit List", description=desc, color=discord.Color.dark_red())
        embed.set_footer(text="Use /bounty claim to attempt a roast and take the money.")
        await interaction.response.send_message(embed=embed)

    @bounty_group.command(name="claim", description="Attempt to claim a bounty by providing a devastating roast")
    async def bounty_claim(self, interaction: discord.Interaction, target: discord.Member, roast: str):
        if target == interaction.user:
            return await interaction.response.send_message("You cannot claim a bounty on yourself, fucking idiot.", ephemeral=True)

        await interaction.response.defer(ephemeral=False)

        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT id, amount FROM aria_bounties WHERE target_id = %s", (target.id,))
                    bounty = await cur.fetchone()

        if not bounty:
            return await interaction.followup.send(f"There is no bounty on {target.display_name}'s head. Don't waste your breath.")

        bounty_id, amount = bounty

        # Aria judges the roast
        prompt = f"The target is {target.display_name}. The roast is: \"{roast}\""
        try:
            response = client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
                config=types.GenerateContentConfig(system_instruction=EVALUATION_INSTRUCTION)
            )
            reply = response.text.strip().split("\n")
            score_str = reply[0]
            comment = "\n".join(reply[1:])
            
            # Extract the integer score
            score = int(''.join(filter(str.isdigit, score_str)))
        except Exception as e:
            logger.error(f"Bounty Judging Error: {e}")
            return await interaction.followup.send("My judging algorithms crashed listening to your voice. Try again.")

        # 7 or higher claims the bounty
        if score >= 7:
            async with aiomysql.create_pool(**DB_CONFIG) as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        # Give the money
                        await cur.execute("UPDATE aria_economy SET balance = balance + %s WHERE user_id = %s", (amount, interaction.user.id))
                        # Remove the bounty
                        await cur.execute("DELETE FROM aria_bounties WHERE id = %s", (bounty_id,))
                        
                        # Add a little negative energy to the vault!
                        await cur.execute("INSERT IGNORE INTO aria_vault (guild_id, energy_level) VALUES (%s, 0)", (interaction.guild.id,))
                        await cur.execute("UPDATE aria_vault SET energy_level = energy_level + 10 WHERE guild_id = %s", (interaction.guild.id,))

            embed = discord.Embed(title="🔪 Bounty Claimed!", color=discord.Color.green())
            embed.add_field(name="The Roast", value=f"\"{roast}\"", inline=False)
            embed.add_field(name="Aria's Judgment", value=f"**{score}/10**\n{comment}", inline=False)
            embed.set_footer(text=f"{interaction.user.display_name} won {amount} coins! Negative Energy added to the Vault.")
            
            await interaction.followup.send(f"{target.mention}, you just got destroyed.", embed=embed)
        
        else:
            embed = discord.Embed(title="❌ Bounty Failed", color=discord.Color.red())
            embed.add_field(name="The Attempt", value=f"\"{roast}\"", inline=False)
            embed.add_field(name="Aria's Judgment", value=f"**{score}/10**\n{comment}", inline=False)
            embed.set_footer(text="Aria demands a score of 7 or higher to pay out.")
            
            await interaction.followup.send(f"That was pathetic, {interaction.user.mention}. The bounty remains active.", embed=embed)

# This function tells the main bot.py how to load this file
async def setup(bot):
    await bot.add_cog(Bounty(bot))