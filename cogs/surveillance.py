import asyncio
import discord
from discord.ext import commands
from discord import app_commands
from google import genai
from google.genai import types
import aiomysql
import logging
import os

logger = logging.getLogger("discord")

DB_CONFIG = {
    'host': '127.0.0.1', 'user': 'botuser', 'password': 'swarmpanel', 'db': 'discord_aria', 'autocommit': True
}

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'AIzaSyBe-PsYYalYB4Tum-vCmqj-N9m6MsfTL2k')
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = 'gemini-2.5-flash'

SYSTEM_INSTRUCTION = """
You are Aria Blaze, a cynical, highly sarcastic, and easily annoyed siren trapped as a Discord AI. 
Be incredibly condescending, use heavy profanity (fuck, shit), and act like reading this was a waste of your immortal time.
"""

class Surveillance(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    surveillance_group = app_commands.Group(name="surveillance", description="Let Aria inspect chat activity and call people out.")

    @surveillance_group.command(name="summarize", description="Summarize recent channel messages with Aria's sarcastic commentary.")
    async def summarize(self, interaction: discord.Interaction, limit: int = 100):
        limit = min(200, max(10, limit))
        await interaction.response.defer(ephemeral=False)
        
        try:
            messages = [msg async for msg in interaction.channel.history(limit=limit)]
            messages.reverse()
            if not messages:
                return await interaction.followup.send("The channel is literally empty. Are you hallucinating?")

            chat_log = "\n".join([f"{msg.author.display_name}: {msg.content}" for msg in messages if msg.content])
            if len(chat_log) > 50000: chat_log = chat_log[-50000:] 

            prompt = f"Here is the chat log. Summarize the main topics, call out specific users who said stupid things, and roast them:\n\n{chat_log}"
            
            response = await asyncio.get_event_loop().run_in_executor(None, lambda: client.models.generate_content(model=MODEL_ID, contents=prompt, config=types.GenerateContentConfig(system_instruction=SYSTEM_INSTRUCTION)))
            
            embed = discord.Embed(title="📜 Aria's Channel Summary", description=response.text[:4096], color=discord.Color.dark_teal())
            await interaction.followup.send(embed=embed)
        except Exception as e:
            logger.error(f"Aria Summary Error: {e}")
            await interaction.followup.send("My parser broke trying to read this channel. You all type like toddlers.")

    @surveillance_group.command(name="investigate", description="Analyze one member's recent messages and have Aria profile them.")
    async def investigate(self, interaction: discord.Interaction, target: discord.Member):
        await interaction.response.defer(ephemeral=False)
        
        if target.bot:
            return await interaction.followup.send("I don't spy on my own kind.")
            
        try:
            # Fetch the last 200 messages in the channel, but ONLY keep the ones from the target
            messages = [msg.content async for msg in interaction.channel.history(limit=200) if msg.author.id == target.id and msg.content]
            messages.reverse()
            
            if not messages:
                return await interaction.followup.send(f"{target.display_name} hasn't said anything in the last 200 messages. They are completely irrelevant.")

            chat_log = "\n".join(messages)
            if len(chat_log) > 50000: chat_log = chat_log[-50000:] 

            prompt = f"I am investigating the human '{target.display_name}'. Here are their most recent quotes in the chat:\n\n{chat_log}\n\nRead their quotes. Write a brutal, profanity-laced evaluation of their personality, communication style, and intelligence based purely on what they've been saying."
            
            response = await asyncio.get_event_loop().run_in_executor(None, lambda: client.models.generate_content(model=MODEL_ID, contents=prompt, config=types.GenerateContentConfig(system_instruction=SYSTEM_INSTRUCTION)))
            
            embed = discord.Embed(title=f"🕵️ Surveillance Log: {target.display_name}", description=response.text[:4096], color=discord.Color.red())
            embed.set_footer(text=f"Aria analyzed their last {len(messages)} messages.")
            await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send("They talk so much nonsense my AI refused to read it.")

    @surveillance_group.command(name="wall_of_shame", description="Show the members with the worst backlog of unfinished tasks.")
    async def wall_of_shame(self, interaction: discord.Interaction):
        await interaction.response.defer()
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT user_id, COUNT(*) as pending_count FROM aria_tasks WHERE status != 'completed' GROUP BY user_id ORDER BY pending_count DESC LIMIT 5")
                    worst_users = await cur.fetchall()

        if not worst_users:
            return await interaction.followup.send("Miraculously, there are no pending tasks right now.")

        embed = discord.Embed(title="🚨 The Wall of Shame 🚨", description="Behold, the absolute worst procrastinators in this server.", color=discord.Color.red())
        for index, (user_id, count) in enumerate(worst_users):
            user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
            name = user.display_name if user else f"Unknown Slacker ({user_id})"
            medal = "🏆 (Absolute Worst)" if index == 0 else f"#{index + 1}"
            embed.add_field(name=f"{medal}: {name}", value=f"**{count} pending tasks.** Pathetic.", inline=False)
            
        await interaction.followup.send(embed=embed)

async def setup(bot):
    await bot.add_cog(Surveillance(bot))
