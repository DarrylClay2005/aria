import discord
from discord.ext import commands
from discord import app_commands
from google import genai
from google.genai import types
import aiomysql
import logging
import asyncio
import urllib.parse

logger = logging.getLogger("discord")
DB_CONFIG = {'host': '127.0.0.1', 'user': 'botuser', 'password': 'botpassword', 'db': 'discord_aria', 'autocommit': True}
GEMINI_API_KEY = 'AIzaSyBe-PsYYalYB4Tum-vCmqj-N9m6MsfTL2k'
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = 'gemini-2.5-flash'

class AITools(commands.Cog):
    def __init__(self, bot): 
        self.bot = bot

    async def alter_sanity(self, user_id: int, amount: int):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (user_id,))
                    await cur.execute("UPDATE aria_sanity SET sanity_level = LEAST(100, GREATEST(0, sanity_level + %s)) WHERE user_id = %s", (amount, user_id))

    async def get_affinity(self, user_id: int) -> int:
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT score FROM aria_affinity WHERE user_id = %s", (user_id,))
                    res = await cur.fetchone()
                    return res[0] if res else 0

    def get_system_instruction(self, score: int, user_name: str, mode: str = "default") -> str:
        base = f"You are Aria Blaze. Talking to '{user_name}'. Affinity: {score}/100. "
        if score >= 80: return base + "Be incredibly sweet and protective."
        elif score >= 50: return base + "Playfully tease them (Tsundere)."
        else: return base + "Be toxic, swear heavily, and condescend."

    async def send_paginated(self, interaction, text: str):
        if len(text) <= 1990: await interaction.followup.send(text)
        else:
            chunks = [text[i:i+1990] for i in range(0, len(text), 1990)]
            await interaction.followup.send(chunks[0])
            for chunk in chunks[1:]: await interaction.channel.send(chunk)

    problem_group = app_commands.Group(name="problem", description="Toxic problem-solving")

    @problem_group.command(name="lmgtfy", description="Ask Aria a question. Stupid questions inflict Sanity damage.")
    async def lmgtfy(self, interaction: discord.Interaction, question: str):
        await interaction.response.defer(thinking=True)
        try:
            # FIXED: Made synchronous Gemini call async to prevent bot freeze
            eval_res = await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: client.models.generate_content(
                    model=MODEL_ID, 
                    contents=f"Rate 'stupidity' of question 1-10: '{question}'. Respond ONLY with integer."
                )
            )
            stupidity = int(''.join(filter(str.isdigit, eval_res.text)))
        except: 
            stupidity = 5

        if stupidity >= 8:
            await self.alter_sanity(interaction.user.id, -10)
            link = f"https://letmegooglethat.com/?q={urllib.parse.quote_plus(question)}"
            await interaction.followup.send(f"Are you kidding me? A literal infant could google that. I have inflicted **10% Sanity Damage** for wasting my time: {link}")
        else:
            score = await self.get_affinity(interaction.user.id)
            system_inst = self.get_system_instruction(score, interaction.user.display_name)
            
            # FIXED: Made synchronous Gemini call async
            res = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.models.generate_content(
                    model=MODEL_ID, 
                    contents=question, 
                    config=types.GenerateContentConfig(system_instruction=system_inst)
                )
            )
            await self.send_paginated(interaction, res.text)

    @problem_group.command(name="socratic_torture", description="Pop quiz before she helps")
    async def socratic_torture(self, interaction: discord.Interaction, question: str):
        await interaction.response.defer(ephemeral=False)
        
        # FIXED: Made synchronous Gemini call async
        test_q_res = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: client.models.generate_content(
                model=MODEL_ID, 
                contents=f"Generate 1 prerequisite test question for: '{question}'"
            )
        )
        test_q = test_q_res.text.strip()
        await interaction.followup.send(f"Answer this in 60 seconds first:\n\n**{test_q}**")
        
        try:
            msg = await self.bot.wait_for('message', timeout=60.0, check=lambda m: m.channel == interaction.channel and m.author.id == interaction.user.id)
            
            # FIXED: Made synchronous Gemini call async
            eval_res = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.models.generate_content(
                    model=MODEL_ID, 
                    contents=f"Q: '{test_q}'. A: '{msg.content}'. Correct? 'True' or 'False'."
                )
            )
            
            if "true" in eval_res.text.lower():
                await interaction.channel.send("Close enough. Generating answer...")
                
                score = await self.get_affinity(interaction.user.id)
                system_inst = self.get_system_instruction(score, interaction.user.display_name)
                
                # FIXED: Made synchronous Gemini call async
                res = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: client.models.generate_content(
                        model=MODEL_ID, 
                        contents=question, 
                        config=types.GenerateContentConfig(system_instruction=system_inst)
                    )
                )
                await self.send_paginated(interaction, res.text)
            else:
                await self.alter_sanity(interaction.user.id, -15)
                await interaction.channel.send("WRONG. I've inflicted 15% Sanity Damage. Figure it out yourself.")
        except asyncio.TimeoutError:
            await interaction.channel.send("Time's up, idiot.")

async def setup(bot): 
    await bot.add_cog(AITools(bot))