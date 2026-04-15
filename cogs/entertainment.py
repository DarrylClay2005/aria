import discord
from discord.ext import commands
from discord import app_commands
import aiomysql
import logging
import random
import asyncio
import os
from google import genai
from google.genai import types
from core.database import db

logger = logging.getLogger("discord")

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'AIzaSyBe-PsYYalYB4Tum-vCmqj-N9m6MsfTL2k')
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = 'gemini-2.5-flash'

JUDGE_INSTRUCTION = """
You are Aria Blaze, a cynical, highly sarcastic siren acting as a debate judge. 
Two humans are debating a ridiculous topic. 
You must read both of their arguments, ruthlessly mock their logic, and definitively declare one of them the WINNER based on who was more convincing (or just who annoyed you less).
Keep it under 3 paragraphs. End your response by clearly stating: "WINNER: [Name]"
"""

class Entertainment(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # Helper function to get/update balance
    async def update_balance(self, user_id: int, amount: int):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT IGNORE INTO aria_economy (user_id, balance) VALUES (%s, 0)", (user_id,))
                await cur.execute("UPDATE aria_economy SET balance = balance + %s WHERE user_id = %s", (amount, user_id))

    # --- THE SING-OFF COMMAND ---
    @app_commands.command(name="game_sing", description="Finish a lyric prompt before time runs out and earn or lose coins.")
    async def sing(self, interaction: discord.Interaction):
        lyrics = [
            {"prompt": "Is this the real life? Is this just fantasy?", "answer": "caught in a landslide"},
            {"prompt": "Just a small town girl, livin' in a lonely world...", "answer": "took the midnight train"},
            {"prompt": "Somebody once told me the world is gonna roll me...", "answer": "i ain't the sharpest tool"},
            {"prompt": "We will, we will...", "answer": "rock you"},
            {"prompt": "Welcome to the grand illusion...", "answer": "come on in and see what's happening"},
            # Aria Blaze Lore Easter Egg!
            {"prompt": "We're the Dazzlings, and we're here to...", "answer": "sing a song"} 
        ]
        
        song = random.choice(lyrics)
        
        await interaction.response.send_message(f"Let's see if you have any cultural value. Finish the lyric. You have 15 seconds.\n\n🎶 **\"{song['prompt']}\"** 🎶")
        
        def check(m):
            return m.author == interaction.user and m.channel == interaction.channel
            
        try:
            msg = await self.bot.wait_for('message', timeout=15.0, check=check)
            if song['answer'] in msg.content.lower():
                await self.update_balance(interaction.user.id, 100)
                await interaction.followup.send(f"Not completely tone-deaf, I see. Correct. I've tossed **100 coins** your way.")
            else:
                await self.update_balance(interaction.user.id, -50)
                await interaction.followup.send(f"Fucking tragic. Your musical taste is as offensive as your voice. The next line is '{song['answer'].title()}'. I'm fining you **50 coins** for hurting my ears.")
        except asyncio.TimeoutError:
            await interaction.followup.send(f"Too slow! Did you choke on the melody? The answer was '{song['answer'].title()}'.")


    # --- THE DEBATE CLUB COMMAND ---
    @app_commands.command(name="battle_debate", description="Challenge another member to a timed debate and let Aria judge the winner.")
    async def debate(self, interaction: discord.Interaction, opponent: discord.Member):
        if opponent == interaction.user or opponent.bot:
            return await interaction.response.send_message("You can only debate another human. Try making some friends.", ephemeral=True)

        topics = [
            "Are hotdogs considered sandwiches?",
            "Is water actually wet?",
            "Which is superior: Tacos or Burgers? (Be careful, Sonata loves Tacos).",
            "Is cereal just cold soup?",
            "Should pineapple be allowed on pizza?"
        ]
        topic = random.choice(topics)

        await interaction.response.send_message(
            f"⚖️ **DEBATE CLUB INITIATED** ⚖️\n"
            f"{interaction.user.mention} has challenged {opponent.mention}!\n\n"
            f"**The Topic:** \"{topic}\"\n\n"
            f"You both have exactly **60 seconds** to type your best argument into the chat. Make it quick, I don't have all day."
        )

        args_collected = {}

        def check(m):
            # Collect messages from either the challenger or the opponent in this channel
            if m.channel == interaction.channel and m.author.id in [interaction.user.id, opponent.id]:
                # Only take their FIRST message
                if m.author.id not in args_collected:
                    args_collected[m.author.id] = m.content
                    return True
            return False

        # Wait for both users to submit an argument, or 60 seconds to pass
        try:
            while len(args_collected) < 2:
                await self.bot.wait_for('message', timeout=60.0, check=check)
        except asyncio.TimeoutError:
            pass # 60 seconds are up!

        if len(args_collected) < 2:
            return await interaction.followup.send("One or both of you failed to speak up in time. Debate cancelled due to overwhelming cowardice.")

        await interaction.followup.send("*Processing your dreadful arguments...*")

        # Format the prompt for Gemini
        p1_arg = args_collected[interaction.user.id]
        p2_arg = args_collected[opponent.id]
        
        prompt = (
            f"The topic is: {topic}\n\n"
            f"Argument from {interaction.user.display_name}: \"{p1_arg}\"\n\n"
            f"Argument from {opponent.display_name}: \"{p2_arg}\"\n\n"
            f"Judge them ruthlessly and declare a winner, swearing at them if their arguments are fucking stupid."
        )

        try:
            # Aria's brain judges the debate
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.models.generate_content(
                    model=MODEL_ID,
                    contents=prompt,
                    config=types.GenerateContentConfig(system_instruction=JUDGE_INSTRUCTION)
                )
            )
            
            judgment = response.text
            
            embed = discord.Embed(title="👩‍⚖️ Aria's Final Judgment", description=judgment[:4096], color=discord.Color.gold())
            
            # Determine the winner to give out the prize
            if f"WINNER: {interaction.user.display_name}".lower() in judgment.lower():
                await self.update_balance(interaction.user.id, 500)
                embed.set_footer(text=f"{interaction.user.display_name} has been awarded 500 Aria Coins!")
            elif f"WINNER: {opponent.display_name}".lower() in judgment.lower():
                await self.update_balance(opponent.id, 500)
                embed.set_footer(text=f"{opponent.display_name} has been awarded 500 Aria Coins!")
            else:
                embed.set_footer(text="Aria hated both arguments so much she refused to pay out.")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Debate Judging Error: {e}")
            await interaction.followup.send("My judging algorithms crashed because your arguments were so incredibly flawed. Nobody wins.")


# This function tells the main bot.py how to load this file
async def setup(bot):
    await bot.add_cog(Entertainment(bot))
