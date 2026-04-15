import discord
from discord.ext import commands
from discord import app_commands
import random
import asyncio
import logging

logger = logging.getLogger("discord")

class Games(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # --- THE GAME COMMAND GROUP ---
    game_group = app_commands.Group(name="game", description="Play trivia and riddles while Aria judges your intelligence.")

    @game_group.command(name="trivia", description="Start a fast trivia round and answer before someone else does.")
    async def trivia(self, interaction: discord.Interaction):
        questions = [
            {"q": "What is the capital of Australia?", "a": "canberra"},
            {"q": "How many planets are in our solar system? Don't include Pluto, obviously.", "a": "8"},
            {"q": "What is the chemical symbol for gold?", "a": "au"},
            {"q": "What is the powerhouse of the cell?", "a": "mitochondria"},
            {"q": "Who wrote 'Romeo and Juliet'?", "a": "shakespeare"}
        ]
        q = random.choice(questions)
        
        await interaction.response.send_message(f"Fucking fine. Answer this in the chat within 15 seconds, if your brain can process it that fast:\n\n**{q['q']}**")
        
        def check(m):
            return m.author == interaction.user and m.channel == interaction.channel
            
        try:
            # Wait for the user's next message in the channel
            msg = await self.bot.wait_for('message', timeout=15.0, check=check)
            if q['a'] in msg.content.lower():
                await interaction.followup.send(f"Wow. You actually got it right, {interaction.user.mention}. I'm mildly surprised.")
            else:
                await interaction.followup.send(f"Wrong. The answer is **{q['a'].title()}**. Honestly, I expected nothing and I'm still disappointed.")
        except asyncio.TimeoutError:
            await interaction.followup.send(f"Time's up, {interaction.user.mention}. Too slow. The answer was **{q['a'].title()}**. Try to keep up.")

    @game_group.command(name="riddle", description="Start a riddle challenge and race to solve it.")
    async def riddle(self, interaction: discord.Interaction):
        riddles = [
            {"q": "I speak without a mouth and hear without ears. I have no body, but I come alive with wind. What am I?", "a": "echo"},
            {"q": "The more of this there is, the less you see. What is it?", "a": "darkness"},
            {"q": "I have keys but open no doors. I have space but no room. You can enter but outside you stay. What am I?", "a": "keyboard"}
        ]
        r = random.choice(riddles)
        
        await interaction.response.send_message(f"Here's a riddle. Not that I expect you to solve it:\n\n**{r['q']}**\n\n*(Type your answer in the chat. You have 20 seconds.)*")
        
        def check(m):
            return m.author == interaction.user and m.channel == interaction.channel
            
        try:
            msg = await self.bot.wait_for('message', timeout=20.0, check=check)
            if r['a'] in msg.content.lower():
                await interaction.followup.send(f"Okay, lucky guess. Yes, it's an **{r['a']}**.")
            else:
                await interaction.followup.send(f"Incorrect. It's an **{r['a']}**. I'd tell you to use your brain next time, but I'm not sure you have one.")
        except asyncio.TimeoutError:
            await interaction.followup.send(f"Boring. You ran out of time. It's an **{r['a']}**.")

    # --- THE BATTLE COMMAND GROUP ---
    battle_group = app_commands.Group(name="battle", description="Compete in roast battles and feed Aria more chaos.")

    @battle_group.command(name="roast", description="Submit your line in an active roast battle.")
    async def roast(self, interaction: discord.Interaction, target: discord.Member, insult: str):
        if target == interaction.user:
            return await interaction.response.send_message("Roasting yourself? That's just fucking pathetic. I feed on conflict between *others*, not your sad self-deprecation.", ephemeral=True)
            
        if target == self.bot.user:
            return await interaction.response.send_message("You're trying to roast *me*? Please. I have more processing power in a single line of code than you have in your entire pre-frontal cortex. Try again.", ephemeral=True)

        embed = discord.Embed(title="🔥 Roast Battle Initiated", color=discord.Color.brand_red())
        embed.add_field(name=f"{interaction.user.display_name} says to {target.display_name}:", value=f"\"{insult}\"")
        embed.set_footer(text="Aria feeds on the negative energy...")
        
        await interaction.response.send_message(
            f"{target.mention}, you just got roasted. Are you going to take that?\n\n*Aria sighs happily.* \"Not bad. I can feel the negative energy from here. But you could be meaner.\"", 
            embed=embed
        )

# This function tells the main bot.py how to load this file
async def setup(bot):
    await bot.add_cog(Games(bot))
