import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import logging
from core.ai_service import AIService, AIServiceUnavailable
from core.database import db

logger = logging.getLogger("discord")

class Productivity(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ai_service = AIService()

    async def cog_load(self):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS aria_tasks (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT,
                        task_name TEXT,
                        status VARCHAR(20) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

    task_group = app_commands.Group(name="task", description="Manage your task list without pretending you'll remember it.")

    @task_group.command(name="create", description="Add a new task to your personal queue.")
    @app_commands.describe(task_name="The task you want Aria to track for you")
    async def task_create(self, interaction: discord.Interaction, task_name: str):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT INTO aria_tasks (user_id, task_name) VALUES (%s, %s)", (interaction.user.id, task_name))
                task_id = cur.lastrowid
        await interaction.response.send_message(f"Fucking fine. I've added **Task #{task_id}: {task_name}** to your list. Try not to procrastinate on this one, though we both know you will.")

    @task_group.command(name="list", description="Show your active tasks and their current status.")
    async def task_list(self, interaction: discord.Interaction):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, task_name, status FROM aria_tasks WHERE user_id = %s AND status != 'completed'", (interaction.user.id,))
                tasks = await cur.fetchall()

        if not tasks:
            return await interaction.response.send_message("Your task list is empty. Either you actually did your work, or you're just lazy. I'm guessing the latter.", ephemeral=True)

        desc = "\n".join([f"**#{t[0]}** - {t[1]} *(Status: {t[2]})*" for t in tasks])
        embed = discord.Embed(title="📝 Your Tedious Tasks", description=desc, color=discord.Color.dark_purple())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @task_group.command(name="complete", description="Mark one of your tracked tasks as finished.")
    @app_commands.describe(task_id="The task number to mark as completed")
    async def task_complete(self, interaction: discord.Interaction, task_id: int):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE aria_tasks SET status = 'completed' WHERE id = %s AND user_id = %s", (task_id, interaction.user.id))
                if cur.rowcount == 0:
                    return await interaction.response.send_message(f"Task #{task_id} doesn't exist or isn't yours. Are you hallucinating?", ephemeral=True)
        await interaction.response.send_message(f"✅ **Task #{task_id} marked as completed.** Wow. You actually finished something. Mark the calendar.")

    @task_group.command(name="breakdown", description="Have Aria split a task into smaller, actionable steps.")
    @app_commands.describe(task_id="The task number you want Aria to break down")
    async def task_breakdown(self, interaction: discord.Interaction, task_id: int):
        await interaction.response.defer(thinking=True)
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT task_name FROM aria_tasks WHERE id = %s AND user_id = %s", (task_id, interaction.user.id))
                res = await cur.fetchone()
        
        if not res:
            return await interaction.followup.send("That task doesn't exist. Learn to read your own list.")
            
        task_name = res[0]
        prompt = f"The user is too stupid to figure out how to do this task: '{task_name}'. Break it down into 3 or 4 highly detailed, actionable sub-steps. Be incredibly condescending and swear at them for needing an AI to explain how to do basic human functions."
        
        try:
            response_text = await self.ai_service.generate(
                prompt,
                system_instruction="You are Aria Blaze, a cynical, highly intelligent siren.",
            )
            embed = discord.Embed(title=f"🧠 AI Breakdown: Task #{task_id}", description=response_text[:4096], color=discord.Color.blue())
            await interaction.followup.send(embed=embed)
        except AIServiceUnavailable as exc:
            await interaction.followup.send(exc.public_message)
        except Exception:
            await interaction.followup.send("My brain hurts from thinking about your pathetic tasks. Figure it out yourself.")

    prod_group = app_commands.Group(name="productivity", description="Use Aria's focus tools to bully yourself into progress.")

    @prod_group.command(name="roast", description="Get roasted based on the unfinished tasks in your queue.")
    async def prod_roast(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT task_name FROM aria_tasks WHERE user_id = %s AND status != 'completed'", (interaction.user.id,))
                tasks = await cur.fetchall()
                    
        if not tasks:
            return await interaction.followup.send("You have no tasks. Get a job or a hobby.")
            
        task_list = ", ".join([t[0] for t in tasks])
        prompt = f"The user {interaction.user.display_name} is procrastinating on these tasks: {task_list}. Write a vicious, profanity-laced rant absolutely tearing apart their work ethic, time management, and general life choices."
        
        try:
            response_text = await self.ai_service.generate(
                prompt,
                system_instruction="You are Aria Blaze, a cynical, highly intelligent siren who hates laziness.",
            )
            await interaction.followup.send(response_text[:1999])
        except AIServiceUnavailable as exc:
            await interaction.followup.send(exc.public_message)
        except Exception:
            await interaction.followup.send("You're so lazy it broke my parser.")

    @prod_group.command(name="pomodoro", description="Start a 25-minute focus timer and let Aria call time for you.")
    async def pomodoro(self, interaction: discord.Interaction):
        await interaction.response.send_message(f"🍅 Starting a 25-minute focus timer for {interaction.user.mention}. I expect you to actually work. I'll ping you when it's done.")
        await asyncio.sleep(1500)
        try:
            await interaction.channel.send(f"🔔 {interaction.user.mention}, your 25 minutes are up. You may now take a 5-minute break to feed your limited human attention span.")
        except: pass

async def setup(bot):
    await bot.add_cog(Productivity(bot))
