import discord
from discord.ext import commands
from discord import app_commands
import aiomysql
import logging
import asyncio
import random

logger = logging.getLogger("discord")

DB_CONFIG = {
    'host': '127.0.0.1', 'user': 'botuser', 'password': 'swarmpanel', 'db': 'discord_aria', 'autocommit': True
}

class PvP(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # --- DATABASE INIT ---
    async def cog_load(self):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Table for 1v1 Battles
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS aria_battles (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            challenger_id BIGINT,
                            target_id BIGINT,
                            battle_type VARCHAR(50),
                            status VARCHAR(20) DEFAULT 'pending'
                        )
                    """)
                    # Tables for Tournaments
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS aria_tournaments (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            guild_id BIGINT,
                            name VARCHAR(255),
                            type VARCHAR(50),
                            status VARCHAR(20) DEFAULT 'open'
                        )
                    """)
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS aria_tournament_players (
                            tournament_id INT,
                            user_id BIGINT,
                            PRIMARY KEY (tournament_id, user_id)
                        )
                    """)

    # --- HELPER FUNCTIONS ---
    async def update_balance(self, user_id: int, amount: int):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_economy (user_id, balance) VALUES (%s, 0)", (user_id,))
                    await cur.execute("UPDATE aria_economy SET balance = balance + %s WHERE user_id = %s", (amount, user_id))

    # --- THE BATTLE COMMAND GROUP ---
    duel_group = app_commands.Group(name="duel", description="Challenge other members to one-on-one battles.")

    @duel_group.command(name="challenge", description="Challenge another member to a duel type Aria supports.")
    @app_commands.choices(battle_type=[
        app_commands.Choice(name="Trivia Duel", value="trivia")
    ])
    async def battle_challenge(self, interaction: discord.Interaction, target: discord.Member, battle_type: str):
        if target == interaction.user or target.bot:
            return await interaction.response.send_message("You can only challenge other living humans. Try to find one.", ephemeral=True)

        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT INTO aria_battles (challenger_id, target_id, battle_type) VALUES (%s, %s, %s)", (interaction.user.id, target.id, battle_type))
                    
        await interaction.response.send_message(f"⚔️ **CHALLENGE ISSUED!** ⚔️\n\n{interaction.user.mention} has challenged {target.mention} to a **{battle_type.title()} Duel**.\n{target.display_name}, you have 60 seconds to run `/battle accept` before I assume you are a little bitch.")

    @duel_group.command(name="accept", description="Accept a pending duel challenge aimed at you.")
    async def battle_accept(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT id, challenger_id, battle_type FROM aria_battles WHERE target_id = %s AND status = 'pending' LIMIT 1", (interaction.user.id,))
                    battle = await cur.fetchone()
                    
                    if not battle:
                        return await interaction.followup.send("You have no pending challenges. Nobody cares enough to fight you.")
                        
                    battle_id, challenger_id, battle_type = battle
                    await cur.execute("UPDATE aria_battles SET status = 'active' WHERE id = %s", (battle_id,))

        challenger = interaction.guild.get_member(challenger_id)
        if not challenger:
            return await interaction.followup.send("Your challenger left the server. Battle canceled.")

        if battle_type == "trivia":
            questions = [
                {"q": "What is the largest organ of the human body?", "a": "skin"},
                {"q": "What planet is closest to the sun?", "a": "mercury"},
                {"q": "Who painted the Mona Lisa?", "a": "da vinci"},
                {"q": "What is the chemical symbol for iron?", "a": "fe"}
            ]
            q = random.choice(questions)
            
            await interaction.followup.send(f"⚔️ **THE DUEL BEGINS** ⚔️\n{challenger.mention} VS {interaction.user.mention}\n\nFirst to type the correct answer wins 500 Coins.\n**Question:** {q['q']}")
            
            def check(m):
                return m.channel == interaction.channel and m.author.id in [challenger.id, interaction.user.id] and q['a'] in m.content.lower()
                
            try:
                msg = await self.bot.wait_for('message', timeout=30.0, check=check)
                winner = msg.author
                loser = challenger if winner.id == interaction.user.id else interaction.user
                
                await self.update_balance(winner.id, 500)
                await interaction.channel.send(f"🏆 **{winner.mention} WINS!** They answered correctly and took 500 Coins.\n{loser.mention}, your brain is officially slower. Pathetic.")
            except asyncio.TimeoutError:
                await interaction.channel.send(f"Time's up! The answer was **{q['a']}**. You both failed. I am taking 100 coins from both of you for wasting my time.")
                await self.update_balance(challenger.id, -100)
                await self.update_balance(interaction.user.id, -100)

        # Mark battle as finished
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("UPDATE aria_battles SET status = 'finished' WHERE id = %s", (battle_id,))

    # --- THE TOURNAMENT COMMAND GROUP ---
    tourney_group = app_commands.Group(name="tournament", description="Create, join, and track server tournaments.")

    @tourney_group.command(name="create", description="[ADMIN] Open registration for a new tournament.")
    @app_commands.default_permissions(administrator=True)
    async def t_create(self, interaction: discord.Interaction, name: str, tournament_type: str):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT INTO aria_tournaments (guild_id, name, type) VALUES (%s, %s, %s)", (interaction.guild.id, name, tournament_type))
                    t_id = cur.lastrowid
                    
        await interaction.response.send_message(f"🏆 **Tournament Created!**\n\n**Name:** {name}\n**Type:** {tournament_type}\n**ID:** {t_id}\n\nPlayers can now use `/tournament join {t_id}` to register.")

    @tourney_group.command(name="join", description="Join a tournament that is currently accepting players.")
    async def t_join(self, interaction: discord.Interaction, tournament_id: int):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT name, status FROM aria_tournaments WHERE id = %s AND guild_id = %s", (tournament_id, interaction.guild.id))
                    tourney = await cur.fetchone()
                    
                    if not tourney:
                        return await interaction.response.send_message("That tournament ID doesn't exist.", ephemeral=True)
                    if tourney[1] != 'open':
                        return await interaction.response.send_message("Registration for that tournament is closed.", ephemeral=True)
                        
                    try:
                        await cur.execute("INSERT INTO aria_tournament_players (tournament_id, user_id) VALUES (%s, %s)", (tournament_id, interaction.user.id))
                        await interaction.response.send_message(f"You have registered for **{tourney[0]}**. Try not to embarrass yourself in the first round.")
                    except aiomysql.IntegrityError:
                        await interaction.response.send_message("You are already registered, genius.", ephemeral=True)

    @tourney_group.command(name="status", description="View the current roster and status of a tournament.")
    async def t_status(self, interaction: discord.Interaction, tournament_id: int):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT name, status FROM aria_tournaments WHERE id = %s AND guild_id = %s", (tournament_id, interaction.guild.id))
                    tourney = await cur.fetchone()
                    
                    if not tourney:
                        return await interaction.response.send_message("Invalid tournament ID.", ephemeral=True)
                        
                    await cur.execute("SELECT user_id FROM aria_tournament_players WHERE tournament_id = %s", (tournament_id,))
                    players = await cur.fetchall()

        embed = discord.Embed(title=f"🏆 Tournament: {tourney[0]}", description=f"Status: **{tourney[1].upper()}**", color=discord.Color.gold())
        
        if players:
            roster = ""
            for (uid,) in players:
                user = interaction.guild.get_member(uid)
                name = user.display_name if user else f"User {uid}"
                roster += f"⚔️ {name}\n"
            embed.add_field(name=f"Registered Players ({len(players)})", value=roster)
        else:
            embed.add_field(name="Registered Players", value="Nobody has registered yet. I don't blame them.")

        await interaction.response.send_message(embed=embed)

async def setup(bot):
    await bot.add_cog(PvP(bot))
