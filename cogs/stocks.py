import discord
from discord.ext import commands, tasks
from discord import app_commands
import logging
import random
from core.database import db

logger = logging.getLogger("discord")

class Stocks(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.market_loop.start()

    def cog_unload(self):
        self.market_loop.cancel()

    # --- DATABASE INIT & SEEDING ---
    async def cog_load(self):
        if not db.pool:
            logger.warning("stocks: database pool unavailable; table init skipped.")
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS aria_stocks (
                        symbol VARCHAR(10) PRIMARY KEY,
                        name VARCHAR(255),
                        price INT,
                        previous_price INT
                    )
                """)
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS aria_portfolio (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id BIGINT,
                        symbol VARCHAR(10),
                        shares INT
                    )
                """)

                await cur.execute("SELECT COUNT(*) FROM aria_stocks")
                count = (await cur.fetchone())[0]
                if count == 0:
                    initial_stocks = [
                        ("DZLG", "Dazzlings Inc.", 500, 500),
                        ("TACO", "Sonata's Taco Stand", 50, 50),
                        ("RAIN", "Rainbooms Tears LLC", 150, 150),
                        ("EGM", "Equestrian Magic Reserves", 1000, 1000)
                    ]
                    await cur.executemany("INSERT INTO aria_stocks (symbol, name, price, previous_price) VALUES (%s, %s, %s, %s)", initial_stocks)

    # --- BACKGROUND MARKET FLUCTUATION ---
    @tasks.loop(minutes=15.0)
    async def market_loop(self):
        # Fluctuate prices every 15 minutes
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT symbol, price FROM aria_stocks")
                stocks = await cur.fetchall()
                for symbol, current_price in stocks:
                    change_percent = random.uniform(-0.25, 0.30)
                    new_price = max(5, int(current_price + (current_price * change_percent)))
                    await cur.execute("UPDATE aria_stocks SET previous_price = %s, price = %s WHERE symbol = %s", (current_price, new_price, symbol))
                    await conn.commit()

    @market_loop.before_loop
    async def before_market_loop(self):
        await self.bot.wait_until_ready()

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

    # --- THE STOCK COMMAND GROUP ---
    stock_group = app_commands.Group(name="stock", description="Buy, sell, and review Aria's volatile stock market.")

    @stock_group.command(name="market", description="View the latest market prices for every listed stock.")
    async def stock_market(self, interaction: discord.Interaction):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT symbol, name, price, previous_price FROM aria_stocks")
                stocks = await cur.fetchall()

        desc = ""
        for symbol, name, price, prev in stocks:
            trend = "📈" if price > prev else "📉" if price < prev else "➖"
            diff = price - prev
            sign = "+" if diff > 0 else ""
            desc += f"**{symbol}** ({name})\n> {trend} **{price} Coins** *( {sign}{diff} )*\n\n"

        embed = discord.Embed(title="📈 The Siren's Stock Market", description=f"Prices update every 15 minutes.\n\n{desc}", color=discord.Color.dark_green())
        embed.set_footer(text="Invest at your own risk. I will laugh if you lose it all.")
        await interaction.response.send_message(embed=embed)

    @stock_group.command(name="portfolio", description="Review the shares and positions you currently hold.")
    async def stock_portfolio(self, interaction: discord.Interaction):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT p.symbol, p.shares, s.price 
                    FROM aria_portfolio p 
                    JOIN aria_stocks s ON p.symbol = s.symbol 
                    WHERE p.user_id = %s AND p.shares > 0
                """, (interaction.user.id,))
                holdings = await cur.fetchall()

        if not holdings:
            return await interaction.response.send_message("Your portfolio is completely empty. How incredibly boring.", ephemeral=True)

        desc = ""
        total_value = 0
        for symbol, shares, price in holdings:
            val = shares * price
            total_value += val
            desc += f"**{symbol}**: {shares} shares *(Value: {val} Coins)*\n"

        embed = discord.Embed(title=f"💼 {interaction.user.display_name}'s Portfolio", description=desc, color=discord.Color.blue())
        embed.add_field(name="Total Market Value", value=f"**{total_value} Coins**")
        await interaction.response.send_message(embed=embed)

    @stock_group.command(name="buy", description="Buy shares of a stock using your Aria Coins.")
    async def stock_buy(self, interaction: discord.Interaction, symbol: str, shares: int):
        symbol = symbol.upper()
        if shares <= 0:
            return await interaction.response.send_message("You can't buy zero or negative shares. Math isn't your strong suit, is it?", ephemeral=True)

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT price, name FROM aria_stocks WHERE symbol = %s", (symbol,))
                stock = await cur.fetchone()
                
                if not stock:
                    return await interaction.response.send_message(f"'{symbol}' is not a valid stock. Stop making things up.", ephemeral=True)
                
                price, name = stock
                total_cost = price * shares
                await cur.execute("INSERT IGNORE INTO aria_economy (user_id, balance) VALUES (%s, 0)", (interaction.user.id,))
                await cur.execute("SELECT balance FROM aria_economy WHERE user_id = %s", (interaction.user.id,))
                bal_row = await cur.fetchone()
                bal = bal_row[0] if bal_row else 0
                
                if bal < total_cost:
                    return await interaction.response.send_message(f"That costs **{total_cost} coins**. You only have **{bal}**. Typical.", ephemeral=True)

                await cur.execute("UPDATE aria_economy SET balance = balance - %s WHERE user_id = %s", (total_cost, interaction.user.id))
                await cur.execute("SELECT id FROM aria_portfolio WHERE user_id = %s AND symbol = %s", (interaction.user.id, symbol))
                existing = await cur.fetchone()
                if existing:
                    await cur.execute("UPDATE aria_portfolio SET shares = shares + %s WHERE id = %s", (shares, existing[0]))
                else:
                    await cur.execute("INSERT INTO aria_portfolio (user_id, symbol, shares) VALUES (%s, %s, %s)", (interaction.user.id, symbol, shares))

        await interaction.response.send_message(f"Fucking fine. You just bought **{shares} shares** of {name} for **{total_cost} coins**. I hope it crashes immediately.")

    @stock_group.command(name="sell", description="Sell shares from your portfolio back into the market.")
    async def stock_sell(self, interaction: discord.Interaction, symbol: str, shares: int):
        symbol = symbol.upper()
        if shares <= 0:
            return await interaction.response.send_message("You can't sell zero shares.", ephemeral=True)

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT id, shares FROM aria_portfolio WHERE user_id = %s AND symbol = %s", (interaction.user.id, symbol))
                holding = await cur.fetchone()
                
                if not holding or holding[1] < shares:
                    return await interaction.response.send_message(f"You don't even own {shares} shares of {symbol}. You can't short-sell here.", ephemeral=True)
                    
                await cur.execute("SELECT price FROM aria_stocks WHERE symbol = %s", (symbol,))
                price = (await cur.fetchone())[0]
                
                total_gained = price * shares
                await cur.execute("UPDATE aria_portfolio SET shares = shares - %s WHERE id = %s", (shares, holding[0]))
                await cur.execute("INSERT IGNORE INTO aria_economy (user_id, balance) VALUES (%s, 0)", (interaction.user.id,))
                await cur.execute("UPDATE aria_economy SET balance = balance + %s WHERE user_id = %s", (total_gained, interaction.user.id))

        await interaction.response.send_message(f"You sold **{shares} shares** of {symbol} and walked away with **{total_gained} coins**. Try not to spend it all on `/economy shop` absolute shit.")

# This function tells the main bot.py how to load this file
async def setup(bot):
    await bot.add_cog(Stocks(bot))
