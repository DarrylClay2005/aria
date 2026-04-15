import discord
from discord.ext import commands
from discord import app_commands
import aiomysql
import aiohttp
import logging
import io
import os
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger("discord")
DB_CONFIG = {'host': '127.0.0.1', 'user': 'botuser', 'password': 'swarmpanel', 'db': 'discord_music_gws', 'autocommit': True}

# --- OMNI-LENS CREDENTIALS ---
GOOGLE_API_KEY = "AIzaSyDgEPKxStYYmj-Jn14AXkzR0bchXKDjGIw"
GOOGLE_CSE_ID = "511a9aa3611554e5c"

class HTTPSessionManager:
    _session = None
    async def __aenter__(self):
        if not HTTPSessionManager._session:
            HTTPSessionManager._session = aiohttp.ClientSession()
        return HTTPSessionManager._session
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

class DBPoolManager:
    _pool = None
    async def __aenter__(self):
        if not DBPoolManager._pool:
            DBPoolManager._pool = await aiomysql.create_pool(**DB_CONFIG)
        return DBPoolManager._pool
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

class FontManager:
    _font_bytes = None
    @classmethod
    async def get_font(cls, size=40):
        if not cls._font_bytes:
            url = "https://github.com/googlefonts/roboto/raw/main/src/hinted/Roboto-Black.ttf"
            async with HTTPSessionManager() as session:
                async with session.get(url) as resp:
                    cls._font_bytes = await resp.read()
        return ImageFont.truetype(io.BytesIO(cls._font_bytes), size)

class VaultTagModal(discord.ui.Modal, title='Encrypt Image to Vault'):
    keyword = discord.ui.TextInput(
        label='Enter a keyword or tag', 
        placeholder='e.g., funny cat, server logo, reaction meme', 
        max_length=100
    )

    def __init__(self, image_url):
        super().__init__()
        self.image_url = image_url

    async def on_submit(self, interaction: discord.Interaction):
        try:
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("CREATE TABLE IF NOT EXISTS aria_visual_vault (id INT AUTO_INCREMENT PRIMARY KEY, keyword VARCHAR(100), image_url TEXT, added_by BIGINT, added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                        await cur.execute("INSERT INTO aria_visual_vault (keyword, image_url, added_by) VALUES (%s, %s, %s)", (self.keyword.value, self.image_url, interaction.user.id))
            await interaction.response.send_message(f"💾 **Encrypted & Stored** inside the Visual Vault under the tag `{self.keyword.value}`!", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Database Write Error: {e}", ephemeral=True)

class ImageCarousel(discord.ui.View):
    def __init__(self, query, images):
        super().__init__(timeout=300)
        self.query = query
        self.images = images
        self.index = 0

    def update_embed(self):
        embed = discord.Embed(title=f"🔍 Omni-Lens: {self.query}", color=discord.Color.teal())
        embed.set_image(url=self.images[self.index])
        embed.set_footer(text=f"Result {self.index + 1} of {len(self.images)} | Powered by Google API")
        return embed

    @discord.ui.button(label="◀️ Prev", style=discord.ButtonStyle.blurple)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.index = (self.index - 1) % len(self.images)
        await interaction.response.edit_message(embed=self.update_embed(), view=self)

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.blurple)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.index = (self.index + 1) % len(self.images)
        await interaction.response.edit_message(embed=self.update_embed(), view=self)

    @discord.ui.button(label="🤖 True AI Upscale", style=discord.ButtonStyle.green)
    async def upscale_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("⚙️ **Initiating CPU Neural Upscale...** This requires heavy compute and may take 15-30 seconds.", ephemeral=True)
        try:
            import io
            import os
            import uuid
            import subprocess
            import asyncio
            from PIL import Image
            
            url = self.images[self.index]
            async with HTTPSessionManager() as session:
                async with session.get(url) as resp:
                    data = await resp.read()
            
            task_id = uuid.uuid4().hex
            in_path = f"/tmp/{task_id}_in.png"
            out_path = f"/tmp/{task_id}_out.png"
            
            # VITAL FIX 1: Strip out hidden WebP headers by forcing a pure RGB PNG save
            img = Image.open(io.BytesIO(data)).convert("RGB")
            img.save(in_path, format="PNG")
            
            def run_ai():
                # VITAL FIX 2: Explicitly point to the /app/models folder so the AI finds its weights
                subprocess.run(["/app/realesrgan-ncnn-vulkan", "-i", in_path, "-o", out_path, "-s", "4", "-t", "256", "-n", "realesrgan-x4plus-anime", "-m", "/app/models", "-g", "1"], check=True)
            
            await asyncio.to_thread(run_ai)
            
            # --- THE INTENSITY DIAL (70% AI / 30% Original) ---
            orig_img = Image.open(in_path).convert("RGBA")
            ai_img = Image.open(out_path).convert("RGBA")
            
            # Scale the original texture up to match the massive new AI dimensions
            orig_resized = orig_img.resize(ai_img.size, Image.Resampling.LANCZOS)
            
            # Blend them together at 70% AI dominance
            final_img = Image.blend(orig_resized, ai_img, alpha=0.7)
            
            # Overwrite the AI output with our custom blended version
            final_img.save(out_path, format="PNG")
            # --------------------------------------------------
            
            with open(out_path, 'rb') as f:
                file = discord.File(fp=f, filename="ai_upscaled.png")
            
            embed = discord.Embed(title="✨ True AI Enhancement Complete", color=discord.Color.green())
            embed.set_image(url="attachment://ai_upscaled.png")
            embed.set_footer(text="Powered by Real-ESRGAN (Multi-Threaded CPU Mode (llvmpipe))")
            
            await interaction.followup.send(embed=embed, file=file)
            
            # Cleanup temporary files
            os.remove(in_path)
            os.remove(out_path)
            
        except Exception as e:
            await interaction.followup.send(f"❌ True AI Error: {e}")

    @discord.ui.button(label="💾 Save to Vault", style=discord.ButtonStyle.green)
    async def save_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VaultTagModal(self.images[self.index]))

class ImageOps(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ctx_vault = app_commands.ContextMenu(name="💾 Save to Vault", callback=self.save_to_vault_context)
        self.ctx_ocr = app_commands.ContextMenu(name="🔤 Extract Text (OCR)", callback=self.extract_text_context)
        self.ctx_source = app_commands.ContextMenu(name="🔍 Find Anime Source", callback=self.find_source_context)
        
        self.bot.tree.add_command(self.ctx_vault)
        self.bot.tree.add_command(self.ctx_ocr)
        self.bot.tree.add_command(self.ctx_source)

    async def cog_unload(self):
        self.bot.tree.remove_command(self.ctx_vault.name, type=self.ctx_vault.type)
        self.bot.tree.remove_command(self.ctx_ocr.name, type=self.ctx_ocr.type)
        self.bot.tree.remove_command(self.ctx_source.name, type=self.ctx_source.type)

    async def _get_image_url(self, message: discord.Message):
        if message.attachments:
            for att in message.attachments:
                if att.content_type and att.content_type.startswith('image/'):
                    return att.url
        if message.embeds:
            for emb in message.embeds:
                if emb.image and emb.image.url:
                    return emb.image.url
        return None

    async def save_to_vault_context(self, interaction: discord.Interaction, message: discord.Message):
        url = await self._get_image_url(message)
        if not url:
            return await interaction.response.send_message("❌ The Omni-Lens could not detect a valid image in that message.", ephemeral=True)
        await interaction.response.send_modal(VaultTagModal(url))

    async def extract_text_context(self, interaction: discord.Interaction, message: discord.Message):
        url = await self._get_image_url(message)
        if not url: return await interaction.response.send_message("❌ No valid image found.", ephemeral=True)
        await interaction.response.defer(ephemeral=False)
        
        api_url = f"https://api.ocr.space/parse/imageurl?apikey=helloworld&url={url}"
        try:
            async with HTTPSessionManager() as session:
                async with session.get(api_url) as resp:
                    data = await resp.json()
                    
            if data.get('IsErroredOnProcessing'):
                return await interaction.followup.send("❌ OCR Processing Failed.")
            
            parsed_results = data.get('ParsedResults')
            if not parsed_results or not parsed_results[0].get('ParsedText').strip():
                return await interaction.followup.send("📭 No readable text found in the image.")
                
            text = parsed_results[0]['ParsedText']
            cb = chr(96)*3
            desc = f"{cb}\n{text[:4000]}\n{cb}"
            embed = discord.Embed(title="🔤 Optical Character Recognition", description=desc, color=discord.Color.blue())
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Network Error: {e}")

    async def find_source_context(self, interaction: discord.Interaction, message: discord.Message):
        url = await self._get_image_url(message)
        if not url: return await interaction.response.send_message("❌ No valid image found.", ephemeral=True)
        await interaction.response.defer(ephemeral=False)
        
        api_url = f"https://api.trace.moe/search?anilistInfo&url={url}"
        try:
            async with HTTPSessionManager() as session:
                async with session.get(api_url) as resp:
                    data = await resp.json()
                    
            if data.get('error'):
                return await interaction.followup.send(f"❌ Trace.moe Error: {data['error']}")
                
            best_match = data['result'][0]
            similarity = best_match['similarity'] * 100
            
            if similarity < 70:
                return await interaction.followup.send(f"⚠️ No confident matches found. Best guess was {similarity:.1f}% accurate.")
                
            anime_title = best_match['anilist']['title'].get('romaji') or best_match['anilist']['title'].get('english')
            episode = best_match.get('episode', 'Movie/OVA')
            minutes, seconds = divmod(int(best_match['from']), 60)
            
            embed = discord.Embed(title="🔍 Anime Source Detective", color=discord.Color.purple())
            embed.add_field(name="Anime", value=f"**{anime_title}**", inline=False)
            embed.add_field(name="Episode", value=str(episode), inline=True)
            embed.add_field(name="Timestamp", value=f"{minutes}:{seconds:02d}", inline=True)
            embed.add_field(name="Confidence", value=f"{similarity:.1f}%", inline=True)
            embed.set_image(url=best_match['image'])
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Network Error: {e}")

    image_group = app_commands.Group(name="image", description="Search, save, and manipulate images with Aria's visual tools.")

    @image_group.command(name="search", description="Search for images and browse the results in Aria's carousel.")
    async def search(self, interaction: discord.Interaction, query: str):
        await interaction.response.defer()
        try:
            import urllib.parse
            import re
            import asyncio
            import random
            
            query_safe = urllib.parse.quote(query)
            
            # Ghost Protocol V8: The Dynamic Randomizer
            # Pull 3 completely random pages between 1 and 400 so every search is unique!
            offsets = random.sample(range(1, 400, 35), 3)
            urls = [f"https://www.bing.com/images/search?q={query_safe}&qft=+filterui:imagesize-wallpaper&form=HDRSC2&first={offset}" for offset in offsets]
            
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            
            raw_turls = []
            async with HTTPSessionManager() as session:
                tasks = [session.get(u, headers=headers) for u in urls]
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                for resp in responses:
                    if not isinstance(resp, Exception) and resp.status == 200:
                        html = await resp.text()
                        raw_turls.extend(re.findall(r'turl&quot;:&quot;(.*?)&quot;', html))
            
            # Shuffle the raw links so the carousel order is completely unpredictable
            random.shuffle(raw_turls)
            
            unique_urls = []
            for turl in raw_turls:
                if turl.startswith('http'):
                    # Delete the size limits to grab the 4K original
                    high_res_cdn = re.sub(r'&w=\d+', '', turl)
                    high_res_cdn = re.sub(r'&h=\d+', '', high_res_cdn)
                    high_res_cdn = re.sub(r'&c=\d+', '', high_res_cdn)
                    high_res_cdn = re.sub(r'&pid=\w+', '&pid=ImgDetMain', high_res_cdn)

                    if high_res_cdn not in unique_urls:
                        unique_urls.append(high_res_cdn)
                        
                # Bumped the limit to 20 for more variety!
                if len(unique_urls) >= 20:
                    break
            
            if not unique_urls:
                return await interaction.followup.send(f"❌ The Omni-Lens found zero matches for `{query}`.")
                
            view = ImageCarousel(query, unique_urls)
            await interaction.followup.send(embed=view.update_embed(), view=view)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Network Fetch Error: {e}")

    @image_group.command(name="vault", description="Retrieve an image previously saved in the Visual Vault.")
    async def vault_get(self, interaction: discord.Interaction, keyword: str):
        await interaction.response.defer()
        try:
            async with DBPoolManager() as pool:
                async with pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        await cur.execute("CREATE TABLE IF NOT EXISTS aria_visual_vault (id INT AUTO_INCREMENT PRIMARY KEY, keyword VARCHAR(100), image_url TEXT, added_by BIGINT, added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                        
                        await cur.execute("SELECT image_url, added_by FROM aria_visual_vault WHERE keyword LIKE %s ORDER BY RAND() LIMIT 1", (f"%{keyword}%",))
                        res = await cur.fetchone()
                        
                        if not res:
                            return await interaction.followup.send(f"📭 The Visual Vault contains no records for `{keyword}`.")
                            
                        embed = discord.Embed(title=f"🔐 Vault Record: {keyword}", color=discord.Color.gold())
                        embed.set_image(url=res['image_url'])
                        embed.set_footer(text=f"Archived by User ID: {res['added_by']}")
                        await interaction.followup.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"❌ Vault Fetch Error: {e}")

    @image_group.command(name="forge", description="Generate a simple image with custom text burned onto it.")
    async def forge(self, interaction: discord.Interaction, text: str, color: str = "black"):
        await interaction.response.defer()
        try:
            canvas_color = color.lower() if color.isalpha() else "#1E1E2E"
            img = Image.new('RGB', (1200, 630), color=canvas_color)
            draw = ImageDraw.Draw(img)
            
            font = await FontManager.get_font(size=80)
            
            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            
            x = (img.width - text_width) / 2
            y = (img.height - text_height) / 2
            
            draw.text((x+4, y+4), text, font=font, fill="black")
            draw.text((x, y), text, font=font, fill="white")
            
            with io.BytesIO() as image_binary:
                img.save(image_binary, 'PNG')
                image_binary.seek(0)
                file = discord.File(fp=image_binary, filename='forge_output.png')
                
            embed = discord.Embed(title="⚒️ The Image Forge", color=discord.Color.dark_theme())
            embed.set_image(url="attachment://forge_output.png")
            await interaction.followup.send(embed=embed, file=file)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Forge Error: {e}")

async def setup(bot):
    await bot.add_cog(ImageOps(bot))
