import os
import requests
import json
import discord
import pickle
import redis

from math import floor
from discord.ext import tasks, commands
from datetime import datetime
from random import random, choice

from constants import *
from cex import Cex
from oracle import Oracle

intents = discord.Intents.default()
client = discord.Client(intents=intents)
bot = commands.Bot(command_prefix='/', intents=intents)
SLEEP_DURATION = 2 # Sleep every loop (in seconds)
ath_file = os.getenv('ATH_FILE')
discord_bot_key = os.getenv('TOKEN')
dex_api_key = os.getenv('DEX')

class Unibot(commands.Cog):

    def __init__(self, bot):
        self.bot = bot
        self.ctx = {}
        self._cex_time = 43200 #12 hours
        self.zeroes = 10**18
        self.loop_counter = 0
        self.sleep_duration = SLEEP_DURATION
        self._ath = self.load_ath()
        self.disable = True
        self.new_rpl_address = NEW_TOKEN_ADDRESS
        self.weth_address = WETH_ADDRESS
        self.reth_address = RETH_ADDRESS
        self.oracle = Oracle()
        self.cex = Cex()
        self.redis = redis.StrictRedis()
        self.latest_ratio = self.oracle.get_ratio(self.new_rpl_address)
        self.latest_eth_price = self.oracle.get_usd_price(self.weth_address)
        self.latest_reth_ratio = self.oracle.get_ratio(self.reth_address)
        self.last_updated = int(datetime.now().timestamp())
        self.loop.start()

    def load_ath(self):
        with open(ath_file) as f:
            d = f.readlines()
            ath, ath_ts = d[0].strip().split()
            usd_ath, usd_ath_ts = d[1].strip().split()
            return [[float(ath), int(ath_ts)], [float(usd_ath), int(usd_ath_ts)]]

    def save_ath(self):
        with open(ath_file, 'w') as f:
            for l in self._ath:
                f.write(' '.join([str(n) for n in l]) + '\n')

    @commands.command()
    async def ath(self, ctx, *args):
        if args and (str(ctx.author) == 'waqwaqattack#7706' or str(ctx.author) == 'vacalaranja#8816'):
            try:
                new_ath = float(args[0])
                embed = discord.Embed(title='New ATH', description='', color=discord.Color.from_rgb(255,255,255))
                if new_ath > 50: # USD
                    self._ath[1][0] = new_ath
                    self._ath[1][1] = int(datetime.now().timestamp())
                    embed.add_field(name='New USD ATH:', value=f'{self._ath[1][0]}', inline=False)
                else: # Ratio
                    self._ath[0][0] = new_ath
                    self._ath[0][1] = int(datetime.now().timestamp())
                    embed.add_field(name='New ATH ratio:', value=f'{self._ath[0][0]}', inline=False)
                self.save_ath()
                embed.set_footer(text='Waqwaqattack is keeper of the ATH.')
                for ctx in self.ctx.values(): #send to all servers
                    await ctx.send(embed=embed)

            except ValueError:
                return await ctx.send('Error updating ATH.')
        else:
            embed = discord.Embed(title='ATH', description='', color=discord.Color.from_rgb(255,255,255))
            embed.add_field(name='Current ATH ratio:', value=f'{self._ath[0][0]}', inline=False)
            embed.add_field(name='Last updated ATH ratio:', value=f'<t:{self._ath[0][1]}:D>', inline=False)
            embed.add_field(name='Current USD ATH:', value=f'{self._ath[1][0]}', inline=False)
            embed.add_field(name='Last updated USD ATH:', value=f'<t:{self._ath[1][1]}:D>', inline=False)
            embed.set_footer(text='Waqwaqattack is keeper of the ATH.')
            return await ctx.send(embed=embed)

    @commands.command()
    @commands.is_owner()
    async def update_eur(self, ctx):
        eur = self.cex.request_eur()
        return await ctx.send(f'Updating EUR, new rate is {eur:,.2f}')

    @commands.command()
    @commands.is_owner()
    async def cex_time(self, ctx, new_time):
        self._cex_time = int(new_time)
        return await ctx.send(f'Cex updates interval set to {new_time}s')

    def add_ctx(self, ctx):
        self.ctx[ctx.guild.id] = ctx

    def remove_ctx(self, ctx):
        if ctx.guild.id in self.ctx:
            del(self.ctx[ctx.guild.id])
        else:
            print(ctx.guild.id, self.ctx)
            print('context not found')

    @tasks.loop(seconds=SLEEP_DURATION)
    async def loop(self):
        self.loop_counter += 1
        self.disable = False # Rate block /ratio (max 1 call / second)
#        if not self.loop_counter % int(10/self.sleep_duration):
#            for c in self.ctx.values():
#                #print(dir(c.guild))
#                if 'Servidor' in str(c.guild):
#                    await c.send('Ping ' + str(datetime.now()))
        if not self.loop_counter % int(10/self.sleep_duration): # Get swaps every 10s.
            self.latest_ratio = self.oracle.get_ratio(self.new_rpl_address)
            self.latest_eth_price = self.oracle.get_usd_price(self.weth_address)
            self.last_updated = int(datetime.now().timestamp())
            self.min_rpl = 25000 / (self.latest_eth_price * self.latest_ratio)
            #print(self.min_rpl, self.latest_ratio, self.latest_eth_price)
            self.latest_reth_ratio = self.oracle.get_ratio(self.reth_address)
            done = True
            while self.redis.exists('embeds'):
                embed = pickle.loads(self.redis.lpop('embeds'))
                for ctx in self.ctx.values(): #send to all servers
                    try:
                        await ctx.send(embed=embed)
                        self.last_embed = embed
                    except:
                        done = False
                        break
            while self.redis.exists('cex'):
                embed = pickle.loads(self.redis.lpop('cex'))
                embed.description = f'{self._cex_time/60/60:.0f}h summary'
                for ctx in self.ctx.values(): #send to all servers
                    try:
                        await ctx.send(embed=embed)
                    except:
                        break
        if not self.loop_counter % int(self._cex_time/self.sleep_duration): #Get kraken and coinbase summary every 'self._cex_time' seconds.
            self.redis.set('cex_request', 1)
            self.loop_counter = 0

    @commands.command()
    async def collateral(self, ctx):
        #return 1.6 ETH in RPL (10%) and 24 ETH in RPL (150%) collateral rates.
        title = 'Collateral'
        embed = discord.Embed(title=title, description=f'',color=discord.Color.orange())
        embed.add_field(name='8 ETH Minipools\n', value=f'\n', inline=False)
        embed.add_field(name='10% (2.4 ETH)', value=f'{2.4/self.latest_ratio:,.2f} RPL')
        embed.add_field(name='150% (12 ETH)', value=f'{12/self.latest_ratio:,.2f} RPL')
        embed.add_field(name='16 ETH Minipools\n', value=f'\n', inline=False)
        embed.add_field(name='10% (1.6 ETH)', value=f'{1.6/self.latest_ratio:,.2f} RPL')
        embed.add_field(name='150% (24 ETH)', value=f'{24/self.latest_ratio:,.2f} RPL')
        embed.set_footer(text=f'{AUTHOR} {ADDRESS}', icon_url=ICON)
        await ctx.send(embed=embed)

    @commands.command()
    async def kraken(self, ctx):
        embed = self.cex.kraken()
        await ctx.send(embed=embed)

    @commands.command()
    async def coinbase(self, ctx, pair='RPL-USD'):
        embed = self.cex.coinbase(pair)
        await ctx.send(embed=embed)

    @commands.command()
    async def dex(self, ctx):
        try:
            url = f'https://api.dev.dex.guru/v1/chain/1/tokens/{self.new_rpl_address}/market'
            r = requests.get(url, params={'api-key':dex_api_key})
            result = json.loads(r.text)
        except:
            return await ctx.send('Too many requests')
        keys = {"volume_24h": 'Volume',
            "liquidity": 'Liquidity',
            "volume_24h_usd": 'USD Volume',
            "liquidity_usd": 'USD Liquidity',
            "price_usd": 'USD Price',
            "volume_24h_delta": '24h Volume delta',
            "liquidity_24h_delta": '24h Liquidity delta',
            "price_24h_delta": '24h Price delta',
            "volume_24h_delta_usd": '24h USD Volume delta',
            "liquidity_24h_delta_usd": '24h USD Liquidity delta',
            "price_24h_delta_usd": '24h USD Price delta',
            "timestamp": 'Timestamp'}
        title = '24h DEX RPL stats'
        embed = discord.Embed(title=title, description=f'',color=discord.Color.orange())
        for k in keys:
            if k == 'timestamp':
                embed.add_field(name=f'{keys[k]}', value=f'<t:{result[k]}>', inline=False)
            elif '_usd' not in k and 'delta' not in k:
                if 'liquidity' in k:
                    usd = f'{result[k + "_usd"]:,.2f} USD ({result[k+"_24h_delta_usd"]:.0%})'
                    eth = f'{result[k]:,.2f} ETH ({result[k+"_24h_delta"]:.0%})'
                elif 'volume' in k:
                    usd = f'{result[k + "_usd"]:,.2f} USD ({result[k+"_delta_usd"]:.0%})'
                    eth = f'{result[k]:,.2f} ETH ({result[k+"_delta"]:.0%})'
                embed.add_field(name=f'{keys[k]}', value=f'{usd}\n{eth}', inline=False)
            elif 'price' in k and '24h' not in k:
                usd = f'{result[k]:,.2f} USD ({result["price_24h_delta_usd"]:.0%})'
                eth = f'{self.latest_ratio:,.5f} ETH ({result["price_24h_delta"]:.0%})'
                embed.add_field(name=f'{keys[k]}', value=f'{usd}\n{eth}', inline=False)
        embed.set_footer(text=f'{AUTHOR} {ADDRESS}', icon_url=ICON)
        await ctx.send(embed=embed)

    @commands.command()
    async def jam(self, ctx):
        today = datetime.today().date()
        if today.month == 4 and today.day == 1:
            fools_videos = ['https://www.youtube.com/watch?v=sQIoB5xwsLU', #CH
                            'https://www.youtube.com/watch?v=7lmNpMSPu0k', #vitalik clapping
                            'https://www.youtube.com/watch?v=yp0diaVLPrQ', #singing wagmi
                            'https://www.youtube.com/watch?v=dQw4w9WgXcQ', #NGGYU
                            'https://www.youtube.com/watch?v=QZShA_a-5r8', #japan
                            'https://www.youtube.com/watch?v=QdoTdG_VNV4', #sugar plum
                            'https://www.youtube.com/watch?v=ZZ5LpwO-An4', #he-man
                            'https://www.youtube.com/watch?v=U9t-slLl30E', #yoda
                            'https://www.youtube.com/watch?v=o0u4M6vppCI', #Shia labeouf'
                            'https://www.youtube.com/watch?v=ryRcPeOM1sY' #Deep
                            ]
            video = choice(fools_videos)
        else:
            video = 'https://www.youtube.com/watch?v=Wmo_XbLjVZs' #jam
        return await ctx.send(video)

    @commands.command()
    async def catjam(self, ctx):
        return await ctx.send('https://www.youtube.com/watch?v=a9f8rxdpmb0')

    @commands.command()
    async def ratio(self, ctx):
        #print('ratio')
        if self.disable:
            return await ctx.send('too many requests')
        self.disable = True
        usd_price = self.latest_eth_price * self.latest_ratio
        title = 'Ratio'
        timestamp = self.last_updated
        embed = discord.Embed(title=title, description=f'',color=discord.Color.orange())
        embed.add_field(name='RPL/ETH', value=f'{self.latest_ratio:,.7}')
        embed.add_field(name='RPL/USD', value=f'${usd_price:,.3f}')
        embed.add_field(name='ETH/USD', value=f'${self.latest_eth_price:,.2f}')
        embed.add_field(name='rETH/ETH', value=f'{self.latest_reth_ratio:,.5f}')
        embed.add_field(name='Last Updated', value=f'<t:{timestamp}>')
        embed.set_footer(text=f'{AUTHOR} {ADDRESS}', icon_url=ICON)
        await ctx.send(embed=embed)

    @commands.command()
    async def smoigel(self, ctx):
        if self.disable:
            return await ctx.send('too many requests')
        self.disable = True
        eth_ten_bagger = self.latest_eth_price * 10
        last_ratio = 0.0017
        last_usd_price = abs(eth_ten_bagger) * last_ratio
        title = "Smoigel's Ratio"
        icon = 'https://cdn.discordapp.com/avatars/223883989948170240/d2b86deabb754e355b1dc56821a59077.png'
        timestamp = int(datetime.now().timestamp())
        embed = discord.Embed(title=title, description=f'',color=discord.Color.orange())
        embed.add_field(name='RPL/ETH', value=f'0.0017000')
        embed.add_field(name='RPL/USD', value=f'${last_usd_price:,.3f}')
        embed.add_field(name='ETH/USD', value=f'${eth_ten_bagger:,.2f}')
        embed.add_field(name='Last Updated', value=f'<t:{timestamp}>')
        embed.set_footer(text=f'{AUTHOR} {ADDRESS}', icon_url=ICON)
        await ctx.send(embed=embed)

    @commands.command()
    async def rpit(self, ctx):
        if self.disable:
            return await ctx.send('too many requests')
        self.disable = True
        title = "Rocket Pool Investment Theses"
        embed = discord.Embed(title=title, description='',color=discord.Color.orange())
        embed.add_field(name="Xer0's RPIT 1.0", value='[Reddit link](https://www.reddit.com/r/ethfinance/comments/m3pug8/the_rocket_pool_investment_thesis/)')
        embed.add_field(name="Xer0's RPIT 2.0", value= '[Reddit link](https://www.reddit.com/r/ethfinance/comments/qwbb8w/rocket_pool_investment_thesis_20/)')
        embed.add_field(name="Boodle's RPIT 3.0", value= '[Reddit link](https://www.reddit.com/r/ethfinance/comments/m4jj0i/rocketpool_investment_thesis_round_3/)', inline=False)
        embed.add_field(name="PSY's RPIT Speculative Edition", value= '[Reddit link](https://www.reddit.com/r/ethtrader/comments/m43r38/the_rocket_pool_investment_thesis_speculative/)', inline=False)
        embed.add_field(name="Logris's RPL Tokenomics", value= '[Link](https://tokenomicsexplained.com/rpl/)', inline=False)
#        embed.add_field(name='Short-term (1-3y) TVL', value='4.5M ETH', inline=False)
#        embed.add_field(name='Short-term (1-3y) RPL/ETH', value='0.05 - 0.08', inline=False)
#        embed.add_field(name='Long-term (10y-15y) TVL', value='45M ETH', inline=False)
#        embed.add_field(name='Long-term (10y-15y) RPL/ETH', value='0.4 - 0.5', inline=False)
        embed.add_field(name="Marceau's Bullish Spreadsheet", value= '[Google docs link](https://docs.google.com/spreadsheets/d/1fVw5sg1-QGOH9_kPO2dHRMi9OUjhFLgwYDsyqJDAwlU/edit#gid=0)', inline=False)
        embed.add_field(name="Marceau's RP Scaling Thread", value= '[Thread](https://twitter.com/marceaueth/status/1548489490116710409)', inline=False)
        embed.add_field(name="Jasper's Bullish Cases", value= '[Layer Zero](https://mirror.xyz/0x04BEE613690e98A1959F236c38AbAa5f2439B14a/CvGJPdUZ7Fnnpa8DsEXtL-W4FxoBoublwsmN-Im0kfg)\n[SaaS](https://mirror.xyz/0x04BEE613690e98A1959F236c38AbAa5f2439B14a/fxc6p0hF_zVj9KX1-xfo6P_3lJ6zrn2Ma2p962b54ek)', inline=False)
        embed.add_field(name="Jasper's essay, or: 'Why Paradigm Was Wrong: How rETH Will Flip stETH'", value= '[Essay](https://mirror.xyz/jasperthefriendlyghost.eth/pnaLyH6W4j58vfypsOKHciF_BM5HFvTkouTd9uThesM)\n[Main points (twitter thread)](https://twitter.com/Jasper_ETH/status/1607056757330939906)\n[Audiobook version  by Waqwaqattack](https://anchor.fm/rocket-fuel/episodes/A-Rocket-Fuel-Special---Why-Paradigm-Was-Wrong--How-rETH-Will-Flip-stETH-e1snfnb)', inline=False)
        embed.add_field(name="Ib1gymnast's Investment Thesis", value='[Google docs link](https://drive.google.com/file/d/1JXXM-QjGMXItLujUOjSb8q7pBzb6C7Md/view)', inline=False)
        embed.add_field(name="CMS's LSD Thesis", value='[Link](https://cmsholdings.substack.com/p/lsds-rocketpool?sd=pf)', inline=False)
        embed.add_field(name="Hanniabu's Theses Collection", value= '[Community site](https://fervent-curie-5c2bfc.netlify.app/thesis/)', inline=False)
        await ctx.send(embed=embed)

    @commands.command()
    async def stop(self, ctx):
        return await ctx.send('Hammer time!')

    @commands.command()
    async def waqboard(self, ctx):
        return await ctx.send('https://docs.google.com/spreadsheets/d/18T5w_w9uf6eOy5tt3GDNLFjNp7pr__1Z9IaW4RBCAQY/')

    @commands.command()
    async def wen(self, ctx):
        now = datetime.now()
        answers = ['In the not-too-distant future.', 'Yesterday.', 'Tonight.',
                "I'm Sorry, Dave (made me sign an NDA)", 'Right now.', 'Before withdraws.',
                'What are you waiting for?', 'You may stake now, my liege.',
                'As soon as gas goes down.', 'There is no better time than now.',
                "I'd rather you /rpit'd me", r'¯\_(ツ)_/¯', 'DO IT!', 'Ask the folks in #trading, they know this kind of stuff',
                'Why are you still here? Go check your grafana.', "Before Maker, that's for sure...",
                'Not right now, Kron is watching.', 'Why are you asking me? Ask Joe!', "After we complete Vitalik's roadmap",
                'Let me call Vitalik for you.', 'A long time ago.', 'Soon.', 'Very soon.', 'Need to finish audits first.', 'After $10k ETH.', 'After $1k RPL.']
        msg = choice(answers)
        print(msg, now)
        return await ctx.send(msg)

    @commands.command()
    async def when(self, ctx):
        embed = discord.Embed(title='When?', description='', color=discord.Color.from_rgb(255,150,150))
        #embed.add_field(name='First rewards period start', value='<t:1637818539>', inline=False) #1635399339 + 28×86400
        ts = datetime.now().timestamp()
        start = 1642656900
        period = (28 * 86400)
        n = floor((ts - start)/period)
        period_ts = start + (period * n)
        next_period_ts = start + (period * (n + 1))
        invis = 1683158400 #2023-05-04
        embed.add_field(name='Current rewards period start', value=f'<t:{period_ts}>(<t:{period_ts}:R>)', inline=False)
        embed.add_field(name='Next rewards period start', value=f'<t:{next_period_ts}>(<t:{next_period_ts}:R>)', inline=False)
        embed.add_field(name='#freeinvis', value=f'<t:{invis}>(<t:{invis}:R>)', inline=False)
        embed.set_footer(text='All dates are displayed in your local timezone.')
        return await ctx.send(embed=embed)

@bot.command()
@commands.is_owner()
async def shutdown(ctx):
    unibot.remove_ctx(ctx)
    print('removed from ', ctx.guild)

@client.event
async def on_ready():
  print('We have logged in as {0.user}'.format(client))

@bot.command()
async def author(ctx):
    await ctx.send(f'{AUTHOR} {ADDRESS}')

@bot.command()
@commands.is_owner()
async def start(ctx):
    print(ctx.author, ctx.channel)
    print('added to ',  ctx.guild)
    unibot.add_ctx(ctx)

unibot = Unibot(bot)
bot.add_cog(unibot)
bot.run(discord_bot_key)
