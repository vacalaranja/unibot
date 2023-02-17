import os
import requests
import json
import discord
import time

from math import floor
from collections import defaultdict
from discord.ext import tasks, commands
from datetime import datetime, timezone, timedelta, date
from dateutil.parser import isoparse
from random import random, choice

from web3 import Web3, IPCProvider
from ens import ENS

OVERRIDE_ADDRESSES = {
  "0xB3A533098485bede3Cb7fA8711AF84FE0bb1e0aD": "oDAO nimbus",
  "0xCCbFF44E0f0329527feB0167bC8744d7D5aEd3e9": "oDAO rocketpool-2",
  "0xAf820bb236FdE6f084641c74a4C62fA61D10b293": "oDAO etherscan",
  "0x8d074ad69b55dD57dA2708306f1c200ae1803359": "oDAO rocketpool-t",
  "0x751683968FD078341C48B90bC657d6bAbc2339F7": "oDAO superphiz",
  "0x1Df9a4f9421ae6306BC7b171Dff8470640f96e72": "oDAO cryptomanufaktur",
  "0xB13fA6Eff52e6Db8e9F0f1B60B744A9a9A01425A": "oDAO blockchaincapital",
  "0x2c6c5809A257ea74a2Df6d20aeE6119196d4bEA0": "oDAO bankless",
  "0xD7F94c53691AFB5A616C6af96e7075c1FFA1D8eE": "oDAO beaconcha.in",
  "0xDf590A63B91E8278a9102BEe9aAfd444F8A4b780": "oDAO consensyscodefi",
  "0x9c69e7FCe961FD837F99c808aD75ED39a2c549Cb": "oDAO fireeyes",
  "0xc5D291607600044348E5014404cc18394BD1D57d": "oDAO lighthouse",
  "0x2354628919e1D53D2a69cF700Cc53C4093977B94": "oDAO rocketpool-1",
  "0x16222268bB682AA34cE60C73F4527F30aCA1b788": "oDAO rocketpool-3",
  "0xE624feD79e8f5353b13Fefa22c82385fdEdFF348": "yinandyangkratom",
  "0xEADB3840596cabF312F2bC88A4Bb0b93A4E1FF5F": "0xEAD",
  "0xF0138d2e4037957D7b37De312a16a88A7f83A32a": "Invis",
  "0x75Cf8e1F8F4fbF4C7BB216E450BCff5F51Ab3E5A": "Invis",
  "0x701F4dcEAD1049FA01F321d49F6dca525cF4A5A5": "MEEK",
  "0x17Fa597cEc16Ab63A7ca00Fb351eb4B29Ffa6f46": "thomas",
  "0x78072BA5f77d01B3f5B1098df73176933da02A7A": "markobarko",
  "0x5e624FAEDc7AA381b574c3C2fF1731677Dd2ee1d": "jamescarnley",
  "0xb8ed9ea221bf33d37360a76ddd52ba7b1e66aa5c": "Lovinall #1",
  "0xca317a4eccbe0dd5832de2a7407e3c03f88b2cdd": "Lovinall #2",
  "0x64627611655C8CdcaFaE7607589b4483a1578f4A": "Darcius",
  "0x33043c521E9c3e80E0c05A2c25f2e894FefC0328": "jcrpt",
  "0xc942B5aA63A3410a13358a7a3aEdF33d9e9D3AC3": "langers",
  "0x8630eE161EF00dE8E70F19Bf5fE5a06046898990": "Marceau.eth #2",
  "0x01A2a10ed806d4e65Ad92c2c6b10bC4D5F37001e": "onethousand.eth",
  "0x75C8F18e401113167A43bB21556cc132BF8C7ca9": "onethousand.eth"
}
n = {}
for k in OVERRIDE_ADDRESSES:
    n[k.lower()] = OVERRIDE_ADDRESSES[k]
OVERRIDE_ADDRESSES.update(n)

intents = discord.Intents.default()
client = discord.Client(intents=intents)
bot = commands.Bot(command_prefix='/', intents=intents)
_author = 'bot made by'
_icon = 'https://i.ibb.co/bJHwgy5/vacalaranja-rocketeer.png'
_address = 'vacalaranja.rpl.eth'
TOKEN_ADDRESS = '0xb4efd85c19999d84251304bda99e90b92300bd93'
NEW_TOKEN_ADDRESS = '0xd33526068d116ce69f19a9ee46f0bd304f21a51f'
WETH_ADDRESS = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
RETH_ADDRESS = '0xae78736cd615f374d3085123a210448e74fc6393'
RPIT_ADDRESS = '0x21d722c340839751d23a4fb5b6d5e593f8cc82eb'
USDC_ADDRESS = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
PRICE_ORACLE = '0x07D91f5fb9Bf7798734C3f606dB065549F6893bb'
DUST = 0.01
ATH_FILE = os.getenv('ATH_FILE')
ethscan_api_key = os.getenv('ETH_TOKEN')
discord_bot_key = os.getenv('TOKEN')
dex_api_key = os.getenv('DEX')
infura_url = os.getenv('INFURA')
openexchange_api_key = os.getenv('OPENEXCHANGE')

web3 = Web3(Web3.HTTPProvider(infura_url))
ns = ENS.fromWeb3(web3)

def ens_resolve(address):
    if address in OVERRIDE_ADDRESSES:
        return OVERRIDE_ADDRESSES[address]
    try:
        name = ns.name(address)
        forward = ns.address(name)
        if forward is None:
            return None
        if address.lower().strip() != forward.lower().strip():
            return None
        return name
    except:
        return None

class Unibot(commands.Cog):

    def __init__(self, bot):
        self.bot = bot
        self.ctx = {}
        self.done = []
        self.coinbase_done = []
        self._cex_time = 14400
        self.kraken_last = 0
        self.zeroes = 10**18
        self.max_done_cache = 5000
        self.counter = 0
        self.limit = 50 #number of transactions to pull every loop
        self.min_rpl = 700 #minimun value of transactions that will be included (updated every 30s to 25k USD)
        self.min_eth = 60 #Ignore the minimun value of transactions if more than this many ETH gets traded.
        self._ath = self.load_ath()
        self.disable = True
        self.rpl_address = TOKEN_ADDRESS
        self.new_rpl_address = NEW_TOKEN_ADDRESS
        self.weth_address = WETH_ADDRESS
        self.reth_address = RETH_ADDRESS
        self.rpit_address = RPIT_ADDRESS
        self.usdc_address = USDC_ADDRESS
        self.rpit_enabled = defaultdict(lambda: False)
        with open('oracle_abi.json') as f:
            abi = json.load(f)['result']
        self.oracle = web3.eth.contract(address=PRICE_ORACLE, abi=abi)
        self.latest_ratio = self.get_ratio(self.new_rpl_address)
        self.latest_eth_price = self.get_usdc_price(self.weth_address)
        self.latest_reth_ratio = self.get_ratio(self.reth_address)
        self.request_eur()
        self.last_updated = int(datetime.now().timestamp())
        for embed in self.get_kraken_swaps():
            pass
        for embed in self.get_coinbase_swaps():
            pass
        self.loop.start()

    def get_ratio(self, address, to_address=None):
        if to_address is None:
            to_address = self.weth_address
        url = 'https://api.1inch.io/v5.0/1/quote'
        params = {'fromTokenAddress': address,
                  'toTokenAddress': to_address,
                  'amount': str(10**18)
        }
        r = requests.get(url, params=params)
        try:
            d = json.loads(r.text)
            return float(d['toTokenAmount'])/float(d['fromTokenAmount'])
            #print(d)
        except:
            #try backup oracle
            to_address = Web3.toChecksumAddress(to_address)
            address = Web3.toChecksumAddress(address)
            ratio = self.oracle.functions.getRate(Web3.toChecksumAddress(address), to_address, False).call()
            ratio = ratio / 10**18
            return ratio

    def get_usdc_price(self, address):
        price = self.get_ratio(address, self.usdc_address)
        price = price * 10**12
        #print(price)
        return price

    def load_ath(self):
        with open(ATH_FILE) as f:
            return [float(n) for n in f.read().strip().split()]

    def save_ath(self):
        with open(ATH_FILE, 'w') as f:
            f.write(' '.join([str(n) for n in self._ath]))

    @commands.command()
    async def ath(self, ctx, *args):
        if args and (str(ctx.author) == 'waqwaqattack#7706' or str(ctx.author) == 'vacalaranja#8816'):
            try:
                new_ath = float(args[0])
                embed = discord.Embed(title='New ATH', description='', color=discord.Color.from_rgb(255,255,255))
                if new_ath > 50: # USD
                    self._ath[1] = new_ath
                    embed.add_field(name='New USD ATH:', value=f'{self._ath[1]}', inline=False)
                else: # Ratio
                    self._ath[0] = new_ath
                    embed.add_field(name='New ATH ratio:', value=f'{self._ath[0]}', inline=False)
                self.save_ath()
                embed.set_footer(text='Waqwaqattack is keeper of the ATH.')
                for ctx in self.ctx.values(): #send to all servers
                    await ctx.send(embed=embed)

            except:
                raise
                return await ctx.send('Error updating ATH.')
        else:
            embed = discord.Embed(title='ATH', description='', color=discord.Color.from_rgb(255,255,255))
            embed.add_field(name='Current ATH ratio:', value=f'{self._ath[0]}', inline=False)
            embed.add_field(name='Current USD ATH:', value=f'{self._ath[1]}', inline=False)
            embed.set_footer(text='Waqwaqattack is keeper of the ATH.')
            return await ctx.send(embed=embed)

    def request_eur(self):
        url = 'https://openexchangerates.org/api/latest.json'
        params = {'app_id':openexchange_api_key, 'symbols':'EUR', 'base':'USD', 'prettyprint':False}
        r = requests.get(url, params=params)
        try:
            d = json.loads(r.text)
            self.latest_eur = float(d['rates']['EUR'])
            #print('eur updated')
        except:
            print('error getting EUR rate')
            self.latest_eur = 0.92 #reasonable rate

    @commands.command()
    @commands.is_owner()
    async def update_eur(self, ctx):
        self.request_eur()
        return await ctx.send(f'Updating EUR, new rate is {self.latest_eur:,.2f}')

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

    def graphql_request(self, limit=None, token0=None, token1=None):
        if limit is None:
            limit = self.limit
        if token0 is None:
            token0 = self.rpl_address
        if token1 is None:
            token1 = self.weth_address
        query = '{swaps(orderBy: timestamp, orderDirection: desc, first:'
        query += str(limit)
        query += ' where: {token0:"'
        query += str(token0)
        query += '", token1:"'
        query += str(token1)
        query += '"'
        query += '''})
      {
        id
        timestamp
        transaction
        {
          id
        }
        token0 {
          symbol
        }
        token1{
          symbol
        }
        amount0
        amount1
        amountUSD
        origin
      }
    }
    '''
        #print(query)
        url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
        #url = 'https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-minimal' backup (unrealiable)
        r = requests.post(url, json={'query': query})
        try:
            d = json.loads(r.text)
        except:
            print('error')
            return None
        #print('data:', d)
        if 'data' in d:
            return d['data']['swaps']
        else:
            return None

    def get_coinbase_swaps(self, pair='RPL-USD'):
        url = f'https://api.exchange.coinbase.com/products/{pair}/trades'
        headers = {'accept': 'application/json'}
        params = {'limit':200}
        r = requests.get(url, headers=headers, params=params)
        try:
            d = json.loads(r.text)
        except:
            print('error')
            raise
        #print('data:', d)
       #{'time': '2022-12-08T07:57:33.426465Z', 'trade_id': 395730524, 'price': '1232.66000000', 'size': '0.57613137', 'side': 'buy'}
        if not d:
            print('no swaps')
            return None
        rpl_amount = 0
        usd_amount = 0
        usd_volume = 0
        rpl_volume = 0
        usd_total = 0
        rpl_total = 0
        trades = 0
        for swap in d:
            if swap['trade_id'] in self.coinbase_done:
                continue
            self.coinbase_done.append(swap['trade_id'])
            if len(swap) != 5:
                print(swap)
                raise 'Coinbase API changed.'
            timestamp = isoparse(swap['time']).timestamp()
            if swap['side'] == 'buy':
                rpl_amount = float(swap['size'])
                usd_amount = -float(swap['price']) * float(swap['size'])
            elif swap['side'] == 'sell':
                rpl_amount = -float(swap['size'])
                usd_amount = float(swap['price']) * float(swap['size'])
            else:
                raise 'Order type is missing.'
            usd_volume += abs(usd_amount)
            usd_total += usd_amount
            rpl_volume += abs(rpl_amount)
            rpl_total += rpl_amount
            trades += 1
        if not rpl_total:
            #print('0 rpl')
            return None
        rpl_price = float(d[0]['price'])
        data = {'Time':f'<t:{int(timestamp)}>','New-RPL':rpl_total,
                'USD value':usd_total, 'USD price':rpl_price, 'Trades':trades}
        if abs(usd_total) <= 1000:
            #print(f'low volume:{usd_total}')
            return
        if data['USD value'] * data['New-RPL'] >= 0: # Both have the same sign
            return
        title = 'Coinbase'
        if data['New-RPL'] > 0:
            title += ' buys'
        else:
            title += ' sells'
        color = discord.Color.purple()
        embed = discord.Embed(title=title, description=f'{self._cex_time/60/60:.0f}h summary', color=color)
        embed.add_field(name='Time', value=f'{data["Time"]}')
        embed.add_field(name='New-RPL', value=f'{data["New-RPL"]:,.2f}')
        embed.add_field(name='USD value', value=f'{data["USD value"]:,.2f}')
        embed.add_field(name='USD price', value=f'{data["USD price"]:,.2f}')
        embed.add_field(name='Trades', value=f'{data["Trades"]}')
        embed.set_footer(text=f'{_author} {_address}', icon_url=_icon)
        yield embed

    @commands.command()
    async def coinbase(self, ctx, pair='RPL-USD'):
        url = f'https://api.exchange.coinbase.com/products/{pair}/stats'
        headers = {'accept': 'application/json'}
        r = requests.get(url, headers=headers)
        try:
            d = json.loads(r.text)
        except:
            print('error')
            raise
        #print(d)
        #{'open': '1230.88', 'high': '1240.02', 'low': '1215', 'last': '1232.5', 'volume': '251082.75145692', 'volume_30day': '12092500.89993325'}
        if d['volume_30day'] is None:
            await ctx.send('No RPL has been traded on Coinbase yet.')
            return
        for k in d:
            d[k] = float(d[k])
        title = 'Coinbase RPL-USD stats'
        embed = discord.Embed(title=title, description=f'',color=discord.Color.blue())
        embed.add_field(name="Latest Price", value=f"${d['last']:,.3f}", inline=False)
        embed.add_field(name="24h Low - High", value=f"${d['low']:,.3f} - ${d['high']:,.3f}", inline=False)
        embed.add_field(name="24h Volume", value=f"{d['volume']:,.2f} RPL", inline=False)
        embed.add_field(name="30d Volume", value=f"{d['volume_30day']:,.2f} RPL", inline=False)
        embed.set_footer(text=f'{_author} {_address}', icon_url=_icon)
        await ctx.send(embed=embed)

    def kraken_request(self, pair='RPLUSD'):
        url = f'https://api.kraken.com/0/public/Trades?pair={pair}'
        params = {'pair':pair}
        if self.kraken_last:
            params['since'] = self.kraken_last
        r = requests.get(url, params=params)
        try:
            d = json.loads(r.text)
        except:
            print('error')
            return None
        #print('data:', d)
        if 'result' not in d:
            return None
        self.kraken_last = d['result']['last']
        if not d['error']:
            #print(f'got {len(d["result"][pair])} swaps')
            swaps = d['result'][pair]
        else:
            print(d['error'])
            return None
        if not swaps:
            #print('no swaps')
            return None
        rpl_amount = 0
        usd_amount = 0
        usd_volume = 0
        rpl_volume = 0
        usd_total = 0
        rpl_total = 0
        for swap in swaps:
            if len(swap) > 7:
                print(swap)
                raise SyntaxError('Kraken API changed.')
            timestamp = int(swap[2])
            miscellaneous = swap[5]
            if swap[3] == 'b':
                rpl_amount = float(swap[1])
                usd_amount = -float(swap[1]) * float(swap[0])
            elif swap[3] == 's':
                rpl_amount = -float(swap[1])
                usd_amount = float(swap[1]) * float(swap[0])
            else:
                raise 'Order type is missing.'
            if 'EUR' in pair:
                usd_amount /= self.latest_eur
            usd_volume += abs(usd_amount)
            usd_total += usd_amount
            rpl_volume += abs(rpl_amount)
            rpl_total += rpl_amount
        if not rpl_total:
            print('0 rpl')
            return None
        rpl_price = float(swap[0])
        swap_summary = {'Time':f'<t:{timestamp}>','New-RPL':rpl_total,
                'USD value':usd_total, 'USD price':rpl_price, 'Trades':len(swaps)}
        return swap_summary

    def get_kraken_swaps(self):
        pairs = ['RPLUSD', 'RPLEUR']
        for pair in pairs:
            data = self.kraken_request(pair)
            if not data:
                return
            if abs(data['USD value']) <= 1000:
#                print(f'USD:{data["USD value"]}')
                return
            if data['USD value'] * data['New-RPL'] >= 0: # Both have the same sign
                return
            title = 'Kraken'
            if data['New-RPL'] > 0:
                title += ' buys'
            else:
                title += ' sells'
            color = discord.Color.purple()
            embed = discord.Embed(title=title, description=f'{self._cex_time/60/60:.0f}h summary', color=color)
            #embed.add_field(name='RPL', value=f'{swap["New-RPL"]:,.2f}')
            for value in data:
                if type(data[value]) == bool:
                    continue
                if type(data[value]) == float:
                    embed.add_field(name=value, value=f'{data[value]:,.2f}')
                else:
                    embed.add_field(name=value, value=data[value])
            embed.set_footer(text=f'{_author} {_address}', icon_url=_icon)
            yield embed

    def get_sender(self, address, cow_swap):
        if cow_swap:
            address = cow_swap
        name = ens_resolve(address)
        if name is None:
            if cow_swap:
                return f"[Sender: {address[:7]}...{address[-4:]}](https://etherscan.io/address/{address})"
            return f"[Sender: {address[:7]}...{address[-4:]}](https://etherscan.io/address/{address})"
        else:
            return f"[Sender: {name}](https://etherscan.io/address/{address})"

    def parse_swaps(self, data, txs, check_sandwich=True):
#        print(len(txs), self.done)
        for swap in data[::-1]:
            swap_id = swap['id']
            if swap_id not in self.done:
                tx_id = swap['transaction']['id']
                amount0 = -float(swap['amount0'])
                amount1 = -float(swap['amount1'])
                token0 = swap['token0']['symbol']
                token1 = swap['token1']['symbol']
                amountUSD = float(swap['amountUSD'])
                timestamp = swap['timestamp']
                sender_address = swap['origin']
                cow_sender = self.cow_request(tx_id)
                origin = self.get_sender(sender_address, cow_sender)
                usdc = False
                rETH = False
                if token0 == 'USDC':
                    weth_amount = amount0/self.latest_eth_price
                    new_rpl_amount = amount1
                    rpl_amount = 0
                    rpl_price = abs(amount0/amount1)
                    usdc = True
                elif token1 == 'RPL' and token0 == 'RPL':
                    new_rpl_amount = amount1
                    rpl_amount = amount0
                    weth_amount = 0
                    rpl_price = abs(amountUSD/rpl_amount)
                elif token1 == 'RPL':
                    new_rpl_amount = amount1
                    rpl_amount = 0
                    weth_amount = amount0
                    rpl_price = abs(amountUSD/new_rpl_amount)
                elif token0 == 'RPL':
                    rpl_amount = amount0
                    new_rpl_amount = 0
                    weth_amount = amount1
                    rpl_price = abs(amountUSD/rpl_amount)
                if token0 == 'rETH':
                    rETH = True
                    weth_amount *= self.latest_reth_ratio
                txs[swap_id] = {'Time':f'<t:{timestamp}>', 'tx':tx_id, 'Old-RPL':rpl_amount,'New-RPL':new_rpl_amount,
                                'WETH':weth_amount, 'Sender':origin, 'USD value':amountUSD, 'USD price':rpl_price,
                                'sandwich':False, 'rETH':rETH, 'USDC':usdc, 'cow':bool(cow_sender)}#'token0':token0, 'token1':token1, 
                if check_sandwich and (swap['origin'] not in self.origins):
                    self.origins[swap['origin']] = swap_id
                elif check_sandwich:
                    maybe_sandwich = self.origins[swap['origin']]
                    if maybe_sandwich in txs and txs[maybe_sandwich]['tx'] != tx_id:
                        maybe_sandwich_eth = txs[maybe_sandwich]['WETH']
                        if (maybe_sandwich_eth > 0) != (weth_amount > 0): #opposite trades
                            txs[swap_id]['sandwich'] = True
                            txs[maybe_sandwich]['sandwich'] = True
                self.done.append(swap_id)
        return txs

    def group_txs(self, txs):
        grouped = {}
        for swap in txs.values():
            if swap['tx'] in grouped:
                grouped[swap['tx']]['WETH'] += swap['WETH']
                grouped[swap['tx']]['Old-RPL'] += swap['Old-RPL']
                grouped[swap['tx']]['New-RPL'] += swap['New-RPL']
                grouped[swap['tx']]['total_swapped'] += abs(swap['WETH'])
                if swap['sandwich']:
                    grouped[swap['tx']]['sandwich'] = True
            else:
                grouped[swap['tx']] = swap
                grouped[swap['tx']]['total_swapped'] = abs(swap['WETH'])
        for tx, swap in grouped.copy().items():
            if abs(swap['New-RPL']) < self.min_rpl and abs(swap['Old-RPL']) < self.min_rpl and swap['total_swapped'] < self.min_eth:
#                print(f'skiping swap: {swap}')
                if 'adada.eth' not in swap['Sender']:
                    continue
            if abs(swap['WETH']) < DUST and abs(swap['New-RPL']) > DUST and abs(swap['Old-RPL']) > DUST:
                grouped[tx]['color'] = discord.Color.blue()
                if swap['New-RPL'] > 0:
                    grouped[tx]['title'] = 'Old > New RPL'
                else:
                    grouped[tx]['title'] = 'New > Old RPL'
                del grouped[tx]['WETH']
            elif abs(swap['WETH']) < DUST:
                continue
            else:
                if abs(swap['New-RPL']) < DUST and abs(swap['Old-RPL']) > DUST:
                    rpl_amount = swap['Old-RPL']
                    grouped[tx]['title'] = 'Old-RPL'
                    del grouped[tx]['New-RPL']
                elif abs(swap['Old-RPL']) < DUST and abs(swap['New-RPL']) > DUST:
                    rpl_amount = swap['New-RPL']
                    grouped[tx]['title'] = 'New-RPL'
                    del grouped[tx]['Old-RPL']
                elif abs(swap['Old-RPL']) > DUST and abs(swap['New-RPL']) > DUST:
                    grouped[tx]['title'] = 'Multi DEX'
                elif abs(swap['WETH']) > 0.05:
                    grouped[tx]['color'] = discord.Color.blue()
                    grouped[tx]['title'] = 'Arbitrage'
                if 'title' not in grouped[tx]:
                    continue
                if swap['sandwich']:
                    grouped[tx]['title'] += ' Sandwich'
                if 'color' not in grouped[tx]:
                    if swap['WETH'] < 0:
                        grouped[tx]['color'] = discord.Color.green()
                        grouped[tx]['title'] += ' Buy'
                    elif swap['WETH'] > 0:
                        grouped[tx]['color'] = discord.Color.red()
                        grouped[tx]['title'] += ' Sell'
                    if swap['sandwich'] or 'DEX' in grouped[tx]['title']:
                        grouped[tx]['color'] = discord.Color.blue()
        for tx in grouped:
            if 'WETH' in grouped[tx]:
                total_value = abs(grouped[tx]['WETH'] * self.latest_eth_price)
                grouped[tx]['USD value'] = total_value
                if 'title' in grouped[tx] and 'Sandwich' not in grouped[tx]['title'] and 'Arbitrage' not in grouped[tx]['title']:
                    if grouped[tx]['USD value'] > 100000:
                        grouped[tx]['title'] = 'Large ' + grouped[tx]['title']
            if grouped[tx]['rETH']:
                if 'title' in grouped[tx]:
                    grouped[tx]['title'] += ' (rETH)'
                if (grouped[tx].get('New-RPL', 0) + grouped[tx].get('Old-RPL', 0)) != 0:
                    grouped[tx]['USD price'] = abs(total_value / (grouped[tx].get('New-RPL', 0) + grouped[tx].get('Old-RPL', 0)))
        return grouped


    def get_swaps(self):
        pairs = [(self.rpl_address, self.weth_address),
                (self.weth_address, self.new_rpl_address),
                (self.reth_address, self.new_rpl_address),
                (self.rpl_address, self.new_rpl_address),
                (self.usdc_address, self.new_rpl_address)]
        txs = {}
        for p in pairs:
            swaps = self.graphql_request(token0=p[0], token1=p[1])
            if swaps is None:
#                print(f'No swaps found for pair {p}')
                return None
            txs = self.parse_swaps(swaps, txs)
        grouped_txs = self.group_txs(txs)
#        print(grouped_txs)
        for k, data in grouped_txs.items():
            if 'title' not in data: #bellow min value
                continue
            description = f'[Transaction: {data["tx"][:7]}...{data["tx"][-4:]}](https://etherscan.io/tx/{data["tx"]})'
            description += '\n'
            if data['cow']:
                description += f'[Cowswap explorer](https://explorer.cow.fi/tx/{data["tx"]}?tab=orders)'
                description += '\n'
            description += data['Sender']
#            print(data['title'])
            if 'adada.eth' in data['Sender']:
                data['title'] = 'adada ' + data['title']
                data['color'] = discord.Color.from_rgb(255, 105, 180)
            embed = discord.Embed(title=data['title'], description=description,color=data['color'])
            for value in data:
                if 'WETH' not in data: #new/old swap
                    if value == 'Old-RPL':
                        embed.add_field(name='Old-RPL', value=f'{data[value]:,.2f}')
                        embed.add_field(name='New-RPL', value=f'{data["New-RPL"]:,.2f}')
                        continue
                    elif value in ['USD price', 'New-RPL', 'USD value']:
                        continue
                if 'Arbitrage' in data['title']: #Arbitrage and Arbitrage Sandwich
                    if value == 'WETH':
                        embed.add_field(name='WETH profit', value=f'{data["WETH"]:,.2f}')
                    if value in ['Old-RPL', 'New-RPL', 'WETH', 'USD value', 'USD price']:
                        continue
                if value in ['Sender', 'tx', 'arb', 'color', 'title', 'total_swapped']:
                    continue
                if type(data[value]) == bool:
                    continue
                if type(data[value]) == float:
                    if value == 'WETH' and data['rETH']:
                        reth_amount = data['WETH'] / self.latest_reth_ratio
                        embed.add_field(name='ETH / rETH', value=f"{data['WETH']:,.2f} / {reth_amount:,.2f}")
                    else:
                        embed.add_field(name=value, value=f'{data[value]:,.2f}')
                elif value != 'sandwich':
                    embed.add_field(name=value, value=data[value])
            if 'WETH' in data and ('Arbitrage' not in data['title'] and 'DEX' not in data['title']):
                ratio = 0
                eth_amount = data['WETH']
                if 'New-RPL' in data and data['New-RPL']:
                    ratio = abs(eth_amount/data['New-RPL'])
                elif 'Old-RPL' in data and data['Old-RPL']:
                    ratio = abs(eth_amount/data['Old-RPL'])
                if ratio:
                    embed.add_field(name='Ratio', value=f'{ratio:.5f}')
            embed.set_footer(text=f'{_author} {_address}', icon_url=_icon)
            yield embed

    @tasks.loop(seconds=1)
    async def loop(self):
        self.counter += 1
        self.disable = False # Rate block /ratio (max 1 call / second)
        if not self.counter % 30: # Update RPL and rETH ratio every 30s.
            self.latest_ratio = self.get_ratio(self.new_rpl_address)
            self.latest_eth_price = self.get_usdc_price(WETH_ADDRESS)
            self.last_updated = int(datetime.now().timestamp())
            self.min_rpl = 25000 / (self.latest_eth_price * self.latest_ratio)
            #print(self.min_rpl, self.latest_ratio, self.latest_eth_price)
        if not self.counter % 60: # Get swaps every minute.
            self.latest_reth_ratio = self.get_ratio(RETH_ADDRESS)
            done = True
            self.origins = {}
            self.possible_arbs = {}
            for embed in self.get_swaps():
                for ctx in self.ctx.values(): #send to all servers
                    try:
                        await ctx.send(embed=embed)
                    except:
                        done = False
                        break
            if done:
                remove_from_done = max(len(self.done) - self.max_done_cache, 0)
                self.done = self.done[remove_from_done:]
        if not self.counter % self._cex_time: #Get kraken and coinbase summary every 'self._cex_time' seconds.
            for embed in self.get_kraken_swaps():
                for ctx in self.ctx.values(): #send to all servers
                    try:
                        await ctx.send(embed=embed)
                    except:
                        done = False
                        break
            for embed in self.get_coinbase_swaps():
                for ctx in self.ctx.values(): #send to all servers
                    try:
                        await ctx.send(embed=embed)
                    except:
                        done = False
                        break
            if len(self.coinbase_done) > 3000:
                self.coinbase_done = self.coinbase_done[1000:]
            self.counter = 0

    @commands.command()
    async def collateral(self, ctx):
        #return 1.6 ETH in RPL (10%) and 24 ETH in RPL (150%) collateral rates.
        title = 'Collateral'
        embed = discord.Embed(title=title, description=f'',color=discord.Color.orange())
        embed.add_field(name='10%', value=f'1.6 ETH = {1.6/self.latest_ratio:,.2f} RPL')
        embed.add_field(name='150%', value=f'24 ETH = {24/self.latest_ratio:,.2f} RPL')
        embed.set_footer(text=f'{_author} {_address}', icon_url=_icon)
        await ctx.send(embed=embed)

    @commands.command()
    async def kraken(self, ctx):
        result = []
        pairs = ['RPLUSD', 'RPLEUR']
        for pair in pairs:
            url = f'https://api.kraken.com/0/public/Ticker?pair={pair}'
            r = requests.get(url)
            try:
                d = json.loads(r.text)
            except:
                print('error')
                return None
            try:
                result.append(d['result'][pair])
            except:
                return await ctx.send('Trading on Kraken has not been enabled yet')
        keys = {'a':'Ask', 'b':'Bid', 'c':'Close', 'v':'Volume', 'p':'Volume weighted average price',
                't':'Trades', 'l':'Low', 'h':'High', 'o':"Today's opening price"}
        title = '24h Kraken RPL stats'
        embed = discord.Embed(title=title, description=f'',color=discord.Color.blue())
        for k in keys:
            if k in 'vp':
                embed.add_field(name=keys[k], value=f'USD: {float(result[0][k][1]):,.2f}\nEUR: {float(result[1][k][1]):,.2f}', inline=False)
            elif k in 't':
                embed.add_field(name=keys[k], value=f'USD: {int(result[0][k][1])}\nEUR: {int(result[1][k][1])}', inline=False)
            elif k in 'a':
                usd_bca = f'USD: {float(result[0]["b"][0]):,.2f} - {float(result[0]["c"][0]):,.2f} - {float(result[0]["a"][0]):,.2f}'
                eur_bca = f'EUR: {float(result[1]["b"][0]):,.2f} - {float(result[1]["c"][0]):,.2f} - {float(result[1]["a"][0]):,.2f}'
                embed.add_field(name='Bid - Close - Ask', value=f'{usd_bca}\n{eur_bca}')
            elif k in 'l':
                embed.add_field(name='Low - High', value=f'USD: {float(result[0]["l"][1]):,.2f} - {float(result[0]["h"][1]):,.2f}\nEUR: {float(result[1]["l"][1]):,.2f} - {float(result[1]["h"][1]):,.2f}', inline=False)
        embed.set_footer(text=f'{_author} {_address}', icon_url=_icon)
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
        embed.set_footer(text=f'{_author} {_address}', icon_url=_icon)
        await ctx.send(embed=embed)

#    @commands.command()
#    async def dashboard(self, ctx):
#        return await ctx.send('https://www.rp-metrics-dashboard.com/')

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
        embed.set_footer(text=f'{_author} {_address}', icon_url=_icon)
        await ctx.send(embed=embed)

    @commands.command()
    async def smoigel(self, ctx):
        if self.disable:
            return await ctx.send('too many requests')
        swap = self.graphql_request(limit=1)#, min_usd=1)
        self.disable = True
        if swap is None:
            return None
        swap = swap[0]
        amount1 = -float(swap['amount1'])
        amountUSD = float(swap['amountUSD'])
        eth_ten_bagger = abs(amountUSD/amount1) * 10
        last_ratio = 0.00170001
        last_usd_price = abs(eth_ten_bagger) * last_ratio
        title = "Smoigel's Ratio"
        icon = 'https://cdn.discordapp.com/avatars/223883989948170240/d2b86deabb754e355b1dc56821a59077.png'
        timestamp = swap['timestamp']
        embed = discord.Embed(title=title, description=f'',color=discord.Color.orange())
        embed.add_field(name='RPL/ETH', value=f'{last_ratio:,.7}')
        embed.add_field(name='RPL/USD', value=f'${last_usd_price:,.3f}')
        embed.add_field(name='ETH/USD', value=f'${eth_ten_bagger:,.2f}')
        embed.add_field(name='Last Updated', value=f'<t:{timestamp}>')
        embed.set_footer(text=f'{_author} {_address}', icon_url=_icon)
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
        embed.add_field(name="Hanniabu's Theses Collection", value= '[Community site](https://fervent-curie-5c2bfc.netlify.app/thesis/)', inline=False)
        await ctx.send(embed=embed)

    def orderbook_request(self, trade='sells'):
        url = 'https://api.0x.org/sra/v3/orderbook'
        query = {'baseAssetData':'0xf47261b0000000000000000000000000b4efd85c19999d84251304bda99e90b92300bd93',
                 'quoteAssetData':'0xf47261b0000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                 'perPage':'500'}
        r = requests.get(url, params=query)
        try:
            d = json.loads(r.text)
        except:
            return None
        if trade == 'sells':
            orders = d['asks']['records']
        elif trade == 'buys':
            orders = d['bids']['records']
        #print(orders)
        return(orders)

    def ethscan_request(self, address):
        url = ' https://api.etherscan.io/api'
        query = {'module':'account',
                 'action':'tokenbalance',
                 'contractaddress':self.rpl_address,
                 'address':address,
                 'tag':'latest',
                 'apikey':ethscan_api_key}
        r = requests.get(url, params=query)
        try:
            d = json.loads(r.text)
        except:
            return None
        value = int(d['result']) / self.zeroes
        return(value)

    def cow_request(self, tx_id):
        url = f'https://api.cow.fi/mainnet/api/v1/transactions/{tx_id}/orders'
        r = requests.get(url)
        try:
            data = json.loads(r.text)
        except:
            print('error')
#           print(f'data:{data}')
            return None
        for d in data:
            buy_token = d['buyToken']
            sell_token = d['sellToken']
            sender = d['receiver']
            rpl_address = self.new_rpl_address.lower()
            if buy_token == rpl_address or sell_token == rpl_address:
                return sender
        return None

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
                'Let me call Vitalik for you.', 'A long time ago.', 'Soon.', 'Very soon.', 'Need to finish audits first.', 'After $3k ETH.']
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
        embed.add_field(name='Current rewards period start', value=f'<t:{period_ts}>(<t:{period_ts}:R>)', inline=False)
        embed.add_field(name='Next rewards period start', value=f'<t:{next_period_ts}>(<t:{next_period_ts}:R>)', inline=False)
        embed.set_footer(text='All dates are displayed in your local timezone.')
        return await ctx.send(embed=embed)

@bot.command()
@commands.is_owner()
async def shutdown(ctx):
    unibot.remove_ctx(ctx)
    print('removed from ', ctx.guild)

@bot.command(hidden=True)
@commands.is_owner()
async def rpit_switch(ctx):
    print(f'rpit: {not unibot.rpit_enabled[ctx.guild]} at {ctx.guild}')
    return await unibot.rpit_switch(ctx)

@bot.command(hidden=True)
@commands.is_owner()
async def rpit_status(ctx):
    return await unibot.rpit_status(ctx)

@client.event
async def on_ready():
  print('We have logged in as {0.user}'.format(client))

@bot.command()
async def author(ctx):
    await ctx.send(f'{_author} {_address}')

@bot.command()
@commands.is_owner()
async def start(ctx):
    print(ctx.author, ctx.channel)
    print('added to ',  ctx.guild)
    unibot.add_ctx(ctx)

unibot = Unibot(bot)
bot.add_cog(unibot)
bot.run(discord_bot_key)
