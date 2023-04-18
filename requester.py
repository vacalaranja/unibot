import os
import aiohttp
import asyncio
import json
import requests
import pickle
import redis
import discord

from dateutil.parser import isoparse
from web3 import Web3, IPCProvider
from ens import ENS
from oracle import Oracle
from cex import Cex
from constants import *

infura_url = os.getenv('INFURA')
web3 = Web3(Web3.HTTPProvider(infura_url))
ns = ENS.fromWeb3(web3)

class Requester():

    def __init__(self):
        self.limit = 200 #number of transactions to pull every loop
        self.min_rpl = 500 #minimun value of transactions that will be included (updated every 30s to 25k USD)
        self.min_eth = 60 #Ignore the minimun value of transactions if more than this many ETH gets traded.
        self.oracle = Oracle()
        self.cex = Cex()
        self.redis = redis.StrictRedis(charset='utf-8', decode_responses=True)
        self.rpl_address = TOKEN_ADDRESS
        self.new_rpl_address = NEW_TOKEN_ADDRESS
        self.weth_address = WETH_ADDRESS
        self.reth_address = RETH_ADDRESS
        self.rpit_address = RPIT_ADDRESS
        self.usdc_address = USDC_ADDRESS
        self.usdt_address = USDT_ADDRESS
        self.latest_eth_price = self.oracle.get_usd_price(self.weth_address)
        self.latest_reth_ratio = self.oracle.get_ratio(self.reth_address)
        self.origins = {}

    async def loop(self):
        while True:
            #print('getting swaps')
            await self.get_swaps()
            #print('done')
            if self.redis.get('cex_request'):
                #print('getting cex swaps')
                self.get_coinbase_swaps()
                self.get_kraken_swaps()
                self.redis.delete('cex_request')
                #print('done')
            await asyncio.sleep(60)


    def cow_request(self, tx_id):
        url = f'https://api.cow.fi/mainnet/api/v1/transactions/{tx_id}/orders'
        r = requests.get(url)
        try:
            data = json.loads(r.text)
        except:
            print('cowswap error')
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

    def ens_resolve(self, address):
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

    def get_sender(self, address, cow_swap):
        if cow_swap:
            address = cow_swap
        name = self.ens_resolve(address)
        if name is None:
            if cow_swap:
                return f"[Sender: {address[:7]}...{address[-4:]}](https://etherscan.io/address/{address})"
            return f"[Sender: {address[:7]}...{address[-4:]}](https://etherscan.io/address/{address})"
        else:
            return f"[Sender: {name}](https://etherscan.io/address/{address})"

    async def get_swaps(self):
        pairs = [(self.rpl_address, self.weth_address),
                (self.weth_address, self.new_rpl_address),
                (self.reth_address, self.new_rpl_address),
                (self.rpl_address, self.new_rpl_address),
                (self.usdc_address, self.new_rpl_address)]
        txs = {}
        for p in pairs:
            swaps = await self.graphql_request(token0=p[0], token1=p[1])
            if swaps is None:
                #print(f'No swaps found for pair {p}')
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
            embed.set_footer(text=f'{AUTHOR} {ADDRESS}', icon_url=ICON)
            self.redis.rpush('embeds', pickle.dumps(embed))
        #print(self.redis.llen('embeds'), ' embeds')

    def get_kraken_swaps(self):
        pairs = ['RPLUSD', 'RPLEUR']
        for pair in pairs:
            data = self.cex.kraken_request(pair)
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
            embed = discord.Embed(title=title, color=color)
            #embed.add_field(name='RPL', value=f'{swap["New-RPL"]:,.2f}')
            for value in data:
                if type(data[value]) == bool:
                    continue
                if type(data[value]) == float:
                    embed.add_field(name=value, value=f'{data[value]:,.2f}')
                else:
                    embed.add_field(name=value, value=data[value])
            embed.set_footer(text=f'{AUTHOR} {ADDRESS}', icon_url=ICON)
            self.redis.rpush('cex', pickle.dumps(embed))
        #print(self.redis.llen('cex'), ' cex embeds')

    def get_coinbase_swaps(self, pair='RPL-USD'):
        url = f'https://api.exchange.coinbase.com/products/{pair}/trades'
        headers = {'accept': 'application/json'}
        params = {'limit':1000}
        r = requests.get(url, headers=headers, params=params)
        try:
            d = json.loads(r.text)
        except:
            print('coinbase error')
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
            if self.redis.sismember('coinbase_done', swap['trade_id']):
                continue
            self.redis.sadd('coinbase_done', swap['trade_id'])
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
        embed = discord.Embed(title=title, color=color)
        embed.add_field(name='Time', value=f'{data["Time"]}')
        embed.add_field(name='New-RPL', value=f'{data["New-RPL"]:,.2f}')
        embed.add_field(name='USD value', value=f'{data["USD value"]:,.2f}')
        embed.add_field(name='USD price', value=f'{data["USD price"]:,.2f}')
        embed.add_field(name='Trades', value=f'{data["Trades"]}')
        embed.set_footer(text=f'{AUTHOR} {ADDRESS}', icon_url=ICON)
        self.redis.rpush('cex', pickle.dumps(embed))

    async def graphql_request(self, limit=None, token0=None, token1=None):
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
        #url = 'https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-minimal' backup (unrealiable)
        url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json={'query': query}) as r:
                    d = json.loads(await r.text())
#                    print('data:', d)
                    if 'data' in d:
                        return d['data']['swaps']
                    else:
                        return None
            except:
                print('graph query error')
                #print(r.text)
                return None

    def parse_swaps(self, data, txs, check_sandwich=True):
        for swap in data[::-1]:
            swap_id = swap['id']
            if self.redis.sismember('done', swap_id):
                continue
            else:
                self.redis.sadd('done', swap_id)
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

if __name__ == '__main__':
    r = Requester()
    asyncio.run(r.loop())

