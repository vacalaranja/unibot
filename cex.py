import discord
import json
import requests
import pickle
import os
from constants import *

class Cex():

    def __init__(self):
        self.openexchange_api_key = os.getenv('OPENEXCHANGE')
        self.kraken_last = 0
        self.latest_eur = 0
        self.request_eur()

    def kraken(self):
        result = []
        pairs = ['RPLUSD', 'RPLEUR']
        for pair in pairs:
            url = f'https://api.kraken.com/0/public/Ticker?pair={pair}'
            r = requests.get(url)
            try:
                d = json.loads(r.text)
            except:
                print('kraken command error')
                return None
            result.append(d['result'][pair])
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
        embed.set_footer(text=f'{AUTHOR} {ADDRESS}', icon_url=ICON)
        return embed

    def request_eur(self):
        url = 'https://openexchangerates.org/api/latest.json'
        params = {'app_id':self.openexchange_api_key, 'symbols':'EUR', 'base':'USD', 'prettyprint':False}
        r = requests.get(url, params=params)
        try:
            d = json.loads(r.text)
            self.latest_eur = float(d['rates']['EUR'])
            print('eur updated')
        except:
            print('error getting EUR rate')
            self.latest_eur = 0.91 #reasonable rate
        return self.latest_eur

    def kraken_request(self, pair='RPLUSD'):
        url = f'https://api.kraken.com/0/public/Trades?pair={pair}'
        params = {'pair':pair}
        if self.kraken_last:
            params['since'] = self.kraken_last
        r = requests.get(url, params=params)
        try:
            d = json.loads(r.text)
        except:
            print('kraken error')
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

    def coinbase(self, pair='RPL-USD'):
        url = f'https://api.exchange.coinbase.com/products/{pair}/stats'
        headers = {'accept': 'application/json'}
        r = requests.get(url, headers=headers)
        try:
            d = json.loads(r.text)
        except:
            print('coinbase command error')
            raise
        #print(d)
        #{'open': '1230.88', 'high': '1240.02', 'low': '1215', 'last': '1232.5', 'volume': '251082.75145692', 'volume_30day': '12092500.89993325'}
        for k in d:
            d[k] = float(d[k])
        title = 'Coinbase RPL-USD stats'
        embed = discord.Embed(title=title, description=f'',color=discord.Color.blue())
        embed.add_field(name="Latest Price", value=f"${d['last']:,.3f}", inline=False)
        embed.add_field(name="24h Low - High", value=f"${d['low']:,.3f} - ${d['high']:,.3f}", inline=False)
        embed.add_field(name="24h Volume", value=f"{d['volume']:,.2f} RPL", inline=False)
        embed.add_field(name="30d Volume", value=f"{d['volume_30day']:,.2f} RPL", inline=False)
        embed.set_footer(text=f'{AUTHOR} {ADDRESS}', icon_url=ICON)
        return embed


