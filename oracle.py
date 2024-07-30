import os
import json
import requests
import redis
import asyncio

from web3 import Web3, IPCProvider
from constants import *

infura_url = os.getenv('INFURA')
openexchange_api_key = os.getenv('OPENEXCHANGE')
oneinch_api_key = os.getenv('INCH_API')

web3 = Web3(Web3.HTTPProvider(infura_url))

class Oracle():

    def __init__(self):
        self.weth_address = WETH_ADDRESS
        self.usdt_address = USDT_ADDRESS
        self.reth_address = RETH_ADDRESS
        self.new_rpl_address = NEW_TOKEN_ADDRESS
        self.redis = redis.StrictRedis(charset='utf-8', decode_responses=True)
        with open('oracle_abi.json') as f:
            abi = json.load(f)['result']
        self.oracle = web3.eth.contract(address=PRICE_ORACLE, abi=abi)

    def get_ratio(self, address, usd=False):
        try:
            url = 'https://api.1inch.dev/price/v1.1/1/' + address
            params = {}
            if usd:
                params['currency'] = 'USD'
            headers = {'accept': 'application/json',
                      'Authorization': f'Bearer {oneinch_api_key}'}
            r = requests.get(url, params=params, headers=headers)
            d = json.loads(r.text)
            if usd:
                return float(d[address])
            else:
                return float(d[address])/10**18
        except:
            #try backup oracle
            if usd:
                to_address = self.usdt_address
            else:
                to_address = self.weth_address
            to_address = Web3.toChecksumAddress(to_address)
            address = Web3.toChecksumAddress(address)
            if usd:
                ratio = self.oracle.functions.getRate(address, to_address, True).call()
                ratio = ratio/10**6
            else:
                ratio = self.oracle.functions.getRateToEth(address, True).call()
                ratio = ratio/10**18
            return ratio

    def get_usd_price(self, address):
        price = self.get_ratio(address, usd=True)
        return price

    async def loop(self):
        while True:
            #print('getting swaps')
            try:
                ratio = self.get_ratio(self.new_rpl_address)
                if ratio:
                    self.redis.set('ratio', ratio)
                else:
                    print('zero')
                await asyncio.sleep(2)
                eth_price = self.get_usd_price(self.weth_address)
                if eth_price > 1:
                    self.redis.set('eth', eth_price)
                await asyncio.sleep(2)
                reth_ratio = self.get_ratio(self.reth_address)
                self.redis.set('reth', reth_ratio)
            except:
                print('Error')
            await asyncio.sleep(60)

if __name__ == '__main__':
    oracle = Oracle()
    asyncio.run(oracle.loop())
