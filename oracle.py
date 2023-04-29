import os
import json
import requests
import redis
import asyncio

from web3 import Web3, IPCProvider
from constants import *

infura_url = os.getenv('INFURA')
openexchange_api_key = os.getenv('OPENEXCHANGE')

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

    def get_ratio(self, address, to_address=None):
        if to_address is None:
            to_address = self.weth_address
        try:
            url = 'https://api.1inch.io/v5.0/1/quote'
            params = {'fromTokenAddress': address,
                      'toTokenAddress': to_address,
                      'amount': str(10**18)
            }
            r = requests.get(url, params=params)
            d = json.loads(r.text)
            return float(d['toTokenAmount'])/float(d['fromTokenAmount'])
            #print(d)
        except:
            #try backup oracle
            to_address = Web3.toChecksumAddress(to_address)
            address = Web3.toChecksumAddress(address)
            ratio = self.oracle.functions.getRate(address, to_address, True).call()
            ratio = ratio / 10**18
            return ratio

    def get_usd_price(self, address):
        price = self.get_ratio(address, self.usdt_address)
        price = price * 10**12
        #print(price)
        return price

    async def loop(self):
        while True:
            #print('getting swaps')
            ratio = self.get_ratio(self.new_rpl_address)
            eth_price = self.get_usd_price(self.weth_address)
            reth_ratio = self.get_ratio(self.reth_address)
            self.redis.set('eth', eth_price)
            self.redis.set('reth', reth_ratio)
            self.redis.set('ratio', ratio)
            await asyncio.sleep(60)

if __name__ == '__main__':
    oracle = Oracle()
    asyncio.run(oracle.loop())
