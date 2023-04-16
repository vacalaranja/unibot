import requests
import os
import json
from web3 import Web3, IPCProvider
from constants import *

infura_url = os.getenv('INFURA')
openexchange_api_key = os.getenv('OPENEXCHANGE')

web3 = Web3(Web3.HTTPProvider(infura_url))

class Oracle():

    def __init__(self):
        self.weth_address = WETH_ADDRESS
        self.usdt_address = USDT_ADDRESS
        with open('oracle_abi.json') as f:
            abi = json.load(f)['result']
        self.oracle = web3.eth.contract(address=PRICE_ORACLE, abi=abi)

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

    def get_usd_price(self, address):
        price = self.get_ratio(address, self.usdt_address)
        price = price * 10**12
        #print(price)
        return price
