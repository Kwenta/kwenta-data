import os
import json
import asyncio
import pandas as pd
from datetime import datetime, timezone
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from decimal import Decimal
from multicall import Call, Multicall
from web3 import Web3
import nest_asyncio
from libs.smart_contract import SmartContract

nest_asyncio.apply()

## constants
INFURA_KEY = os.getenv('INFURA_KEY')
RPC_ENDPOINT = f'https://optimism-mainnet.infura.io/v3/{INFURA_KEY}'

## contracts and ABIs
with open('./src/abi/PerpsV2Data.json', 'r') as file:
    PerpsV2DataAbi = json.load(file)

PerpsV2DataAddress = "0xF7D3D05cCeEEcC9d77864Da3DdE67Ce9a0215A9D"

# get a web3 provider
w3 = Web3(Web3.HTTPProvider(RPC_ENDPOINT))

async def main():
  perpsData = SmartContract(PerpsV2DataAbi, PerpsV2DataAddress, RPC_ENDPOINT)
  marketSummaries = perpsData.call_function('allProxiedMarketSummaries')
  print(marketSummaries)
  pass

if __name__ == '__main__':
  asyncio.run(main())