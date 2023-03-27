import os
import asyncio
import json
import sqlite3
import argparse
import pandas as pd
import nest_asyncio
from utils.data import clean_df
from copy import deepcopy
from web3 import Web3
from web3.middleware import geth_poa_middleware

nest_asyncio.apply()

## constants
INFURA_KEY = os.getenv('INFURA_KEY')
RPC_ENDPOINT = f'https://optimism-mainnet.infura.io/v3/{INFURA_KEY}'


def read_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)


def create_table(cursor, table_name, fields):
    fields_sql = ', '.join([f"{key} {value}" for key, value in fields.items()])
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} ({fields_sql})")


def get_market_debt(w3, block_number):
    # data contract
    with open('./src/abi/PerpsV2Data.json', 'r') as file:
        PerpsV2DataAbi = json.dumps(json.load(file))

    PerpsV2DataAddress = "0xF7D3D05cCeEEcC9d77864Da3DdE67Ce9a0215A9D"
    perpsV2Data = w3.eth.contract(address=PerpsV2DataAddress, abi=PerpsV2DataAbi)

    # get the block
    block = w3.eth.get_block(block_number)

    try:
        marketSummaries = perpsV2Data.functions.allProxiedMarketSummaries().call(
            block_identifier=block_number)
    except:
        marketSummaries = []

    markets = [{
        'asset': market[1].decode().replace('\x00', ''),
        'marketDebt': w3.fromWei(market[7], unit='ether'),
    } for market in marketSummaries]

    if len(markets) > 0:
        df_markets = pd.DataFrame(markets)
        df_markets['marketDebt'] = df_markets['marketDebt'].astype(float)
        df_markets = df_markets.set_index('asset').transpose()
        df_markets['block'] = block['number']
        df_markets['timestamp'] = block['timestamp']
        df_markets = df_markets.melt(id_vars=['block', 'timestamp'],
                var_name='asset', value_name='market_debt')

        return df_markets
    else:
        return None

async def main():
    parser = argparse.ArgumentParser(
        description="Export marketDebt data from each market across blocks")
    parser.add_argument('-c', '--config', required=True,
                        help="Path to the JSON configuration file")
    parser.add_argument('-f', '--from-block', required=True, type=int,
                        help="First block to check")
    parser.add_argument('-t', '--to-block', required=True, type=int,
                        help="Last block to check")
    parser.add_argument('-i', '--increment', required=True, type=int,
                        help="Amount of blocks to increment per step")
    parser.add_argument('-b', '--backfill', required=False, type=bool,
                        help="Remove the table and backfill from the beginning")
    args = parser.parse_args()

    # Read config
    config = read_config(args.config)
    table_name = config['table_name']
    fields = config['fields']
    field_names = fields.keys()

    # Parse arguments
    print(f"ARGS {args}")
    from_block = args.from_block
    to_block = args.to_block
    increment = args.increment

    # Set up web3
    w3 = Web3(Web3.HTTPProvider(RPC_ENDPOINT))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    # Connect to your SQLite database
    conn = sqlite3.connect(config['database_file'])
    cursor = conn.cursor()

    # Set up tables if this is a backfill
    if(args.backfill == True):
        # Call the create_table function to drop and re-create the table
        print(f"DROPPING TABLE {table_name}")
        create_table(cursor, table_name, fields)

    # Get the existing data
    df = pd.read_sql_query(f"SELECT block FROM {table_name}", conn)

    # Loop through queries and insert data
    num_incs = int((to_block - from_block) / increment)
    print(f'RUNNING {num_incs} TIMES')
    for inc in range(num_incs):
        check_block = from_block + inc * increment
        if check_block in df['block'].unique():
            print(f'SKIPPING BLOCK {check_block}')
            continue
    
        print(f'CHECKING BLOCK {check_block}')
        df_block = get_market_debt(w3, check_block)
        if df_block is not None:
            df_block['id'] = df_block.apply(lambda x: f"{str(x['block'])}-{x['asset']}", axis=1)
            df_block = df_block[field_names]

            # Prepare the data for insertion
            data_to_insert = [tuple(event[1].values) for event in df_block.iterrows()]
            print(f'INSERTING {df_block.shape[0]} ROWS')

            # Insert the data into the SQLite database using executemany
            placeholders = ', '.join(['?' for _ in field_names])
            cursor.executemany(
                f"INSERT OR REPLACE INTO {table_name} ({', '.join(field_names)}) VALUES ({placeholders})", data_to_insert)
        else:
            print(f"CALL FAILED AT BLOCK {check_block}")
        

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

if __name__ == '__main__':
    asyncio.run(main())
