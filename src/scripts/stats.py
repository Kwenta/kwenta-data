import os
import asyncio
import pandas as pd
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from decimal import Decimal
from web3 import Web3
import nest_asyncio

nest_asyncio.apply()

# constants
INFURA_KEY = os.getenv('INFURA_KEY')

# mainnet
SUBGRAPH_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/kwenta/optimism-perps'
RPC_ENDPOINT = f'https://optimism-mainnet.infura.io/v3/{INFURA_KEY}'

# get a web3 provider
w3 = Web3(Web3.HTTPProvider(RPC_ENDPOINT))

# functions


def convertDecimals(x): return Decimal(x) / Decimal(10**18)


def convertBytes(x): return bytearray.fromhex(
    x[2:]).decode().replace('\x00', '')


def clean_df(df, decimal_cols=[], bytes_cols=[]):
    for col in decimal_cols:
        if col in df.columns:
            df[col] = df[col].apply(convertDecimals)
        else:
            print(f"{col} not in DataFrame")
    for col in bytes_cols:
        if col in df.columns:
            df[col] = df[col].apply(convertBytes)
        else:
            print(f"{col} not in DataFrame")
    return df


async def run_query(query, params, endpoint=SUBGRAPH_ENDPOINT):
    transport = AIOHTTPTransport(url=endpoint)

    async with Client(
        transport=transport,
        fetch_schema_from_transport=True,
    ) as session:

        # Execute single query
        query = query

        result = await session.execute(query, variable_values=params)
        df = pd.DataFrame(result)
        return df


async def run_recursive_query(query, params, accessor, endpoint=SUBGRAPH_ENDPOINT):
    transport = AIOHTTPTransport(url=endpoint)

    async with Client(
        transport=transport,
        fetch_schema_from_transport=True,
    ) as session:
        done_fetching = False
        all_results = []
        while not done_fetching:
            result = await session.execute(query, variable_values=params)
            if len(result[accessor]) > 0:
                all_results.extend(result[accessor])
                params['last_id'] = all_results[-1]['id']
            else:
                done_fetching = True

        df = pd.DataFrame(all_results)
        return df

# queries
aggregateStats = gql("""
query aggregateStats(
  $last_id: ID!
) {
  futuresAggregateStats(
    where: {
      id_gt: $last_id,
      period: "86400",
      asset: "0x",
    },
    first: 1000
  ) {
    id
    timestamp
    volume
    trades
    feesSynthetix
    feesKwenta
  }
}
""")

traders = gql("""
query traders(
  $last_id: ID!
) {
  futuresTrades(
    where: {
      id_gt: $last_id,
    },
    first: 1000
  ) {
    id
    account
    timestamp
  }
}
""")


async def main():
    # get aggregate data
    agg_params = {
        'last_id': ''
    }

    agg_decimal_cols = [
        'volume',
        'feesSynthetix',
        'feesKwenta'
    ]

    agg_response = await run_recursive_query(aggregateStats, agg_params, 'futuresAggregateStats')
    df_agg = pd.DataFrame(agg_response)
    print(f'agg result size: {df_agg.shape[0]}')
    df_agg = clean_df(df_agg, decimal_cols=agg_decimal_cols).drop(
        'id', axis=1).sort_values('timestamp')
    df_agg['timestamp'] = df_agg['timestamp'].astype(int)
    df_agg['trades'] = df_agg['trades'].astype(int)
    df_agg['cumulativeTrades'] = df_agg['trades'].cumsum()

    # get trader data
    trader_params = {
        'last_id': ''
    }

    trader_response = await run_recursive_query(traders, trader_params, 'futuresTrades')
    df_trader = pd.DataFrame(trader_response).drop(
        'id', axis=1).sort_values('timestamp')

    # create the aggregates
    df_trader['dateTs'] = df_trader['timestamp'].apply(
        lambda x: int(int(x) / 86400) * 86400)
    df_trader['cumulativeTraders'] = (
        ~df_trader['account'].duplicated()).cumsum()

    df_trader_agg = df_trader.groupby(
        'dateTs')['account'].nunique().reset_index()
    df_trader_agg.columns = ['timestamp', 'uniqueTraders']
    df_trader_agg['cumulativeTraders'] = df_trader.groupby(
        'dateTs')['cumulativeTraders'].max().reset_index()['cumulativeTraders']

    print(f'trader result size: {df_trader.shape[0]}')
    print(f'trader agg result size: {df_trader_agg.shape[0]}')

    # combine the two datasets
    df_write = df_agg.merge(df_trader_agg, on='timestamp')
    print(f'combined result size: {df_write.shape[0]}')

    # make sure the directory exists
    outdirs = ['data', 'data/stats']
    for outdir in outdirs:
        if not os.path.exists(outdir):
            os.mkdir(outdir)

    # write out json data
    df_write.to_json(
        f'{outdir}/daily_stats.json',
        orient='records'
    )

if __name__ == '__main__':
    asyncio.run(main())
