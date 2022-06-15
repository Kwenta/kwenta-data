import os
import asyncio
import pandas as pd
import json
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from decimal import Decimal
from multicall import Call, Multicall
from web3 import Web3
import nest_asyncio

nest_asyncio.apply()

## constants
INFURA_KEY = os.getenv('INFURA_KEY')

# mainnet
FUTURES_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/kwenta/optimism-main'
RPC_ENDPOINT = f'https://optimism-mainnet.infura.io/v3/{INFURA_KEY}'

# testnet
# FUTURES_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/kwenta/optimism-kovan-main' 
# RPC_ENDPOINT = f'https://optimism-kovan.infura.io/v3/{INFURA_KEY}'

# get a web3 provider
w3 = Web3(Web3.HTTPProvider(RPC_ENDPOINT))

# functions
def convertDecimals(value):
  return Decimal(value) / Decimal(10**18)

def just_return(value):
  return value

def clean_df(df, decimal_cols):
  for col in decimal_cols:
    if col in df.columns:
      df[col] = df[col].apply(convertDecimals)
    else:
      print(f"{col} not in DataFrame")
  return df

async def run_query(query, params, endpoint=FUTURES_ENDPOINT):
  transport = AIOHTTPTransport(url=endpoint)

  async with Client(
      transport=transport,
      fetch_schema_from_transport=True,
  ) as session:
    # Execute single query
    query = query

    result = await session.execute(query, variable_values=params)
    return result


async def run_recursive_query(query, params, accessor, endpoint=FUTURES_ENDPOINT):
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

    return all_results

# queries
allMarginAccountsBlock = gql("""
query marginAccounts(
  $block_number: Int!
  $last_id: ID!
) {
  futuresMarginAccounts(
    where: {
      id_gt: $last_id
    }
    block: {
      number: $block_number
    }
    first: 1000
  ) {
    id
    timestamp
    account
    market
    asset
    margin
    deposits
    withdrawals
  }  
}
""")

openPositionsBlock = gql("""
query openPositions(
  $block_number: Int!
  $last_id: ID!
) {
  futuresPositions(
    where: {
      isOpen: true
      id_gt: $last_id
    }
    block: {
      number: $block_number
    }
    first: 1000
  ) {
    id
    account
    market
  }  
}
""")

async def main():
  ## Query all users between two blocks
  # lb_blocks = [
  #     2557167,
  #     3860759
  # ]

  lb_blocks = [
    8720778,
    11236395
  ]

  lb_results = {}
  for block in lb_blocks:
      params = {
          'block_number': block,
          'last_id': ''
      }

      decimal_cols = [
          'margin',
          'deposits',
          'withdrawals'
      ]

      margin_response = await run_recursive_query(allMarginAccountsBlock, params, 'futuresMarginAccounts')
      df_margin = pd.DataFrame(margin_response)
      print(f'{block} result size: {df_margin.shape[0]}')
      df_margin = clean_df(df_margin, decimal_cols)

      # summarize the data by account
      df_margin = df_margin.groupby('account')[[
          'margin',
          'deposits',
          'withdrawals'
      ]].sum().reset_index()

      lb_results[block] = df_margin


  ## Get unrealized pnl from contracts
  # get list of open positions
  upnl_results = {}
  for block in lb_blocks:
    params = {
        'block_number': block,
      'last_id': ''
    }

    open_position_response = await run_recursive_query(openPositionsBlock, params, 'futuresPositions')
    df_positions = pd.DataFrame(open_position_response)

    # use multicall to get current unrealized pnl and funding
    pnlCalls = [
        Call(
            row['market'],
            ['profitLoss(address)(int256)', row['account']],
            [[row['account'], convertDecimals]]
        ) for _, row in df_positions.iterrows()
    ]

    fundingCalls = [
        Call(
            row['market'],
            ['accruedFunding(address)(int256)', row['account']],
            [[row['account'], convertDecimals]]
        ) for _, row in df_positions.iterrows()
    ]

    pnlMulti = Multicall(
        pnlCalls,
        block_id=block,
        _w3=w3
    )

    fundingMulti = Multicall(
        fundingCalls,
        block_id=block,
        _w3=w3
    )

    pnlResult = pnlMulti()
    fundingResult = fundingMulti()

    # clean the pnl
    pnlResult = [(k, v) for k, v in pnlResult.items()]
    fundingResult = [(k, v) for k, v in fundingResult.items()]

    df_pnl = pd.DataFrame.from_records(pnlResult, columns=['account', 'upnl'])
    df_funding = pd.DataFrame.from_records(fundingResult, columns=['account', 'funding'])
    df_pnl = df_pnl.merge(df_funding, on='account')

    upnl_results[block] = df_pnl

  ## Calculate the leaderboard
  # get the start and end data
  start_lb_df = lb_results[lb_blocks[0]]
  end_lb_df = lb_results[lb_blocks[1]]

  start_pnl_df = upnl_results[lb_blocks[0]]
  end_pnl_df = upnl_results[lb_blocks[1]]

  # merge together
  df_lb = start_lb_df.merge(
    end_lb_df,
    on='account',
    how='outer',
    suffixes=('_start', '_end')
  )

  df_upnl = start_pnl_df.merge(
      end_pnl_df,
    on='account',
    how='outer',
    suffixes=('_start', '_end')
  )

  df_lb = df_lb.merge(
    df_upnl,
    how='outer',
    on='account'
  ).fillna(0)

  # calculated fields
  df_lb['margin_change'] = df_lb['margin_end'] - df_lb['margin_start']
  df_lb['deposits_change'] = df_lb['deposits_end'] - df_lb['deposits_start']
  df_lb['withdrawals_change'] = df_lb['withdrawals_end'] - df_lb['withdrawals_start']
  df_lb['upnl_change'] = df_lb['upnl_end'] - df_lb['upnl_start']
  df_lb['funding_change'] = df_lb['funding_end'] - df_lb['funding_start']


  df_lb['pnl'] = df_lb['margin_change'] - df_lb['deposits_change'] + df_lb['withdrawals_change'] + df_lb['upnl_change'] + df_lb['funding_change']
  df_lb['pnl_pct'] = df_lb['pnl'] / (df_lb['margin_start'] + df_lb['deposits_change']).apply(lambda x: max(500, x))

  df_lb.sort_values('pnl_pct', ascending=False)[[
      'account',
      'pnl',
      'pnl_pct'
  ]]

  df_lb.sort_values('pnl_pct', ascending=False).to_csv(
      'data/pnl_result.csv', index=False)

if __name__ == '__main__':
  asyncio.run(main())