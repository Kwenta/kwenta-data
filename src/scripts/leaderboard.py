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


# queries
userMarginAccountsBlock = gql("""
query marginAccounts($block_number: Int!) {
  futuresMarginAccounts(
    block: {
      number: $block_number
    }
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

allMarginAccountsBlock = gql("""
query marginAccounts($block_number: Int!) {
  futuresMarginAccounts(
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
query openPositions($block_number: Int!) {
  futuresPositions(
    where: {
      isOpen: true
    }
    block: {
      number: $block_number
    }
    first: 1000
  ) {
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
          'block_number': block
      }

      decimal_cols = [
          'margin',
          'deposits',
          'withdrawals'
      ]

      margin_response = await run_query(allMarginAccountsBlock, params)
      df_margin = pd.DataFrame(margin_response['futuresMarginAccounts'])
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
  params = {
      'block_number': lb_blocks[1]
  }

  open_position_response = await run_query(openPositionsBlock, params)
  df_positions = pd.DataFrame(open_position_response['futuresPositions'])

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
      block_id=lb_blocks[1],
      _w3=w3
  )

  fundingMulti = Multicall(
      fundingCalls,
      block_id=lb_blocks[1],
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


  ## Calculate the leaderboard
  # get the start and end data
  start_df = lb_results[lb_blocks[0]]
  end_df = lb_results[lb_blocks[1]]

  # merge together
  df_lb = start_df.merge(
    end_df,
    on='account',
    how='outer',
    suffixes=('_start', '_end')
  )

  df_lb = df_lb.merge(
    df_pnl,
    how='outer',
    on='account'
  )

  # calculated fields
  df_lb['margin_change'] = df_lb['margin_end'] - df_lb['margin_start']
  df_lb['deposits_change'] = df_lb['deposits_end'] - df_lb['deposits_start']
  df_lb['withdrawals_change'] = df_lb['withdrawals_end'] - df_lb['withdrawals_start']
  
  # fix missing data
  df_lb['upnl'] = df_lb['upnl'].fillna(0)
  df_lb['funding'] = df_lb['funding'].fillna(0)

  df_lb['pnl'] = df_lb['margin_change'] - df_lb['deposits_change'] + df_lb['withdrawals_change'] + df_lb['upnl'] + df_lb['funding']
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