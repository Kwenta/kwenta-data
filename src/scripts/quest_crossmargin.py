import os
import asyncio
import pandas as pd
from datetime import datetime, timezone
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from decimal import Decimal
from multicall import Call, Multicall
from web3 import Web3
from web3.middleware import geth_poa_middleware
import nest_asyncio

nest_asyncio.apply()

## constants
INFURA_KEY = os.getenv('INFURA_KEY')

# mainnet
FUTURES_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/kwenta/optimism-futures'
RPC_ENDPOINT = f'https://optimism-mainnet.infura.io/v3/{INFURA_KEY}'

# get a web3 provider
w3 = Web3(Web3.HTTPProvider(RPC_ENDPOINT))
w3.middleware_onion.inject(geth_poa_middleware, layer=0)

# functions
def convertDecimals(value):
  return Decimal(value) / Decimal(10**18)


def clean_df(df, decimal_cols, number_cols=[]):
  for col in decimal_cols:
    if col in df.columns:
      df[col] = df[col].apply(convertDecimals)
    else:
      print(f"{col} not in DataFrame")

  for col in number_cols:
    if col in df.columns:
      df[col] = df[col].astype(float)
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
latestBlock = gql("""
  query latestBlock {
    _meta {
      block {
        number
      }
    }
  }
""")

crossMarginAccounts = gql("""
query marginAccounts(
  $last_id: ID!
) {
  crossMarginAccounts(
    where: {
      id_gt: $last_id
    }
    first: 1000
  ) {
    id
    owner
  }  
}
""")

marginAccountsBlock = gql("""
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
    abstractAccount
    market
  }  
}
""")

accountStatsBlock = gql("""
query accountStats(
  $last_id: ID!
  $block_number: Int!
) {
  futuresStats(
    where: {
      id_gt: $last_id
    }
    block: {
      number: $block_number
    }
    first: 1000
  ) {
    id
    account
    liquidations
    totalTrades
    totalVolume
    crossMarginVolume
  }
}
""")

async def main():
  ## Query all users between a start block and the latest
  # get the latest block
  # block = w3.eth.get_block('latest')
  block_response = await run_query(latestBlock, {})
  block = block_response['_meta']['block']

  lb_blocks = [
      23987945,
      block['number']
  ]

  # get cross margin accounts
  cross_margin_params = {
      'last_id': ''
  }
  print(f'FETCHING: cross margin accounts: block current')
  cm_account_response = await run_recursive_query(crossMarginAccounts, cross_margin_params, 'crossMarginAccounts', endpoint=FUTURES_ENDPOINT)
  df_cm_account = pd.DataFrame(cm_account_response)
  print(f'RECEIVED: cross margin accounts: block current: {df_cm_account.shape[0]} accounts')
  df_cm_account.columns = ['crossMarginAccount', 'accountOwner']

  lb_results = {}
  for block in lb_blocks:
      # get margin data
      if block:
        margin_params = {
            'block_number': block,
            'last_id': ''
        }
      else:
        margin_params = {
            'last_id': ''
        }

      margin_decimal_cols = [
          'margin',
          'deposits',
          'withdrawals'
      ]

      print(f'FETCHING: margin accounts: block {block}')
      margin_response = await run_recursive_query(marginAccountsBlock, margin_params, 'futuresMarginAccounts')
      df_margin = pd.DataFrame(margin_response)
      print(f'RECEIVED: margin accounts: block {block}: {df_margin.shape[0]} accounts')
      df_margin = clean_df(df_margin, margin_decimal_cols)

      # get stat data
      stat_params = {
          'block_number': block,
          'last_id': ''
      }

      stat_decimal_cols = [
          'totalVolume',
          'crossMarginVolume'
      ]

      stat_number_cols = [
          'totalTrades',
          'liquidations'
      ]

      print(f'FETCHING: account stats: block {block}')
      stats_response = await run_recursive_query(accountStatsBlock, stat_params, 'futuresStats')
      df_stats = pd.DataFrame(stats_response)
      df_stats = clean_df(df_stats, stat_decimal_cols, stat_number_cols)
      print(f'RECEIVED: account stats: block {block}: {df_stats.shape[0]} accounts')

      # merge cross margin accounts and fix columns
      df_margin = df_margin.merge(
          df_cm_account, how='left', left_on='account', right_on='crossMarginAccount'
      )

      # fix the account field
      df_margin['account'] = df_margin.apply(lambda row: row['accountOwner'] if pd.notnull(row['accountOwner']) else row['account'], axis=1)

      # summarize margin data by account
      df_margin = df_margin.groupby('account')[[
          'margin',
          'deposits',
          'withdrawals'
      ]].sum().reset_index()

      # merge with stat data
      df_stats = df_stats.merge(
          df_margin,
          on='account'
      ).fillna(0)

      lb_results[block] = df_stats

  ## Get unrealized pnl from contracts
  # get list of open positions
  upnl_results = {}
  for block in lb_blocks:
    params = {
        'block_number': block,
        'last_id': ''
    }

    print(f'FETCHING: open positions: block {block}')
    open_position_response = await run_recursive_query(openPositionsBlock, params, 'futuresPositions')
    df_positions = pd.DataFrame(open_position_response)
    print(f'RECEIVED: open positions: block {block}: {df_positions.shape[0]} accounts')

    # use multicall to get current unrealized pnl and funding
    pnlCalls = [
        Call(
            row['market'],
            ['profitLoss(address)(int256)', row['abstractAccount']],
            [[f"{row['abstractAccount']},{row['market']}", convertDecimals]]
        ) for _, row in df_positions.iterrows()
    ]

    fundingCalls = [
        Call(
            row['market'],
            ['accruedFunding(address)(int256)', row['abstractAccount']],
            [[f"{row['abstractAccount']},{row['market']}", convertDecimals]]
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

    print(f'FETCHING: open position pnl: block {block}')
    pnlResult = pnlMulti()
    print(f'RECEIVED: open position pnl: block {block}: {len(pnlResult)} accounts')

    print(f'FETCHING: open position funding: block {block}')
    fundingResult = fundingMulti()
    print(f'RECEIVED: open position funding: block {block}: {len(fundingResult)} accounts')

    # clean the pnl
    pnlResult = [(k.split(',')[0], k.split(',')[1], v) for k, v in pnlResult.items()]
    fundingResult = [(k.split(',')[0], k.split(',')[1], v) for k, v in fundingResult.items()]

    df_pnl = pd.DataFrame.from_records(pnlResult, columns=['abstractAccount', 'market', 'upnl']).groupby('abstractAccount')[['upnl']].sum().reset_index()
    df_funding = pd.DataFrame.from_records(fundingResult, columns=['abstractAccount', 'market', 'funding']).groupby('abstractAccount')[['funding']].sum().reset_index()

    df_pnl = df_pnl.merge(df_funding, on='abstractAccount').merge(df_cm_account, how='left', left_on='abstractAccount', right_on='crossMarginAccount')

    # fix the account field
    df_pnl['account'] = df_pnl.apply(lambda row: row['accountOwner'] if pd.notnull(
        row['accountOwner']) else row['abstractAccount'], axis=1)

    df_pnl = df_pnl[[
      'account',
      'upnl',
      'funding'
    ]].groupby('account').sum().reset_index()

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
  df_lb['volume_change'] = df_lb['totalVolume_end'] - df_lb['totalVolume_start']
  df_lb['crossMarginVolume_change'] = df_lb['crossMarginVolume_end'] - df_lb['crossMarginVolume_start']
  df_lb['liquidations_change'] = df_lb['liquidations_end'] - df_lb['liquidations_start']
  df_lb['trades_change'] = df_lb['totalTrades_end'] - df_lb['totalTrades_start']
  df_lb['margin_change'] = df_lb['margin_end'] - df_lb['margin_start']
  df_lb['deposits_change'] = df_lb['deposits_end'] - df_lb['deposits_start']
  df_lb['withdrawals_change'] = df_lb['withdrawals_end'] - \
      df_lb['withdrawals_start']
  df_lb['upnl_change'] = df_lb['upnl_end'] - df_lb['upnl_start']
  df_lb['funding_change'] = df_lb['funding_end'] - df_lb['funding_start']

  df_lb['pnl'] = df_lb['margin_change'] - df_lb['deposits_change'] + \
      df_lb['withdrawals_change'] + \
      df_lb['upnl_change'] + df_lb['funding_change']
  df_lb['pnl_pct'] = df_lb['pnl'] / \
      (df_lb['margin_start'] + df_lb['upnl_start'] + df_lb['funding_start'] + df_lb['deposits_change']
       ).apply(lambda x: max(500, x))
  
  # filter people with no volume
  df_lb = df_lb[df_lb['crossMarginVolume_change'] >= 99]

  # read the static file
  df_static = pd.read_csv('src/scripts/static/op_quest_export.csv')

  # export the data
  write_cols = [
    'account',
    'volume_change',
    'crossMarginVolume_change',
    'trades_change',
    'liquidations_change',
    'pnl',
    'pnl_pct'
  ]

  df_write = df_lb[write_cols].sort_values('pnl_pct', ascending=False)
  df_write.columns = [col.replace('_change', '') for col in df_write.columns]

  # combine the files and append the new addresses
  df_append = df_write[~df_write['account'].isin(df_static['account'])]
  print(f'append size before: {df_append.shape[0]}')
  print(f'static size before: {df_static.shape[0]}')
  df_static = pd.concat([df_static, df_append])
  print(f'static size after: {df_static.shape[0]}')

  # make sure the directory exists
  outdir = 'data'
  if not os.path.exists(outdir):
    os.mkdir(outdir)

  df_static.to_csv(
    f'{outdir}/op_quest.csv',
    index=False
  )

if __name__ == '__main__':
  asyncio.run(main())