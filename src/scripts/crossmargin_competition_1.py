import os
import asyncio
import pandas as pd
from datetime import datetime, timezone
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from decimal import Decimal
from multicall import Call, Multicall
from web3 import Web3
import nest_asyncio

nest_asyncio.apply()

## input parameters
DATE_COMPETITION_START = '2022-10-31'
DATE_COMPETITION_END = '2022-11-16'

## constants
INFURA_KEY = os.getenv('INFURA_KEY')

# mainnet
FUTURES_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/kwenta/optimism-futures'
RPC_ENDPOINT = f'https://optimism-mainnet.infura.io/v3/{INFURA_KEY}'

# testnet
# FUTURES_ENDPOINT = 'https://api.thegraph.com/subgraphs/name/kwenta/optimism-goerli-futures'
# RPC_ENDPOINT = f'https://optimism-goerli.infura.io/v3/{INFURA_KEY}'

# get a web3 provider
w3 = Web3(Web3.HTTPProvider(RPC_ENDPOINT))

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

def get_trading_tier(volume):
  if volume > 250000:
    return 'gold'
  elif volume > 50000:
    return 'silver'
  else:
    return 'bronze'


# queries
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
      accountType: cross_margin
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
  $block_number: Int!
  $last_id: ID!
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
  ## Query all users between two blocks
  blocks = pd.read_json('data/blocks_mainnet.json')
  
  ts_start = datetime.strptime(DATE_COMPETITION_START, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000
  ts_end = datetime.strptime(DATE_COMPETITION_END, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000

  blocks = blocks[(blocks['ts'] <= ts_end) & (blocks['ts'] >= ts_start)]

  if blocks.shape[0] >= 2:
    lb_blocks = [
        int(blocks.head(1).iloc[0]['block']),
        int(blocks.tail(1).iloc[-1]['block'])
    ]
  else:
    raise RuntimeError("The block file does not contain blocks during the competition window")

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
      margin_params = {
          'block_number': block,
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
      # inner join will exclude all isolated margin accounts
      df_margin = df_margin.merge(
          df_cm_account, left_on='account', right_on='crossMarginAccount'
      )

      # summarize margin data by account
      df_margin = df_margin.groupby('accountOwner')[[
          'margin',
          'deposits',
          'withdrawals'
      ]].sum().reset_index()

      # merge with stat data
      df_stats = df_stats.merge(
          df_margin,
          how='left',
          left_on='account',
          right_on='accountOwner'
      ).drop('accountOwner', axis=1).fillna(0)

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

    df_pnl = df_pnl.merge(df_funding, on='abstractAccount').merge(df_cm_account, left_on='abstractAccount', right_on='crossMarginAccount')

    df_pnl = df_pnl[[
      'accountOwner',
      'upnl',
      'funding'
    ]]
    df_pnl.columns = ['account', 'upnl', 'funding']

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
  df_lb['volume_change'] = df_lb['crossMarginVolume_end'] - df_lb['crossMarginVolume_start']
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
  df_lb = df_lb[df_lb['volume_change'] > 0]

  # add tier and rank
  df_lb['tier'] = df_lb['volume_change'].apply(get_trading_tier)
  df_lb['rank'] = df_lb.groupby('tier')['pnl_pct'].rank('dense', ascending=False)

  # export the data
  write_cols = [
    'account',
    'tier',
    'rank',
    'volume_change',
    'trades_change',
    'liquidations_change',
    'pnl',
    'pnl_pct'
  ]

  df_write = df_lb[write_cols].sort_values(['tier', 'pnl_pct'], ascending=False)
  df_write.columns = [col.replace('_change', '') for col in df_write.columns]

  # make sure the directory exists
  outdir = 'data/crossmargin_competition_1'
  if not os.path.exists(outdir):
    os.mkdir(outdir)

  df_write.to_json(
      f'{outdir}/leaderboard_latest.json',
      orient='records'
  )

  print(f"WRITTEN: cross margin leaderboard: {df_write.shape[0]} participants")

if __name__ == '__main__':
  asyncio.run(main())