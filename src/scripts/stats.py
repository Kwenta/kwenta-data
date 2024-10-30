import os
import asyncio
import pandas as pd
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from decimal import Decimal
from web3 import Web3
import nest_asyncio

nest_asyncio.apply()

# Constants
INFURA_KEY = os.getenv("INFURA_KEY")

# Stats configurations for different versions
V3_Query = {
    "aggregate_stats": {
        "query": gql(
            """
                    query aggregateStats($last_id: ID!) {
                        perpsV3AggregateStats(
                            where: {
                                id_gt: $last_id,
                                period: "86400",
                                marketId: "0",
                            },
                            first: 1000
                        ) {
                            id
                            timestamp
                            volume
                            trades
                        }
                    }
                """
        ),
        "accessor": "perpsV3AggregateStats",
    },
    "traders": {
        "query": gql(
            """
                    query traders($last_id: ID!) {
                        orderSettleds(
                            where: {
                                id_gt: $last_id,
                            },
                            first: 1000
                        ) {
                            id
                            accountId
                            timestamp
                        }
                    }
                """
        ),
        "accessor": "orderSettleds",
    },
}

CONFIGS = {
    "v3": {
        "subgraph_endpoint": "https://subgraph.satsuma-prod.com/05943208e921/kwenta/base-perps-v3/api",
        "rpc_endpoint": f"https://base-mainnet.infura.io/v3/{INFURA_KEY}",
        "queries": V3_Query,
    },
    "v3_arb": {
        "subgraph_endpoint": "https://subgraph.satsuma-prod.com/05943208e921/kwenta/arbitrum-one-perps-v3/api",
        "rpc_endpoint": f"https://arbitrum-mainnet.infura.io/v3/{INFURA_KEY}",
        "queries": V3_Query,
    },
    "v2": {
        "subgraph_endpoint": "https://subgraph.satsuma-prod.com/05943208e921/kwenta/optimism-perps/api",
        "rpc_endpoint": f"https://optimism-mainnet.infura.io/v3/{INFURA_KEY}",
        "queries": {
            "aggregate_stats": {
                "query": gql(
                    """
                    query aggregateStats($last_id: ID!) {
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
                """
                ),
                "accessor": "futuresAggregateStats",
            },
            "traders": {
                "query": gql(
                    """
                    query traders($last_id: ID!) {
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
                """
                ),
                "accessor": "futuresTrades",
            },
        },
    },
    "perennial": {
        "subgraph_endpoint": "https://subgraph.perennial.finance/arbitrum",
        "rpc_endpoint": f"https://arbitrum-mainnet.infura.io/v3/{INFURA_KEY}",
        "queries": {
            "aggregate_stats": {
                "query": gql(
                    """
                    query aggregateStats($last_id: Bytes!) {
                        marketAccumulations(
                            where: {
                                id_gt: $last_id,
                                bucket: daily,
                            },
                            first: 1000
                        ) {
                            id
                            timestamp
                            longNotional
                            shortNotional
                            trades
                            traders
                        }
                    }
                """
                ),
                "accessor": "marketAccumulations",
            },
        },
    },
}


# Functions
def convert_decimals(x):
    return Decimal(x) / Decimal(10**18)


def convert_bytes(x):
    return bytearray.fromhex(x[2:]).decode().replace("\x00", "")


def clean_df(df, decimal_cols=[], bytes_cols=[]):
    for col in decimal_cols:
        if col in df.columns:
            df[col] = df[col].apply(convert_decimals)
        else:
            print(f"{col} not in DataFrame")
    for col in bytes_cols:
        if col in df.columns:
            df[col] = df[col].apply(convert_bytes)
        else:
            print(f"{col} not in DataFrame")
    return df


async def run_recursive_query(query, params, accessor, endpoint):
    headers = {"origin": "https://subgraph.satsuma-prod.com"}
    transport = AIOHTTPTransport(url=endpoint, headers=headers)
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
                params["last_id"] = all_results[-1]["id"]
            else:
                done_fetching = True
        return pd.DataFrame(all_results)


async def main(config_key):
    config = CONFIGS[config_key]

    # Aggregate data query and cleaning
    agg_query = config["queries"]["aggregate_stats"]
    df_agg = pd.DataFrame(
        await run_recursive_query(
            agg_query["query"],
            {"last_id": ""},
            agg_query["accessor"],
            config["subgraph_endpoint"],
        )
    )
    print(f"agg result size: {df_agg.shape[0]}")
    df_agg = (
        clean_df(
            df_agg,
            decimal_cols=(
                ["volume", "feesSynthetix", "feesKwenta"]
                if config_key == "v2"
                else ["volume"]
            ),
        )
        .drop("id", axis=1)
        .sort_values("timestamp")
    )
    df_agg["timestamp"] = df_agg["timestamp"].astype(int)
    df_agg["trades"] = df_agg["trades"].astype(int)

    if config_key == "perennial":
        df_agg["volume"] = (
            df_agg["longNotional"].astype(float) / 1_000_000
            + df_agg["shortNotional"].astype(float) / 1_000_000
        )
        df_agg["uniqueTraders"] = df_agg["traders"].astype(int)

        df_agg = df_agg.groupby("timestamp").sum().reset_index()
        df_agg["cumulativeTrades"] = df_agg["trades"].cumsum()
        df_agg["cumulativeTraders"] = df_agg["uniqueTraders"].cumsum()
        df_write = df_agg[
            [
                "timestamp",
                "volume",
                "trades",
                "cumulativeTrades",
                "uniqueTraders",
                "cumulativeTraders",
            ]
        ]
    else:
        df_agg["cumulativeTrades"] = df_agg["trades"].cumsum()
        # Trader data query and processing
        trader_query = config["queries"]["traders"]
        df_trader = (
            pd.DataFrame(
                await run_recursive_query(
                    trader_query["query"],
                    {"last_id": ""},
                    trader_query["accessor"],
                    config["subgraph_endpoint"],
                )
            )
            .drop("id", axis=1)
            .sort_values("timestamp")
        )
        df_trader["dateTs"] = df_trader["timestamp"].apply(
            lambda x: int(int(x) / 86400) * 86400
        )
        df_trader["cumulativeTraders"] = (
            ~df_trader["accountId" if "v3" in config_key else "account"].duplicated()
        ).cumsum()
        df_trader_agg = (
            df_trader.groupby("dateTs")[
                "accountId" if "v3" in config_key else "account"
            ]
            .nunique()
            .reset_index()
        )
        df_trader_agg.columns = ["timestamp", "uniqueTraders"]
        df_trader_agg["cumulativeTraders"] = (
            df_trader.groupby("dateTs")["cumulativeTraders"]
            .max()
            .reset_index()["cumulativeTraders"]
        )

        print(f"trader result size: {df_trader.shape[0]}")
        print(f"trader agg result size: {df_trader_agg.shape[0]}")

        # Combine the two datasets
        df_write = df_agg.merge(df_trader_agg, on="timestamp")

    # Ensure directory exists
    outdir = f"data/stats"
    os.makedirs(outdir, exist_ok=True)

    # Write out JSON data
    filename = (
        "daily_stats.json"
        if config_key == "v2"
        else f"daily_stats_{
            config_key}.json"
    )
    df_write.to_json(f"{outdir}/{filename}", orient="records")

    # Print results
    print(f"Combined result size for {config_key}: {df_write.shape[0]}")


if __name__ == "__main__":
    asyncio.run(main("v3"))
    asyncio.run(main("v3_arb"))
    asyncio.run(main("v2"))
    asyncio.run(main("perennial"))
