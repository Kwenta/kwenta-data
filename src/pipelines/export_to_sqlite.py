import asyncio
import json
import sqlite3
import argparse
import pandas as pd
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
import nest_asyncio
from utils.data import clean_df
from copy import deepcopy

nest_asyncio.apply()


def read_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)


def create_table(cursor, table_name, fields):
    fields_sql = ', '.join([f"{key} {value}" for key, value in fields.items()])
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} ({fields_sql})")


def get_last_ts(conn, table_name):
    df_last_ts = pd.read_sql_query(f"SELECT timestamp FROM {table_name} ORDER BY timestamp DESC LIMIT 1", conn)
    if(df_last_ts.shape[0] != 1):
        last_ts = ''
    else:
        last_ts = df_last_ts['timestamp'][0]
    return last_ts


async def run_query(query, params, url):
    transport = AIOHTTPTransport(url=url)

    async with Client(
        transport=transport,
        fetch_schema_from_transport=True,
    ) as session:

        # Execute single query
        query = query

        result = await session.execute(query, variable_values=params)
        df = pd.DataFrame(result)
        return df


async def run_refresh_query(query, params, accessor, url):
    transport = AIOHTTPTransport(url=url)

    async with Client(
        transport=transport,
        fetch_schema_from_transport=True,
    ) as session:
        done_fetching = False
        all_results = []
        last_result = None
        while not done_fetching:
            result = await session.execute(query, variable_values=params)
            if result != last_result:
                all_results.extend(result[accessor])
                params['last_ts'] = all_results[-1]['timestamp']
                last_result = deepcopy(result)
            else:
                done_fetching = True

        df = pd.DataFrame(all_results)
        return df


async def run_backfill_query(query, params, accessor, url):
    transport = AIOHTTPTransport(url=url)

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


async def main():
    parser = argparse.ArgumentParser(
        description="Export data from a GraphQL API to an SQLite database")
    parser.add_argument('-c', '--config', required=True,
                        help="Path to the JSON configuration file")
    parser.add_argument('-b', '--backfill', required=False, type=bool,
                        help="Remove the table and backfill from the beginning")
    args = parser.parse_args()

    config = read_config(args.config)
    table_name = config['table_name']
    graphql_entity = config['graphql_entity']
    fields = config['fields']
    url = config['graphql_url']
    graphql_types = config['graphql_types']
    graphql_query_fields = list(graphql_types.keys())

    # Connect to your SQLite database
    conn = sqlite3.connect(config['database_file'])
    cursor = conn.cursor()

    # Define your GraphQL query
    query_fields = ', '.join(graphql_query_fields)

    # Set up tables if this is a backfill
    if(args.backfill == True):
        # Call the create_table function to drop and re-create the table
        print(f"DROPPING TABLE {table_name}")
        create_table(cursor, table_name, fields)
        query = gql(f"""
            query(
                $last_id: ID!
            ) {{
            {graphql_entity}(
                where: {{
                    id_gt: $last_id
                }}
                first: 1000
            ) {{
                {query_fields}
            }}
            }}
        """)

        params = {'last_id': ''}

        # Fetch data from the GraphQL API
        response = await run_backfill_query(query, params, graphql_entity, url=url)
        events = clean_df(response, graphql_types).drop_duplicates()

    else:
        last_ts = get_last_ts(conn, table_name)
        query = gql(f"""
            query(
                $last_ts: BigInt!
            ) {{
            {graphql_entity}(
                where: {{
                    timestamp_gte: $last_ts
                }}
                orderBy: timestamp
                orderDirection: asc
                first: 1000
            ) {{
                {query_fields}
            }}
            }}
        """)
        params = {'last_ts': f'{last_ts}'}

        # Fetch data from the GraphQL API
        response = await run_refresh_query(query, params, graphql_entity, url=url)
        events = clean_df(response, graphql_types).drop_duplicates()

    # Prepare the data for insertion
    data_to_insert = [tuple(event[1].values) for event in events.iterrows()]
    print(f'INSERTING {events.shape[0]} ROWS')

    # Insert the data into the SQLite database using executemany
    placeholders = ', '.join(['?' for _ in graphql_query_fields])
    cursor.executemany(
        f"INSERT OR REPLACE INTO {table_name} ({', '.join(graphql_query_fields)}) VALUES ({placeholders})", data_to_insert)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

if __name__ == '__main__':
    asyncio.run(main())
