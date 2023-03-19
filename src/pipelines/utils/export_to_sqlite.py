import asyncio
import json
import sqlite3
import argparse
import pandas as pd
from decimal import Decimal
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
import nest_asyncio

nest_asyncio.apply()


def read_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)


def create_table(cursor, table_name, fields):
    fields_sql = ', '.join([f"{key} {value}" for key, value in fields.items()])
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} ({fields_sql})")

# functions
def convertDecimals(x): 
    try:
        return float(Decimal(x) / Decimal(10**18))
    except:
        return x

def convertBytes(x): return bytearray.fromhex(
    x[2:]).decode().replace('\x00', '')

def clean_df(df, types):
    for col in df.columns:
        type = types[col]
        if type == 'decimal':
            df[col] = df[col].apply(convertDecimals)
        elif type == 'bytes':
            df[col] = df[col].apply(convertBytes)
    return df


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


async def run_recursive_query(query, params, accessor, url):
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

    # Call the create_table function to drop and re-create the table
    create_table(cursor, table_name, fields)

    # Define your GraphQL query
    query_fields = ', '.join(graphql_query_fields)
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

    # Fetch data from the GraphQL API
    response = await run_recursive_query(query, {'last_id': ''}, graphql_entity, url=url)
    events = clean_df(response, graphql_types)

    # Prepare the data for insertion
    data_to_insert = [tuple(event[1].values) for event in events.iterrows()]

    # Insert the data into the SQLite database using executemany
    placeholders = ', '.join(['?' for _ in graphql_query_fields])
    cursor.executemany(
        f"INSERT INTO {table_name} ({', '.join(graphql_query_fields)}) VALUES ({placeholders})", data_to_insert)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

if __name__ == '__main__':
    asyncio.run(main())
