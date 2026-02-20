"""ClickHouse utilities for Binance ETL pipeline"""
import os
import time
import pandas as pd
from clickhouse_driver import Client


def connect_clickhouse(host='localhost', port=9000, database='binance_v2',
                       username='claude', password='iamsentient', secure=False):
    """Initialize connection to ClickHouse database"""
    try:
        if isinstance(port, str):
            port = int(port)

        client = Client(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            secure=secure,
            settings={
                'use_numpy': True,
                'max_memory_usage': 16 * 1024 * 1024 * 1024,  # 16GB
                'max_threads': min((os.cpu_count() or 1) - 1, 8) if (os.cpu_count() or 1) > 1 else 1
            }
        )

        return client

    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        raise


def clickhouse_query(client, query, params=None):
    """Execute query and return results as DataFrame"""
    start_time = time.time()
    result = client.execute(query, params, with_column_types=True)
    df = pd.DataFrame(result[0], columns=[col[0] for col in result[1]])
    end_time = time.time()
    print(f'Elapsed time: {end_time - start_time:.4f} seconds')
    return df


def clickhouse_insert(client, table: str, df: pd.DataFrame):
    """Insert DataFrame into ClickHouse table"""
    if df.empty:
        return 0

    # Use columnar insert with numpy arrays
    columns = list(df.columns)
    data = [df[col].to_numpy() for col in columns]

    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
    client.execute(query, data, columnar=True)
    return len(df)
