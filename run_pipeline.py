#!/usr/bin/env python3
"""
Binance ETL Pipeline Runner
Runs the pipeline on a schedule, configurable via config.json
"""
import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent / 'app' / 'src'))

from crypto_data_pipeline_clickhouse import CryptoDataPipeline, load_config
from utils_clickhouse import connect_clickhouse, clickhouse_query


def run_pipeline(config_path: str = None):
    """Run the pipeline once"""
    config = load_config(config_path)
    ch_config = config['clickhouse']

    client = connect_clickhouse(
        host=ch_config['host'],
        port=ch_config['port'],
        database=ch_config['database'],
        username=ch_config['username'],
        password=ch_config['password'],
        secure=ch_config.get('secure', False)
    )

    print(f"[{datetime.now(timezone.utc)}] Starting pipeline run...")

    try:
        pipeline = CryptoDataPipeline(
            con=client,
            config=config
        )

        pipeline.update_all()

        # Log success
        log_run(client, 'success', 'Pipeline completed successfully')
        print(f"[{datetime.now(timezone.utc)}] Pipeline run completed")

        return True, client

    except Exception as e:
        print(f"[{datetime.now(timezone.utc)}] Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        try:
            log_run(client, 'failure', str(e))
        except:
            pass
        return False, client


def log_run(client, status: str, comment: str = ''):
    """Log pipeline run to ClickHouse"""
    db = 'binance_v2'
    try:
        # Ensure log table exists
        client.execute(f"""
            CREATE TABLE IF NOT EXISTS {db}.pipeline_logs (
                timestamp DateTime DEFAULT now(),
                status LowCardinality(String),
                comment String
            )
            ENGINE = MergeTree()
            ORDER BY timestamp
        """)

        client.execute(
            f"INSERT INTO {db}.pipeline_logs (timestamp, status, comment) VALUES",
            [{'timestamp': datetime.now(timezone.utc), 'status': status, 'comment': comment}]
        )
    except Exception as e:
        print(f"Failed to log run: {e}")


def main():
    """Main entry point - runs continuously on schedule"""
    config = load_config()
    interval = config['rate_limits'].get('update_interval_seconds', 3600)

    print(f"Starting Binance ETL Pipeline")
    print(f"Update interval: {interval} seconds")
    print(f"Intervals to fetch: {config['bars']['intervals']}")

    while True:
        success, client = run_pipeline()

        print(f"Sleeping for {interval} seconds...")
        time.sleep(interval)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        # Run once and exit
        run_pipeline()
    else:
        # Run continuously
        main()
