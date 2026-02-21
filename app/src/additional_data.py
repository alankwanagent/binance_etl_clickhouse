"""
Additional Binance data types: funding rates, margin interest rates
"""
import time
import logging
import threading
from typing import Union, Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from tqdm import tqdm
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential

from utils_clickhouse import clickhouse_query, clickhouse_insert
from pathlib import Path

# Cache directory
CACHE_DIR = Path(__file__).parent.parent.parent / 'cache'


class FundingRatesFetcher:
    """Fetch funding rates from Binance USD-M futures"""

    FR_LIMIT = 1000
    FR_PERIOD = 60 * 5  # 5 minutes

    def __init__(self, um_futures_client, con, config=None):
        self.client = um_futures_client
        self.con = con
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
    @sleep_and_retry
    @limits(calls=FR_LIMIT, period=FR_PERIOD)
    def _rate_limited_funding_rates(self, symbol: str, **kwargs):
        try:
            return self.client.funding_rate(symbol=symbol, **kwargs)
        except Exception as e:
            if '418' in str(e):
                self._handle_rate_limit(e)
                return self._rate_limited_funding_rates(symbol, **kwargs)
            raise

    def _handle_rate_limit(self, e):
        import re
        error_str = str(e)
        if '418' in error_str:
            match = re.search(r'banned until (\d+)', error_str)
            if match:
                ban_ts = int(match.group(1))
                wait_time = (ban_ts - int(time.time() * 1000)) / 1000
                if wait_time > 0:
                    self.logger.warning(f"IP banned, waiting {wait_time:.0f}s")
                    time.sleep(wait_time + 60)

    def get_funding_rate(self, symbol: str, start_time: int = None, end_time: int = None, limit: int = 1000):
        try:
            data = self._rate_limited_funding_rates(
                symbol=symbol, startTime=start_time, endTime=end_time, limit=limit
            )
            return pd.DataFrame(data) if data else pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error fetching funding rate for {symbol}: {e}")
            raise

    def get_historical_funding_rates(self, symbol: str, start_time, end_time, delivery_date='2100-12-25'):
        all_rates = []
        start_ms = int(pd.Timestamp(start_time).timestamp() * 1000) if isinstance(start_time, str) else start_time
        end_ms = int(pd.Timestamp(end_time).timestamp() * 1000) if isinstance(end_time, str) else end_time

        if delivery_date:
            delivery_ms = int(pd.Timestamp(delivery_date).timestamp() * 1000)
            end_ms = min(end_ms, delivery_ms)

        current_start = start_ms
        while current_start <= end_ms:
            df = self.get_funding_rate(symbol=symbol, start_time=current_start, end_time=end_ms, limit=1000)
            if df.empty:
                break
            all_rates.append(df)
            current_start = int(df['fundingTime'].iloc[-1]) + 1

        if all_rates:
            result = pd.concat(all_rates)
            result = result.sort_values(['fundingTime']).drop_duplicates(subset=['fundingTime'], keep='last')
            result['symbol'] = symbol
            return result
        return pd.DataFrame()

    def fetch_all_to_cache(self, start_time, end_time, cache_dir=None):
        cache_dir = cache_dir or CACHE_DIR / 'funding_rates'
        cache_dir.mkdir(parents=True, exist_ok=True)

        progress_file = cache_dir / 'progress.json'
        import json
        completed = set()
        if progress_file.exists():
            completed = set(json.load(open(progress_file)).get('completed', []))

        symbols_df = clickhouse_query(self.con,
            f"SELECT symbol, delivery_date FROM bn_perp_symbols WHERE delivery_date >= '{pd.Timestamp(start_time)}'")

        symbols_to_fetch = [s for s in symbols_df['symbol'].tolist() if s not in completed]
        self.logger.info(f"Fetching funding rates for {len(symbols_to_fetch)} symbols")

        lock = threading.Lock()
        total_rows = 0

        def process(symbol, delivery_date):
            nonlocal total_rows
            try:
                df = self.get_historical_funding_rates(symbol, start_time, end_time, delivery_date)
                if not df.empty:
                    df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
                    df['fundingRate'] = pd.to_numeric(df['fundingRate'], errors='coerce').fillna(0)
                    df['markPrice'] = pd.to_numeric(df.get('markPrice', 0), errors='coerce').fillna(0)
                    df['exchange'] = 'binance'
                    df['type'] = 'PERPETUAL'
                    df = df[['symbol', 'exchange', 'type', 'fundingTime', 'fundingRate', 'markPrice']]

                    parquet_file = cache_dir / f"{symbol}.parquet"
                    df.to_parquet(parquet_file, index=False)

                    with lock:
                        completed.add(symbol)
                        total_rows += len(df)
                        if len(completed) % 20 == 0:
                            json.dump({'completed': list(completed)}, open(progress_file, 'w'))
                    self.logger.info(f"[{len(completed)}] {symbol}: {len(df)} funding rates")
            except Exception as e:
                self.logger.error(f"Error {symbol}: {e}")

        with ThreadPoolExecutor(max_workers=4) as executor:
            list(tqdm(executor.map(lambda r: process(r['symbol'], r['delivery_date']), symbols_df.to_dict('records')),
                     total=len(symbols_df), desc="Funding rates"))

        json.dump({'completed': list(completed)}, open(progress_file, 'w'))
        self.logger.info(f"Cached {total_rows} funding rates")
        return total_rows


class MarginRatesFetcher:
    """Fetch margin interest rates from Binance spot"""

    MR_LIMIT = 1000
    MR_PERIOD = 60

    def __init__(self, spot_client, con, config=None):
        self.client = spot_client
        self.con = con
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
    @sleep_and_retry
    @limits(calls=MR_LIMIT, period=MR_PERIOD)
    def _rate_limited_margin_rates(self, asset: str, **kwargs):
        try:
            return self.client.margin_interest_rate_history(asset=asset, **kwargs)
        except Exception as e:
            if '-1102' in str(e):  # Asset not supported
                return []
            if '418' in str(e):
                self._handle_rate_limit(e)
                return self._rate_limited_margin_rates(asset, **kwargs)
            raise

    def _handle_rate_limit(self, e):
        import re
        error_str = str(e)
        if '418' in error_str:
            match = re.search(r'banned until (\d+)', error_str)
            if match:
                ban_ts = int(match.group(1))
                wait_time = (ban_ts - int(time.time() * 1000)) / 1000
                if wait_time > 0:
                    time.sleep(wait_time + 60)

    def get_margin_rates(self, asset: str, start_time: int = None, end_time: int = None, vip_level: int = 0):
        try:
            data = self._rate_limited_margin_rates(
                asset=asset, startTime=start_time, endTime=end_time, vipLevel=vip_level
            )
            return pd.DataFrame(data) if data else pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error fetching margin rates for {asset}: {e}")
            raise

    def get_historical_margin_rates(self, asset: str, start_time, end_time, vip_level: int = 0):
        all_rates = []
        start_ms = int(pd.Timestamp(start_time).timestamp() * 1000) if isinstance(start_time, str) else start_time
        end_ms = int(pd.Timestamp(end_time).timestamp() * 1000) if isinstance(end_time, str) else end_time

        current_start = start_ms
        while current_start <= end_ms:
            df = self.get_margin_rates(asset=asset, start_time=current_start, end_time=end_ms, vip_level=vip_level)
            if df.empty:
                break
            all_rates.append(df)
            current_start = int(df['timestamp'].iloc[-1]) + 1

        if all_rates:
            result = pd.concat(all_rates)
            result = result.sort_values(['timestamp']).drop_duplicates(subset=['timestamp'], keep='last')
            result['asset'] = asset
            result['vipLevel'] = vip_level
            return result
        return pd.DataFrame()

    def fetch_all_to_cache(self, start_time, end_time, cache_dir=None):
        cache_dir = cache_dir or CACHE_DIR / 'margin_rates'
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Get unique assets from spot symbols
        assets_df = clickhouse_query(self.con,
            "SELECT DISTINCT base_asset as asset FROM bn_spot_symbols WHERE is_margin_trading_allowed = 1")

        progress_file = cache_dir / 'progress.json'
        import json
        completed = set()
        if progress_file.exists():
            completed = set(json.load(open(progress_file)).get('completed', []))

        assets_to_fetch = [a for a in assets_df['asset'].tolist() if a not in completed]
        self.logger.info(f"Fetching margin rates for {len(assets_to_fetch)} assets")

        lock = threading.Lock()
        total_rows = 0

        def process(asset):
            nonlocal total_rows
            try:
                df = self.get_historical_margin_rates(asset, start_time, end_time)
                if not df.empty:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df['dailyInterestRate'] = pd.to_numeric(df['dailyInterestRate'], errors='coerce').fillna(0)
                    df['exchange'] = 'binance'
                    df['type'] = 'MARGIN'
                    df = df[['asset', 'exchange', 'type', 'timestamp', 'dailyInterestRate', 'vipLevel']]

                    parquet_file = cache_dir / f"{asset}.parquet"
                    df.to_parquet(parquet_file, index=False)

                    with lock:
                        completed.add(asset)
                        total_rows += len(df)
                        if len(completed) % 20 == 0:
                            json.dump({'completed': list(completed)}, open(progress_file, 'w'))
                    self.logger.info(f"[{len(completed)}] {asset}: {len(df)} margin rates")
            except Exception as e:
                self.logger.error(f"Error {asset}: {e}")

        with ThreadPoolExecutor(max_workers=4) as executor:
            list(tqdm(executor.map(process, assets_df['asset'].tolist()),
                     total=len(assets_df), desc="Margin rates"))

        json.dump({'completed': list(completed)}, open(progress_file, 'w'))
        self.logger.info(f"Cached {total_rows} margin rates")
        return total_rows
