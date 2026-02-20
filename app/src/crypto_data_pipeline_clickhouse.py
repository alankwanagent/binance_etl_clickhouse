"""
Binance ETL Pipeline for ClickHouse - Optimized for multiple bar intervals
"""
import os
import re
import json
import time
import logging
import threading
from dataclasses import dataclass
from typing import Dict, List, Any, Callable, Optional, Union
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from tqdm import tqdm
from ratelimit import limits, sleep_and_retry
from tenacity import retry, stop_after_attempt, wait_exponential

from binance.spot import Spot
from binance.um_futures import UMFutures

from utils_clickhouse import connect_clickhouse, clickhouse_query, clickhouse_insert


# Load config
def load_config(config_path: str = None):
    """Load configuration from JSON file"""
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.json')

    with open(config_path, 'r') as f:
        return json.load(f)


class BinanceDataFetcher:
    """Fetch market data from Binance REST API"""

    # Rate limits
    SPOT_WEIGHT_LIMIT = 5500
    SPOT_KLINE_WEIGHT = 2
    FUTURES_WEIGHT_LIMIT = 2300
    FUTURES_KLINE_WEIGHT = 2
    RATE_LIMIT_PERIOD = 60

    SPOT_MAX_WORKERS = 10
    FUTURES_MAX_WORKERS = 8

    def __init__(self, con, api_key: Optional[str] = None, api_secret: Optional[str] = None,
                 config: dict = None):
        self.spot_client = Spot(api_key=api_key, api_secret=api_secret)
        self.um_futures_client = UMFutures(key=api_key, secret=api_secret)
        self.con = con
        self.config = config or load_config()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _handle_rate_limit_error(self, e: Exception) -> None:
        """Handle rate limit and IP ban errors"""
        error_str = str(e)

        if '418' in error_str:
            ban_time_match = re.search(r'banned until (\d+)', error_str)
            if ban_time_match:
                ban_timestamp = int(ban_time_match.group(1))
                current_timestamp = int(time.time() * 1000)
                wait_time = (ban_timestamp - current_timestamp) / 1000

                if wait_time > 0:
                    self.logger.warning(f"IP banned. Waiting {wait_time:.0f} seconds...")
                    time.sleep(wait_time + 60)
                    return

            self.logger.warning("Unparseable IP ban, waiting 3 minutes")
            time.sleep(180)

        elif '429' in error_str:
            self.logger.warning("Rate limit hit, backing off")
            time.sleep(10)
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
    def get_spot_symbols(self) -> pd.DataFrame:
        """Fetch all available spot trading pairs"""
        try:
            exchange_info = self.spot_client.exchange_info(permissions=['SPOT'])
            symbols_data = []

            for symbol in exchange_info['symbols']:
                price_filter = next((f for f in symbol['filters'] if f['filterType'] == 'PRICE_FILTER'), None)
                lot_size = next((f for f in symbol['filters'] if f['filterType'] == 'LOT_SIZE'), None)

                symbol_info = {
                    'symbol': symbol['symbol'],
                    'base_asset': symbol['baseAsset'],
                    'quote_asset': symbol['quoteAsset'],
                    'exchange': 'binance',
                    'type': 'SPOT',
                    'status': symbol['status'],
                    'is_spot_trading_allowed': symbol['isSpotTradingAllowed'],
                    'is_margin_trading_allowed': symbol['isMarginTradingAllowed'],
                    'base_precision': symbol['baseAssetPrecision'],
                    'quote_precision': symbol['quoteAssetPrecision'],
                    'min_price': float(price_filter['minPrice']) if price_filter else None,
                    'max_price': float(price_filter['maxPrice']) if price_filter else None,
                    'tick_size': float(price_filter['tickSize']) if price_filter else None,
                    'min_qty': float(lot_size['minQty']) if lot_size else None,
                    'max_qty': float(lot_size['maxQty']) if lot_size else None,
                    'step_size': float(lot_size['stepSize']) if lot_size else None
                }
                symbols_data.append(symbol_info)

            df = pd.DataFrame(symbols_data)
            self.logger.info(f"Fetched {len(df)} spot symbols")
            return df

        except Exception as e:
            self.logger.error(f"Error fetching spot symbols: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
    def get_um_perpetual_symbols(self) -> pd.DataFrame:
        """Fetch all USD-M perpetual futures trading pairs"""
        try:
            exchange_info = self.um_futures_client.exchange_info()
            symbols_data = []

            for symbol in exchange_info['symbols']:
                if symbol['contractType'] == 'PERPETUAL':
                    symbol_info = {
                        'symbol': symbol['symbol'],
                        'base_asset': symbol['baseAsset'],
                        'quote_asset': symbol['quoteAsset'],
                        'margin_asset': symbol['marginAsset'],
                        'exchange': 'binance',
                        'type': symbol['contractType'],
                        'underlyingSubType': ','.join(symbol['underlyingSubType']),
                        'status': symbol['status'],
                        'onboard_date': symbol['onboardDate'],
                        'delivery_date': symbol['deliveryDate'],
                        'price_precision': symbol['pricePrecision'],
                        'quantity_precision': symbol['quantityPrecision'],
                        'min_price': float(symbol['filters'][0]['minPrice']),
                        'max_price': float(symbol['filters'][0]['maxPrice']),
                        'tick_size': float(symbol['filters'][0]['tickSize']),
                        'min_qty': float(symbol['filters'][1]['minQty']),
                        'max_qty': float(symbol['filters'][1]['maxQty']),
                        'step_size': float(symbol['filters'][1]['stepSize'])
                    }
                    symbols_data.append(symbol_info)

            df = pd.DataFrame(symbols_data)
            df['delivery_date'] = pd.to_datetime(df['delivery_date'], unit='ms')
            df['onboard_date'] = pd.to_datetime(df['onboard_date'], unit='ms')
            self.logger.info(f"Fetched {len(df)} perpetual futures symbols")
            return df

        except Exception as e:
            self.logger.error(f"Error fetching perpetual futures symbols: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
    @sleep_and_retry
    @limits(calls=SPOT_WEIGHT_LIMIT // SPOT_KLINE_WEIGHT, period=RATE_LIMIT_PERIOD)
    def _rate_limited_spot_klines(self, symbol: str, **kwargs) -> List:
        """Rate-limited spot klines API call"""
        try:
            return self.spot_client.klines(symbol=symbol, **kwargs)
        except Exception as e:
            self._handle_rate_limit_error(e)
            if '418' in str(e):
                return self._rate_limited_spot_klines(symbol, **kwargs)
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
    @sleep_and_retry
    @limits(calls=FUTURES_WEIGHT_LIMIT // FUTURES_KLINE_WEIGHT, period=RATE_LIMIT_PERIOD)
    def _rate_limited_futures_klines(self, symbol: str, **kwargs) -> List:
        """Rate-limited futures klines API call"""
        try:
            return self.um_futures_client.klines(symbol=symbol, **kwargs)
        except Exception as e:
            self._handle_rate_limit_error(e)
            if '418' in str(e):
                return self._rate_limited_futures_klines(symbol, **kwargs)
            raise

    def get_klines(self, symbol: str, type: str, start_time: int = None,
                   end_time: int = None, interval: str = '1m', limit: int = 1000) -> pd.DataFrame:
        """Fetch kline/candlestick data"""
        try:
            if type == 'PERPETUAL':
                api_call = self._rate_limited_futures_klines
            elif type == 'SPOT':
                api_call = self._rate_limited_spot_klines
            else:
                raise ValueError(f"Invalid type: {type}")

            klines = api_call(
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_time,
                endTime=end_time
            )

            if not klines:
                return pd.DataFrame()

            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close',
                'volume', 'close_time', 'quote_volume', 'trades_count',
                'taker_buy_volume', 'taker_buy_quote_volume', 'ignore'
            ])
            return df

        except Exception as e:
            self.logger.error(f"Error fetching klines for {symbol}: {e}")
            raise

    def get_historical_klines(self, symbol: str, type: str,
                              start_time: Union[str, datetime, int],
                              end_time: Union[str, datetime, int],
                              delivery_date: Union[str, datetime, int] = '2100-12-25 08:00:00',
                              interval: str = '1m') -> pd.DataFrame:
        """Fetch historical kline data with pagination"""
        try:
            all_klines = []

            if isinstance(start_time, (str, datetime)):
                start_time_ms = int(pd.Timestamp(start_time).timestamp() * 1000)
            else:
                start_time_ms = start_time

            if isinstance(end_time, (str, datetime)):
                end_time_ms = int(pd.Timestamp(end_time).timestamp() * 1000)
            else:
                end_time_ms = end_time

            if type == 'PERPETUAL' and delivery_date and isinstance(delivery_date, (str, datetime)):
                delivery_date_ms = int(pd.Timestamp(delivery_date).timestamp() * 1000)
                end_time_ms = min(end_time_ms, delivery_date_ms)

            limit = 499 if type == 'PERPETUAL' else 1000
            current_start_ms = start_time_ms

            while end_time_ms >= current_start_ms:
                df = self.get_klines(
                    symbol=symbol,
                    type=type,
                    start_time=current_start_ms,
                    end_time=end_time_ms,
                    interval=interval,
                    limit=limit,
                )

                if df is None or df.empty:
                    break

                all_klines.append(df)
                current_start_ms = int(df['timestamp'].iloc[-1]) + 1

            if all_klines:
                result = pd.concat(all_klines, axis=0)
                result = result.drop_duplicates(subset=['timestamp'], keep='last')
                result = result.sort_values(['timestamp']).reset_index(drop=True)
                result['symbol'] = symbol
                return result

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error fetching historical kline data for {symbol}: {e}")
            raise

    def fetch_market_klines_threadpool(self, type: str,
                                        start_time: Union[str, datetime, int],
                                        end_time: Union[str, datetime, int],
                                        interval: str = '1m') -> pd.DataFrame:
        """Fetch historical kline data using thread pool"""
        try:
            if type == 'PERPETUAL':
                symbols_df = clickhouse_query(self.con,
                    f"""SELECT symbol, delivery_date
                        FROM bn_perp_symbols
                        WHERE delivery_date >= '{pd.Timestamp(start_time)}'""")
                max_workers = self.FUTURES_MAX_WORKERS
            elif type == 'SPOT':
                symbols_df = clickhouse_query(self.con,
                    """SELECT symbol FROM bn_spot_symbols
                       WHERE quote_asset IN ('USDT','USDC')""")
                max_workers = self.SPOT_MAX_WORKERS
            else:
                raise ValueError(f"Invalid type: {type}")

            self.logger.info(
                f"Fetching {interval} klines for {len(symbols_df)} "
                f"{type} symbols using {max_workers} threads"
            )

            if end_time is None:
                end_time = datetime.now(timezone.utc)

            all_results = []
            failed_symbols = []
            result_lock = threading.Lock()

            def process_symbol(symbol, delivery_date=None):
                try:
                    klines = self.get_historical_klines(
                        symbol=symbol,
                        type=type,
                        start_time=start_time,
                        end_time=end_time,
                        delivery_date=delivery_date,
                        interval=interval
                    )

                    if klines is not None and not klines.empty:
                        with result_lock:
                            all_results.append(klines)
                            self.logger.info(
                                f"Fetched {len(klines)} klines for {symbol} "
                                f"from {pd.to_datetime(klines['timestamp'].min(), unit='ms')}"
                            )

                except Exception as e:
                    if '418' in str(e):
                        self._handle_rate_limit_error(e)
                        return process_symbol(symbol, delivery_date)
                    with result_lock:
                        failed_symbols.append(symbol)
                        self.logger.error(f"Error fetching {symbol}: {e}")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []

                for _, row in symbols_df.iterrows():
                    symbol = row['symbol']
                    delivery_date = row.get('delivery_date') if type == 'PERPETUAL' else None
                    futures.append(executor.submit(process_symbol, symbol, delivery_date))

                with tqdm(total=len(futures), desc=f"Fetching {interval} klines") as pbar:
                    for future in futures:
                        future.result()
                        pbar.update(1)

            if failed_symbols:
                self.logger.warning(f"Failed to fetch data for {len(failed_symbols)} symbols")

            if all_results:
                result = pd.concat(all_results, axis=0)
                result = (result
                          .sort_values(['symbol', 'timestamp'])
                          .drop_duplicates(subset=['symbol', 'timestamp'], keep='last')
                          .reset_index(drop=True))

                if not result.empty:
                    result['timestamp'] = pd.to_datetime(result['timestamp'], unit='ms')
                    result['close_time'] = pd.to_datetime(result['close_time'], unit='ms')

                    numeric_cols = ['open', 'high', 'low', 'close', 'volume',
                                    'quote_volume', 'taker_buy_volume', 'taker_buy_quote_volume']
                    result[numeric_cols] = result[numeric_cols].astype(float)

                    result['exchange'] = 'binance'
                    result['type'] = type
                    result['interval'] = interval

                    columns = ['symbol', 'exchange', 'type', 'interval', 'timestamp',
                               'close_time', 'open', 'high', 'low', 'close', 'volume',
                               'quote_volume', 'taker_buy_volume', 'taker_buy_quote_volume',
                               'trades_count']
                    return result[columns]

            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error in fetch_market_klines: {e}")
            raise


class CryptoDataPipeline:
    """Main pipeline class for managing Binance data in ClickHouse"""

    # Optimized schemas with LowCardinality for better compression
    KLINES_SCHEMA = """
        symbol LowCardinality(String),
        exchange LowCardinality(String),
        type LowCardinality(String),
        interval LowCardinality(String),
        timestamp DateTime,
        close_time DateTime,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        quote_volume Float64,
        taker_buy_volume Float64,
        taker_buy_quote_volume Float64,
        trades_count UInt32
    """

    SPOT_SYMBOLS_SCHEMA = """
        symbol LowCardinality(String),
        base_asset LowCardinality(String),
        quote_asset LowCardinality(String),
        exchange LowCardinality(String),
        type LowCardinality(String),
        status LowCardinality(String),
        is_spot_trading_allowed UInt8,
        is_margin_trading_allowed UInt8,
        base_precision Int32,
        quote_precision Int32,
        min_price Float64,
        max_price Float64,
        tick_size Float64,
        min_qty Float64,
        max_qty Float64,
        step_size Float64
    """

    PERP_SYMBOLS_SCHEMA = """
        symbol LowCardinality(String),
        base_asset LowCardinality(String),
        quote_asset LowCardinality(String),
        margin_asset LowCardinality(String),
        exchange LowCardinality(String),
        type LowCardinality(String),
        underlyingSubType String,
        status LowCardinality(String),
        onboard_date DateTime,
        delivery_date DateTime,
        price_precision Int32,
        quantity_precision Int32,
        min_price Float64,
        max_price Float64,
        tick_size Float64,
        min_qty Float64,
        max_qty Float64,
        step_size Float64
    """

    def __init__(self, con, api_key: Optional[str] = None, api_secret: Optional[str] = None,
                 config: dict = None):
        self.con = con
        self.config = config or load_config()
        self.fetcher = BinanceDataFetcher(con, api_key, api_secret, config)

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def _initialize_database(self):
        """Create database and tables if they don't exist"""
        db = self.config['clickhouse']['database']

        # Create database
        self.con.execute(f"CREATE DATABASE IF NOT EXISTS {db}")

        # Create spot symbols table
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {db}.bn_spot_symbols (
                {self.SPOT_SYMBOLS_SCHEMA}
            )
            ENGINE = ReplacingMergeTree()
            PRIMARY KEY (symbol, exchange)
            ORDER BY (symbol, exchange)
            SETTINGS index_granularity = 8192
        """)

        # Create perpetual symbols table
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {db}.bn_perp_symbols (
                {self.PERP_SYMBOLS_SCHEMA}
            )
            ENGINE = ReplacingMergeTree()
            PRIMARY KEY (symbol, exchange)
            ORDER BY (symbol, exchange)
            SETTINGS index_granularity = 8192
        """)

        # Create klines tables (one per interval for better partitioning)
        for interval in self.config['bars']['intervals']:
            interval_safe = interval.replace('m', 'min').replace('h', 'hour')

            # Spot klines
            self.con.execute(f"""
                CREATE TABLE IF NOT EXISTS {db}.bn_spot_klines_{interval_safe} (
                    {self.KLINES_SCHEMA}
                )
                ENGINE = ReplacingMergeTree()
                PRIMARY KEY (symbol, interval, timestamp)
                ORDER BY (symbol, interval, timestamp)
                PARTITION BY toYYYYMM(timestamp)
                SETTINGS index_granularity = 8192
            """)

            # Perpetual klines
            self.con.execute(f"""
                CREATE TABLE IF NOT EXISTS {db}.bn_perp_klines_{interval_safe} (
                    {self.KLINES_SCHEMA}
                )
                ENGINE = ReplacingMergeTree()
                PRIMARY KEY (symbol, interval, timestamp)
                ORDER BY (symbol, interval, timestamp)
                PARTITION BY toYYYYMM(timestamp)
                SETTINGS index_granularity = 8192
            """)

        self.logger.info(f"Database {db} initialized successfully")

    def update_symbols(self):
        """Update symbol tables"""
        # Update spot symbols
        spot_df = self.fetcher.get_spot_symbols()
        if not spot_df.empty:
            clickhouse_insert(self.con, 'bn_spot_symbols', spot_df)
            self.logger.info(f"Updated {len(spot_df)} spot symbols")

        # Update perpetual symbols
        perp_df = self.fetcher.get_um_perpetual_symbols()
        if not perp_df.empty:
            clickhouse_insert(self.con, 'bn_perp_symbols', perp_df)
            self.logger.info(f"Updated {len(perp_df)} perpetual symbols")

    def update_klines(self, interval: str = '1m', start_time: str = None, end_time: str = None):
        """Update klines for a specific interval"""
        if start_time is None:
            start_time = self.config['bars']['start_date']
        if end_time is None:
            end_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        interval_safe = interval.replace('m', 'min').replace('h', 'hour')

        # Update spot klines
        if self.config['bars']['symbols']['spot']['enabled']:
            spot_df = self.fetcher.fetch_market_klines_threadpool(
                type='SPOT',
                start_time=start_time,
                end_time=end_time,
                interval=interval
            )
            if not spot_df.empty:
                clickhouse_insert(self.con, f'bn_spot_klines_{interval_safe}', spot_df)
                self.logger.info(f"Updated {len(spot_df)} spot klines ({interval})")

        # Update perpetual klines
        if self.config['bars']['symbols']['perpetual']['enabled']:
            perp_df = self.fetcher.fetch_market_klines_threadpool(
                type='PERPETUAL',
                start_time=start_time,
                end_time=end_time,
                interval=interval
            )
            if not perp_df.empty:
                clickhouse_insert(self.con, f'bn_perp_klines_{interval_safe}', perp_df)
                self.logger.info(f"Updated {len(perp_df)} perpetual klines ({interval})")

    def update_all(self):
        """Run full update for all configured intervals"""
        self._initialize_database()
        self.update_symbols()

        for interval in self.config['bars']['intervals']:
            self.logger.info(f"Updating {interval} klines...")
            self.update_klines(interval=interval)

        self.logger.info("Full update completed")
