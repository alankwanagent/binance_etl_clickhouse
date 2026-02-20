# Binance ETL ClickHouse

ETL pipeline for fetching Binance market data (spot and perpetual klines) into ClickHouse.

## Features

- Fetches klines for multiple intervals (1m, 1h, configurable)
- Supports both spot and perpetual futures
- Optimized ClickHouse schemas with LowCardinality and Decimal types
- Rate-limited API calls with automatic retry and IP ban handling
- Configurable via `config.json`
- Docker support

## Quick Start

### Prerequisites

- Python 3.11+
- ClickHouse server running locally (port 9000)
- Docker (optional)

### Configuration

Edit `config.json` to configure:

```json
{
  "clickhouse": {
    "host": "localhost",
    "port": 9000,
    "database": "binance_v2",
    "username": "claude",
    "password": "iamsentient"
  },
  "bars": {
    "intervals": ["1m", "1h"],
    "start_date": "2025-01-01"
  }
}
```

### Local Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run once
python run_pipeline.py --once

# Run continuously (every hour by default)
python run_pipeline.py
```

### Docker Run

```bash
# Build and run
docker-compose up -d

# View logs
docker-compose logs -f
```

## Database Schema

### Tables Created

- `bn_spot_symbols` - Spot trading pairs metadata
- `bn_perp_symbols` - Perpetual futures metadata
- `bn_spot_klines_1min`, `bn_spot_klines_1hour`, etc. - Spot klines per interval
- `bn_perp_klines_1min`, `bn_perp_klines_1hour`, etc. - Perpetual klines per interval
- `pipeline_logs` - Pipeline run history

### Optimizations

- `LowCardinality` for string columns with few distinct values
- `Decimal64(8)` for prices/volumes instead of Float64 for precision
- Monthly partitioning on klines tables
- ReplacingMergeTree for deduplication

## Adding New Intervals

Edit `config.json`:

```json
{
  "bars": {
    "intervals": ["1m", "5m", "15m", "1h", "4h", "1d"]
  }
}
```

Tables will be created automatically on next run.
