# stock-data-access

Reusable stock price data access layer for Mongo-backed OHLCV and minute bars.

## Install

```bash
pip install -e /home/shuyolin/own/data-access-lib
```

## Environment

Set Mongo connection via env vars or pass a `db` explicitly.

- `MONGO_URI` (default: `mongodb://localhost:27017`)
- `MONGO_DB` (default: `finance`)

## Usage

```python
from stock_data_access import StockPriceDataAccess

loader = StockPriceDataAccess(minute=True)  # or minute=False
pm = loader.fetch_batch(["300722"], "202511010930", "202511301500")
frame = loader.fetch_frame(["300722"], "202511010930", "202511301500")
names = loader.fetch_names(["300722"])  # {"300722": "<name>"}
```
