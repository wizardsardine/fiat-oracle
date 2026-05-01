# Historical Price Sources

Reference for all exchanges used in historical 1-minute BTC close-price backfill.

Currencies in scope: USD, EUR, GBP, JPY, KRW. Every (source × currency) below
satisfies the **1-year rule** (≥ 1 year of 1m data). CAD is currently
unreachable historically (no source has a real BTC/CAD pair with ≥ 1 year of
1m data) and is intentionally excluded. KRW only has Upbit as a 1m source
and stays gated under `MIN_PRICE_SOURCES = 3`.

---

## Summary table

| Source   | USD | EUR | GBP | JPY | KRW | Candles/req | Rate limit    | min/s    |
|----------|:---:|:---:|:---:|:---:|:---:|-------------|---------------|----------|
| Bitstamp |  v  |  v  |  v  |     |     | 1,000       | ~17 req/s [1] | ~17,000  |
| Bitfinex |  v  |  v  |  v  |     |     | 10,000      | 1.5 req/s     | ~15,000  |
| Coinbase |  v  |  v  |  v  |     |     | 300         | 10 req/s      | ~3,000   |
| OKX      |  v  |  v  |     |     |     | 300         | 20 req/s      | ~6,000   |
| BitMEX   |  v  |  v  |     |     |     | 12,000      | 3 req/s       | ~36,000  |
| KuCoin   |  v  |  v  |     |     |     | 1,500       | ~22 req/s [2] | ~33,000  |
| Binance  |  v  |  v  |     |  v  |     | 1,000       | ~20 req/s     | ~20,000  |
| Bitbank  |     |     |     |  v  |     | 1,440 (day) | (undoc.)      | ~daily   |
| Upbit    |     |     |     |     |  v  | 200         | ~10 req/s     | ~2,000   |

Dropped sources / pairs (no longer usable for historical backfill):
- **Kraken (all currencies)** — sliding 1000-candle window: 1m = ~16 h,
  1h = ~41 d. Fails the 1-year rule even at hourly granularity. Per the
  "spot AND historical" rule, this also disqualifies it from the spot
  consensus, so Kraken is removed from `SpotSource` end-to-end.
- **Coinbase BTCJPY / BTCCAD** — pairs do not exist on Coinbase Exchange
  (`/products` listing has no JPY or CAD). The `/v2/exchange-rates` API
  *does* expose them, so the spot path keeps using them.
- **Binance BTCGBP** — symbol status `BREAK`, last klines candle 2023-12-29.
- **Bitfinex tBTCJPY** — no longer in the active pair config; last `/hist`
  candle Nov 2025.
- **BitMEX XBTJPY** — instrument lookup returns `[]`; `/trade/bucketed`
  empty for any recent date. Delisted.

`min/s` = minutes of 1-minute candle history covered per second of wall time
(= candles/req * rate_limit). Coinbase is now the slow path at ~3,000 min/s.

USD on KuCoin and OKX is proxied via USDT (no direct BTC/USD pair).

[1] 400 req/s burst; secondary cap of 10,000/10min (~16.7 req/s sustained).
[2] Shared pool of 2,000 pts/30s; klines endpoint weight = 3.

---

## Bitstamp

**Docs:** https://www.bitstamp.net/api/#tag/Market-Data-–-Tickers/operation/GetOHLCData

**Endpoint:**
```
GET https://www.bitstamp.net/api/v2/ohlc/{pair}/
  ?step={seconds}   # 60 for 1-minute candles
  &limit={n}        # max 1000
  &start={unix_sec} # pagination cursor
```

**Currency pairs:**

| Currency | Pair   |
|----------|--------|
| EUR      | btceur |
| USD      | btcusd |
| GBP      | btcgbp |

**Response shape:**
```json
{
    "data": {
        "ohlc": [
            {
                "timestamp": "...",
                "open": "...",
                "high": "...",
                "low": "...",
                "close": "...",
                "volume": "..."
            }
        ],
        "pair": "..."
    }
}
```

**Pagination:** `start` cursor, forward direction. Advance
`cursor = last_ts + step`. Stop when response returns fewer than `limit`
entries.

**Rate limit:** 400 req/s primary; secondary cap of 10,000 req per 10 minutes
(~16.7 req/s sustained).

**Candles per request:** 1000.

---

## Bitfinex

**Docs:** https://docs.bitfinex.com/reference/rest-public-candles

**Endpoint:**
```
GET https://api-pub.bitfinex.com/v2/candles/trade:{timeframe}:{symbol}/hist
  ?start={unix_ms}  # range start (inclusive)
  &end={unix_ms}    # range end (inclusive)
  &limit={n}        # max 10,000
  &sort=1           # 1 = ascending (oldest first)
```

**Currency pairs:**

| Currency | Symbol  |
|----------|---------|
| EUR      | tBTCEUR |
| USD      | tBTCUSD |
| GBP      | tBTCGBP |

`tBTCJPY` is no longer in Bitfinex's active pair config — `/hist` returns
empty for any recent window, so it is excluded from historical.

**Timeframe:** `1m` for 1-minute candles (current code uses `1h`).

**Response shape:**
```json
[
    [unix_ms, open, close, high, low, volume],
    ...
]
```

**Pagination:** `start` cursor in milliseconds, forward direction. Advance
`cursor_ms = last_candle_ms + interval_ms`.

**Rate limit:** 90 req/min = 1.5 req/s (policy across all public endpoints;
candles-specific limit not broken out).

**Candles per request:** 10,000 (current code uses 1,000 — leaving 10x
throughput unused).

---

## Coinbase Exchange

**Docs:** https://docs.cdp.coinbase.com/exchange/reference/exchangerestapi_getproductcandles

**Endpoint:**
```
GET https://api.exchange.coinbase.com/products/{pair}/candles
  ?granularity={seconds}  # 60 for 1-minute candles
  &start={ISO8601}        # range start
  &end={ISO8601}          # range end
```

**Currency pairs:**

| Currency | Pair    |
|----------|---------|
| USD      | BTC-USD |
| EUR      | BTC-EUR |
| GBP      | BTC-GBP |

BTC-JPY and BTC-CAD do not exist on Coinbase Exchange — `/products` returns
404 for those pairs. The `/v2/exchange-rates?currency=BTC` API (used by the
spot path) does include JPY and CAD, but it is a synthetic rate, not a real
on-exchange market, so it can't backfill OHLC.

**Response shape** (newest first):
```json
[
    [unix_sec, low, high, open, close, volume],
    ...
]
```

**Pagination:** Fixed time-window. Max 300 candles per request. Advance
`start = end + 1` through the requested range.

**Rate limit:** 10 req/s (unauthenticated, IP-based).

**Candles per request:** 300.

---

## OKX

**Docs:** https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-history-candlesticks

**Endpoint:**
```
GET https://www.okx.com/api/v5/market/history-candles
  ?instId={instId}  # instrument ID
  &bar={bar}        # "1m" for 1-minute candles (current code uses "1H")
  &after={unix_ms}  # returns candles OLDER than this timestamp (cursor)
  &limit={n}        # max 300 (default 100)
```

**Currency pairs:**

| Currency | instId   | Note       |
|----------|----------|------------|
| EUR      | BTC-EUR  |            |
| USD      | BTC-USDT | USDT proxy |

**Response shape** (newest first):
```json
{
    "code": "0",
    "data": [
        ["unix_ms", "open", "high", "low", "close",
         "vol", "volCcy", "volCcyQuote", "confirm"],
        ...
    ]
}
```

**Pagination:** `after` cursor in milliseconds, reverse direction (newest to
oldest). Advance `after_ms = oldest_candle_ms`. Stop when oldest returned
candle is at or before `from_ts`.

**Rate limit:** 40 req per 2s = 20 req/s (IP-based).

**Candles per request:** 300 (current code uses default 100 — leaving 3x
throughput unused).

---

## BitMEX

**Docs:** https://www.bitmex.com/api/explorer/#!/Trade/Trade_getBucketed

**Endpoint:**
```
GET https://www.bitmex.com/api/v1/trade/bucketed
  ?binSize={size}      # "1m" for 1-minute candles (current code uses "1h")
  &partial=false       # exclude the current open candle
  &symbol={symbol}
  &startTime={ISO8601} # e.g. "2024-01-01T00:00:00.000Z"
  &endTime={ISO8601}
  &count={n}           # max 12,000 (docs say 1000 but that is stale)
  &reverse=false       # ascending order
```

**Currency pairs:**

| Currency | Symbol |
|----------|--------|
| EUR      | XBTEUR |
| USD      | XBTUSD |

`XBTJPY` is dropped: instrument lookup returns `[]` and `/trade/bucketed`
returns empty for any recent date — the contract is delisted.

**Response shape:**
```json
[
    {
        "timestamp": "ISO8601",
        "symbol": "...",
        "open": 0,
        "high": 0,
        "low": 0,
        "close": 0,
        "volume": 0
    }
]
```

**Pagination:** Time-window forward. Advance `startTime` by `count * interval`
per request. The `count=1000` note in the docs and error messages is stale;
the actual hard limit is 12,000 (verified via `x-ratelimit-limit` response
headers).

**Rate limit:** 180 req/min = 3 req/s (confirmed via `x-ratelimit-limit`
response header; 60s window).

**Candles per request:** 12,000 (current code uses 750 — leaving 16x
throughput unused).

---

## KuCoin

**Docs:** https://www.kucoin.com/docs-new/api-221752070

**Endpoint:**
```
GET https://api.kucoin.com/api/v1/market/candles
  ?type={type}        # "1min" for 1-minute candles (current code uses "1hour")
  &symbol={symbol}
  &startAt={unix_sec}
  &endAt={unix_sec}
```

**Currency pairs:**

| Currency | Symbol   | Note       |
|----------|----------|------------|
| EUR      | BTC-EUR  |            |
| USD      | BTC-USDT | USDT proxy |

**Response shape** (newest first):
```json
{
    "code": "200000",
    "data": [
        ["unix_sec", "open", "close", "high", "low", "volume", "turnover"],
        ...
    ]
}
```

**Pagination:** Time-window forward. `startAt`/`endAt` define a window of up
to 1500 candles. Advance `startAt = endAt + 1`.

**Rate limit:** Public IP pool: 2,000 points per 30s. Klines endpoint weight
= 3, giving ~666 kline calls per 30s = ~22 req/s.

**Candles per request:** 1500.

---

## Binance

**Docs:** https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data

**Endpoint:**
```
GET https://api.binance.com/api/v3/klines
  ?symbol={symbol}
  &interval={interval}  # 1m for 1-minute candles
  &startTime={unix_ms}  # pagination cursor (open time of first candle)
  &limit={n}            # max 1000
```

**Currency pairs:**

| Currency | Symbol  |
|----------|---------|
| EUR      | BTCEUR  |
| USD      | BTCUSDT |
| JPY      | BTCJPY  |

`BTCGBP` is in `status: "BREAK"` (trading halted; last klines candle from
2023-12-29) and is excluded from historical.

**Response shape:**
```json
[
    [open_time_ms, "open", "high", "low", "close", "volume",
     close_time_ms, "quote_volume", num_trades,
     "taker_buy_base", "taker_buy_quote", "ignore"],
    ...
]
```

**Pagination:** `startTime` cursor in milliseconds, forward direction. Advance
`startTime = last_open_time_ms + 60_000`. Stop on partial page.

**Rate limit:** 6,000 request weight per minute (klines weight = 2 with
limit ≤ 1000) → ~50 req/s headroom; current code paces well below this.

**Candles per request:** 1,000.

---

## Bitbank (JPY)

**Docs:** https://github.com/bitbankinc/bitbank-api-docs/blob/master/public-api.md

**Endpoint:**
```
GET https://public.bitbank.cc/{pair}/candlestick/1min/{YYYYMMDD}
```

**Currency pairs:**

| Currency | Pair    |
|----------|---------|
| JPY      | btc_jpy |

**Response shape:**
```json
{
    "success": 1,
    "data": {
        "candlestick": [{
            "type": "1min",
            "ohlcv": [
                ["open", "high", "low", "close", "volume", timestamp_ms],
                ...
            ]
        }],
        "timestamp": <int>
    }
}
```

**Pagination:** Date-paged. Each request returns a full UTC day of 1m candles
(~1,440 entries). Advance `cursor = cursor + 86400` (one day). Stop when the
page is short (current in-progress day).

**Rate limit:** Not officially documented; use ≤ 1 req/s as a courteous default.

**Candles per request:** 1,440 (one day).

---

## Upbit (KRW)

**Docs:** https://global-docs.upbit.com/reference/minute-candle

**Endpoint:**
```
GET https://api.upbit.com/v1/candles/minutes/1
  ?market={market}        # KRW-BTC
  &count={n}              # max 200
  &to={YYYY-MM-DDTHH:MM:SS}  # UTC; cursor (returns candles older than `to`)
```

**Currency pairs:**

| Currency | Market   |
|----------|----------|
| KRW      | KRW-BTC  |

**Response shape** (newest first):
```json
[
    {
        "market": "KRW-BTC",
        "candle_date_time_utc": "2024-05-01T00:00:00",
        "trade_price": 90000000.0,
        "timestamp": 1714521600000,
        ...
    }
]
```

**Pagination:** `to` cursor in ISO 8601 (UTC, no trailing `Z`), reverse
direction. Advance `to = oldest_candle_iso`. Stop when the oldest returned
timestamp ≤ `from_ts` or the response is empty.

**Rate limit:** 10 req/s public IP-based.

**Candles per request:** 200.

