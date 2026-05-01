# fiat-oracle

BTC historical fiat-price oracle. Stores hourly BTC/fiat prices in per-currency redb
databases and serves them over a simple HTTP API.

## Running locally

### Normal mode (live price polling only)

```sh
cd fiat-oracle
RUST_LOG=info cargo run -- --config ./config.dev.toml
```

The server starts on `127.0.0.1:8085`. On a cold start, prices are only available
after the first live-poll tick (~64 s). Query example:

```sh
curl -X POST http://127.0.0.1:8085/price \
  -H 'content-type: application/json' \
  -d '{"currency":"USD","timestamps":[1714579200]}'
# {"prices":[null]}   ← null until backfill or live data arrives
```

Query for "now":

```sh
curl -X POST http://127.0.0.1:8085/price \
  -H 'content-type: application/json' \
  -d "{\"currency\":\"USD\",\"timestamps\":[$(date +%s)]}"
```

### Backfill mode (historical data, then serve)

Backfill runs to completion for each currency before the HTTP server starts
accepting requests.

```sh
cd fiat-oracle
RUST_LOG=info cargo run -- \
  --config ./config.dev.toml \
  --rescan-from $(date -d '7 days ago' +%s)
```

After backfill, querying a timestamp within the filled window returns a real price.
