
# hl-md-ingest

Tiny market-data ingest prototype in Rust:
- Connects to a WebSocket stream (default: Binance BTCUSDT trades) — override with `WS_URL`.
- Bounded queue with back-pressure (drops when full).
- Reconnect with quadratic backoff.
- Exposes Prometheus `/metrics` on `0.0.0.0:9000` (override with `METRICS_ADDR`).

## Quickstart

```bash
# in repo root
cargo run
# or custom stream
WS_URL=wss://stream.binance.com:9443/ws/btcusdt@trade cargo run

# metrics
curl -s localhost:9000/metrics | head
```

## Notes
This is a vertical slice you can extend with:
- JSON→binary conversion
- Parquet writer
- Read API (depth snapshot, OHLCV)
- Deterministic replay + tests
