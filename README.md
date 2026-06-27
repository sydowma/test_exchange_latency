# Bybit Latency Bench

A small, scriptable CLI for measuring Bybit public WebSocket latency across
cloud regions, availability zones, and network providers.

It is designed for traders, market-data engineers, and infrastructure builders
who need a quick way to answer practical questions:

- Which region is closest to Bybit public streams?
- How long does a WebSocket connect and subscription acknowledgement take?
- What is the p50/p95/p99 ticker delay after clock-syncing against Bybit REST time?
- Are latency spikes coming from the network, the stream, or local runtime noise?

## What It Measures

The tool connects to Bybit v5 public WebSocket streams and reports:

- `connect_ms`: time to establish the WebSocket connection.
- `subscribe_ack_ms`: time from subscription request to acknowledgement.
- `first_ticker_ms`: time from subscription request to first ticker message.
- `ping_rtt_ms`: WebSocket or application ping round-trip latency.
- `ticker_latency_ms`: estimated one-way delay from exchange timestamp to local receive time.
- `time_sync`: REST time samples used to estimate local clock offset.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python bybit_latency.py --market spot --symbol BTCUSDT --duration 60
```

For linear perpetual streams:

```bash
python bybit_latency.py --market linear --symbol BTCUSDT --duration 60
```

Run a longer region comparison:

```bash
python bybit_latency.py \
  --market spot \
  --symbol BTCUSDT \
  --duration 600 \
  --ping-interval 2 \
  --time-sync-samples 20 \
  --json-out results-sg.json
```

## Docker

```bash
docker build -t bybit-latency-bench .
docker run --rm bybit-latency-bench --market spot --symbol BTCUSDT --duration 60
```

## Example Output

```text
Bybit WS latency test (mainnet spot)
  market: spot
  symbol: BTCUSDT
  duration_s: 60.0  ping_interval_s: 2.0

time_sync:
  offset_ms: -1.842  best_rtt_ms: 6.327  samples: 20
ws:
  connect_ms: 16.880
  subscribe_ack_ms: 2.202
  first_ticker_ms: 2.581
ping_rtt_ms:
  count=29 min=1.642 avg=2.184 p50=2.083 p95=2.911 p99=4.310 max=4.310
ticker_latency_ms:
  count=250 min=1.904 avg=4.882 p50=4.181 p95=6.935 p99=18.292 max=55.073
```

## Interpreting Results

Use at least 10-30 minutes of samples when comparing p95/p99 latency. A 60
second run is useful for smoke testing, but tail latency needs more messages.

For fair comparisons:

- Run the same command in each region.
- Use the same market and symbol.
- Keep `--time-sync-samples` and `--ping-interval` consistent.
- Repeat each run at least twice to catch transient stream or VM noise.

## Notes

- The tool uses public market-data endpoints only.
- One-way delay is an estimate. It depends on Bybit timestamps, REST time-sync
  quality, local clock behavior, and the selected market stream.
- This is an infrastructure measurement tool, not trading or investment advice.

## License

Apache License 2.0
