from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import socket
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

DEFAULT_WS_URL_LINEAR = "wss://stream.bybit.com/v5/public/linear"
DEFAULT_WS_URL_SPOT = "wss://stream.bybit.com/v5/public/spot"
DEFAULT_REST_URL = "https://api.bybit.com/v5/market/time"
DEFAULT_TOPIC_PREFIX = "tickers."

LOG_LEVELS: Dict[str, int] = {"quiet": 0, "info": 1, "debug": 2}


def wall_time_ms() -> float:
    """Wall-clock milliseconds since Unix epoch."""
    return time.time_ns() / 1_000_000


def mono_time_ms() -> float:
    """Monotonic milliseconds (safe for duration measurements)."""
    return time.perf_counter_ns() / 1_000_000


def utc_iso_from_wall_ms(ms: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000.0)) + f".{int(ms % 1000):03d}Z"


class Logger:
    __slots__ = ("_level", "_stream")

    def __init__(self, level: str, stream: Any = None) -> None:
        self._level = LOG_LEVELS.get(level, 1)
        self._stream = stream if stream is not None else sys.stderr

    def _log(self, level: int, msg: str) -> None:
        if self._level < level:
            return
        ts = utc_iso_from_wall_ms(wall_time_ms())
        print(f"[{ts}] {msg}", file=self._stream, flush=True)

    def info(self, msg: str) -> None:
        self._log(1, msg)

    def debug(self, msg: str) -> None:
        self._log(2, msg)

    @property
    def level(self) -> int:
        return self._level


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            return None
    return None


def _normalize_epoch_to_ms(epoch_value: int) -> Optional[float]:
    """Try to normalize a timestamp (s/ms/us/ns) to epoch milliseconds."""
    if epoch_value <= 0:
        return None
    # Seconds are ~1e9..1e10; ms are ~1e12..1e13; ns are ~1e18.
    if epoch_value < 100_000_000_000:  # < 1e11 -> seconds
        return float(epoch_value) * 1000.0
    if epoch_value > 10_000_000_000_000_000:  # > 1e16 -> likely ns
        return float(epoch_value) / 1_000_000.0
    if epoch_value > 10_000_000_000_000:  # > 1e13 -> likely us
        return float(epoch_value) / 1000.0
    return float(epoch_value)  # ms


def percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    """Nearest-rank percentile on pre-sorted samples."""
    if not sorted_vals:
        return None
    if p <= 0:
        return sorted_vals[0]
    if p >= 100:
        return sorted_vals[-1]
    k = int(math.ceil((p / 100.0) * len(sorted_vals))) - 1
    k = max(0, min(k, len(sorted_vals) - 1))
    return sorted_vals[k]


@dataclass(frozen=True)
class StatsSummary:
    count: int
    min_ms: Optional[float]
    avg_ms: Optional[float]
    p50_ms: Optional[float]
    p95_ms: Optional[float]
    p99_ms: Optional[float]
    max_ms: Optional[float]


class Stats:
    __slots__ = ("_samples",)

    def __init__(self) -> None:
        self._samples: List[float] = []

    def add(self, value_ms: float) -> None:
        if math.isnan(value_ms) or math.isinf(value_ms):
            return
        self._samples.append(float(value_ms))

    def summary(self) -> StatsSummary:
        if not self._samples:
            return StatsSummary(
                count=0,
                min_ms=None,
                avg_ms=None,
                p50_ms=None,
                p95_ms=None,
                p99_ms=None,
                max_ms=None,
            )
        vals = self._samples
        sorted_vals = sorted(vals)
        avg = sum(vals) / len(vals)
        return StatsSummary(
            count=len(vals),
            min_ms=sorted_vals[0],
            avg_ms=avg,
            p50_ms=percentile(sorted_vals, 50),
            p95_ms=percentile(sorted_vals, 95),
            p99_ms=percentile(sorted_vals, 99),
            max_ms=sorted_vals[-1],
        )


@dataclass(frozen=True)
class TimeSyncSample:
    rtt_ms: float
    offset_ms: float
    server_time_ms: float


@dataclass(frozen=True)
class TimeSyncResult:
    best: TimeSyncSample
    rtt_ms: StatsSummary
    samples: List[TimeSyncSample]


def parse_bybit_server_time_ms(payload: Dict[str, Any]) -> Optional[float]:
    """
    Parse Bybit /v5/market/time response.
    Common shapes:
      {"result":{"timeSecond":"...","timeNano":"..."}}
    """
    result = payload.get("result")
    if isinstance(result, dict):
        nano = _to_int(result.get("timeNano"))
        if nano is not None:
            return _normalize_epoch_to_ms(nano)
        sec = _to_int(result.get("timeSecond"))
        if sec is not None:
            return _normalize_epoch_to_ms(sec)
    # Some APIs include top-level time fields; keep it as last resort.
    top = _to_int(payload.get("time"))
    if top is not None:
        return _normalize_epoch_to_ms(top)
    return None


async def measure_time_sync(
    session: aiohttp.ClientSession,
    rest_url: str,
    samples: int,
    timeout_s: float,
    logger: Logger,
) -> TimeSyncResult:
    results: List[TimeSyncSample] = []
    rtts = Stats()
    timeout = aiohttp.ClientTimeout(total=timeout_s)

    n = max(1, samples)
    logger.info(f"time_sync: sampling n={n} rest_url={rest_url}")

    for i in range(n):
        t0 = wall_time_ms()
        async with session.get(rest_url, timeout=timeout) as resp:
            data = await resp.json(content_type=None)
        t1 = wall_time_ms()

        server_ms = parse_bybit_server_time_ms(data)
        if server_ms is None:
            raise RuntimeError(f"Failed to parse server time from REST response: {data}")

        rtt_ms = t1 - t0
        midpoint_ms = (t0 + t1) / 2.0
        offset_ms = server_ms - midpoint_ms

        rtts.add(rtt_ms)
        results.append(TimeSyncSample(rtt_ms=rtt_ms, offset_ms=offset_ms, server_time_ms=server_ms))
        logger.debug(f"time_sync: sample={i + 1}/{n} rtt_ms={rtt_ms:.3f} offset_ms={offset_ms:.3f}")

        # Small stagger helps avoid bursting; doesn't affect results meaningfully.
        await asyncio.sleep(0.05)

    best = min(results, key=lambda s: s.rtt_ms)
    logger.info(f"time_sync: best_rtt_ms={best.rtt_ms:.3f} offset_ms={best.offset_ms:.3f}")
    return TimeSyncResult(best=best, rtt_ms=rtts.summary(), samples=results)


def extract_ws_ts_ms(msg: Dict[str, Any]) -> Optional[float]:
    """
    Try to extract a usable exchange/server timestamp from a WS message.
    Bybit v5 often includes top-level 'ts' (ms). Some payloads include timestamps inside data.
    """
    candidates: List[Any] = []
    candidates.append(msg.get("ts"))

    data = msg.get("data")
    if isinstance(data, dict):
        candidates.extend([data.get("ts"), data.get("t"), data.get("time"), data.get("timestamp")])
    elif isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            candidates.extend([first.get("ts"), first.get("t"), first.get("time"), first.get("timestamp")])

    for c in candidates:
        v = _to_int(c)
        if v is None:
            continue
        ms = _normalize_epoch_to_ms(v)
        if ms is None:
            continue
        # Sanity: reject obviously wrong epochs (before 2001 or far future).
        if 978_307_200_000 <= ms <= 4_102_444_800_000:  # 2001-01-01 .. 2100-01-01
            return ms
    return None


@dataclass(frozen=True)
class WsMetrics:
    connect_ms: float
    subscribe_ack_ms: Optional[float]
    first_ticker_ms: Optional[float]


@dataclass(frozen=True)
class RunResult:
    meta: Dict[str, Any]
    time_sync: Dict[str, Any]
    ws: Dict[str, Any]
    ping_rtt_ms: Dict[str, Any]
    ticker_latency_ms: Dict[str, Any]
    ticker_ts_missing: bool
    counts: Dict[str, Any]


async def run_ws_latency_test(
    session: aiohttp.ClientSession,
    ws_url: str,
    market: str,
    symbol: str,
    duration_s: float,
    ping_interval_s: float,
    clock_offset_ms: float,
    timeout_s: float,
    log_interval_s: float,
    logger: Logger,
) -> Tuple[WsMetrics, StatsSummary, StatsSummary, bool, Dict[str, int]]:
    topic = f"{DEFAULT_TOPIC_PREFIX}{symbol}"
    connect_t0 = mono_time_ms()
    logger.info(f"ws: connecting market={market} ws_url={ws_url} topic={topic}")

    counters: Dict[str, int] = {
        "msgs_total": 0,
        "msgs_ticker": 0,
        "parse_errors": 0,
        "pings_sent": 0,
        "pongs_recv": 0,
        "ping_send_errors": 0,
    }

    subscribe_ack_ms: Optional[float] = None
    first_ticker_ms: Optional[float] = None
    ticker_ts_missing = False

    ping_rtts = Stats()
    ticker_latencies = Stats()

    pending_pings: Dict[str, float] = {}  # opaque_id -> mono send time (req_id if supported)
    stop = asyncio.Event()
    last_ping_rtt_ms: Optional[float] = None
    last_ticker_latency_ms: Optional[float] = None

    async def ping_sender(ws: aiohttp.ClientWebSocketResponse) -> None:
        if ping_interval_s <= 0:
            return
        seq = 0
        while not stop.is_set():
            await asyncio.sleep(ping_interval_s)
            if stop.is_set():
                break
            seq += 1
            try:
                if market == "spot":
                    opaque_id = f"{int(wall_time_ms())}-{seq}"
                    pending_pings[opaque_id] = mono_time_ms()
                    # Spot stream may not respond to JSON ping; use WS ping frames for RTT.
                    await ws.ping(opaque_id.encode("utf-8"))
                else:
                    req_id = f"{int(wall_time_ms())}-{seq}"
                    pending_pings[req_id] = mono_time_ms()
                    await ws.send_json({"op": "ping", "req_id": req_id})
                counters["pings_sent"] += 1
            except Exception:
                counters["ping_send_errors"] += 1
                return

    async def progress_reporter(start_mono: float) -> None:
        if log_interval_s <= 0 or logger.level < LOG_LEVELS["info"]:
            return
        while not stop.is_set():
            await asyncio.sleep(log_interval_s)
            if stop.is_set():
                break
            elapsed_s = max(0.0, (mono_time_ms() - start_mono) / 1000.0)
            logger.info(
                "progress: "
                f"elapsed_s={elapsed_s:.1f} "
                f"ticker={counters['msgs_ticker']} "
                f"pings_sent={counters['pings_sent']} pongs_recv={counters['pongs_recv']} "
                f"last_ping_rtt_ms={last_ping_rtt_ms} "
                f"last_ticker_latency_ms={last_ticker_latency_ms}"
            )

    async with session.ws_connect(
        ws_url,
        autoping=False,
        heartbeat=None,
        max_msg_size=8 * 1024 * 1024,
    ) as ws:
        connect_ms = mono_time_ms() - connect_t0
        logger.info(f"ws: connected connect_ms={connect_ms:.3f}")

        sub_sent_mono = mono_time_ms()
        await ws.send_json({"op": "subscribe", "args": [topic]})
        logger.info("ws: subscribe sent")

        ping_task = asyncio.create_task(ping_sender(ws))
        progress_task = asyncio.create_task(progress_reporter(sub_sent_mono))
        end_at = mono_time_ms() + (duration_s * 1000.0)

        try:
            while mono_time_ms() < end_at:
                try:
                    msg = await ws.receive(timeout=timeout_s)
                except asyncio.TimeoutError:
                    continue
                counters["msgs_total"] += 1

                if msg.type == aiohttp.WSMsgType.PING:
                    # Reply to WS-level ping frames.
                    await ws.pong(msg.data)
                    continue

                if msg.type == aiohttp.WSMsgType.PONG:
                    # WS-level pong frames (spot uses these for RTT).
                    pong_id: Optional[str] = None
                    if isinstance(msg.data, (bytes, bytearray)) and msg.data:
                        with contextlib.suppress(UnicodeDecodeError):
                            pong_id = bytes(msg.data).decode("utf-8")
                    if pong_id and pong_id in pending_pings:
                        send_mono = pending_pings.pop(pong_id)
                    elif pending_pings:
                        oldest_key = next(iter(pending_pings))
                        send_mono = pending_pings.pop(oldest_key)
                    else:
                        send_mono = None
                    if send_mono is not None:
                        last_ping_rtt_ms = mono_time_ms() - send_mono
                        ping_rtts.add(last_ping_rtt_ms)
                    counters["pongs_recv"] += 1
                    continue

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        payload = json.loads(msg.data)
                    except Exception:
                        counters["parse_errors"] += 1
                        continue

                    if not isinstance(payload, dict):
                        continue

                    op = payload.get("op")
                    if op == "pong":
                        req_id = payload.get("req_id")
                        req_id_s = req_id if isinstance(req_id, str) else None
                        if req_id_s and req_id_s in pending_pings:
                            send_mono = pending_pings.pop(req_id_s)
                        elif pending_pings:
                            # Some streams return pong without req_id. Fall back to the oldest pending ping.
                            oldest_key = next(iter(pending_pings))
                            send_mono = pending_pings.pop(oldest_key)
                        else:
                            send_mono = None
                        if send_mono is not None:
                            last_ping_rtt_ms = mono_time_ms() - send_mono
                            ping_rtts.add(last_ping_rtt_ms)
                        counters["pongs_recv"] += 1
                        continue

                    if op == "ping":
                        # Server-initiated ping: respond to be safe.
                        req_id = payload.get("req_id")
                        resp: Dict[str, Any] = {"op": "pong"}
                        if isinstance(req_id, str):
                            resp["req_id"] = req_id
                        await ws.send_json(resp)
                        continue

                    if op == "subscribe" and subscribe_ack_ms is None:
                        # Typical ack: {"success":true,"op":"subscribe",...}
                        subscribe_ack_ms = mono_time_ms() - sub_sent_mono
                        logger.info(f"ws: subscribe_ack_ms={subscribe_ack_ms:.3f}")

                    topic_val = payload.get("topic")
                    if topic_val == topic:
                        counters["msgs_ticker"] += 1
                        if first_ticker_ms is None:
                            first_ticker_ms = mono_time_ms() - sub_sent_mono
                            logger.info(f"ws: first_ticker_ms={first_ticker_ms:.3f}")

                        msg_ts_ms = extract_ws_ts_ms(payload)
                        if msg_ts_ms is None:
                            ticker_ts_missing = True
                            continue
                        recv_wall_ms = wall_time_ms()
                        last_ticker_latency_ms = (recv_wall_ms + clock_offset_ms) - msg_ts_ms
                        ticker_latencies.add(last_ticker_latency_ms)

                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        finally:
            stop.set()
            ping_task.cancel()
            progress_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await ping_task
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await progress_task

    ws_metrics = WsMetrics(connect_ms=connect_ms, subscribe_ack_ms=subscribe_ack_ms, first_ticker_ms=first_ticker_ms)
    return ws_metrics, ping_rtts.summary(), ticker_latencies.summary(), ticker_ts_missing, counters


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Measure Bybit (v5, mainnet) WebSocket latency: connect/subscription/ping RTT/ticker one-way delay.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--market",
        choices=["linear", "spot"],
        default="linear",
        help="Market type for default WS URL selection",
    )
    p.add_argument("--symbol", default="BTCUSDT", help="Symbol to subscribe, e.g. BTCUSDT")
    p.add_argument("--duration", type=float, default=60.0, help="Run duration in seconds")
    p.add_argument(
        "--ws-url",
        default=None,
        help="Bybit public WS URL (defaults to market-specific endpoint if omitted)",
    )
    p.add_argument("--rest-url", default=DEFAULT_REST_URL, help="Bybit REST time URL")
    p.add_argument("--ping-interval", type=float, default=10.0, help="Send application-level ping every N seconds; 0 disables")
    p.add_argument("--time-sync-samples", type=int, default=10, help="REST time samples used to estimate clock offset")
    p.add_argument("--timeout", type=float, default=5.0, help="Per-request timeout (seconds)")
    p.add_argument(
        "--log-level",
        choices=["quiet", "info", "debug"],
        default="info",
        help="Runtime progress logs (printed to stderr)",
    )
    p.add_argument(
        "--log-interval",
        type=float,
        default=5.0,
        help="Progress log interval in seconds during WS run; 0 disables",
    )
    p.add_argument("--json-out", default="", help="If set, write full JSON result to this path")
    return p


def print_summary(result: RunResult) -> None:
    meta = result.meta
    ws = result.ws

    def _fmt_num(v: Any) -> str:
        if v is None:
            return "null"
        if isinstance(v, (int, float)):
            return f"{float(v):.3f}"
        return str(v)

    print(f"Bybit WS latency test (mainnet {meta.get('market')})")
    print(f"  host: {meta.get('hostname')}  pid: {meta.get('pid')}")
    print(f"  market: {meta.get('market')}")
    print(f"  symbol: {meta.get('symbol')}")
    print(f"  ws_url: {meta.get('ws_url')}")
    print(f"  rest_url: {meta.get('rest_url')}")
    print(f"  duration_s: {meta.get('duration_s')}  ping_interval_s: {meta.get('ping_interval_s')}")
    print(f"  started_at_utc: {meta.get('started_at_utc')}")
    print(f"  ended_at_utc: {meta.get('ended_at_utc')}")

    ts = result.time_sync
    print("time_sync:")
    print(f"  offset_ms: {ts.get('offset_ms'):.3f}  best_rtt_ms: {ts.get('best_rtt_ms'):.3f}  samples: {ts.get('sample_count')}")

    print("ws:")
    print(f"  connect_ms: {ws.get('connect_ms'):.3f}")
    print(f"  subscribe_ack_ms: {_fmt_num(ws.get('subscribe_ack_ms'))}")
    print(f"  first_ticker_ms: {_fmt_num(ws.get('first_ticker_ms'))}")

    def _fmt_stats(name: str, stats: Dict[str, Any]) -> None:
        print(f"{name}:")
        print(
            "  "
            f"count={stats.get('count')} min={_fmt_num(stats.get('min_ms'))} avg={_fmt_num(stats.get('avg_ms'))} "
            f"p50={_fmt_num(stats.get('p50_ms'))} p95={_fmt_num(stats.get('p95_ms'))} "
            f"p99={_fmt_num(stats.get('p99_ms'))} max={_fmt_num(stats.get('max_ms'))}"
        )

    _fmt_stats("ping_rtt_ms", result.ping_rtt_ms)
    _fmt_stats("ticker_latency_ms", result.ticker_latency_ms)
    print(f"ticker_ts_missing: {str(result.ticker_ts_missing).lower()}")
    print(f"counts: {result.counts}")


async def main_async(argv: List[str]) -> int:
    args = build_arg_parser().parse_args(argv)

    logger = Logger(args.log_level)
    started_wall = wall_time_ms()
    hostname = socket.gethostname()
    ws_url = args.ws_url
    if not ws_url:
        ws_url = DEFAULT_WS_URL_LINEAR if args.market == "linear" else DEFAULT_WS_URL_SPOT

    async with aiohttp.ClientSession() as session:
        time_sync = await measure_time_sync(
            session=session,
            rest_url=args.rest_url,
            samples=args.time_sync_samples,
            timeout_s=args.timeout,
            logger=logger,
        )

        ws_metrics, ping_stats, ticker_stats, ticker_ts_missing, counters = await run_ws_latency_test(
            session=session,
            ws_url=ws_url,
            market=args.market,
            symbol=args.symbol,
            duration_s=args.duration,
            ping_interval_s=args.ping_interval,
            clock_offset_ms=time_sync.best.offset_ms,
            timeout_s=args.timeout,
            log_interval_s=args.log_interval,
            logger=logger,
        )

    ended_wall = wall_time_ms()

    result = RunResult(
        meta={
            "hostname": hostname,
            "pid": os.getpid(),
            "market": args.market,
            "symbol": args.symbol,
            "ws_url": ws_url,
            "rest_url": args.rest_url,
            "duration_s": args.duration,
            "ping_interval_s": args.ping_interval,
            "time_sync_samples": args.time_sync_samples,
            "timeout_s": args.timeout,
            "started_at_utc": utc_iso_from_wall_ms(started_wall),
            "ended_at_utc": utc_iso_from_wall_ms(ended_wall),
        },
        time_sync={
            "offset_ms": time_sync.best.offset_ms,
            "best_rtt_ms": time_sync.best.rtt_ms,
            "server_time_ms": time_sync.best.server_time_ms,
            "sample_count": len(time_sync.samples),
            "rtt_ms": asdict(time_sync.rtt_ms),
        },
        ws={
            "connect_ms": ws_metrics.connect_ms,
            "subscribe_ack_ms": ws_metrics.subscribe_ack_ms,
            "first_ticker_ms": ws_metrics.first_ticker_ms,
        },
        ping_rtt_ms=asdict(ping_stats),
        ticker_latency_ms=asdict(ticker_stats),
        ticker_ts_missing=ticker_ts_missing,
        counts=counters,
    )

    print_summary(result)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(asdict(result), f, ensure_ascii=False, indent=2)

    return 0


def main() -> int:
    try:
        return asyncio.run(main_async(sys.argv[1:]))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

