from __future__ import annotations

import asyncio
import contextlib
import copy
import logging
import random
import statistics
import time
from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from uuid import uuid4

from config_manager import ConfigManager
from data_provider import (
    get_randomized_interval,
    get_randomized_point,
    get_rnd_point_interval,
    get_session_label,
    is_day_off,
)
import data_provider

# =========================
# Config / constants
# =========================

COINMETRICS_URL = "https://community-api.coinmetrics.io/v4/timeseries/pair-candles"
PAIR_SYMBOLS = ("btc-usd", "eth-usd", "tao_bittensor-usd")
PAIR_TO_ASSET = {
    "btc-usd": "btc",
    "eth-usd": "eth",
    "tao_bittensor-usd": "tao_bittensor",
}
ASSETS_CANONICAL = ("btc", "eth", "tao_bittensor")
FREQUENCY = "5m"
PAGE_SIZE = 10000
LIMIT_PER_PAIR = 2016  # ~7 days of 5m candles
HTTP_TIMEOUT = 30.0
FETCH_MINUTE_MARK = 1
FALLBACK_DELAY = timedelta(minutes=5)
SESSION_KEYS = ("asia", "europe", "us", "off")

REALTIME_URL = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
REALTIME_METRIC = "ReferenceRate"
REALTIME_FREQUENCY = "1s"
REALTIME_PAGE_SIZE = 3000
REALTIME_LIMIT_CAP = 1500
REALTIME_REFRESH_INTERVAL = 1.0
REALTIME_RETENTION = timedelta(minutes=20)
MARGINS = {
    "btc": 500.0,
    "eth": 30.0,
    "tao_bittensor": 5.0,
}

ERROR_LOG_PATH = "error.log"
ERROR_LOG_MAX_BYTES = 512 * 1024
ERROR_LOG_BACKUP_COUNT = 1
ERROR_LOG_TAIL_BYTES = 64 * 1024

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# Re-initialize error log on startup so each process run starts fresh.
with open(ERROR_LOG_PATH, "w", encoding="utf-8"):
    pass
_error_handler = RotatingFileHandler(
    ERROR_LOG_PATH,
    maxBytes=ERROR_LOG_MAX_BYTES,
    backupCount=ERROR_LOG_BACKUP_COUNT,
    encoding="utf-8",
)
_error_handler.setLevel(logging.ERROR)
_error_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_error_handler)


def _req_id() -> str:
    return uuid4().hex[:8]


_POINT_LOG_EVERY = 30
_point_counter = -1
_REALTIME_LOG_EVERY = 60
_realtime_fetch_counter = -1

# =========================
# IP tracking
# =========================
_ip_counts: Dict[str, int] = {}
_ip_counts_lock = asyncio.Lock()
_ip_total_requests = 0


def _extract_client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    xri = request.headers.get("x-real-ip")
    if xri:
        return xri.strip()
    return request.client.host if request.client else "unknown"


def _register_ip_logging_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def ip_logging_middleware(request: Request, call_next):
        global _ip_total_requests
        if request.url.path == "/ip_stats":
            return await call_next(request)
        client_ip = _extract_client_ip(request)
        async with _ip_counts_lock:
            _ip_counts[client_ip] = _ip_counts.get(client_ip, 0) + 1
            _ip_total_requests += 1
            total = _ip_total_requests
            unique_ips = len(_ip_counts)
        if total % 100 == 0:
            log.info(
                "IP_STATS total=%d unique=%d recent_ip=%s count=%d",
                total,
                unique_ips,
                client_ip,
                _ip_counts.get(client_ip, 0),
            )
        return await call_next(request)


config_mgr = ConfigManager()


async def _ensure_ip_allowed(request: Request) -> str:
    client_ip = _extract_client_ip(request)
    blacklist = await config_mgr.get_ip_blacklist()
    if client_ip in blacklist:
        raise HTTPException(status_code=403, detail="Forbidden")
    return client_ip

# =========================
# Utilities
# =========================


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_seconds(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_coinmetrics_time(raw: str) -> datetime:
    body = raw[:-1] if raw.endswith("Z") else raw
    if "." in body:
        main, frac = body.split(".", 1)
        frac6 = (frac[:6]).ljust(6, "0")
        dt = datetime.strptime(f"{main}.{frac6}", "%Y-%m-%dT%H:%M:%S.%f")
    else:
        dt = datetime.strptime(body, "%Y-%m-%dT%H:%M:%S")
    return dt.replace(tzinfo=timezone.utc)


def to_datetime(timestamp: Union[str, float, int, datetime]) -> datetime:
    if isinstance(timestamp, datetime):
        return timestamp.astimezone(timezone.utc)
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    if isinstance(timestamp, str):
        try:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except ValueError:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    raise TypeError("timestamp must be ISO8601 str, POSIX number, or datetime")


def normalize_asset(name: str) -> Optional[str]:
    alias = {
        "btc": "btc",
        "bitcoin": "btc",
        "xbt": "btc",
        "eth": "eth",
        "ethereum": "eth",
        "tao": "tao_bittensor",
        "bittensor": "tao_bittensor",
        "tao_bittensor": "tao_bittensor",
    }
    key = name.strip().lower().replace(" ", "").replace("-", "_")
    return alias.get(key)


def round5(value: float) -> float:
    return round(value, 5)


async def _wait_cache_ready(wait_coro, timeout: float, name: str, required: bool = True) -> bool:
    try:
        await asyncio.wait_for(wait_coro, timeout)
        return True
    except asyncio.TimeoutError:
        log.error("%s cache not ready within %.1fs", name, timeout)
        if required:
            raise HTTPException(status_code=503, detail=f"{name} cache unavailable")
        return False


# =========================
# Candle + dataset helpers
# =========================


@dataclass
class Candle:
    time: datetime
    price_open: float
    price_high: float
    price_low: float
    price_close: float


def _empty_dataset() -> Dict[str, Any]:
    return {
        asset: {
            day_type: {session: [[], []] for session in SESSION_KEYS}
            for day_type in ("workday", "day_off")
        }
        for asset in ASSETS_CANONICAL
    }


def _group_candles_by_hour(candles: List[Candle]) -> Dict[datetime, List[Candle]]:
    grouped: Dict[datetime, List[Candle]] = {}
    for c in candles:
        hour_start = c.time.replace(minute=0, second=0, microsecond=0)
        grouped.setdefault(hour_start, []).append(c)
    return grouped


def _build_hourly_dataset(candles_by_asset: Dict[str, List[Candle]]) -> Tuple[Dict[str, Any], Dict[str, int]]:
    dataset = _empty_dataset()
    hour_counts = {asset: 0 for asset in ASSETS_CANONICAL}
    for asset, candles in candles_by_asset.items():
        grouped = _group_candles_by_hour(candles)
        for hour_start in sorted(grouped.keys()):
            hour_candles = grouped[hour_start]
            if len(hour_candles) < 2:
                continue
            base_price = hour_candles[0].price_close
            if base_price <= 0:
                continue
            final_price = hour_candles[-1].price_close
            low_price = min(c.price_low for c in hour_candles)
            high_price = max(c.price_high for c in hour_candles)
            point_ratio = (final_price - base_price) / base_price
            low_ratio = (low_price - base_price) / base_price
            high_ratio = (high_price - base_price) / base_price
            day_type = "day_off" if is_day_off(hour_start) else "workday"
            session = get_session_label(hour_start)
            bucket = dataset[asset][day_type][session]
            bucket[0].append(point_ratio)
            bucket[1].append([low_ratio, high_ratio])
            hour_counts[asset] += 1
    return dataset, hour_counts


@dataclass
class Series:
    times: List[datetime] = field(default_factory=list)
    prices: List[float] = field(default_factory=list)

    def upsert_many(self, items: List[Tuple[datetime, float]]) -> None:
        if not items:
            return
        items.sort(key=lambda x: x[0])
        for ts, price in items:
            idx = bisect_left(self.times, ts)
            if idx < len(self.times) and self.times[idx] == ts:
                self.prices[idx] = price
            else:
                self.times.insert(idx, ts)
                self.prices.insert(idx, price)

    def prune_older_than(self, threshold: datetime) -> None:
        idx = bisect_left(self.times, threshold)
        if idx > 0:
            del self.times[:idx]
            del self.prices[:idx]

    def latest_time(self) -> Optional[datetime]:
        return self.times[-1] if self.times else None

    def latest_at_or_before(self, target: datetime) -> Optional[Tuple[datetime, float]]:
        if not self.times:
            return None
        idx = bisect_right(self.times, target) - 1
        if idx < 0:
            return None
        return self.times[idx], self.prices[idx]


class CandleCache:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._candles: Dict[str, List[Candle]] = {asset: [] for asset in ASSETS_CANONICAL}
        self._times: Dict[str, List[datetime]] = {asset: [] for asset in ASSETS_CANONICAL}
        self._dataset: Dict[str, Any] = _empty_dataset()
        self._hour_counts: Dict[str, int] = {asset: 0 for asset in ASSETS_CANONICAL}
        self.last_refresh_ok: bool = False
        self.last_refresh_at: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.started_at = time.time()

    async def replace(self, candles: Dict[str, List[Candle]]) -> None:
        dataset, hour_counts = _build_hourly_dataset(candles)
        async with self._lock:
            self._candles = candles
            self._times = {asset: [c.time for c in candles_list] for asset, candles_list in candles.items()}
            self._dataset = dataset
            self._hour_counts = hour_counts
            self.last_refresh_ok = True
            self.last_refresh_at = utc_now()
            self.last_error = None
            self._ready.set()

    async def mark_failure(self, error: str) -> None:
        async with self._lock:
            self.last_refresh_ok = False
            self.last_error = error
            self.last_refresh_at = utc_now()

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def hour_window(self, asset: str, requested: datetime) -> Optional[List[Tuple[datetime, float]]]:
        async with self._lock:
            times = self._times.get(asset) or []
            candles = self._candles.get(asset) or []
            if not times:
                return None
            idx = bisect_right(times, requested) - 1
            if idx < 0:
                return None
            result: List[Tuple[datetime, float]] = []
            end_ts = requested + timedelta(hours=1)
            for i in range(idx, len(candles)):
                candle = candles[i]
                if candle.time > end_ts:
                    break
                result.append((candle.time, candle.price_close))
            return result if result else None

    async def dataset_snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return copy.deepcopy(self._dataset)

    async def health(self) -> Dict[str, Any]:
        async with self._lock:
            rows_per_asset = {asset: len(candles) for asset, candles in self._candles.items()}
            hour_counts = dict(self._hour_counts)
            last_refresh_at = self.last_refresh_at.isoformat() if self.last_refresh_at else None
            last_error = self.last_error
            ready = self._ready.is_set()
            last_age = (utc_now() - self.last_refresh_at).total_seconds() if self.last_refresh_at else None
        return {
            "uptime_sec": time.time() - self.started_at,
            "dataset_ready": ready,
            "last_refresh_ok": self.last_refresh_ok,
            "last_refresh_at": last_refresh_at,
            "last_refresh_age_sec": last_age,
            "last_error": last_error,
            "rows_per_asset": rows_per_asset,
            "hour_windows_per_asset": hour_counts,
        }


candle_cache = CandleCache()


class RealTimeCache:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self.series: Dict[str, Series] = {asset: Series() for asset in ASSETS_CANONICAL}
        self.last_fetch_ok: bool = False
        self.last_fetch_at: Optional[float] = None
        self.last_error: Optional[str] = None
        self.started_at = time.time()

    async def merge(self, payload: Dict[str, Any]) -> None:
        per_asset: Dict[str, List[Tuple[datetime, float]]] = {asset: [] for asset in ASSETS_CANONICAL}
        for row in payload.get("data", []):
            asset = (row.get("asset") or "").lower()
            if asset not in per_asset:
                continue
            try:
                ts = parse_coinmetrics_time(row["time"])
                price = float(row[REALTIME_METRIC])
            except (KeyError, TypeError, ValueError):
                continue
            per_asset[asset].append((ts, price))
        updated = any(per_asset[asset] for asset in per_asset)
        if not updated:
            return
        async with self._lock:
            threshold = utc_now() - REALTIME_RETENTION
            for asset, items in per_asset.items():
                if items:
                    self.series[asset].upsert_many(items)
                    self.series[asset].prune_older_than(threshold)
            self.last_fetch_ok = True
            self.last_fetch_at = time.time()
            self.last_error = None
            self._ready.set()

    async def mark_failure(self, error: str) -> None:
        async with self._lock:
            self.last_fetch_ok = False
            self.last_error = error
            self.last_fetch_at = time.time()

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def snapshot_latest_times(self) -> Dict[str, Optional[datetime]]:
        async with self._lock:
            return {asset: self.series[asset].latest_time() for asset in ASSETS_CANONICAL}

    async def resolve_price(self, asset: str, target: datetime) -> Optional[Tuple[datetime, float]]:
        async with self._lock:
            series = self.series.get(asset)
            if not series:
                return None
            return series.latest_at_or_before(target)

    async def health(self) -> Dict[str, Any]:
        async with self._lock:
            latest = {asset: (series.latest_time().isoformat() if series.latest_time() else None)
                      for asset, series in self.series.items()}
            last_fetch_at = self.last_fetch_at
            last_error = self.last_error
            ready = self._ready.is_set()
        last_age = (time.time() - last_fetch_at) if last_fetch_at else None
        return {
            "uptime_sec": time.time() - self.started_at,
            "ready": ready,
            "last_fetch_ok": self.last_fetch_ok,
            "last_fetch_age_sec": last_age,
            "last_error": last_error,
            "latest_per_asset": latest,
        }


realtime_cache = RealTimeCache()


async def _compute_realtime_limit_per_asset() -> int:
    latest_map = await realtime_cache.snapshot_latest_times()
    end_dt = utc_now() - timedelta(seconds=1)
    latest_candidates = [ts for ts in latest_map.values() if ts is not None]
    if latest_candidates:
        min_latest = min(latest_candidates)
    else:
        min_latest = end_dt - REALTIME_RETENTION
    seconds_needed = max(1, int((end_dt - min_latest).total_seconds()))
    return min(seconds_needed, int(REALTIME_RETENTION.total_seconds()), REALTIME_LIMIT_CAP)


async def fetch_realtime_and_merge() -> None:
    assert _http is not None
    limit_per_asset = await _compute_realtime_limit_per_asset()
    end_time_str = iso_seconds(utc_now() - timedelta(seconds=1))
    params = {
        "assets": ",".join(ASSETS_CANONICAL),
        "metrics": REALTIME_METRIC,
        "frequency": REALTIME_FREQUENCY,
        "end_time": end_time_str,
        "limit_per_asset": str(limit_per_asset),
        "page_size": str(REALTIME_PAGE_SIZE),
    }
    try:
        resp = await _http.get(REALTIME_URL, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        await realtime_cache.merge(payload)
        global _realtime_fetch_counter
        _realtime_fetch_counter += 1
        if _realtime_fetch_counter % _REALTIME_LOG_EVERY == 0:
            log.info("Realtime fetch merged limit_per_asset=%d", limit_per_asset)
    except Exception as exc:
        await realtime_cache.mark_failure(str(exc))
        log.error("Realtime fetch error: %s", exc)
        raise


async def realtime_refresh_loop() -> None:
    while True:
        started = time.time()
        try:
            await fetch_realtime_and_merge()
        except Exception:
            pass
        sleep_for = max(0.0, REALTIME_REFRESH_INTERVAL - (time.time() - started))
        await asyncio.sleep(sleep_for)
_http: Optional[httpx.AsyncClient] = None
_refresh_task: Optional[asyncio.Task] = None
_realtime_task: Optional[asyncio.Task] = None

# =========================
# CoinMetrics fetcher
# =========================


async def fetch_pair_candles() -> Dict[str, List[Candle]]:
    assert _http is not None
    params = {
        "pairs": ",".join(PAIR_SYMBOLS),
        "frequency": FREQUENCY,
        "page_size": str(PAGE_SIZE),
        "limit_per_pair": str(LIMIT_PER_PAIR),
    }
    resp = await _http.get(COINMETRICS_URL, params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    payload = resp.json()
    candles: Dict[str, List[Candle]] = {asset: [] for asset in ASSETS_CANONICAL}
    for row in payload.get("data", []):
        pair = (row.get("pair") or "").lower()
        asset = PAIR_TO_ASSET.get(pair)
        if not asset:
            continue
        try:
            candle = Candle(
                time=parse_coinmetrics_time(row["time"]),
                price_open=float(row["price_open"]),
                price_high=float(row["price_high"]),
                price_low=float(row["price_low"]),
                price_close=float(row["price_close"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            log.warning("Skipping malformed candle for %s: %s", pair, exc)
            continue
        candles[asset].append(candle)
    for asset in candles:
        candles[asset].sort(key=lambda c: c.time)
    return candles


async def refresh_from_coinmetrics(tag: str) -> bool:
    attempts = 0
    last_error = ""
    while attempts < 3:
        attempts += 1
        try:
            candles = await fetch_pair_candles()
            total_rows = sum(len(v) for v in candles.values())
            await candle_cache.replace(candles)
            log.info("Fetched %d candles tag=%s attempt=%d", total_rows, tag, attempts)
            return True
        except Exception as exc:
            last_error = str(exc)
            log.warning("Fetch attempt %d (%s) failed: %s", attempts, tag, exc)
    await candle_cache.mark_failure(last_error or "unknown error")
    return False


def _next_minute_mark(minute: int) -> datetime:
    now = utc_now()
    candidate = now.replace(minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(hours=1)
    return candidate


async def _sleep_until(target: datetime) -> None:
    while True:
        now = utc_now()
        delta = (target - now).total_seconds()
        if delta <= 0:
            return
        await asyncio.sleep(min(delta, 30.0))


async def refresh_scheduler() -> None:
    await refresh_from_coinmetrics("startup")
    while True:
        next_run = _next_minute_mark(FETCH_MINUTE_MARK)
        await _sleep_until(next_run)
        ok = await refresh_from_coinmetrics("scheduled")
        if ok:
            continue
        fallback_run = next_run + FALLBACK_DELAY
        await _sleep_until(fallback_run)
        await refresh_from_coinmetrics("fallback")


# =========================
# FastAPI app
# =========================


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http, _refresh_task, _realtime_task
    _http = httpx.AsyncClient(headers={"User-Agent": "precog-cm-fetcher/4.0"})
    _refresh_task = asyncio.create_task(refresh_scheduler())
    _realtime_task = asyncio.create_task(realtime_refresh_loop())
    log.info("Startup complete; schedulers running")
    yield
    for task in (_refresh_task, _realtime_task):
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
    if _http:
        await _http.aclose()
    log.info("Shutdown complete")


app = FastAPI(title="Precog CoinMetrics API", version="4.0.0", lifespan=lifespan)
_register_ip_logging_middleware(app)

# =========================
# Algo helpers
# =========================


def _apply_scale(asset: str, value: float, scale_info: Dict[str, Dict[str, float]]) -> float:
    cfg = scale_info.get(asset) or scale_info.get("default") or {"base_point": 0.0, "ratio": 1.0}
    base_point = cfg.get("base_point", 0.0)
    ratio = cfg.get("ratio", 1.0)
    return (value - base_point) * ratio + base_point


async def algo_default(
    request_ts: datetime,
    assets: List[str],
    extra_info: Dict[str, Any],
    price_refs: Dict[str, Tuple[datetime, float]],
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for asset in assets:
        match = price_refs.get(asset)
        if not match:
            results[asset] = {"error": "no realtime data"}
            continue
        matched_ts, base_price = match
        margin = MARGINS[asset]
        pred_price = base_price + random.uniform(-margin, margin)
        low = base_price - random.uniform(0.0, margin)
        high = base_price + random.uniform(0.0, margin)
        low, high = sorted((low, high))
        results[asset] = {
            "requested_timestamp": iso_seconds(request_ts),
            "matched_timestamp": matched_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "base_price": round5(base_price),
            "pred_price": round5(pred_price),
            "pred_interval": [round5(low), round5(high)],
        }
    return {"ok": True, "result": results, "algo": "algo_default"}


def _convert_ratios(base_price: float, point: float, interval: List[float]) -> Tuple[float, List[float]]:
    point_abs = point * base_price
    interval_abs = [interval[0] * base_price, interval[1] * base_price]
    return point_abs, interval_abs


async def algo_01(
    request_ts: datetime,
    assets: List[str],
    extra_info: Dict[str, Any],
    data_ready: Dict[str, Any],
    scale_info: Dict[str, Dict[str, float]],
    price_refs: Dict[str, Tuple[datetime, float]],
    random_jitter_ratio: float,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for asset in assets:
        match = price_refs.get(asset)
        if not match:
            results[asset] = {"error": "no realtime data"}
            continue
        matched_ts, base_price = match
        ok, point, interval = get_rnd_point_interval(asset, matched_ts, data_ready, picker=None, scheme=None)
        if not ok:
            results[asset] = {"error": "internal algo error"}
            continue
        point_abs, interval_abs = _convert_ratios(base_price, point, interval)
        point_abs *= random.uniform(1 - random_jitter_ratio, 1 + random_jitter_ratio)
        interval_abs = [
            interval_abs[0] * random.uniform(1 - random_jitter_ratio, 1 + random_jitter_ratio),
            interval_abs[1] * random.uniform(1 - random_jitter_ratio, 1 + random_jitter_ratio),
        ]
        point_abs = _apply_scale(asset, point_abs, scale_info)
        interval_abs = [
            _apply_scale(asset, interval_abs[0], scale_info),
            _apply_scale(asset, interval_abs[1], scale_info),
        ]
        pred_price = base_price + point_abs
        interval_low = base_price + interval_abs[0]
        interval_high = base_price + interval_abs[1]
        low, high = sorted((interval_low, interval_high))
        bucket_label = f"{'day_off' if is_day_off(matched_ts) else 'workday'}:{get_session_label(matched_ts)}"
        results[asset] = {
            "requested_timestamp": iso_seconds(request_ts),
            "matched_timestamp": matched_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "base_price": round5(base_price),
            "pred_price": round5(pred_price),
            "pred_interval": [round5(low), round5(high)],
            "algo_bucket": bucket_label,
        }
    return {"ok": True, "result": results, "algo": "algo_01"}


async def algo_02(
    request_ts: datetime,
    assets: List[str],
    extra_info: Dict[str, Any],
    data_ready: Dict[str, Any],
    scale_info: Dict[str, Dict[str, float]],
    price_refs: Dict[str, Tuple[datetime, float]],
    random_jitter_ratio: float,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for asset in assets:
        match = price_refs.get(asset)
        if not match:
            results[asset] = {"error": "no realtime data"}
            continue
        matched_ts, base_price = match
        f1, point = get_randomized_point(asset, matched_ts, data_ready, picker=None, scheme=None)
        f2, interval = get_randomized_interval(asset, matched_ts, data_ready, picker=None, scheme=None)
        if not (f1 and f2):
            results[asset] = {"error": "internal algo error"}
            continue
        point_abs, interval_abs = _convert_ratios(base_price, point, interval)
        point_abs *= random.uniform(1 - random_jitter_ratio, 1 + random_jitter_ratio)
        interval_abs = [
            interval_abs[0] * random.uniform(1 - random_jitter_ratio, 1 + random_jitter_ratio),
            interval_abs[1] * random.uniform(1 - random_jitter_ratio, 1 + random_jitter_ratio),
        ]
        point_abs = _apply_scale(asset, point_abs, scale_info)
        interval_abs = [
            _apply_scale(asset, interval_abs[0], scale_info),
            _apply_scale(asset, interval_abs[1], scale_info),
        ]
        pred_price = base_price + point_abs
        interval_low = base_price + interval_abs[0]
        interval_high = base_price + interval_abs[1]
        low, high = sorted((interval_low, interval_high))
        bucket_label = f"{'day_off' if is_day_off(matched_ts) else 'workday'}:{get_session_label(matched_ts)}"
        results[asset] = {
            "requested_timestamp": iso_seconds(request_ts),
            "matched_timestamp": matched_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "base_price": round5(base_price),
            "pred_price": round5(pred_price),
            "pred_interval": [round5(low), round5(high)],
            "algo_bucket": bucket_label,
        }
    return {"ok": True, "result": results, "algo": "algo_02"}


# =========================
# Models & endpoints
# =========================


class PointAndIntervalRequest(BaseModel):
    timestamp: Union[str, float, int, datetime] = Field(..., description="ISO 8601 Z, POSIX seconds, or datetime")
    assets: List[str] = Field(..., description='Asset names, e.g. ["btc","eth","tao"]')
    extra: Dict[str, Any] = Field(default_factory=dict)


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "uptime_sec": time.time() - candle_cache.started_at,
        "timestamp": iso_seconds(utc_now()),
    }


@app.get("/cache_health")
async def cache_health() -> dict:
    rid = _req_id()
    start = time.time()
    hist = await candle_cache.health()
    rt = await realtime_cache.health()
    resp = {
        "status": "ok",
        "timestamp": iso_seconds(utc_now()),
        "historical": hist,
        "realtime": rt,
    }
    dur = (time.time() - start) * 1000.0
    log.info("CACHE_HEALTH rid=%s status=200 dur=%.1fms", rid, dur)
    return resp


@app.post("/point_and_interval")
async def point_and_interval(request: Request, req: PointAndIntervalRequest) -> Dict[str, Any]:
    rid = _req_id()
    start = time.time()
    global _point_counter
    _point_counter += 1
    log_func = log.info if _point_counter % _POINT_LOG_EVERY == 0 else log.debug
    log_func("POINT rid=%s recv assets=%s", rid, req.assets)

    await _ensure_ip_allowed(request)
    await _wait_cache_ready(candle_cache.wait_ready(), timeout=5.0, name="historical", required=True)
    realtime_ready = await _wait_cache_ready(realtime_cache.wait_ready(), timeout=3.0, name="realtime", required=False)

    try:
        base_ts = to_datetime(req.timestamp)
    except Exception as exc:
        dur = (time.time() - start) * 1000.0
        log.warning("POINT rid=%s status=400 dur=%.1fms err=%s", rid, dur, exc)
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {exc}")

    normalized: List[str] = []
    unknown: List[str] = []
    for asset in req.assets:
        can = normalize_asset(asset)
        if can:
            normalized.append(can)
        else:
            unknown.append(asset)

    if not normalized:
        raise HTTPException(status_code=400, detail=f"No supported assets from {req.assets}. Unknown={unknown}")
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unsupported assets in request: {unknown}")

    price_refs: Dict[str, Tuple[datetime, float]] = {}
    if realtime_ready:
        for asset in normalized:
            match = await realtime_cache.resolve_price(asset, base_ts)
            if match:
                price_refs[asset] = match
            else:
                log.warning("Realtime data unavailable for %s at %s", asset, iso_seconds(base_ts))
    else:
        log.warning("Realtime cache unavailable; responding with per-asset errors")

    try:
        data_ready = await candle_cache.dataset_snapshot()
        scale_info = await config_mgr.get_scale_ratio_info()
        flip_rate = await config_mgr.get_flip_rate()
        random_jitter_ratio = await config_mgr.get_random_jitter_ratio()
        data_provider.flip_rate = flip_rate
    except Exception as exc:
        dur = (time.time() - start) * 1000.0
        log.warning("POINT rid=%s status=500 dur=%.1fms config_error=%s", rid, dur, exc)
        raise HTTPException(status_code=500, detail=f"Config error: {exc}")

    algo_name = (req.extra or {}).get("algo", "algo_default")
    if algo_name == "algo_default":
        resp = await algo_default(base_ts, normalized, req.extra, price_refs)
    elif algo_name == "algo_01":
        resp = await algo_01(
            base_ts, normalized, req.extra, data_ready, scale_info, price_refs, random_jitter_ratio
        )
    elif algo_name == "algo_02":
        resp = await algo_02(
            base_ts, normalized, req.extra, data_ready, scale_info, price_refs, random_jitter_ratio
        )
    else:
        resp = await algo_default(base_ts, normalized, req.extra, price_refs)

    dur = (time.time() - start) * 1000.0
    log_func = log.info if _point_counter % _POINT_LOG_EVERY == 0 else log.debug
    log_func("POINT rid=%s status=200 dur=%.1fms algo=%s assets=%s", rid, dur, algo_name, normalized)
    return resp


@app.get("/ip_stats")
async def ip_stats(request: Request) -> Dict[str, Any]:
    await _ensure_ip_allowed(request)
    async with _ip_counts_lock:
        counts_copy = dict(_ip_counts)
        total = _ip_total_requests
        unique_ips = len(counts_copy)
    return {
        "timestamp": iso_seconds(utc_now()),
        "total_requests": total,
        "unique_ips": unique_ips,
        "counts": counts_copy,
    }


@app.get("/")
async def root() -> Dict[str, Any]:
    return {"message": "Okay, good! [CM pair candles]"}


def _summarize_dataset(data_ready: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for asset, buckets in data_ready.items():
        asset_stats: Dict[str, Any] = {}
        for day_type, sessions in buckets.items():
            for session, pair in sessions.items():
                points, intervals = pair
                key = f"{day_type}:{session}"
                n_points = len(points)
                n_intervals = len(intervals)
                if n_points == 0 or n_intervals == 0:
                    asset_stats[key] = {
                        "n_points": n_points,
                        "n_intervals": n_intervals,
                        "empty": True,
                    }
                    continue
                widths = [hi - lo for lo, hi in intervals]
                mean_p = statistics.mean(points)
                std_p = statistics.pstdev(points) if n_points > 1 else 0.0
                mean_abs_p = statistics.mean(abs(p) for p in points)
                mean_w = statistics.mean(widths)
                asset_stats[key] = {
                    "n_points": n_points,
                    "n_intervals": n_intervals,
                    "mean_point": mean_p,
                    "std_point": std_p,
                    "mean_abs_point": mean_abs_p,
                    "mean_interval_width": mean_w,
                }
        out[asset] = asset_stats
    return out


@app.get("/dataset_stats")
async def dataset_stats(request: Request) -> Dict[str, Any]:
    await _ensure_ip_allowed(request)
    rid = _req_id()
    start = time.time()
    try:
        data_ready = await candle_cache.dataset_snapshot()
    except Exception as exc:
        dur = (time.time() - start) * 1000.0
        log.warning("DATASET_STATS rid=%s status=500 dur=%.1fms err=%s", rid, dur, exc)
        raise HTTPException(status_code=500, detail=f"Dataset error: {exc}")

    stats = _summarize_dataset(data_ready)
    dur = (time.time() - start) * 1000.0
    log.info("DATASET_STATS rid=%s status=200 dur=%.1fms", rid, dur)
    return {"ok": True, "timestamp": iso_seconds(utc_now()), "stats": stats}


@app.get("/config_data")
async def config_data(request: Request) -> Dict[str, Any]:
    await _ensure_ip_allowed(request)
    cfg = await config_mgr.get_config()
    return {"timestamp": iso_seconds(utc_now()), "config": cfg}


def _read_error_log_tail(limit_bytes: int = ERROR_LOG_TAIL_BYTES) -> str:
    try:
        with open(ERROR_LOG_PATH, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            seek_pos = max(0, size - limit_bytes)
            fh.seek(seek_pos)
            data = fh.read()
            return data.decode("utf-8", errors="replace")
    except FileNotFoundError:
        return ""


@app.get("/error_log")
async def error_log(request: Request) -> Dict[str, Any]:
    await _ensure_ip_allowed(request)
    content = _read_error_log_tail()
    return {"timestamp": iso_seconds(utc_now()), "log_tail": content}
