## CoinMetrics Pair-Candle Cache

FastAPI service that downloads 5-minute pair-candle data for `btc`, `eth`, and `tao_bittensor` from the CoinMetrics Community API, aggregates it into hourly windows, and serves randomized 1-hour point/interval predictions.

### Run

```bash
uvicorn app:app --host 0.0.0.0 --port 5500 --workers 1
```

### How it works

- Every hour at `HH:01:00` UTC the app fetches up to 7 days (`limit_per_pair=2016`) of 5-minute candles from `/v4/timeseries/pair-candles`.  
- If that pull fails, it retries immediately (3 attempts). After three failures it waits until `HH:06:00` (5 minutes later) and retries.  
- The fetched candles are cached in-memory and re-aggregated into hourly buckets:
  - price change ratio: `(price[h+1] - price[h]) / price[h]`
  - low/high ratios within the hour
  - buckets are split by `workday` vs `day_off` (weekends + major US holidays) and by UTC sessions (`asia/europe/us/off`).
- The cached dataset replaces old local JSON files; algo_01/algo_02 still use the same request contract but now rely entirely on the live CoinMetrics cache.
- A separate real-time cache hits `/v4/timeseries/asset-metrics` every second (frequency=`1s`) so the last 20 minutes of per-second prices are always available without extra API calls. These values power `point_and_interval`, which never looks into the future.

### Endpoints

- `GET /` – simple heartbeat
- `GET /health` – uptime info
- `GET /cache_health` – background fetch stats, last refresh, dataset sizes
- `GET /ip_stats` – in-process IP counters
- `GET /dataset_stats` – summary stats per asset/day-type/session
- `GET /config_data` – hot-reloaded view of `config.json`
- `GET /error_log` – returns the current contents of `error.log` for debugging
- `POST /point_and_interval` – main prediction endpoint (algos: `algo_default`, `algo_01`, `algo_02`)

Sample request:

```json
{
  "timestamp": "2025-10-29T10:00:00Z",
  "assets": ["btc", "eth"],
  "extra": { "algo": "algo_01" }
}
```

### Runtime config (`config.json`)

`config.json` lives in the repo root and is hot-reloaded (mtime watch) so edits apply without restarting.

```json
{
  "logging_level": "INFO",
  "scale_ratio": {
    "btc": { "base_point": 0.0, "ratio": 1.0 },
    "eth": { "base_point": 0.0, "ratio": 1.0 },
    "tao_bittensor": { "base_point": 0.0, "ratio": 1.0 },
    "default": { "base_point": 0.0, "ratio": 1.0 }
  },
  "ip_blacklist": ["203.0.113.42"]
}
```

- **logging_level** – applied to the root logger whenever the file changes.
- **scale_ratio** – per-asset scaling of raw values used in algo_01/algo_02: `scaled = (v - base_point) * ratio + base_point`. Absent entries fall back to the `default`.
- **ip_blacklist** – list of IPs blocked from `POST /point_and_interval`, `GET /ip_stats`, `GET /dataset_stats`, and `GET /config_data`.

### Monitoring & notes

- The service is designed for a single worker so that the in-memory dataset remains authoritative.
- Random scaling ratio (`±5%`) and flip rate (`10%`, defined in `data_provider.py`) are unchanged.
- Logs for `/point_and_interval` are still sampled (every 30th request at INFO; others at DEBUG).
- Errors and unexpected behaviors additionally stream into `error.log` (rewritten on startup) so you can tail/debug without sifting through info-level output.

### Notes

- The service is designed for a single worker so that the in-memory dataset remains authoritative.
- Random scaling ratio (`±5%`) and flip rate (`10%`, defined in `data_provider.py`) are unchanged.
- Logs for `/point_and_interval` are still sampled (every 30th request at INFO; others at DEBUG).

