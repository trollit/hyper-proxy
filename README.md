# Hyper Proxy

A caching proxy for the [Hyperliquid](https://hyperliquid.xyz) API written in Go, modeled after [binance-proxy](https://github.com/trollit/binance-proxy). It answers the common market data requests from a websocket fed cache and forwards everything else, so multiple bots (for example [Freqtrade](https://github.com/freqtrade/freqtrade) instances) can share one IP without running into Hyperliquid's REST rate limit of 1200 weight per minute.

## ⚡ Quick Start

```bash
go install github.com/trollit/hyper-proxy/cmd/hyper-proxy@latest
hyper-proxy
```

or with Docker:

```bash
docker run --rm -d -p 8092:8092 ghcr.io/trollit/hyper-proxy:latest
```

The proxy listens on port **8092** and mirrors the Hyperliquid API paths (`/info`, `/exchange`), so clients only need a different base URL. Status: `http://localhost:8092/status`.

### 🪙 Example Usage with Freqtrade

ccxt uses the `public` URL for all `/info` requests. Point it at the proxy:

```json
{
    "exchange": {
        "name": "hyperliquid",
        "ccxt_config": {
            "enableRateLimit": false,
            "urls": {
                "api": {
                    "public": "http://127.0.0.1:8092"
                }
            }
        },
        "ccxt_async_config": {
            "enableRateLimit": false
        }
    }
}
```

Orders (`/exchange`) keep going directly to Hyperliquid through the `private` URL. Setting `private` to the proxy as well works too: signatures don't depend on the host, and the requests then count against the proxy's shared rate limiter.

## ➡️ Cached requests

All Hyperliquid market data is requested with `POST /info` and a `type` in the JSON body. The proxy handles these types itself:

| Info type | Source | Comments |
|-----------|--------|----------|
| `candleSnapshot` | websocket `candle` | The first request for a coin/interval loads upstream's most recent ~5000 candles via REST, then the websocket keeps them current. Responses use the same range filter as upstream (candles overlapping `startTime`..`endTime`). Closed after `max(2 × interval, 2 min)` without requests. |
| `l2Book` | websocket `l2Book` | Default aggregation only (no `nSigFigs`/`mantissa`), 20 levels per side. Closed after 2 minutes without requests. |
| `allMids` | websocket `allMids` | Default dex only. Closed after 2 minutes without requests. |
| `meta`, `spotMeta`, `perpDexs` | REST, TTL cache | Cached for `--meta-ttl` (60s). |
| `metaAndAssetCtxs`, `spotMetaAndAssetCtxs` | REST, TTL cache | Cached for `--asset-ctx-ttl` (5s). These back ccxt's `fetch_markets` and `fetch_tickers`. |
| `fundingHistory` | REST, series cache | Kept per coin as one series that is refreshed after each hourly funding time, so any `startTime` is answered from cache (ccxt derives it from the current time). Freqtrade uses this to calculate funding fees. Ranges longer than 5000 hours, such as backtest downloads, are forwarded. Dropped after 2 hours without requests. |

Everything else, including all user-specific info requests and `/exchange`, is **forwarded** 1:1 to the upstream API.

Responses carry a `Data-Source` header: `websocket`, `cache`, `stale-cache`, `upstream` or `ban-protection`.

### How it works

- All websocket feeds share **one connection**, because Hyperliquid allows only 10 connections and 1000 subscriptions per IP. The connection is pinged every 30s, and it reconnects and resubscribes automatically. After a reconnect each candle feed fetches the candles it may have missed before it serves from cache again.
- A coin is only subscribed after a REST request has confirmed that it exists. Hyperliquid closes the whole websocket connection when a subscription names an unknown coin, so one bad request would otherwise disconnect every feed. Unknown coins are forwarded instead.
- REST snapshots and websocket updates are merged per candle, and the version with the most trades (`n`) wins. A slow REST response therefore never overwrites newer websocket data.
- Every upstream request, whether forwarded or made by the proxy itself, waits on a shared rate limiter that uses the [documented request weights](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits). If upstream answers `429`, the proxy stops forwarding until `Retry-After`. Meanwhile cached data is still served, and other requests get `429` with `Retry-After`.
- Until the first trade of a new period there is no candle for it upstream. By default the proxy then returns a flat "fake" candle at the previous close. Disable this with `-c`.

## 📊 Status Endpoint

`GET /status` returns request statistics, the ban state, and cache details:

```json
{
  "proxy_status": { "service": "github.com/trollit/hyper-proxy", "healthy": true, "uptime": "2h15m30s", "requests": 1542, "errors": 3, "error_rate": 0.19 },
  "upstream": "https://api.hyperliquid.xyz",
  "ban_info": { "banned": false, "recovery_time": null },
  "cache": {
    "websocket": { "connected": true, "subscriptions": 42 },
    "feeds": { "candle": 40, "l2Book": 1, "allMids": 1, "failed": 0 },
    "info_cache_entries": 3,
    "funding_series": 5,
    "rate_limit": { "weight_per_minute": 1200, "weight_available": 1104 }
  },
  "config": { "fake_candles_enabled": true, "always_show_forwards": false }
}
```

`GET /restart` shuts the process down gracefully after 2 seconds. It relies on a process manager or a Docker restart policy (`restart: unless-stopped`) to start it again. The endpoint has no authentication, so restrict network access to the proxy.

## ⚙️ Options

| Option | Environment Variable | Description | Default |
|--------|----------------------|-------------|---------|
| `-v` | `HPX_VERBOSE` | Debug logging | `false` |
| `-p` | `HPX_PORT` | Listen port | `8092` |
| `-u` | `HPX_UPSTREAM` | Hyperliquid API base URL | `https://api.hyperliquid.xyz` |
| `--testnet` | `HPX_TESTNET` | Use `https://api.hyperliquid-testnet.xyz` | `false` |
| `-c` | `HPX_DISABLE_FAKE_CANDLES` | Disable fake candles | `false` |
| `--candle-cache-size` | `HPX_CANDLE_CACHE_SIZE` | Candles kept per coin/interval (1–5000). Below 5000, requests reaching further back are forwarded. | `5000` |
| `--meta-ttl` | `HPX_META_TTL` | TTL for `meta`, `spotMeta`, `perpDexs` (`0` disables) | `60s` |
| `--asset-ctx-ttl` | `HPX_ASSET_CTX_TTL` | TTL for `metaAndAssetCtxs`, `spotMetaAndAssetCtxs` (`0` disables) | `5s` |
| `-a` | `HPX_ALWAYS_SHOW_FORWARDS` | Log every forwarded request even without `-v` | `false` |

## 🐳 Docker images

Every push to `main` and every `v*` tag builds a multi-arch (`linux/amd64`, `linux/arm64`) image and pushes it to `ghcr.io/trollit/hyper-proxy` and `registry.trollit.tech/hyper-proxy`. Tags: `latest` and `main` (from `main`), `sha-<short>`, and `<version>` / `<major>.<minor>` for releases. Release binaries are built by GoReleaser.

To build locally: `docker build -t hyper-proxy .`

## 💻 Development

```bash
make build   # bin/hyper-proxy
make lint    # golangci-lint (downloaded into ./bin)
go test -race ./...
```

## ⚠️ License

MIT, see [LICENSE](LICENSE). The log suppression and retry helpers come from binance-proxy, which is also MIT licensed.
