# Market Data Feed

_Last updated: 2026-06-07._

Two independent price-feed mechanisms run in parallel and feed the same injection point
(`TradingWebSocketHandler.injectMockQuote`). They can be enabled together or independently.

---

## 1 · Tiingo live feed (`fxoee.tiingo.enabled=true`)

Replaces synthetic prices with real FX data from [Tiingo](https://tiingo.com) (free tier supports
all 7 pairs).

### Startup — historical OHLC (REST)

On `@PostConstruct`, `TiingoMarketDataService` makes one REST call per **(pair × timeframe)**
combination (49 calls total) to seed the chart history:

```
GET https://api.tiingo.com/tiingo/fx/prices
    ?tickers=eurusd&startDate=2026-05-08&resampleFreq=1Min&token=KEY
```

Tiingo returns a flat array of OHLC bars; one call per pair avoids the 10 000-row response cap
on the free tier. Each bar maps directly to the internal `Candle(t, open, high, low, close)` record
and is written via `handler.seedCandleHistory(pair, tf, candles)`.

Supported timeframes and their Tiingo `resampleFreq` equivalents:

| Internal TF | Tiingo `resampleFreq` |
|---|---|
| `1m` | `1Min` |
| `5m` | `5Min` |
| `15m` | `15Min` |
| `30m` | `30Min` |
| `1h` | `1Hour` |
| `4h` | `4Hour` |
| `1d` | `1Day` |

### Live feed — WebSocket

After history loads, a persistent WebSocket connection is opened to `wss://api.tiingo.com/fx`:

```json
{
  "eventName": "subscribe",
  "authorization": "TOKEN",
  "eventData": {
    "thresholdLevel": 5,
    "tickers": ["eurusd","gbpusd","usdjpy","usdchf","audusd","usdcad","nzdusd"]
  }
}
```

`thresholdLevel` is configurable — lower = more frequent ticks, higher = less noise for dev.

Incoming `"type":"Q"` messages are mapped to `CurrencyPair` and injected into the engine:

```
eurusd → CurrencyPair.EUR_USD → handler.injectMockQuote(pair, bid, ask, quantity)
```

Non-`"Q"` messages (heartbeat `"H"`, subscription ack `"I"`) are ignored. On disconnect the
service reconnects automatically after 5 seconds using a virtual thread.

### Market-closed behaviour

The FX market is closed Friday ~22:00 UTC → Sunday ~22:00 UTC. During this window:
- The WebSocket stays connected but Tiingo sends only heartbeats.
- No `injectMockQuote` calls are made → order book is static.
- Enable `MockMarketMaker` in parallel (`MOCK_MARKET_ENABLED=true`) to keep synthetic depth
  during the weekend.

### Configuration

| Key | Env var | Default | Meaning |
|---|---|---|---|
| `fxoee.tiingo.enabled` | `TIINGO_ENABLED` | `false` | Activate REST history + WebSocket feed. |
| `fxoee.tiingo.api-key` | `TIINGO_API_KEY` | _(required)_ | Tiingo API token — goes in `backend-secret`. |
| `fxoee.tiingo.history-days` | `TIINGO_HISTORY_DAYS` | `7` | Days of OHLC history loaded per timeframe on startup. |
| `fxoee.tiingo.threshold-level` | `TIINGO_THRESHOLD_LEVEL` | `5` | WebSocket throttle (ms between price updates). |
| `fxoee.tiingo.quantity` | `TIINGO_QUANTITY` | `1000000` | Size of each LIMIT order injected per quote. |

> **Secret hygiene:** `TIINGO_API_KEY` must be placed in `k8s/backend/secret.yaml`, which is
> listed in `.gitignore` and never committed.

---

## 2 · MockMarketMaker (`fxoee.mock-market.enabled=true`)

Generates synthetic FX prices locally — no external dependencies. Used as:

- **Primary feed** when no Tiingo key is available.
- **Weekend fallback** alongside Tiingo when the real FX market is closed.

### Price model

`FxMockDataGenerator` implements an **Ornstein-Uhlenbeck** process with GARCH-lite volatility
clustering:

```
mid[t] = mid[t-1] + θ·(μ - mid[t-1]) + σ[t]·ε        (OU mean reversion)
σ[t]   = σ_base · volMultiplier · (1 + α·|ε[t-1]|)    (GARCH-lite clustering)
```

| Parameter | Value | Effect |
|---|---|---|
| θ (theta) | 0.003 | Half-life ~230 ticks (~2 min at 500 ms/tick) — slow drift back to target |
| α (GARCH alpha) | 0.35 | Moderate volatility clustering after large moves |
| Spike probability | 0.4% | Rare 4–8× moves simulating news events |
| Dynamic spread | ×vol ratio | Spread widens proportionally when vol is elevated |

After a price shock the **OU target (μ) is updated to the shocked level** — the process
continues from the new price rather than snapping back to the pre-shock base.

### Configuration

| Key | Env var | Default | Meaning |
|---|---|---|---|
| `fxoee.mock-market.enabled` | `MOCK_MARKET_ENABLED` | `false` | Enable `MockMarketMaker`. |
| `fxoee.mock-market.interval-ms` | — | `500` | Tick interval in milliseconds. |
| `fxoee.mock-market.quantity` | — | `1000000` | Size of house bid/ask LIMIT orders. |

---

## 3 · Runtime controls (DEBUG panel → MOCK MARKET tab)

When `MockMarketMaker` is active, its parameters can be changed live without a restart.

### REST API

All endpoints are unauthenticated (debug-only, same policy as `/api/debug/*`).

#### `GET /api/debug/mock-market/status`

Returns current state:

```json
{
  "enabled": true,
  "volatilityMultiplier": 1.0,
  "currentMids": {
    "EUR/USD": 1.08512,
    "GBP/USD": 1.27043,
    ...
  }
}
```

#### `POST /api/debug/mock-market/volatility`

Set the global volatility multiplier (applied on top of the per-pair base σ):

```json
{ "multiplier": 5.0 }
```

Range: `[0.1, 50]`. `1.0` = normal market conditions; `10.0` = extreme stress.

#### `POST /api/debug/mock-market/shock`

Instantly shift a pair's mid price by a percentage. The OU target is updated so the
generator continues from the new level rather than reverting immediately:

```json
{ "pair": "EUR_USD", "pct": -0.10 }
```

`pct` range: `[-0.5, 0.5]` (±50%). The engine injects the new price on the very next tick
(within `interval-ms`).

### Debug panel

The **MOCK MARKET** tab in the DEBUG screen surfaces all of the above:

- **Status banner** — enabled/disabled indicator, current vol multiplier, live mid prices for all 7 pairs.
- **Volatility slider** — drag 0.1×–20×, then APPLY. RESET returns to 1×.
- **Price shock** — pair selector, ±2/5/10% preset buttons (or custom input), ⚡ SHOCK fires the shock
  with a confirmation prompt. Colour coding: red = downward shock, green = upward.

---

## 4 · Injection point

Both feeds converge on the same method:

```java
TradingWebSocketHandler.injectMockQuote(CurrencyPair pair, BigDecimal bid, BigDecimal ask, BigDecimal qty)
```

This cancels the previous house LIMIT orders for the pair and places fresh BUY @ bid / SELL @ ask
against `HouseAccount.HOUSE_UUID`, providing permanent counterparty depth. After injection,
`broadcastSnapshot(pair)` and `broadcastAccountUpdate()` push the updated state to all connected
WebSocket clients.

---

## 5 · Provider selection summary

| Scenario | Config |
|---|---|
| Local dev, no API key | `MOCK_MARKET_ENABLED=true` |
| Production with live feed | `TIINGO_ENABLED=true`, `TIINGO_API_KEY=<key>`, `MOCK_MARKET_ENABLED=false` |
| Production, weekend fallback | `TIINGO_ENABLED=true`, `MOCK_MARKET_ENABLED=true` |
| Stress-test (no real prices needed) | `MOCK_MARKET_ENABLED=true`, set vol multiplier high via DEBUG panel |
