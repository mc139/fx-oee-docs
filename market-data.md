# Market Data Feed

_Last updated: 2026-06-21 BST._

Two independent price-feed mechanisms feed the same injection point
(`TradingWebSocketHandler.injectMockQuote`). They can be enabled together or independently. When
both are on, they coordinate automatically: the mock maker stands down while Tiingo is streaming
real quotes and takes over again the moment the live feed goes quiet (weekends, network drops). The
section on [market-closed behaviour](#market-closed-behaviour) explains how that hand-off works.

---

## 1 · Tiingo live feed (`fxoee.tiingo.enabled=true`)

Replaces synthetic prices with real FX data from [Tiingo](https://tiingo.com) (free tier supports
all 7 pairs).

### Startup: historical OHLC (REST)

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

### Live feed: WebSocket

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

`thresholdLevel` is configurable: lower = more frequent ticks, higher = less noise for dev.

Incoming `"type":"Q"` messages are mapped to `CurrencyPair` and injected into the engine:

```
eurusd → CurrencyPair.EUR_USD → handler.injectMockQuote(pair, bid, ask, quantity)
```

Non-`"Q"` messages (heartbeat `"H"`, subscription ack `"I"`) are ignored. On disconnect the
service reconnects automatically after 5 seconds using a virtual thread.

### Market-closed behaviour

The FX market is closed Friday ~22:00 UTC to Sunday ~22:00 UTC. During this window the WebSocket
stays connected but Tiingo sends only heartbeats, so no real quotes arrive.

`TiingoMarketDataService.isLive()` is the signal everything keys off:

```java
private volatile long lastQuoteMs = 0;            // stamped on every "Q" message

public boolean isLive() {
    return lastQuoteMs > 0 && (System.currentTimeMillis() - lastQuoteMs) < 30_000;
}
```

If a real quote landed in the last 30 seconds, the feed counts as live. Once the gap passes 30
seconds (the weekend, or a dropped connection), `isLive()` flips to `false`. That is the trigger for
the automatic fallback below: if `MockMarketMaker` is enabled it simply resumes ticking and fills
the gap with synthetic depth. No restart, no manual switch. When Tiingo comes back, the first real
quote sets `lastQuoteMs` again and the mock maker stands back down on its next tick.

So the recommended weekend setup is to leave **both** feeds enabled and let them trade places on
their own, rather than toggling `MOCK_MARKET_ENABLED` by hand around the weekend.

### Configuration

| Key | Env var | Default | Meaning |
|---|---|---|---|
| `fxoee.tiingo.enabled` | `TIINGO_ENABLED` | `false` | Activate REST history + WebSocket feed. |
| `fxoee.tiingo.api-key` | `TIINGO_API_KEY` | _(required)_ | Tiingo API token; goes in `backend-secret`. |
| `fxoee.tiingo.history-days` | `TIINGO_HISTORY_DAYS` | `7` | Days of OHLC history loaded per timeframe on startup. |
| `fxoee.tiingo.threshold-level` | `TIINGO_THRESHOLD_LEVEL` | `5` | Tiingo `thresholdLevel` subscription field (not ms): lower = more frequent ticks, higher = quieter. |
| `fxoee.tiingo.quantity` | `TIINGO_QUANTITY` | `1000000` | Size of each LIMIT order injected per quote. |

> **Secret hygiene:** `TIINGO_API_KEY` must be placed in `k8s/base/backend/secret.yaml`, which is
> listed in `.gitignore` and never committed (copy `secret.example.yaml` as a starting point).

---

## 2 · MockMarketMaker (`fxoee.mock-market.enabled=true`)

Generates synthetic FX prices locally, with no external dependencies. Used as:

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
| θ (theta) | 0.003 | Half-life ~230 ticks (~2 min at 500 ms/tick); slow drift back to target |
| α (GARCH alpha) | 0.35 | Moderate volatility clustering after large moves |
| Spike probability | 0.4% | Rare 4-8× moves simulating news events |
| Dynamic spread | ×vol ratio | Spread widens proportionally when vol is elevated |

After a price shock the **OU target (μ) is updated to the shocked level**, so the process
continues from the new price rather than snapping back to the pre-shock base.

### The tick gate

Every tick checks two things before it touches the book:

```java
@Scheduled(fixedRateString = "${fxoee.mock-market.interval-ms:500}")
public void tick() {
    if (!enabled) return;
    if (tiingo != null && tiingo.isLive()) return;  // Tiingo streaming live quotes; stand down
    ...
}
```

The first guard is the on/off switch. The second is the coordination with Tiingo: while real quotes
are flowing the mock maker does nothing, which is why you can leave both feeds enabled at once
without them fighting over the book. The moment `isLive()` goes false (see
[market-closed behaviour](#market-closed-behaviour)) this guard stops short-circuiting and synthetic
ticks resume.

### Restart seeding

On startup `MockMarketMaker` seeds OHLC candle history for every (pair, timeframe) by replaying its
own generator backwards from "now". The last 1-minute close of that history is captured and pushed
into the live generator:

```java
if ("1m".equals(tf)) liveSeedMids = gen.currentMids();
...
liveGenerator.seedMids(liveSeedMids);   // first live tick continues from end-of-history price
```

Without this the live generator would start from its hardcoded `basePrice` and the first real tick
would print a huge candle jumping from the seeded history to the base. Seeding the generator makes
the chart continuous across a restart.

### Configuration

| Key | Env var | Default | Meaning |
|---|---|---|---|
| `fxoee.mock-market.enabled` | `MOCK_MARKET_ENABLED` | `false` | Enable `MockMarketMaker`. The constructor `@Value` fallback is `true`, but `application.yml` ships `false`, so it is off unless `MOCK_MARKET_ENABLED=true`. |
| `fxoee.mock-market.interval-ms` | none | `500` | Tick interval in milliseconds (used as `@Scheduled(fixedRateString=...)`). |
| `fxoee.mock-market.quantity` | none | `1000000` | Size of house bid/ask LIMIT orders. |
| `fxoee.mock-market.min-price-ratio` | none | `0.5` | Price floor: a generated mid cannot drop below `basePrice x ratio` (OU crash guard). `0.0` disables the floor. |

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

- **Status banner**: enabled/disabled indicator, current vol multiplier, live mid prices for all 7 pairs.
- **Volatility slider**: drag 0.1×-20×, then APPLY. RESET returns to 1×.
- **Price shock**: pair selector, ±2/5/10% preset buttons (or custom input), ⚡ SHOCK fires the shock
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

## 5 · Spread & stale-order metrics

A separate poller, `MarketDataBroadcaster` (`fxoee.market-data.enabled=true`, disabled by default),
runs on a fixed delay of `polling-interval-ms` (default 1000 ms). On each tick it pulls every pair's
mid from the active `MarketDataService` (the default `orderbook` provider, selectable via
`fxoee.market-data.provider`), broadcasts it as a `MARKET_DATA` WebSocket message, and then publishes
the health metrics below. It is independent of the two feeds above: it reads whatever depth is
currently resting, regardless of who put it there.

Per pair, on each tick it also computes the spread and how far the resting best bid/ask sit from an
external reference mid, then exposes them as Micrometer gauges:

| Metric | Type | Meaning |
|---|---|---|
| `fxoee.market.spread.bps` | gauge | `(bestAsk − bestBid) / mid × 10 000`, in basis points |
| `fxoee.market.bid.deviation.pips` | gauge | how far the best bid sits below the external mid, in pips |
| `fxoee.market.ask.deviation.pips` | gauge | how far the best ask sits above the external mid, in pips |
| `fxoee.market.stale.orders` | counter | incremented each time a level crosses the stale threshold |

A pip is `0.01` for USD/JPY and `0.0001` for every other pair. When a resting level drifts further
than `fxoee.market-data.stale-order-pip-threshold` pips (default 50) from the mid, the counter ticks
up and a `STALE_ORDER` event is pushed to every client subscribed to that pair:

```json
{
  "type": "STALE_ORDER",
  "payload": {
    "pair": "EUR_USD",
    "side": "BID",
    "bookPrice": "1.08200",
    "marketMid": "1.08720",
    "deviationPips": 52
  }
}
```

This is what flags house depth that the price feed has left behind, for example a mock level that
did not get refreshed, or a Tiingo gap that froze the book.

---

## 6 · Provider selection summary

The mock maker auto-suppresses while Tiingo is live (see [the tick gate](#the-tick-gate)), so leaving
both on is safe in every row below.

| Scenario | Config |
|---|---|
| Local dev, no API key | `MOCK_MARKET_ENABLED=true` |
| Production with live feed | `TIINGO_ENABLED=true`, `TIINGO_API_KEY=<key>`, `MOCK_MARKET_ENABLED=false` |
| Production, weekend fallback | `TIINGO_ENABLED=true`, `MOCK_MARKET_ENABLED=true` (mock fills in automatically when the feed goes quiet) |
| Stress-test (no real prices needed) | `MOCK_MARKET_ENABLED=true`, set vol multiplier high via DEBUG panel |
| Book-health monitoring | add `MARKET_DATA_ENABLED=true` for spread / stale-order metrics |
