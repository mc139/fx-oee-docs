# Circuit Breaker

_Last updated: 2026-06-05 BST._

Price-deviation circuit breaker. Watches the relative jump between consecutive executed trade prices
per currency pair and halts a pair whose price moves too far in one step.

## How it works

1. After every `MatchingService.submit(order)` produces trades, the executed trade prices are fed to
   [`CircuitBreaker.onTrade`](../src/main/java/com/fxoee/service/CircuitBreaker.java) — **outside the
   per-pair book lock** (a halt broadcasts over WebSocket, which must never run under the hot-path
   lock). The breaker is resolved lazily via an `ObjectProvider` to break the
   `MatchingService → TradingWebSocketHandler → CircuitBreaker` construction cycle.
2. The first trade for a pair only records a baseline (`lastPrice`). Each subsequent trade computes
   `deviation = |price − lastPrice| / lastPrice`.
3. If `deviation > threshold` **and** the pair is currently `OPEN`, the breaker:
   - sets the pair to `HALTED` via [`TradingStatusService`](../src/main/java/com/fxoee/service/TradingStatusService.java);
   - broadcasts a `STATUS_UPDATE` WebSocket message;
   - logs `WARN  Circuit breaker triggered for {pair} — deviation {d}`.
4. `lastPrice` is always advanced to the latest trade price.

State is per-pair and in `ConcurrentHashMap`; `onTrade` runs on the matching thread.

## Configuration

| Key | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `fx.circuit-breaker.price-deviation-threshold` | `CIRCUIT_BREAKER_PRICE_DEVIATION_THRESHOLD` | `0.005` | Max allowed relative jump between consecutive trades (0.005 = 0.5%). |

## Endpoints

**Read status** (existing) — [`StatusController`](../src/main/java/com/fxoee/api/controller/rest/StatusController.java):

```
GET /api/status            → { "EUR_USD": "OPEN", "GBP_USD": "HALTED", ... }
GET /api/status/EUR_USD    → { "pair": "EUR_USD", "status": "HALTED" }
```

**Resume a halted pair** — [`CircuitBreakerController`](../src/main/java/com/fxoee/api/controller/rest/CircuitBreakerController.java):

```
POST /api/circuit-breaker/EUR_USD/reset   → 200 { "pair": "EUR_USD", "status": "OPEN" }
```

`reset` sets the pair back to `OPEN`, broadcasts `STATUS_UPDATE`, and drops the price baseline so the
next trade re-arms the breaker without comparing against a stale price. An unknown pair returns `400`.

## WebSocket message

Same `type` / `payload` envelope as the other broadcasts (sent to sessions subscribed to the pair):

```json
{ "type": "STATUS_UPDATE", "payload": { "pair": "EUR_USD", "status": "HALTED" } }
```

## Known limitations

- **The breaker only signals; the pre-trade risk gate enforces the halt.** The breaker itself
  surfaces a halt via the status API, the `STATUS_UPDATE` broadcast, and the frontend badge. Hard
  enforcement lives in the risk gate ([doc 11](11-risk-controls.md)): it reads
  `TradingStatusService.getStatus(pair)` before the book lock and **rejects** new orders on a halted
  pair with `MARKET_HALTED`. So the breaker sets the halt and the risk gate makes it a trading stop.
- **In-memory, resets on restart.** `lastPrice` and pair status live only in the JVM; a restart clears
  them (every pair starts `OPEN`).
- **No automatic cool-down.** A halted pair stays halted until `reset` is called; there is no timed
  auto-resume.
- **First trade is never a trigger** — it only establishes the baseline.
