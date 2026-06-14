# 06 - API reference

_Last updated: 2026-06-13 BST._

The API layer ([com.fxoee.api](../src/main/java/com/fxoee/api)) is a thin adapter: it parses requests,
calls the `TradingEngine` (or reads a projection), and serializes the result. There are two order-entry
surfaces (a generic `/api` one and the engine-native `/api/engine` one) plus account, debug,
simulation, auth, and WebSocket endpoints. A FIX 4.4 acceptor offers a third order-entry surface when
enabled (see [fix-session.md](fix-session.md)).

Endpoints that live in other docs: [risk limits](11-risk-controls.md#rest-api) (`/api/risk`),
[trading status + circuit breaker](circuit-breaker.md#endpoints) (`/api/status`, `/api/circuit-breaker`),
and [mock-market controls](market-data.md#rest-api) (`/api/debug/mock-market`).

```mermaid
flowchart LR
    subgraph entry["Order entry"]
        E1["POST /api/engine/orders<br/>EngineOrderController"]
        E2["POST /api/orders<br/>OrderController"]
        WS["WebSocket /ws/trading<br/>TradingWebSocketHandler"]
    end
    subgraph read["Read / account"]
        A1["GET /api/engine/accounts/{id}"]
        A2["GET /api/account/balance"]
        A3["GET /api/orderbook/{pair}"]
        A4["GET /api/trades, /api/positions"]
    end
    entry --> MS["TradingEngine"]
    A1 --> MS
    A2 & A3 & A4 --> PROJ["projections / books"]
```

## Order entry

### Engine-native: [EngineOrderController](../src/main/java/com/fxoee/api/controller/rest/EngineOrderController.java) (`/api/engine`)

| Method | Path | Body / params | Returns |
|--------|------|---------------|---------|
| POST | `/accounts/{id}/deposit` | `amount` (request param) | `AccountView` (cash, reserved, free, positions) |
| POST | `/orders` | `SubmitOrderRequest` | `ExecutionReport` |
| GET | `/accounts/{id}` | none | `AccountView` |

Heads up on `/deposit`: it calls `ledger.seed(id, amount)`, which **sets** cash to that amount
(and zeroes reserved + realized P&L). It's a seeding tool, not an additive deposit.

`SubmitOrderRequest = { accountId, pair, side, type, price, quantity }` (a record on
`EngineOrderController`). It has **no** `clientOrderId` field; that lives only on the generic
`/api/orders` body below. The controller builds an `Order` and calls `TradingEngine.submit`; the
returned `ExecutionReport` carries status, fills, remaining qty, reject reason, and taker fee. This
surface is unauthenticated: the account is taken from the request body, not a JWT.

### Generic: [OrderController](../src/main/java/com/fxoee/api/controller/rest/OrderController.java) (`/api`)

Every route here requires an `Authorization: Bearer <jwt>` header; the account is the JWT subject, not
a body field.

| Method | Path | Body / params | Returns |
|--------|------|---------------|---------|
| POST | `/orders` | `PlaceOrderRequest` (body) | `ExecutionReport` (200) |
| DELETE | `/orders/{id}` | `?pair=` (required request param) | the cancelled `Order` (200) or 404 |
| GET | `/orderbook/{pair}` | `?depth=` request param, default 20 | `OrderBook.OrderBookSnapshot` |
| GET | `/trades` | optional `?pair=`, `?page=` (0), `?size=` (50) | `List<Trade>` |
| GET | `/positions` | none | open positions for the JWT's account |

`PlaceOrderRequest = { pair, side, type, price, quantity, clientOrderId }` (all `String`; see
[OrderService](../src/main/java/com/fxoee/service/OrderService.java)). `price` is null/omitted for
MARKET orders; `clientOrderId` is optional and echoed back on the report.

The `fx.orderbook.snapshot-depth` key in `application.yml` is **not** read by this endpoint (or any
code); depth comes from the request param. See [doc 10](10-configuration.md#still-scaffolding).

## Account

| Method | Path | Returns | Source |
|--------|------|---------|--------|
| GET | `/api/account/balance` | `{ "balance": <decimal> }`; 401 if no/invalid `Authorization: Bearer`, 404 if account unknown | [AccountController](../src/main/java/com/fxoee/api/controller/rest/AccountController.java) |

When `fxoee.engine.authoritative=true`, account reads reflect the in-JVM engine state (Kafka still
projects fills to the DB asynchronously).

## Debug & simulation

### [DebugController](../src/main/java/com/fxoee/api/controller/rest/DebugController.java) (`/api/account/debug`)

JWT-bound to the caller's own account (`Authorization: Bearer` required; 401 on a bad token).

| Method | Path | Body / params | Purpose |
|--------|------|---------------|---------|
| GET | `` | none | account debug view (DB + engine snapshot, open lots, active orders, mids) |
| GET | `/closed-lots` | `?limit=` (100), `?offset=` (0) | realized/closed lots (currently always an empty page) |
| GET | `/transactions` | `?limit=` (100), `?offset=` (0) | account transactions (currently always an empty page) |
| POST | `/close-lot` | `{ "lotId": "..." }` | close a specific lot owned by the caller |
| POST | `/cancel-order` | `{ "pair": "...", "orderId": "..." }` | cancel a resting order owned by the caller |

### [OrderBookDebugController](../src/main/java/com/fxoee/api/controller/rest/OrderBookDebugController.java) (`/api/debug`)

Unauthenticated operator/dev endpoints (no JWT).

| Method | Path | Body / params | Purpose |
|--------|------|---------------|---------|
| POST | `/seed-orders` | `?count=` (required); optional `?side=` on the `/seed-orders/` variant | submit `count` MARKET orders per pair across DB accounts; returns submit timing |
| POST | `/close-all-positions` | none | cancel resting orders, then force-flatten every account's positions |
| POST | `/reset` | none | drop positions + reseed cash to 10M, wipe books/registry/trade history |
| GET | `/orderbook` | none | full depth per pair |
| GET | `/state` | none | per-account JVM-vs-DB cash/equity/lot drift |
| GET | `/pending-orders` | none | all resting orders grouped by pair |
| POST | `/simulate/start` | optional `SimulatorService.SimConfig` body | start the simulator |
| POST | `/simulate/stop` | none | stop the simulator |
| GET | `/simulate/status` | none | simulator status |
| GET | `/engine-stats` | none | live Micrometer counters: submitted/placed/cancelled, structural+risk rejects, volume, matching-latency percentiles |

The simulator submits orders from many accounts across pairs on background threads. It's used for
throughput testing and to populate a lively book.

## Auth: [AuthController](../src/main/java/com/fxoee/infrastructure/auth/AuthController.java) (`/api/auth`)

| Method | Path | Body | Returns |
|--------|------|------|---------|
| POST | `/login` | `{ "username": "...", "password": "..." }` | `{ "token": <jwt>, "accountId": <uuid> }` (200) or 401 |

Tokens are HS256, signed with `jwt.secret`, valid for `jwt.expiry-days` (= 7). REST routes read the
JWT from the `Authorization: Bearer` header; WebSocket handshakes are authenticated by
`JwtHandshakeInterceptor` (see below).

## WebSocket: [TradingWebSocketHandler](../src/main/java/com/fxoee/api/websocket/TradingWebSocketHandler.java) (`/ws/trading`)

A thin handler that parses client messages, dispatches order actions to the engine, and streams
market data + account snapshots back. The handshake is authenticated by
[JwtHandshakeInterceptor](../src/main/java/com/fxoee/infrastructure/auth/JwtHandshakeInterceptor.java):
the JWT is passed as a `?token=<jwt>` query parameter (not a header), and a missing/invalid token gets
a 401 before the socket opens. Idle sessions time out after 30s.

Supported chart timeframes: `1m, 5m, 15m, 30m, 1h, 4h, 1d`. Live ticks come from either the live
[Tiingo feed](market-data.md) or the
[MockMarketMaker](../src/main/java/com/fxoee/infrastructure/marketdata/MockMarketMaker.java)
when `fxoee.mock-market.enabled=true` (it injects matched LIMIT BUY/SELL depth for the house account
every 500ms and seeds OHLC candle history at startup).

### Client to server messages

| `type` | Fields | Action |
|--------|--------|--------|
| `SUBSCRIBE` / `UNSUBSCRIBE` | `pair` | start / stop receiving snapshots for a pair |
| `NEW_ORDER` | `pair`, `side`, `orderType`, `price?`, `quantity`, `clientOrderId?` | submit an order (a random `clientOrderId` is generated if omitted) |
| `CANCEL_ORDER` | `orderId` | cancel a resting order |
| `CLOSE_POSITION` | `pair`, `lotId?` | close one lot, or the whole pair if `lotId` is omitted |

### Server to client envelopes

Every broadcast is `{ "type": ..., "payload": ... }`. The full set is `ORDERBOOK`, `TRADE`,
`ORDER_UPDATE`, `ACCOUNT_UPDATE`, `HISTORY`, `MARKET_DATA`, `STATUS_UPDATE`, `STALE_ORDER`, and `ERROR`.
The notable ones:

| `type` | Payload | Sent when |
|--------|---------|-----------|
| `ORDERBOOK` | `{ pair, bids[], asks[] }` | a subscribed pair's book changes |
| `TRADE` | `{ pair, price, quantity, side }` | a trade prints on a subscribed pair |
| `ACCOUNT_UPDATE` | account snapshot (cash, equity, lots, ...) | the caller's positions or mids change |
| `MARKET_DATA` | `{ pair, mid }` (mid scaled to 5 dp) | each price tick |
| `STATUS_UPDATE` | `{ pair, status }` | a pair is HALTED / resumed by the circuit breaker |
| `STALE_ORDER` | `{ pair, side, bookPrice, marketMid, deviationPips }` | a resting level drifts past the stale-order pip threshold (needs `fxoee.market-data.enabled=true`; see [market-data.md](market-data.md#5--spread--stale-order-metrics)) |

## Errors

Domain rejections ([RejectReason](../src/main/java/com/fxoee/engine/validate/RejectReason.java):
`INVALID_QUANTITY`, `UNSUPPORTED_PAIR`, `INSUFFICIENT_FUNDS`) are returned in the `ExecutionReport`.
Load shedding adds a non-enum reason string `OVERLOADED` (set directly on the report when the async
fill queue is saturated). The pre-trade risk gate adds its own reasons
([RiskRejectReason](../src/main/java/com/fxoee/risk/RiskRejectReason.java): `KILLSWITCH`,
`MARKET_HALTED`, `ORDER_NOTIONAL_LIMIT`, `POSITION_LIMIT`, `EXPOSURE_LIMIT`; see
[doc 11](11-risk-controls.md)). Transport-level mapping to HTTP statuses is handled by
[GlobalExceptionHandler](../src/main/java/com/fxoee/api/controller/rest/GlobalExceptionHandler.java).

| HTTP | `code` | Trigger |
|------|--------|---------|
| 401 | `UNAUTHORIZED` | `UnauthorizedException` or missing `Authorization` header (`MissingRequestHeaderException`) |
| 400 | `BAD_REQUEST` | `IllegalArgumentException` |
| 409 | `CONFLICT` | `IllegalStateException` |
| 422 | `INSUFFICIENT_FUNDS` | `InsufficientFundsException` |
| 500 | `INTERNAL_ERROR` | Any unhandled `Exception` |
