# 07 — Data model

_Last updated: 2026-06-04 21:57 BST._

Two data models coexist: the **in-memory domain** the engine operates on, and the **PostgreSQL
schema** the projection writes. They are deliberately separate — the DB is a read-model, not the
source of truth.

## In-memory domain ([com.fxoee.domain](../src/main/java/com/fxoee/domain))

```mermaid
classDiagram
    class Order {
        String id
        CurrencyPair pair
        OrderSide side
        OrderType type
        BigDecimal price
        BigDecimal quantity
        BigDecimal remainingQuantity
        OrderStatus status
        UUID accountId
        +fill(qty)
        +cancel()
        +reject()
    }
    class Trade {
        String id
        CurrencyPair pair
        String buyOrderId
        String sellOrderId
        UUID buyAccountId
        UUID sellAccountId
        BigDecimal price
        BigDecimal quantity
    }
    class PositionLot {
        String id
        CurrencyPair pair
        OrderSide side
        BigDecimal quantity
        BigDecimal entryPrice
        long timestamp
    }
    class FillOutcome {
        BigDecimal realizedPnlUsd
        BigDecimal marginReservedUsd
        BigDecimal marginReleasedUsd
        List~LotEvent~ lotEvents
        +cashDelta()
    }
    class ExecutionReport {
        OrderStatus status
        List~Trade~ trades
        BigDecimal lastFilledQty
        BigDecimal lastFilledPrice
        BigDecimal takerFeeUsd
        String rejectReason
    }
    Order --> Trade : produces
    Trade --> FillOutcome : applied to PositionBook
    FillOutcome --> PositionLot : LotEvent Open/Partial/Full
    Order --> ExecutionReport : result of submit
```

`Order.price` is null for MARKET orders; `Order.accountId` is null for mock/internal orders (which
are exempt from funds tracking, fees, and self-trade prevention).

### Enums

| Enum | Values |
|------|--------|
| [CurrencyPair](../src/main/java/com/fxoee/domain/enums/CurrencyPair.java) | EUR_USD, GBP_USD, USD_JPY, USD_CHF, AUD_USD, USD_CAD, NZD_USD — each carries `marginRate`, `tickSize`, `minLotSize`, `isUsdBase()` |
| [OrderSide](../src/main/java/com/fxoee/domain/enums/OrderSide.java) | BUY, SELL |
| [OrderType](../src/main/java/com/fxoee/domain/enums/OrderType.java) | LIMIT, MARKET |
| [OrderStatus](../src/main/java/com/fxoee/domain/enums/OrderStatus.java) | NEW, PENDING, PARTIALLY_FILLED, FILLED, CANCELLED, REJECTED |

### LotEvent (sealed)

[LotEvent](../src/main/java/com/fxoee/domain/model/LotEvent.java) is the unit of position change
carried on `TradeExecuted` and applied verbatim by `FillConsumer`:

- `Open(PositionLot lot)` — a new lot added.
- `PartialClose(lotId, newQuantity, closePrice, realizedPnlUsd)` — lot reduced; `newQuantity` remains.
- `FullClose(lotId, closePrice, realizedPnlUsd)` — lot removed.

## Database schema (Flyway migrations)

[src/main/resources/db/migration](../src/main/resources/db/migration):

| Migration | Creates |
|-----------|---------|
| V1 | `customer_account` (id, name, `account_balance`) + `account_transaction` (audit ledger) |
| V2 | `users` (auth) |
| V3 | `position_lot` |
| V4 | `pending_lot_closes` |
| V5 | `processed_events` (consumer dedup) |
| V6 | seed sim accounts |
| V7 | seed trader accounts |
| V8 | seed house account (`HOUSE_UUID`) |
| V9 | `trade_events` (durable log) |

```mermaid
erDiagram
    customer_account ||--o{ position_lot : owns
    customer_account ||--o{ account_transaction : logs
    customer_account {
        UUID id PK
        VARCHAR name
        NUMERIC account_balance
    }
    account_transaction {
        UUID id PK
        UUID account_id FK
        UUID order_id
        NUMERIC amount
        VARCHAR type
        NUMERIC balance_after
    }
    position_lot {
        UUID id PK
        UUID account_id FK
        VARCHAR pair
        VARCHAR side
        NUMERIC quantity
        NUMERIC entry_price
        TIMESTAMPTZ opened_at
        TIMESTAMPTZ closed_at "null while open"
        NUMERIC close_price
        NUMERIC realized_pnl
    }
    trade_events {
        BIGSERIAL seq PK
        UUID event_id UK
        VARCHAR pair
        JSONB payload "serialized TradeExecuted"
        BOOLEAN published
        TIMESTAMPTZ occurred_at
    }
    processed_events {
        UUID event_id PK
        VARCHAR consumer
        TIMESTAMPTZ processed_at
    }
```

Notes on `position_lot`:

- Open lots have `closed_at IS NULL`; partial closes update `quantity` in place (entry price never
  changes — spec §11.2). A full close stamps `closed_at`, `close_price`, `realized_pnl`.
- Indexed for the two hot reads: open lots per account (`WHERE closed_at IS NULL`) and lots by
  `(account_id, pair)`.

Access is via **jOOQ** repositories ([com.fxoee.persistence](../src/main/java/com/fxoee/persistence))
— `CustomerAccountRepository`, `PositionLotRepository`, `FillBatchRepository` (batched fill writes),
`TradeEventRepository` (append-only log).

## Mapping: engine ↔ DB

| Engine (in-memory) | DB projection | Written by |
|--------------------|---------------|------------|
| `MarginLedger.cash` | `customer_account.account_balance` | `FillConsumer` (applies stamped cash delta) |
| `PositionBook` lots | `position_lot` rows | `FillConsumer` (Open→insert, Partial→qty update, Full→close) |
| `TradeExecuted` events | `trade_events.payload` | `PersistenceWorker` (before publish) |

The DB row keys (lot ids) are the **engine-assigned** ids carried on `LotEvent`, so the projection
indexes positions identically to the engine — that's what keeps them in lockstep.
