# FX Post-Fill Pipeline — Current (Event-Driven)

Post-fill fan-out on `feature/event-driven-orders`. Replaces in-process callbacks
shown in `fx_post_fill_english.svg`. Fills, account snapshots, and WS push now run
as independent Kafka consumer groups with batched DB writes.

```mermaid
flowchart LR
    Trade>"🧮 MatchingConsumer emits<br/>TradeExecuted + OrderMatched"]

    Trade -.->|"Kafka<br/>trades.executed<br/>(key = pair, 7 parts)"| FC
    Trade -.->|"Kafka<br/>orders.matched<br/>(key = orderId, 7 parts)"| SC
    Trade -.->|"orders.matched"| WS

    subgraph FillFlow["💾 fx-oee-fills (BATCH consumer group)"]
        direction TB
        FC["📥 FillConsumer.onBatch<br/>━━━━━━━━━<br/>max.poll = 500<br/>idempotent (tradeId:side LRU 200k)"]
        FC --> Apply["⚙️ AccountState.applyFill<br/>both sides per Trade<br/>(business logic verbatim)"]
        Apply --> Group["🗂️ Group deltas per account<br/>DeltaCmd · LotInsert ·<br/>LotQtyUpdate · LotClose"]
        Group --> Flush["💽 FillBatchRepository.flush<br/>━━━━━━━━━<br/>SELECT FOR UPDATE accounts<br/>batched UPDATE customer_account<br/>batched INSERT account_transaction<br/>batched lot mutations<br/>1 txn / poll"]
        Flush --> EmitFill["📤 publish FillApplied<br/>(per tradeId:side)"]
    end

    subgraph SnapFlow["📊 fx-oee-snapshot (consumer group)"]
        direction TB
        SC["📥 SnapshotConsumer.onMessage<br/>━━━━━━━━━<br/>idempotent (eventId LRU)"]
        SC --> RejCheck{status == REJECTED?}
        RejCheck -- yes --> Release["🔓 OrderFundsValidator.releaseReservation<br/>(return reserved funds)"]
        RejCheck -- no --> Snap
        Release --> Snap
        Snap["📸 AccountService.snapshot<br/>(throttled 1s/account)"]
        Snap --> EmitSnap["📤 publish AccountSnapshotted"]
    end

    EmitFill -.->|"fills.applied<br/>(key = accountId)"| WS
    EmitSnap -.->|"accounts.snapshot<br/>(key = accountId)"| WS

    Flush --> DB[("🐘 PostgreSQL<br/>customer_account<br/>account_transaction<br/>order_lot")]
    Snap -.->|"read"| DB

    WS["🔌 TradingWebSocketHandler<br/>Spring event bridge<br/>━━━━━━━━━<br/>TradeEvent · FillApplied ·<br/>AccountSnapshotEvent ·<br/>OrderStatusChangedEvent"]
    WS --> UI(["👤 React LiveAdapter<br/>balance · positions · fills"])

    classDef producer fill:#7c2d12,stroke:#fb923c,color:#fff
    classDef consumer fill:#064e3b,stroke:#34d399,color:#fff
    classDef db fill:#1e3a8a,stroke:#60a5fa,color:#fff
    classDef ws fill:#4c1d95,stroke:#a78bfa,color:#fff

    class Trade producer
    class FC,Apply,Group,Flush,EmitFill,SC,Snap,EmitSnap,Release consumer
    class DB db
    class WS,UI ws
```

## Key changes from `fx_post_fill_english.svg`

| Spec (SVG)                          | Current (event-driven)                                                |
|-------------------------------------|----------------------------------------------------------------------|
| Sequential in-process callbacks     | Independent Kafka consumer groups (`fx-oee-fills`, `fx-oee-snapshot`) |
| Per-fill DB writes (or none)        | **Batched JDBC** via `FillBatchRepository.batch(...)` — 1 txn / poll |
| In-memory account state             | PostgreSQL persistence with `SELECT FOR UPDATE` row locking          |
| Margin debited at fill              | Reservation released here only on `REJECTED`; debit was pre-trade    |
| WS push from match callback         | WS push from Spring events fired by `FillConsumer` / `SnapshotConsumer` |
| No replay safety                    | In-memory dedup (`tradeId:side`, `eventId`) — bounded LRU            |

## Ordering note

`FillConsumer` (writes state) and `SnapshotConsumer` (reads state) read different
topics → no happens-before guarantee between them. A snapshot consumed before its
fill is persisted will report stale balance. Mitigation: per-account 1s throttle
absorbs the gap in practice. See `docs/kafka-event-flow/ordering.md`.

## Files

- `src/main/java/com/fxoee/events/kafka/FillConsumer.java`
- `src/main/java/com/fxoee/events/kafka/SnapshotConsumer.java`
- `src/main/java/com/fxoee/persistence/FillBatchRepository.java`
- `src/main/java/com/fxoee/persistence/CustomerAccountRepository.java`
- `src/main/java/com/fxoee/api/websocket/TradingWebSocketHandler.java`
- `frontend/src/simulator.jsx` (LiveAdapter)
