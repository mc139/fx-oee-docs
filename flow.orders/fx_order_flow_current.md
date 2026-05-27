# FX Order Flow — Current (Event-Driven)

Placement → execution path on `feature/event-driven-orders`. Replaces the synchronous
spec in `fx_order_flow_english.svg`. The HTTP request now returns `202 Accepted`
after pre-trade funds reservation; matching happens asynchronously via Kafka.

```mermaid
flowchart TD
    Start([👤 Client places order<br/>BUY/SELL · qty · price]) --> Rest

    Rest["📡 REST POST /api/orders<br/>OrderController"]
    Rest --> Val{"🛡️ Bean validation<br/>+ CurrencyPair whitelist"}
    Val -- invalid --> Rej1[/"❌ 400 Bad Request"/]
    Val -- valid --> Reserve

    Reserve["💰 OrderFundsValidator.reserveFunds<br/>━━━━━━━━━<br/>SELECT FOR UPDATE account<br/>debit available → reserved"]
    Reserve --> ReserveOk{Funds ok?}
    ReserveOk -- no --> Rej2[/"❌ 402 Insufficient funds"/]
    ReserveOk -- yes --> Registry

    Registry["🧾 OrderRegistry.record(PENDING)<br/>clientOrderId · placedAt"]
    Registry --> Publish["📤 OrderEventProducer<br/>publish OrderPlaced<br/>(key = pair.name)"]
    Publish --> Ack[/"✅ 202 Accepted<br/>PlacementAck{orderId, statusUrl}"/]

    Ack -.->|"GET /api/orders/{id}/status"| Poll["📥 OrderRegistry.lookup"]

    Publish -.->|"Kafka orders.placed"| MC

    subgraph Async["⚡ Async pipeline (per-pair partitioned)"]
        direction TB
        MC["🧮 MatchingConsumer<br/>fx-oee-matching group<br/>━━━━━━━━━<br/>idempotent (eventId dedup)"]
        MC --> Match["📈 OrderBook.match<br/>(in-memory, ~0.2ms)<br/>price-time priority"]
        Match --> HasMatch{counter-party?}
        HasMatch -- no --> Park["⏳ park in book<br/>(GTC effective)"]
        HasMatch -- partial --> Partial["✂️ PARTIALLY_FILLED<br/>remainder stays in book"]
        HasMatch -- full --> Full["✅ FILLED"]

        Park --> EmitMatched
        Partial --> EmitTrade
        Full --> EmitTrade

        EmitTrade["📤 publish TradeExecuted<br/>(per Trade)"]
        EmitTrade --> EmitMatched
        EmitMatched["📤 publish OrderMatched<br/>(terminal status)"]
        EmitMatched --> Record["🧾 OrderRegistry.recordMatched"]
    end

    Record -.-> Poll

    classDef sync fill:#1e3a8a,stroke:#60a5fa,color:#fff
    classDef async fill:#1f2937,stroke:#f59e0b,color:#fbbf24
    classDef ok fill:#064e3b,stroke:#34d399,color:#fff
    classDef bad fill:#7f1d1d,stroke:#f87171,color:#fff

    class Rest,Reserve,Registry,Publish,Poll sync
    class MC,Match,EmitTrade,EmitMatched,Record,Park,Partial,Full async
    class Ack ok
    class Rej1,Rej2 bad
```

## Key changes from `fx_order_flow_english.svg`

| Spec (SVG)                       | Current (event-driven)                                         |
|----------------------------------|----------------------------------------------------------------|
| Synchronous match, sync response | `202 Accepted` then async Kafka pipeline                       |
| Margin reserved post-fill        | **Pre-trade reservation** via `OrderFundsValidator`            |
| In-process callbacks             | Kafka topics `orders.placed` → `trades.executed` / `orders.matched` |
| Single-threaded match            | Per-pair single-writer via partition key = `pair.name()`       |
| ClOrdID generated, no audit      | `OrderRegistry` (in-mem) + `account_transaction` ledger (DB)   |

## Files

- `src/main/java/com/fxoee/api/controller/rest/OrderController.java`
- `src/main/java/com/fxoee/service/OrderSubmissionService.java`
- `src/main/java/com/fxoee/matching/OrderFundsValidator.java`
- `src/main/java/com/fxoee/events/kafka/OrderEventProducer.java`
- `src/main/java/com/fxoee/events/kafka/MatchingConsumer.java`
- `src/main/java/com/fxoee/application/OrderRegistry.java`
