# FIX Session

FIX 4.4 support is **planned but not yet implemented**.

## Dependency

`quickfixj-core` and `quickfixj-messages-fix44` are declared in `pom.xml` and on the classpath.

## Planned design

| Component | Description |
|-----------|-------------|
| `FixSessionAdapter` | `Application` implementor — routes `NewOrderSingle (D)`, `OrderCancelRequest (F)` into `OrderService` |
| `fix.cfg` | Session config (SocketAcceptPort, SenderCompID, TargetCompID, DataDictionary) |
| Execution Reports | `ExecutionReport (8)` sent back for fills, rejections, and cancels |
| Market data | `MarketDataSnapshotFullRefresh (W)` / `MarketDataIncrementalRefresh (X)` from `MarketDataBroadcaster` |

## Status

No `FixSessionAdapter`, no `fix.cfg`, and no FIX message handlers exist yet. Institutional FIX
clients cannot connect. See [ADR-0004](adr/0004-async-fill-queue-over-disruptor.md) for the
broader architectural context.
