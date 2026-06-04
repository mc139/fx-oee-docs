# 03 — Engine core

_Last updated: 2026-06-04 21:57 BST._

This is the spine of the system. [MatchingService](../src/main/java/com/fxoee/engine/MatchingService.java)
is the **source of truth**; everything else projects from it. It coordinates five collaborators, all
pure (no Spring/Kafka/DB):

| Collaborator | Responsibility | Source |
|--------------|----------------|--------|
| `MatchingEngine` + `OrderBook` | price-time matching per pair | [doc 02](02-matching-engine.md) |
| `PreTradeValidator` | structural + funds checks, reserve margin | [PreTradeValidator.java](../src/main/java/com/fxoee/engine/validate/PreTradeValidator.java) |
| `PositionBook` | FIFO position netting, realized P&L, lot events | [PositionBook.java](../src/main/java/com/fxoee/engine/position/PositionBook.java) |
| `MarginLedger` | cash + locked margin per account | [MarginLedger.java](../src/main/java/com/fxoee/engine/ledger/MarginLedger.java) |
| `Margin` | the single funding-requirement calculator | [Margin.java](../src/main/java/com/fxoee/engine/ledger/Margin.java) |

## The submit pipeline

`MatchingService.submit(Order)` runs five phases. Phases 1–4 are inside the pair's book lock; phase 5
runs after release (see [Architecture — ABBA](01-architecture.md#the-abba-deadlock-and-how-its-avoided)).

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant MS as MatchingService
    participant V as PreTradeValidator
    participant L as MarginLedger
    participant E as MatchingEngine
    participant P as PositionBook
    participant Q as FillQueue

    C->>MS: submit(order)
    MS->>V: checkStructural(order)
    alt malformed
        V-->>MS: Rejected(reason)
        MS-->>C: ExecutionReport.rejected
    end
    Note over MS: if FillQueue overloaded → reject OVERLOADED (load shed)
    rect rgb(235,245,255)
    Note over MS,P: ── inside book lock ──
    MS->>MS: reserve(order)  [worst-case margin]
    alt insufficient funds
        MS->>L: (nothing locked)
        MS-->>C: ExecutionReport.rejected(INSUFFICIENT_FUNDS)
    else accepted
        MS->>E: match(order) → trades
        loop each trade, each side
            MS->>P: applyFill(acct, pair, side, qty, price)
            P-->>MS: FillOutcome(realizedPnl, reserved, released, lotEvents)
            MS->>L: credit(acct, realizedPnl)
        end
        MS->>MS: applyTakerFee (0.1% notional, trader-to-trader)
    end
    end
    Note over MS: ── book lock released ──
    loop each touched account
        MS->>MS: reconcileGuarded(acct) → setReserved(authoritative)
    end
    MS->>Q: enqueue(PendingFill) [~1µs]  (Kafka path)
    MS-->>C: ExecutionReport
```

### Phase 1 — structural validation

`PreTradeValidator.checkStructural` enforces (spec §10.1): pair present and supported; quantity > 0
and a multiple of `minLotSize`; for LIMIT orders, price > 0 and a multiple of `tickSize`. Any failure
→ `RejectReason` (`UNSUPPORTED_PAIR` / `INVALID_QUANTITY`) and an immediate rejected report.

### Phase 2 — funds reservation (worst case)

`MatchingService.reserve` computes the **worst-case** margin and locks it on the ledger before
matching. The whole-order rule applies: an order is fully funded or fully rejected — never partially.

- **LIMIT / MARKET SELL** → `PreTradeValidator.validate(order, fundsPrice)`. `fundsPrice` is the limit
  price, or for a MARKET SELL the current **best bid**.
- **Pure close** (a SELL that only reduces a long, or a BUY that only reduces a short) reserves
  **nothing** — that margin is already held. The validator computes `openShort = qty − netLong` (or
  `openLong = qty − netShort`); when ≤ 0 it returns `Accepted(0, 0)`.
- **Flip** (close then open opposite) uses one **atomic** ledger pass: `reserveNet(release, reserve)`
  succeeds iff `free + released − reserve ≥ 0`, where `released` is the margin currently held on the
  side being closed. This is spec §10.3's single worst-case test.

#### MARKET BUY funding

A MARKET BUY has no price, so [MarketBuyEstimator](../src/main/java/com/fxoee/engine/match/MarketBuyEstimator.java)
walks the ask depth (best → worst) under the **same book lock** as the match. Because the book can't
change between estimate and sweep, the depth-walk cost **equals** the actual sweep cost — the
reservation is exact, not over-reserved. (Best-ask alone is not worst-case; asks ascend, so deeper
levels cost more.) Any unfilled remainder (STP or thin liquidity) is released afterward by reconcile.

### Phase 3 — match

`engine.match(order)` runs the [matching algorithm](02-matching-engine.md) and returns the trades.

### Phase 4 — apply fills to positions + credit P&L

For each trade, `applyFills` applies **both** sides to the `PositionBook` (aggressor and resting
account) and credits each side's realized P&L to the ledger. **Cash moves only by realized P&L** —
margin is locked, never spent. Each side's effect is captured as a `TradeExecuted` event carrying the
exact cash delta, realized P&L, and engine-assigned lot ids, so downstream projections apply it
verbatim. Mock/house counterparties (null account) are skipped.

The **taker fee** (0.1% of notional) is charged to the aggressor only when *both* sides are real,
non-house traders — see [doc 04](04-funding-pnl-conservation.md#taker-fee).

### Phase 5 — reconcile (authoritative margin)

After the book lock is released, `reconcile(account)` sets locked margin to its **authoritative**
value: held-position margin + the margin required by each live resting order (each netted against the
current position, so a resting order that merely closes a position locks nothing). This replaces the
worst-case amount reserved in phase 2 and **releases any excess**.

Hard invariant (§10.2): `reserved ≤ cash`. Held-position margin is fixed (positions can't be
un-opened), so the budget for resting orders is `cash − held`. Resting orders are funded
**oldest-first**; any that no longer fit — typically a close-order whose position another fill already
flattened, leaving it a naked open the account can't afford — are flagged `unfunded` and **cancelled**
once the lock is released. This is what auto-rejects over-reserving orders.

```mermaid
flowchart TD
    r["reconcile(account)"] --> held["held = Σ margin of open lots (all pairs)"]
    held --> gather["for each pair, for each resting order:<br/>newOpen = restQty − qty that nets off position<br/>margin = Margin.usd(pair, newOpen, restingPrice)"]
    gather --> sort["sort pending oldest-first (stable)"]
    sort --> fund["reserved = held<br/>for each pending: if reserved+margin ≤ cash keep,<br/>else flag UNFUNDED → cancel"]
    fund --> set["ledger.setReserved(account, reserved)"]
```

## PositionBook — FIFO netting

No-hedge, FIFO-netting position store. Lots are held per `(account, pair)` in arrival order. One
`applyFill` turns a fill into position changes + realized P&L and returns a
[FillOutcome](../src/main/java/com/fxoee/engine/position/FillOutcome.java); it never touches cash.

```mermaid
flowchart TD
    f["applyFill(side, qty, price)"] --> net{"net position<br/>vs fill side?"}
    net -->|"FLAT or same direction"| open["append new lot at price<br/>reserved += margin"]
    net -->|"opposite, qty &lt; abs(net)"| partial["FIFO partial-reduce<br/>oldest lots first;<br/>remaining lot keeps entry price"]
    net -->|"opposite, qty == abs(net)"| close["FIFO close all → FLAT"]
    net -->|"opposite, qty &gt; abs(net)"| flip["FIFO close all,<br/>then open ONE opposite lot<br/>for the surplus"]
    open --> out["FillOutcome:<br/>realizedPnl, reserved, released, lotEvents"]
    partial --> out
    close --> out
    flip --> out
```

`FillOutcome.cashDelta() = realizedPnlUsd + marginReleasedUsd − marginReservedUsd`. Invariants upheld
(spec §13): an `(account, pair)` never holds LONG and SHORT lots at once; a fully-closed lot is
removed and never reappears; `netQty == Σ signed lot qty`. Each mutation emits a `LotEvent`
(`Open` / `PartialClose` / `FullClose`) carrying the engine lot id, so projections key their lots
identically.

## MarginLedger — cash & locked margin

Per account: `cash`, `reserved` (locked margin), `realizedPnl`. The derived value `free = cash −
reserved` is what new orders may draw on.

| Method | Semantics |
|--------|-----------|
| `seed(id, cash)` | reset account (cash set, reserved & realizedPnl zeroed) |
| `reserveNet(id, release, reserve)` | atomically swap; succeeds iff `cash − (reserved − release + reserve) ≥ 0` |
| `tryReserve(id, amt)` | `reserveNet(id, 0, amt)` — whole-order reservation |
| `setReserved(id, amt)` | authoritative set (floored at 0); used by reconcile |
| `release(id, amt)` | unconditional release, floored at 0; no-op on null/≤0 |
| `credit(id, delta)` | apply realized cash movement; also accrues `realizedPnl`; no-op on null/0 |

All mutations synchronize on the per-account object. `reserveNet` is the one primitive that gates
affordability — on failure it leaves state completely unchanged.

## Other entry points on MatchingService

| Method | Purpose |
|--------|---------|
| `cancel(acct, pair, orderId)` | cancel a resting order + reconcile the owner |
| `closeNet(acct, pair)` | flatten the whole net position with an opposing MARKET order |
| `closeLot(acct, pair, lotId)` | close one lot's quantity (FIFO, so closes oldest up to that qty) |
| `forceFlat(acct, mids)` | credit unrealized P&L at given mids, then wipe positions + margin (used when a MARKET close finds no counterparty) |
| `snapshot(acct, mids)` | account view: cash, notional, unrealized/realized P&L, equity, positions |
| `seedForReplay` / `replayFill` / `reconcileReserved` | warm-restart recovery — see [doc 05](05-event-sourcing-persistence.md) |
| `reset(acct, cash)` | drop positions and reseed cash (debug) |

Tested by `MatchingServiceTest`, `MatchingServiceCornerCasesTest`, `PositionBookTest`,
`PositionBookCornerCasesTest`, `MarginLedgerTest`, `LedgerCornerCasesTest`, `PreTradeValidatorTest`,
`MarketBuyEstimatorTest`, and the `EngineConservationFuzzTest`. See [Testing](08-testing.md).
