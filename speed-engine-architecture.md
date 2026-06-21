# Speed Engine: Thread & Architecture Guide

_Last updated: 2026-06-21 BST._

This document is a **visual map** of how the speed engine runs: which threads exist, what each one owns, how commands flow through the Disruptor ring, and where work happens. It complements the deeper semantics guide in [speed-engine.md](speed-engine.md).

Enable speed mode with `fxoee.engine.mode=speed` (or env `FXOEE_ENGINE_MODE=speed`). It is the active engine here: [performance.properties](../src/main/resources/performance.properties) (imported via `optional:classpath:performance.properties` in [application.yml](../src/main/resources/application.yml)) sets `fxoee.engine.mode=speed`, and the local profile defaults to it (`mode: ${FXOEE_ENGINE_MODE:speed}` in [application-local.yml](../src/main/resources/application-local.yml)). Spring wires everything in [SpeedEngineConfig.java](../src/main/java/com/fxoee/engine/speed/SpeedEngineConfig.java).

---

## At a glance

| Concept | What it means |
|---------|----------------|
| **Single writer** | One thread (`speed-engine`) owns all mutable trading state, no locks on the hot path |
| **Multi producer** | Any number of request threads can publish commands into the ring at once |
| **Synchronous submit** | Each caller blocks on its own `ResultBuffer` until the engine answers |
| **Edge conversion** | `BigDecimal` only on request threads; the engine thread uses fixed-point `long`s |
| **LMAX pattern** | The same LMAX Disruptor 4.0.0 library that the optional [`DisruptorFillQueue`](../src/main/java/com/fxoee/engine/DisruptorFillQueue.java) can use downstream, but here it carries commands *into* the engine. The engine command ring is always a Disruptor; the [fill queue](05-event-sourcing-persistence.md) defaults to Agrona |

---

## Thread topology

This is the picture to keep in your head. **One dedicated engine thread** does all matching; **many request threads** publish work and wait for answers.

```mermaid
flowchart TB
    subgraph requestThreads["Request threads (many)"]
        direction TB
        T1["Tomcat HTTP worker"]
        T2["WebSocket handler thread"]
        T3["FIX session thread"]
        T4["Simulator / load-gen thread"]
        TN["… any thread calling TradingEngine"]
    end

    subgraph perThread["Per request thread (ThreadLocal)"]
        RB1["ResultBuffer<br/>(reused, one per thread)"]
        RB2["ResultBuffer"]
        RBN["ResultBuffer"]
    end

    subgraph disruptor["Disruptor ring buffer (shared, pre-allocated)"]
        direction LR
        S0["EngineSlot"] --> S1["EngineSlot"] --> S2["…"] --> SN["EngineSlot"]
    end

    subgraph engineThread["ONE engine thread: speed-engine"]
        direction TB
        WS["WaitStrategy<br/>(busy-spin / yielding / blocking)"]
        DISPATCH["onEvent() dispatch"]
        SE["SpeedEngine core"]
        STATE["Owned state:<br/>SpeedBook ×7 · SpeedPositions · SpeedLedger · AccountRegistry"]
        WS --> DISPATCH --> SE --> STATE
    end

    subgraph async["Downstream (separate threads, not on hot path)"]
        FQ["FillQueue consumer"]
        PW["PersistenceWorker"]
        K["Kafka consumers"]
    end

    T1 --> RB1
    T2 --> RB2
    TN --> RBN

    T1 -- "1. claim slot, write longs, publish" --> disruptor
    T2 --> disruptor
    TN --> disruptor

    disruptor -- "2. consumer picks up slot" --> DISPATCH

    SE -- "3. write primitives into caller's buffer" --> RB1
    SE --> RB2
    SE --> RBN

    RB1 -- "4. spin → yield → park until done" --> T1
    RB2 --> T2
    RBN --> TN

    T1 -- "5. build ExecutionReport, Kafka events, PendingFill" --> FQ
    FQ --> PW --> K
```

### Thread roles

| Thread | Name / source | Role | Allocates on hot path? |
|--------|---------------|------|------------------------|
| **Engine** | `speed-engine` (daemon) | Sole mutator of books, positions, ledger | **No** (steady state) |
| **Submitters** | Tomcat, WS, FIX, simulator, … | Convert `Order` → longs, publish, await, build reports/events | Yes (domain objects, events) |
| **Observers** | WS snapshot schedulers, debug endpoints | Read book via `SpeedOrderBookView` → `EXEC` on engine thread | Yes (BigDecimal copies) |
| **Projection** | `FillQueue` / `PersistenceWorker` / Kafka | Persist fills asynchronously | Yes (expected, off hot path) |

### CPU pinning (optional)

When `fxoee.engine.speed.cpu` is set (≥ 0), the engine thread is pinned to that core via OpenHFT Affinity (3.23.3). This keeps the single writer on one CPU and avoids cache-migration stalls. Pinning is Linux-only: on macOS dev hosts and other unsupported platforms `Affinity.setAffinity` is a no-op, and any failure is logged rather than allowed to kill the engine thread. `-1` disables it entirely. This branch sets `cpu=2` in `performance.properties`.

---

## Component map

Who talks to whom, useful when tracing a stack trace.

```mermaid
flowchart LR
    subgraph api["API / callers"]
        TC["TradingEngine API<br/>(REST, WS, FIX)"]
        OBV["SpeedOrderBookView<br/>(per pair)"]
        SME["SpeedMatchingEngine<br/>(mock quotes)"]
    end

    subgraph facade["Facade layer (request thread)"]
        SMS["SpeedMatchingService"]
        EC["EngineClient"]
    end

    subgraph transport["Transport"]
        RB["ResultBuffer<br/>(ThreadLocal)"]
        RING["RingBuffer&lt;EngineSlot&gt;"]
    end

    subgraph core["Engine thread"]
        ENG["SpeedEngine"]
        BK["SpeedBook ×7"]
        POS["SpeedPositions"]
        LED["SpeedLedger"]
        REG["AccountRegistry"]
    end

    TC --> SMS
    OBV --> EC
    SME --> EC
    SMS --> EC
    EC --> RB
    EC --> RING
    RING --> ENG
    ENG --> BK & POS & LED & REG
    ENG --> RB
```

| Class | Package | Thread | Responsibility |
|-------|---------|--------|----------------|
| [SpeedMatchingService](../src/main/java/com/fxoee/engine/speed/SpeedMatchingService.java) | `engine.speed` | Request | `TradingEngine` facade: validation, convert, publish, materialise reports |
| [EngineClient](../src/main/java/com/fxoee/engine/speed/EngineClient.java) | `engine.speed` | Request | ThreadLocal `ResultBuffer`, `publishAndWait`, `exec` |
| [SpeedEngine](../src/main/java/com/fxoee/engine/speed/SpeedEngine.java) | `engine.speed` | **Engine** | Disruptor consumer, dispatch, match, funds, reconcile |
| [EngineSlot](../src/main/java/com/fxoee/engine/speed/EngineSlot.java) | `engine.speed` | Both | Ring event: command type + primitive inputs |
| [ResultBuffer](../src/main/java/com/fxoee/engine/speed/ResultBuffer.java) | `engine.speed` | Both | Caller-owned result arrays + done handshake |
| [SpeedBook](../src/main/java/com/fxoee/engine/speed/SpeedBook.java) | `engine.speed` | **Engine** | Long-native order book (pooled nodes) |
| [SpeedPositions](../src/main/java/com/fxoee/engine/speed/SpeedPositions.java) | `engine.speed` | **Engine** | FIFO lots, net qty, held margin |
| [SpeedLedger](../src/main/java/com/fxoee/engine/speed/SpeedLedger.java) | `engine.speed` | **Engine** | Cash, reserved, per-pair pending margin |
| [AccountRegistry](../src/main/java/com/fxoee/engine/speed/AccountRegistry.java) | `engine.speed` | **Engine** | `UUID` → dense `int` index |
| [Fixed](../src/main/java/com/fxoee/engine/speed/Fixed.java) | `engine.speed` | Both | Scale constants, `long` ↔ `BigDecimal` conversion |
| [SpeedOrderBookView](../src/main/java/com/fxoee/engine/speed/SpeedOrderBookView.java) | `engine.speed` | Request | `OrderBook` API over engine thread reads |
| [SpeedMatchingEngine](../src/main/java/com/fxoee/engine/speed/SpeedMatchingEngine.java) | `engine.speed` | Request | Book-only match for mock quote injection |

---

## Order submit: end-to-end sequence

Track one `submit(Order)` from HTTP to engine and back.

```mermaid
sequenceDiagram
    autonumber
    participant RT as Request thread
    participant SMS as SpeedMatchingService
    participant EC as EngineClient
    participant RB as ResultBuffer
    participant RING as Ring buffer
    participant ET as speed-engine thread
    participant BK as SpeedBook + ledger + positions
    participant FQ as FillQueue

    RT->>SMS: submit(order)
    SMS->>SMS: checkStructural (request thread)
    alt structural reject
        SMS-->>RT: ExecutionReport.rejected
    end
    SMS->>SMS: BigDecimal → long (Fixed.toRaw)
    SMS->>EC: buffer().reset()
    SMS->>RING: next() → claim EngineSlot
    SMS->>RING: fill slot (SUBMIT, longs, out=rb)
    SMS->>EC: publishAndWait(ring, seq, rb)
    EC->>RING: publish(seq)
    EC->>RB: await() spin, yield, park

    RING->>ET: onEvent(slot)
    ET->>ET: riskCheck
    ET->>BK: reserve margin
    ET->>BK: match (price-time, STP)
    ET->>BK: applyFills → positions + ledger
    ET->>BK: reconcile touched accounts
    ET->>RB: write fills, lot events, resting deltas
    ET->>RB: markDone() → unpark waiter if parked
    ET->>RING: slot.clear()

    RB-->>EC: done observed
    EC-->>SMS: return rb
    SMS->>SMS: mutate Order status
    SMS->>SMS: build Trade, TradeExecuted, OrderMatched
  SMS->>FQ: enqueue PendingFill (async)
    SMS-->>RT: ExecutionReport
```

### Where time is spent

```mermaid
flowchart LR
    subgraph request["Request thread"]
        V["Validation<br/>~µs"]
        C["BigDecimal→long<br/>~µs"]
        W["await() wait<br/>~µs to ms"]
        M["Materialise events<br/>~µs to 10s µs"]
    end

    subgraph engine["Engine thread (the work you care about)"]
        R["risk + reserve"]
        MATCH["match sweep"]
        AF["apply fills"]
        REC["reconcile"]
    end

    V --> C --> W
    W -.->|"blocks until engine finishes"| engine
    engine --> M
```

- **Clean rest** (LIMIT rests, no fills): reconcile is skipped → ~O(1), ~1.5 M orders/s in bench.
- **Matching** (fills occur): reconcile runs → ~1.5 µs/order service time, ~600 k/s ceiling on one core.

---

## Ring commands

Every interaction with mutable state goes through an `EngineSlot` command. The engine thread dispatches in `SpeedEngine.onEvent()`.

`EngineSlot.cmd` is one of five `byte` constants: `SUBMIT=0`, `BOOK_ADD=1`, `CANCEL=2`, `EXEC=3`, `MATCH_RAW=4`.

```mermaid
flowchart TD
    PUB["Producer publishes EngineSlot"] --> DISPATCH{"cmd?"}

    DISPATCH -->|SUBMIT| SUB["onSubmit(funds=true):<br/>risk → reserve → match → applyFills → reconcile"]
    DISPATCH -->|MATCH_RAW| MR["onSubmit(funds=false):<br/>match + rest only, no funds/positions/reconcile"]
    DISPATCH -->|CANCEL| CAN["onCancel: unlink order → reconcile account"]
    DISPATCH -->|BOOK_ADD| BA["onBookAdd: rest order, no funds check<br/>(warm-restart recovery)"]
    DISPATCH -->|EXEC| EX["s.task.run() on engine thread<br/>(reads, admin, snapshots)"]

    SUB --> FIN["finally:<br/>if out != null out.markDone()<br/>then s.clear()"]
    MR --> FIN
    CAN --> FIN
    BA --> FIN
    EX --> FIN
```

The `finally` block in `onEvent` is uniform: it calls `out.markDone()` whenever `slot.out != null`, then always runs `slot.clear()`. Whether a caller waits is therefore decided by whether it set `slot.out`, not by the command type.

| Command | `out` set? | Caller waits? | Typical caller |
|---------|------------|---------------|----------------|
| `SUBMIT` | Yes | Yes | `SpeedMatchingService.submit` |
| `CANCEL` | Yes | Yes | `SpeedMatchingService.cancel`, `SpeedOrderBookView.cancelOrder` |
| `MATCH_RAW` | Yes | Yes | `SpeedMatchingEngine.match` (mock quotes) |
| `BOOK_ADD` | Yes (via `command()`) | Yes | recovery, `SpeedOrderBookView.addOrder` (the handler ignores `out` but the caller still awaits `markDone`) |
| `EXEC` | Yes (via `command()`) | Yes | `EngineClient.exec`: snapshots, `cash()`, book depth, `reconcileReserved` |

Every caller goes through `EngineClient.command()` / `publishAndWait()`, both of which set `slot.out` to the thread-local `ResultBuffer` and `await()` it, so all five command types are synchronous round-trips. `BOOK_ADD` and `EXEC` simply leave the result arrays empty (or the `EXEC` task fills `slot.out` itself, e.g. `reconcileReserved` writes resting deletes).

**Critical rule:** an `EXEC` task must **never** publish back to the ring. The single consumer cannot drain behind itself, so a full ring deadlocks.

---

## Result handshake (request ↔ engine)

Each submitting thread owns one reusable [ResultBuffer](../src/main/java/com/fxoee/engine/speed/ResultBuffer.java). Results never travel in the ring slot, only primitives and refs written into the caller's buffer.

```mermaid
stateDiagram-v2
    [*] --> Reset: caller reset()
    Reset --> Published: ring.publish()
    Published --> Spin: await() phase 1
    Spin --> Done: done==1 within 8192 spins
    Spin --> Yield: spin exhausted
    Yield --> Done: done==1 within 100 yields
    Yield --> Park: yield exhausted
    Park --> Done: markDone() unparks
    Done --> [*]: caller reads arrays, builds objects

    state EngineSide {
        [*] --> Processing: onEvent()
        Processing --> WriteResults: fill arrays
        WriteResults --> MarkDone: done.setVolatile(1)
        MarkDone --> [*]
    }

    Published --> EngineSide
```

### Why waiters park (not spin forever)

| Who | Wait behaviour | Why |
|-----|----------------|-----|
| **Engine thread** | `BusySpinWaitStrategy` by default | Pick up new commands in sub-µs |
| **Submitting threads** | Spin briefly → yield → **park** | N busy-spinning waiters starve the engine off CPU |

Under many concurrent submitters, parked threads release cores so `speed-engine` stays scheduled. Throughput scales with submitter count instead of collapsing.

---

## State owned by the engine thread

All of this lives in [SpeedEngine](../src/main/java/com/fxoee/engine/speed/SpeedEngine.java). **No other thread may mutate it** (reads go through `EXEC` or volatile top-of-book mirrors).

```mermaid
flowchart TB
    subgraph engine["speed-engine thread (sole owner)"]
        REG["AccountRegistry<br/>UUID → int idx"]
        LED["SpeedLedger<br/>cash[] · reserved[] · pending[acct][pair]"]
        POS["SpeedPositions<br/>FIFO lots · netQty · heldMargin"]
        subgraph books["SpeedBook × 7 pairs"]
            B0["EUR/USD"]
            B1["GBP/USD"]
            B2["…"]
            B6["USD/CAD"]
        end
        SCRATCH["Reused scratch arrays<br/>(touched, pendOrd, unfunded, …)"]
    end

    REG --> LED & POS & books
    POS --> LED
    books --> LED
```

### Per-book structure (inside `SpeedBook`)

```mermaid
flowchart LR
    subgraph bid["Bid side (better prices ←)"]
        BL1["Lvl price chain"] --> BL2["Lvl"] --> BL3["…"]
    end
    subgraph ask["Ask side (better prices ←)"]
        AL1["Lvl"] --> AL2["Lvl"]
    end
    subgraph level["Inside each Lvl"]
        O1["Ord FIFO"] --> O2["Ord"] --> O3["Ord"]
    end
    BL1 --- O1
    IDX["byId map<br/>O(1) cancel"] -.-> O2
    ACC["per-account chain<br/>for reconcile"] -.-> O2
    POOL["free pools<br/>Ord + Lvl reuse"] -.-> O3
    TOP["volatile bestBidPx / bestAskPx<br/>(lock-free mid reads)"] -.-> BL1 & AL1
```

---

## SUBMIT pipeline (engine thread detail)

What `onSubmit()` does for a normal order, the inner loop of the architecture.

```mermaid
flowchart TD
    START["SUBMIT received"] --> RISK{riskCheck}
    RISK -->|reject| OUT_R["status=REJECTED, riskReason"]
    RISK -->|pass| RES{reserve margin}
    RES -->|fail| OUT_F["status=REJECTED, INSUFFICIENT_FUNDS"]
    RES -->|ok| MATCH["match(): sweep opposite side<br/>price-time, STP cancel-newest"]
    MATCH --> APPLY["applyFills(): positions + ledger + taker fee"]
    APPLY --> DISP{aggressor disposition}
    DISP -->|STP| CAN["CANCELLED, delete"]
    DISP -->|filled| FIL["FILLED, delete"]
    DISP -->|market remainder| REJ["REJECTED, delete"]
    DISP -->|limit remainder| REST["rest on book<br/>PENDING or PARTIALLY_FILLED"]
    REST --> REC_CHECK{clean rest?<br/>no fills + PENDING}
    CAN --> REC
    FIL --> REC
    REJ --> REC
    REC_CHECK -->|yes| FAST["recordCleanRest O(1)<br/>skip full reconcile"]
    REC_CHECK -->|no| REC["reconcile touched accounts"]
    FAST --> DONE["markDone"]
    REC --> DONE
    OUT_R --> DONE
    OUT_F --> DONE
```

---

## Read paths (observers)

Anything that needs a consistent view of book state routes through the engine thread.

```mermaid
flowchart LR
    subgraph observers["Observer threads"]
        WS["WebSocket book snapshot"]
        MID["Mid-price provider"]
        DBG["Debug / metrics depth()"]
        REC["Warm-restart recovery"]
    end

    subgraph view["SpeedOrderBookView"]
        API["OrderBook API"]
    end

    subgraph paths["How it reads"]
        VOL["bestBidPx / bestAskPx<br/>volatile read, no round-trip"]
        EXEC["client.exec() → scan book on engine thread"]
        CMD["client.command(BOOK_ADD / CANCEL)"]
    end

    observers --> API
    API --> VOL
    API --> EXEC
    API --> CMD
    EXEC --> ET["speed-engine"]
    CMD --> ET
    VOL -.->|"mirrored on match/rest"| ET
```

| Read | Mechanism | Blocks engine? |
|------|-----------|----------------|
| `bestBid()` / `bestAsk()` | Volatile mirror on `SpeedBook` | No |
| `depth()` gauge | `EXEC`: count levels | Briefly |
| `getSnapshot()` bids/asks | `EXEC`: walk book, convert to `BigDecimal` | Briefly |
| `cash()` / `netQty()` / `snapshot()` | `EXEC` on engine thread | Briefly |

Reads cost microseconds, fine for 200 ms WebSocket snapshots; they never need locks because they run on the only mutator thread (or read volatiles it maintains).

---

## Integration with the rest of the app

Speed mode swaps the engine implementation; the outer shell stays the same.

```mermaid
flowchart TB
    subgraph clients["Clients"]
        REST["REST"]
        WS["WebSocket"]
        FIX["FIX (opt-in, fx.fix.enabled)"]
    end

    subgraph spring["Spring beans (speed mode)"]
        SMS["SpeedMatchingService<br/>implements TradingEngine"]
        OB["Map&lt;CurrencyPair, OrderBook&gt;<br/>→ SpeedOrderBookView"]
        ME["Map&lt;CurrencyPair, MatchingEngine&gt;<br/>→ SpeedMatchingEngine"]
    end

    subgraph speed["Speed engine package"]
        EC["EngineClient"]
        ENG["SpeedEngine"]
    end

    subgraph kafka["Durability lane A: Kafka / FillQueue (WAL off)"]
        FQ["FillQueue<br/>(agrona default)"]
        PW["PersistenceWorker"]
        KAFKA["Kafka"]
        DB[("PostgreSQL")]
    end

    subgraph wal["Durability lane B: Aeron WAL (WAL on, ADR 0007)"]
        WP["WalPublisher"]
        AR["Aeron Archive"]
        POLL["3 pollers<br/>(see below)"]
        DB2[("PostgreSQL")]
    end

    REST & WS & FIX --> SMS
    WS --> OB
    SMS --> EC --> ENG
    OB --> EC
    ME --> EC
    SMS -->|"PendingFill (WAL off)"| FQ --> PW --> KAFKA --> DB
    ENG -->|"fill record (WAL on)"| WP --> AR --> POLL --> DB2
```

- **Two mutually exclusive durability lanes**, chosen at boot by `fxoee.wal.aeron.enabled` (see [the WAL section](#durable-trade-history-the-aeron-archive-wal) below). With WAL off the request thread enqueues a `PendingFill` to the `FillQueue` (Kafka lane); with WAL on the engine thread records each fill to the Aeron Archive and the facade is wired with a `null` `FillQueue` + `null` Kafka producer. On this branch WAL is the active lane.
- **Trading rules** are identical to default mode (see [speed-engine.md](speed-engine.md)).
- **Metrics** keep the same names (`matching.latency`, `orders.placed.total`, ...).
- **Risk gate** runs long-native inside the engine thread, mirroring `InProcessRiskService`'s checks against the shared `RiskLimits` bean. The facade's `setRiskGate(RiskService, TradingStatusService)` only forwards `TradingStatusService` (the halt source) into the engine; `RiskService` itself is never called per order, since it would allocate request/decision records.

---

## Durable trade history: the Aeron Archive WAL

When `fxoee.wal.aeron.enabled` is on, the durability lane is the **Aeron Archive write-ahead log** (ADR 0007) instead of the Kafka / `FillQueue` path. The engine becomes authoritative for balances in the JVM, and the Archive is the durable record that downstream pollers replay. The semantics live in [speed-engine.md](speed-engine.md#durable-trade-history-the-aeron-archive-wal-adr-0007); this is the thread-level map.

```mermaid
flowchart LR
    subgraph engineThread["speed-engine thread (zero alloc)"]
        SE["SpeedEngine<br/>match → applyFills"]
        ENC["encode fill into reused<br/>FillRecordData (SBE FillCodec)"]
        WP["WalPublisher.append<br/>then flush per Disruptor batch"]
        SE --> ENC --> WP
    end

    WP -->|"Aeron IPC<br/>ExclusivePublication"| AR["Aeron Archive<br/>(durable WAL)"]

    subgraph pollers["Three independent pollers (own threads, off the engine thread)"]
        PJ["AeronWalProjector<br/>(wal-projector poll thread)"]
        DBP["WalDbProjector<br/>(wal-db-projector thread)"]
    end

    AR -->|"following replay<br/>(subscribeFillsFollowing)"| PJ
    AR -->|"periodic catch-up<br/>(durable cursor)"| DBP

    PJ -->|"WalFillConverter.toTrade"| TS["TradeStore"]
    PJ -->|"offer TradeEvent (Fix A)"| BQ["bounded SPSC queue"]
    BQ -->|"wal-broadcaster thread"| WSB["WebSocket broadcast"]
    PJ -. "fxoee.wal.questdb.enabled" .-> QD["QuestDbTapeSink (ILP)<br/>flush on poll thread"]
    DBP -->|"FillBatchRepository<br/>(idempotent fill_dedup)"| PG[("PostgreSQL<br/>balances + lots")]
```

### Where each piece runs

| Stage | Thread | What it does | Allocates? |
|-------|--------|--------------|------------|
| Encode + append | `speed-engine` | `FillCodec` writes the fill into a reused `FillRecordData`; `WalPublisher.append` buffers it, `flush` emits the whole Disruptor batch as one Aeron message | **No** (record + buffer reused) |
| Following-replay tape | `AeronWalProjector` poll thread | `subscribeFillsFollowing` drains the Archive (a slow tape consumer can't gate the live publication), `WalFillConverter.toTrade` builds the `Trade`, adds to `TradeStore` | Yes (off hot path) |
| WS broadcast (**Fix A**) | `wal-broadcaster` | Poll thread `offer`s each `TradeEvent` to an `OneToOneConcurrentArrayQueue`; the broadcaster does the JSON publish; on overflow the broadcast is dropped (`wal.broadcast.dropped.total`), never blocking the drain | Yes (off hot path) |
| QuestDB tape | `AeronWalProjector` poll thread | When `fxoee.wal.questdb.enabled`, writes the QuestDB history tape over ILP, flushing on the poll thread because the ILP `Sender` is not thread-safe | Yes (off hot path) |
| Postgres balances | `wal-db-projector` | Periodically (default 200ms) reads a durable cursor, replays the Archive tail, applies per-account legs in batches via `FillBatchRepository`, idempotent through a `fill_dedup` guard; the cursor advances only after a batch commits, so a crash re-replays at most one batch | Yes (off hot path) |

- **Engine-stamped fill sequence.** Every fill carries a monotonic `fillSeq` stamped on the engine thread ([SpeedEngine.java:364](../src/main/java/com/fxoee/engine/speed/SpeedEngine.java)). It is the WAL ordering key and the source of the deterministic, replay-stable trade id `WalIds.tradeId(seq) = UUID(0x54, seq)` (lot ids use `UUID(0, seq)`, a disjoint range). No random UUIDs are minted on the hot path.
- **Batched flush.** `onEvent` flushes the pending WAL batch when the Disruptor signals end-of-batch ([SpeedEngine.java:239](../src/main/java/com/fxoee/engine/speed/SpeedEngine.java) calls `flushWal`, inside the `try` so a stall surfaces as `INTERNAL_ERROR`; the buffering lives in [WalPublisher.java](../src/main/java/com/fxoee/wal/WalPublisher.java)), so per-message recorder overhead is paid once per Disruptor batch, not per fill.

### Ingress shed under WAL lag (Fix B)

A fill burst on an accepted order must always fit above the back-pressure trip point of the IPC term buffer. So a **NEW** order is rejected `OVERLOADED` **before any state mutation** when the WAL lag (`aeronWal.walLagBytes()`) exceeds `fxoee.wal.aeron.lag-threshold-bytes` (default 48 MiB, capped to 90% of the term buffer at wiring time) ([SpeedMatchingService.java:177](../src/main/java/com/fxoee/engine/speed/SpeedMatchingService.java), [SpeedEngineConfig.java:136](../src/main/java/com/fxoee/engine/speed/SpeedEngineConfig.java)). The lag read is a lock-free counter load, cheap from every request thread; the raw load-generator lane gates on the same check.

### Bounded restart over the Archive (Phase E snapshots)

[Snapshotter](../src/main/java/com/fxoee/wal/Snapshotter.java) captures a consistent whole-engine snapshot in **one engine-thread command** (`SpeedMatchingService.captureSnapshot`, [SpeedMatchingService.java:742](../src/main/java/com/fxoee/engine/speed/SpeedMatchingService.java)): the live WAL recording position and fill-sequence high-water, plus every account's cash, realized P&L and open lots. `walPosition()` flushes the pending WAL batch first so the cut covers every already-applied fill. Recovery loads the snapshot and replays only the **tail** of the Archive past that position, then raises `fillSeq` so resumed trading never re-issues a deterministic trade id. Restart cost is bounded by the trades since the last snapshot, not the whole history. Opt-in via `fxoee.wal.snapshot.enabled`; surviving a real process restart also needs `persist-archive=true` and a stable snapshot path.

---

## Configuration quick reference

| Property | Default (binding) | In `performance.properties` | Effect |
|----------|-------------------|-----------------------------|--------|
| `fxoee.engine.mode` | `default` | `speed` | Set to `speed` to activate this architecture |
| `fxoee.engine.speed.wait-strategy` | `busy-spin` | (unset, so `busy-spin`) | How the **engine thread** waits for ring events: `busy-spin` / `yielding` / `blocking` |
| `fxoee.engine.speed.ring-size` | `65536` | `65536` | Command ring slots, rounded up to a power of 2 (`toPowerOfTwo`); `<=0` falls back to `DEFAULT_RING_SIZE = 1 << 16` |
| `fxoee.engine.speed.book-map-capacity` | `65536` | `65536` | Agrona open-addressing map capacity per `SpeedBook` (five maps per book) |
| `fxoee.engine.speed.cpu` | `-1` | `2` | CPU core to pin `speed-engine` via OpenHFT Affinity (`-1` = off; Linux only, no-op elsewhere) |

The defaults come from the `@Value` fallbacks in [SpeedEngineConfig](../src/main/java/com/fxoee/engine/speed/SpeedEngineConfig.java); `performance.properties` ([src/main/resources/performance.properties](../src/main/resources/performance.properties), loaded via an `optional:` import) overrides them.

The **command** ring is bound from `fxoee.engine.speed.ring-size` ([SpeedEngineConfig.java:66](../src/main/java/com/fxoee/engine/speed/SpeedEngineConfig.java)), default `65536` slots (~0.4 s of burst headroom at 150k orders/s), rounded up to a power of two. It is a **different** ring from the persistence-side `fxoee.disruptor.ring-buffer-size` (set to `1048576` in `performance.properties`, code default `131072`), which sizes the optional [`DisruptorFillQueue`](../src/main/java/com/fxoee/engine/DisruptorFillQueue.java) between the facade and `PersistenceWorker`. The boot-default `FillQueue` is the unbounded `agrona` queue, not the Disruptor (`fxoee.queue.type`).

---

## Mental model checklist

Use this when reading code or debugging:

1. **Is this thread `speed-engine`?** → It may mutate `SpeedBook`, `SpeedPositions`, `SpeedLedger`.
2. **Any other thread?** → Convert at the edge, publish a command, await `ResultBuffer`, build Java objects.
3. **Need consistent read?** → `EngineClient.exec()`; never read engine arrays directly from Tomcat.
4. **Inside `EXEC`?** → Never call `ring.publish()` (deadlock).
5. **Who spins hot?** → Engine thread (busy-spin). Submitters park under load.
6. **Where do results live?** → Caller's `ResultBuffer`, not the ring slot.
7. **One core ceiling?** → All pairs share one writer; shard per pair is the scale-out path (future).

---

## Related docs

| Doc | Contents |
|-----|----------|
| [speed-engine.md](speed-engine.md) | Fixed-point scales, performance numbers, parity with default engine, the Aeron WAL semantics |
| [03-engine-core.md](03-engine-core.md) | Default engine pipeline (semantic reference) |
| [05-event-sourcing-persistence.md](05-event-sourcing-persistence.md) | `FillQueue` (agrona / clq / disruptor) to Kafka to DB |
| [11-risk-controls.md](11-risk-controls.md) | Risk limits and kill-switch |
| [01-architecture.md](01-architecture.md) | Whole-system layout (default + speed) |
