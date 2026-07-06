# 08 - Testing

_Last updated: 2026-06-21 BST._

The engine is tested as a **pure unit**: `EngineTestSupport.newService(mode)` wires a fully in-memory
`MatchingService` (all 7 pairs, no Spring/Kafka/DB) so tests run in milliseconds. Maven is the build
tool (`mvn test`).

Three Maven profiles gate the slower edges (see `pom.xml`):

- default `mvn test` runs **everything** except the `perf` micro-benchmarks (those are `@Tag("perf")`,
  excluded by `<excludedGroups>perf</excludedGroups>`).
- `mvn -DPerformance test` adds the JMH benchmarks under `com.fxoee.perf` and the `perf`-tagged tests.
- `mvn -Pci-fast test` is the PR fast lane: it excludes `perf`, `integration`, **and `slow`** so the
  Aeron MediaDriver/Archive WAL, QuestDB Testcontainer, and snapshot/WAL-replay suites are skipped. A
  plain local `mvn test` still runs them.

## Suite map

```mermaid
flowchart TB
    subgraph core["Core engine - com.fxoee.engine"]
        c1["MatchingServiceTest<br/>end-to-end submit flow"]
        c2["MatchingServiceCornerCasesTest ★<br/>MARKET SELL, USD/JPY, replay, forceFlat, closeLot, fee"]
        c3["InvariantConservationTest<br/>§13 invariants over a session"]
        c4["SpecWorkedExamplesTest<br/>spec §6 worked examples"]
        c5["EngineConservationFuzzTest ★<br/>1500 randomized orders, invariants every step"]
        c6["EnginePerformanceTest ★<br/>throughput floors"]
    end
    subgraph units["Component units"]
        u1["PositionBookTest + PositionBookCornerCasesTest ★"]
        u2["MarginLedgerTest + LedgerCornerCasesTest ★"]
        u3["MarginTest"]
        u4["PreTradeValidatorTest"]
        u5["MarketBuyEstimatorTest"]
    end
    subgraph match["Matching - com.fxoee.matching"]
        m1["MatchingEngineTest"]
        m2["MatchingEngineParameterizedTest"]
        m3["MatchingEngineMarketSimulationTest"]
        m4["OrderBookTest"]
        m5["OrderFundsValidatorTest"]
    end
    subgraph perf["Perf - com.fxoee.perf"]
        p1["MatchingEngineBenchmark (JMH)"]
        p2["MatchingEnginePerfTest"]
    end
    subgraph boot["Bootstrap / recovery - com.fxoee.bootstrap"]
        b1["AccountBootstrapperTest ★<br/>fresh vs warm start, resting rebuild, relay, bad JSON, missing repo (Mockito)"]
        b2["WarmRestartIntegrationTest ★<br/>real Postgres + EmbeddedKafka: trade_events + resting_orders → recoverFromLog → engine + books"]
    end
    subgraph speed["Speed engine - com.fxoee.engine.speed"]
        s1["SpeedMatchingServiceTest + SpeedMatchingEngineTest"]
        s2["EngineDifferentialFuzzTest<br/>speed vs default parity"]
        s3["SpeedFillMaterializeTest<br/>deferred event/JSON build, idempotent"]
        s4["SnapshotterTest + SpeedSnapshotRestartTest ◇<br/>bounded warm restart over the Archive tail"]
        s5["SpeedSubmitAllocProbe<br/>submit-thread B/op (ThreadMXBean)"]
    end
    subgraph wal["Aeron WAL + tape - com.fxoee.wal / .speed ◇"]
        w1["AeronArchiveWalRoundTripTest<br/>record → replayForRecovery lossless"]
        w2["AeronWalBatchRoundTripTest<br/>batched WAL frames"]
        w3["AeronWalProjectorAsyncDrainTest<br/>poll-thread drain + SPSC broadcast hand-off"]
        w4["SpeedWalReplayTest<br/>Phase C: 2nd engine rebuilt from WAL, conservation"]
        w5["SpeedWalCrossRestartRecoveryTest<br/>REAL kill+restart, persist-archive, no DB"]
        w6["QuestDbTapeSinkTest<br/>Phase D: ILP write → queryable over PG wire (Testcontainer)"]
        w7["WalProjectionInputTest + FillCodecTest"]
    end
    subgraph e2e["End-to-end pipeline - com.fxoee.it ◆"]
        e0["AbstractKafkaE2ETest<br/>shared base: singleton Postgres + EmbeddedKafka, full Spring context"]
        e1["BalanceDbE2ETest (9)<br/>cash + reserved land in DB"]
        e2["OrderAuditDbE2ETest (10)<br/>orders-table rows + status transitions"]
        e3["PositionLotDbE2ETest (7)<br/>position_lot open/close projections"]
        e4["RestingOrderDbE2ETest (8)<br/>resting_orders upsert/delete"]
        e5["OrderTradeWebSocketIntegrationTest (1)<br/>REST → engine → Kafka → WS push"]
    end
```

★ = added in the comprehensive-tests pass (34 tests). The rest predate it.
◆ = the `com.fxoee.it` end-to-end suite (see [End-to-end pipeline tests](#end-to-end-pipeline-tests)).
◇ = `@Tag("slow")`: embedded Aeron MediaDriver/Archive, QuestDB Testcontainer, or snapshot/WAL-replay.
Run by a plain `mvn test`, skipped by `-Pci-fast` (see [Speed engine and Aeron WAL coverage](#speed-engine-and-aeron-wal-coverage)).

## What the invariant/fuzz tests assert

`EngineConservationFuzzTest` drives 1500 random orders across 6 accounts × 4 pairs (USD-quote and
USD-base) with a seeded RNG, and after **every** order checks:

| # | Invariant | Meaning |
|---|-----------|---------|
| 1 | `cash == deposit + Σ realized P&L` | cash moves only by P&L ([doc 04](04-funding-pnl-conservation.md#the-conservation-invariant)) |
| 2 | `free ≥ 0` | reserved never exceeds cash (solvency) |
| 3 | no-hedge | an (account, pair) never holds LONG + SHORT at once |
| 4 | `netQty == Σ signed lot qty` | netting integrity |
| 5 | `reserved == held margin` (when no resting orders) | reconcile is exact |

Determinism: the seed is fixed, so any failure reproduces exactly.

## Corner cases covered by the ★ suites

- **USD-base P&L conversion**: LONG/SHORT close and flip on USD/JPY (÷ close price), exact to 10 dp.
- **Ledger guards**: null/zero/negative no-ops, flooring at zero, account isolation, `reserveNet`
  atomic swap, exact funding boundary, concurrent no-overdraw.
- **`Margin`**: HALF_UP rounding, zero quantity, USD-base vs USD-quote notional, `MARGIN ==
  marginRate × FULL_NOTIONAL`.
- **PositionBook**: `cashDelta` identity, cross-pair held margin, close→flat→reopen, FIFO depth,
  `clear` isolation, and **concurrency** (parallel same-account fills serialize correctly; distinct
  accounts never interfere).
- **MatchingService**: MARKET SELL open, USD/JPY round-trip conservation, warm-restart replay
  round-trip, **warm-restart recovers resting orders 1:1** (filled position and resting LIMIT both
  restored, full margin re-locked), `forceFlat`, `closeLot`, USD-base taker fee, snapshot view.

## Bootstrap / recovery: warm restart, three layers

Warm restart (rebuilding the engine from the `trade_events` log instead of wiping to a fresh balance,
see [doc 05](05-event-sourcing-persistence.md#warm-restart-recovery-engine-replay)) is covered at three
layers, each cheaper and more granular than the last:

| Layer | Test | Infra | What it pins down |
|-------|------|-------|-------------------|
| Engine unit | `MatchingServiceCornerCasesTest.replayRoundTrip` + `…warmRestartRecoversRestingOrders` | none | `seedForReplay` → replay fills → `reconcileReserved` reproduces position + cash + margin; and that resting (unfilled) LIMIT orders are rebuilt 1:1 on restart with full margin re-locked (`positions + resting`) |
| Bootstrap unit | `AccountBootstrapperTest` (10) | Mockito only | both boot paths (fresh vs warm), **resting-order rebuild + reconcile**, the Kafka relay of `published=false` rows, bad-JSON tolerance, and fallback to fresh start when no `TradeEventRepository` bean exists |
| Bootstrap IT | `WarmRestartIntegrationTest` (5) | Testcontainers Postgres + EmbeddedKafka | the **DB-to-engine glue**: `trade_events` → `recoverFromLog` → `MatchingService.snapshot`; relay re-publish; **resting-order recovery** (incl. a partially-filled row); `RestingOrderRepository` upsert/update/delete; and end-to-end `submit → PersistenceWorker persist → restart → back on the book` |

```mermaid
flowchart LR
    t["trade_events"] -->|replayFill| ms["MatchingService (engine)"]
    r["resting_orders"] -->|addOrder| bk["OrderBooks"]
    t & r -->|recoverFromLog| ab["AccountBootstrapper"]
    bk -->|reconcileReserved reads books| ms
    ms -->|snapshot + book| assert["assert cash / positions / reserved / resting orders"]
```

The unit layer runs in milliseconds with no daemon; the IT needs Docker. End-to-end on a live cluster
is in the runbook [testing-event-sourcing-minikube.md](testing-event-sourcing-minikube.md).

The warm-restart layers above cover **Lane 1** (default engine, `trade_events` log). The **speed
engine's** own durable warm restart, over the Aeron Archive WAL rather than Postgres, is covered
separately below.

## Speed engine and Aeron WAL coverage

The `speed` engine (`com.fxoee.engine.speed`) is verified
the same way the default engine is, plus a durability lane of its own (ADR 0007). The differential
fuzz test keeps the two engines honest against each other; the WAL suites prove the Aeron Archive is a
faithful, replayable source of truth that can rebuild the engine across a real process kill, with no
database in the loop.

| Test | Tag | What it pins down |
|------|-----|-------------------|
| `EngineDifferentialFuzzTest` | - | speed vs default engine **parity**: identical orders produce identical cash, P&L, and positions |
| `SpeedFillMaterializeTest` (3) | - | `SpeedFill` defers the event/JSON build to `PersistenceWorker`; the build is memoized and idempotent and reuses the `Trade` list |
| `SpeedSubmitAllocProbe` | - | submit-thread bytes/op via `ThreadMXBean` (the engine thread itself is zero-alloc; this watches the submit thread) |
| `AeronArchiveWalRoundTripTest` | `slow` | a WAL-attached engine records fills, then `replayForRecovery` reads them back losslessly |
| `AeronWalBatchRoundTripTest` | `slow` | the batched WAL frame path round-trips |
| `AeronWalProjectorAsyncDrainTest` | `slow` | the projector poll thread drains the subscription at memory speed and hands each `TradeEvent` to the bounded SPSC broadcast queue (Fix A) |
| `SpeedWalReplayTest` | `slow` | **Phase C**: a second, fresh engine rebuilt purely by replaying the WAL agrees account-for-account, and Σ cash is conserved |
| `SpeedSnapshotRestartTest` | `slow` | **Phase E**: snapshot at an exact Archive position + replay only the tail rebuilds the engine (bounded restart, same process) |
| `SpeedWalCrossRestartRecoveryTest` | `slow` | the **real-kill** proof: closes the `AeronWal` entirely and reopens a fresh one over the same persisted Archive dirs, then `Snapshotter.recover()` rebuilds from snapshot + tail. This is what `persist-archive=true` buys |
| `QuestDbTapeSinkTest` (2) | `slow` | **Phase D**: `QuestDbTapeSink` writes the trade tape to a QuestDB Testcontainer over ILP, and the rows are queryable back over QuestDB's Postgres wire |
| `WalProjectionInputTest`, `FillCodecTest` (4) | mixed | the `FillCodec` encode/decode and the WAL projection input contract |

`EngineDifferentialFuzzTest` is the load-bearing one: it feeds the default `MatchingService` and the
speed `SpeedMatchingService` (seeded identically, `producer=null`, `fillQueue=null`, no risk gate) the
same pseudo-random LIMIT/cancel stream and asserts they agree on report status, per-trade (price, qty,
aggressor side), positions (netQty + lots), and every monetary field within +/-0.01 (once in
margin-locked funding mode, once in full-notional mode). It proves the speed engine is observationally
equivalent to the reference engine, so the rest of the docs' semantics apply unchanged in speed mode.

All the `slow`-tagged suites spin up an **embedded Aeron MediaDriver + Archive** (or, for the tape, a
QuestDB container). They run under a plain `mvn test` but are skipped by the `-Pci-fast` PR lane so
those builds stay quick. The Postgres-side projector (`WalDbProjector`) shares `FillBatchRepository
.flushLegs` with Lane 1, so its DB writes are exercised by the same `*E2ETest` projection assertions.

## End-to-end pipeline tests

The `com.fxoee.it` suite proves the whole write path actually reaches Postgres, not just that the
engine computed the right numbers. A request goes in over REST and the test waits for the row to show
up in the database, exercising every hop in between:

```
REST → MatchingService (engine) → FillQueue → PersistenceWorker → Kafka → FillConsumer → Postgres
                                                                  └→ OrderAuditConsumer → orders
```

Everything hangs off one base class, `AbstractKafkaE2ETest`:

- **Real infrastructure, started once.** A singleton Testcontainers Postgres (`postgres:16-alpine`)
  boots once per JVM and is shared across every subclass, and an in-process `@EmbeddedKafka` broker
  stands in for real Kafka. Because `@DynamicPropertySource` hands every subclass the same datasource
  URL, they share one Spring context, so the heavy boot cost is paid a single time.
- **Full async stack live.** `PersistenceWorker`, `FillConsumer`, `OrderAuditConsumer`, and
  `RestingOrderRepository` are all wired and running, the same beans as production. The feeds and
  warm-restart replay are switched off (`market-data`, `mock-market`, `sample-data`,
  `recovery.replay-on-startup` all false) so each run starts from a known, quiet state.
- **Async writes are awaited, not slept on.** Since the DB write happens on a Kafka consumer thread,
  assertions poll through an `awaitTrue` helper rather than a fixed sleep, which keeps the tests both
  fast and non-flaky.
- **Clean slate per test.** `resetState()` runs before each test: it clears the order books, resets
  in-memory positions, ledger, and account mirrors, resets DB balances, and truncates the audit
  tables. Every test therefore starts flat, with no open positions and `INITIAL_BALANCE` cash.

| Test | Tests | What it pins down |
|------|-------|-------------------|
| `BalanceDbE2ETest` | 9 | cash and reserved funds reach `customer_account` correctly after fills, reserves, and releases |
| `OrderAuditDbE2ETest` | 10 | the `orders` table captures the full status trail (placed → filled / cancelled / rejected) |
| `PositionLotDbE2ETest` | 7 | `position_lot` rows open and close in step with the engine's FIFO netting |
| `RestingOrderDbE2ETest` | 8 | `resting_orders` is upserted on a resting LIMIT and deleted on fill or cancel |
| `OrderTradeWebSocketIntegrationTest` | 1 | a REST order produces the matching trade and pushes it out over the `/ws` WebSocket |

These need Docker for the Postgres container; the embedded Kafka runs in-process. They are the
slowest tests in the suite, so they sit behind the same Docker gate as the integration tests below.

## Performance floors

`EnginePerformanceTest` (`@Tag("perf")`) measures hot-path throughput with **generous floors**. It
exists to trip a red test on a gross regression (an accidental O(n²) or a reintroduced global lock),
not to micro-benchmark. Representative measurements on the dev machine:

| Path | Floor | Measured |
|------|-------|----------|
| `PositionBook.applyFill` | > 100k ops/sec | ~4.4M ops/sec |
| `MatchingService.submit` | > 5k orders/sec | ~218k orders/sec |

For real micro-benchmarks use the JMH harnesses: `com.fxoee.perf.MatchingEngineBenchmark` and
`OrderBookBenchmark` (default engine), and `com.fxoee.engine.speed.SpeedEngineBench` (speed engine).
`SpeedSubmitAllocProbe` measures submit-thread bytes-per-op via `ThreadMXBean`. `EnginePerfReportTest`
benchmarks **both** engines in one fork-sweep and renders a self-contained HTML report under `target/`
with a `GCProfiler` allocation column; it runs only when `-DPerformance` is passed:

```bash
mvn test -DPerformance -Dtest='EnginePerfReportTest'   # both engines → target/*.html
```

## Running

```bash
mvn test                                   # full suite (incl. slow WAL/QuestDB/snapshot tests)
mvn -Pci-fast test                         # PR fast lane: skips perf + integration + slow
mvn -DPerformance test                     # adds the JMH benchmarks + @Tag("perf") tests
mvn -o test -Dtest='EngineConservationFuzzTest,EnginePerformanceTest'   # just these
mvn -o test -Dtest='*CornerCasesTest'      # all corner-case suites

# Warm-restart coverage:
mvn -o test -Dtest='AccountBootstrapperTest,MatchingServiceCornerCasesTest'  # offline, ~1s
mvn test -Dtest='WarmRestartIntegrationTest'                                 # needs Docker, ~30s

# Speed engine + Aeron WAL (embedded MediaDriver/Archive; QuestDbTapeSinkTest needs Docker):
mvn test -Dtest='SpeedWalReplayTest,SpeedSnapshotRestartTest,SpeedWalCrossRestartRecoveryTest'
mvn test -Dtest='QuestDbTapeSinkTest'                                        # QuestDB Testcontainer

# End-to-end DB + WS pipeline (needs Docker):
mvn test -Dtest='com.fxoee.it.*E2ETest'                                      # all DB E2E suites
mvn test -Dtest='OrderTradeWebSocketIntegrationTest'                         # REST → WS round-trip
```

### Docker-dependent tests

`CustomerAccountRepositoryTest`, `PositionLotRepositoryTest`, the `*IntegrationTest` suites,
`WarmRestartIntegrationTest`, and the whole `com.fxoee.it` end-to-end suite use **Testcontainers** and
require a running Docker daemon (they spin up a real PostgreSQL; the `*E2ETest` classes and
`WarmRestartIntegrationTest` also start an in-process `@EmbeddedKafka` broker, so no container is
needed for the Kafka side). `QuestDbTapeSinkTest` likewise needs Docker (a `questdb/questdb`
container). Without Docker they error with *"Could not find a valid Docker environment"*, which is
environmental, not a code failure. The `slow`-tagged Aeron WAL suites (other than the QuestDB one) do
**not** need Docker: they run an **embedded** Aeron MediaDriver + Archive in-process, so they pass on
a daemon-less box but take seconds rather than milliseconds. Every other test (including all engine,
matching, the bootstrap unit test `AccountBootstrapperTest`, and the ★ suites) runs with no external
dependencies.
