# 08 — Testing

_Last updated: 2026-06-07 BST._

The engine is tested as a **pure unit** — `EngineTestSupport.newService(mode)` wires a fully in-memory
`MatchingService` (all 7 pairs, no Spring/Kafka/DB) so tests run in milliseconds. Maven is the build
tool (`mvn test`).

## Suite map

```mermaid
flowchart TB
    subgraph core["Core engine — com.fxoee.engine"]
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
    subgraph match["Matching — com.fxoee.matching"]
        m1["MatchingEngineTest"]
        m2["MatchingEngineParameterizedTest"]
        m3["MatchingEngineMarketSimulationTest"]
        m4["OrderBookTest"]
        m5["OrderFundsValidatorTest"]
    end
    subgraph perf["Perf — com.fxoee.perf"]
        p1["MatchingEngineBenchmark (JMH)"]
        p2["MatchingEnginePerfTest"]
    end
    subgraph boot["Bootstrap / recovery — com.fxoee.bootstrap"]
        b1["AccountBootstrapperTest ★<br/>fresh vs warm start, resting rebuild, relay, bad JSON, missing repo (Mockito)"]
        b2["WarmRestartIntegrationTest ★<br/>real Postgres + EmbeddedKafka: trade_events + resting_orders → recoverFromLog → engine + books"]
    end
```

★ = added in the comprehensive-tests pass (34 tests). The rest predate it.

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

- **USD-base P&L conversion** — LONG/SHORT close and flip on USD/JPY (÷ close price), exact to 10 dp.
- **Ledger guards** — null/zero/negative no-ops, flooring at zero, account isolation, `reserveNet`
  atomic swap, exact funding boundary, concurrent no-overdraw.
- **`Margin`** — HALF_UP rounding, zero quantity, USD-base vs USD-quote notional, `MARGIN ==
  marginRate × FULL_NOTIONAL`.
- **PositionBook** — `cashDelta` identity, cross-pair held margin, close→flat→reopen, FIFO depth,
  `clear` isolation, and **concurrency** (parallel same-account fills serialize correctly; distinct
  accounts never interfere).
- **MatchingService** — MARKET SELL open, USD/JPY round-trip conservation, warm-restart replay
  round-trip, **warm-restart recovers resting orders 1:1** (filled position and resting LIMIT both
  restored, full margin re-locked), `forceFlat`, `closeLot`, USD-base taker fee, snapshot view.

## Bootstrap / recovery — warm restart, three layers

Warm restart (rebuilding the engine from the `trade_events` log instead of wiping to a fresh balance,
see [doc 05](05-event-sourcing-persistence.md#warm-restart-recovery-engine-replay)) is covered at three
layers, each cheaper and more granular than the last:

| Layer | Test | Infra | What it pins down |
|-------|------|-------|-------------------|
| Engine unit | `MatchingServiceCornerCasesTest.replayRoundTrip` + `…warmRestartRecoversRestingOrders` | none | `seedForReplay` → replay fills → `reconcileReserved` reproduces position + cash + margin; and that resting (unfilled) LIMIT orders are rebuilt 1:1 on restart with full margin re-locked (`positions + resting`) |
| Bootstrap unit | `AccountBootstrapperTest` (7) | Mockito only | both boot paths (fresh vs warm), **resting-order rebuild + reconcile**, the Kafka relay of `published=false` rows, bad-JSON tolerance, and fallback to fresh start when no `TradeEventRepository` bean exists |
| Bootstrap IT | `WarmRestartIntegrationTest` (5) | Testcontainers Postgres + EmbeddedKafka | the **DB→engine glue**: `trade_events` → `recoverFromLog` → `MatchingService.snapshot`; relay re-publish; **resting-order recovery** (incl. a partially-filled row); `RestingOrderRepository` upsert/update/delete; and end-to-end `submit → PersistenceWorker persist → restart → back on the book` |

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

## Performance floors

`EnginePerformanceTest` (`@Tag("perf")`) measures hot-path throughput with **generous floors** — it
exists to trip a red test on a gross regression (an accidental O(n²) or a reintroduced global lock),
not to micro-benchmark. Representative measurements on the dev machine:

| Path | Floor | Measured |
|------|-------|----------|
| `PositionBook.applyFill` | > 100k ops/sec | ~4.4M ops/sec |
| `MatchingService.submit` | > 5k orders/sec | ~218k orders/sec |

For real micro-benchmarks use the JMH harness in `com.fxoee.perf.MatchingEngineBenchmark`.

## Running

```bash
mvn test                                   # full suite
mvn -o test -Dtest='EngineConservationFuzzTest,EnginePerformanceTest'   # just these
mvn -o test -Dtest='*CornerCasesTest'      # all corner-case suites

# Warm-restart coverage:
mvn -o test -Dtest='AccountBootstrapperTest,MatchingServiceCornerCasesTest'  # offline, ~1s
mvn test -Dtest='WarmRestartIntegrationTest'                                 # needs Docker, ~30s
```

### Docker-dependent tests

`CustomerAccountRepositoryTest`, `PositionLotRepositoryTest`, the `*IntegrationTest` suites, and
`WarmRestartIntegrationTest` use **Testcontainers** and require a running Docker daemon (they spin up a
real PostgreSQL; `WarmRestartIntegrationTest` also starts an in-process `@EmbeddedKafka` broker — no
container needed for that part). Without Docker they error with *"Could not find a valid Docker
environment"* — this is environmental, not a code failure. Every other test (including all engine,
matching, the bootstrap unit test `AccountBootstrapperTest`, and the ★ suites) runs with no external
dependencies.
