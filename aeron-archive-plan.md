# Aeron Archive WAL + clean speed engine + QuestDB tape

_Last updated: 2026-06-20 BST._

> **DELIVERED / historical.** This was the Phase A-F handoff plan for ADR 0007. It has shipped: the
> Aeron Archive WAL, SBE `FillRecord`, clean speed-engine rewrite, and QuestDB tape are all in the tree
> (`com.fxoee.wal` + `com.fxoee.engine.speed[.wal]`), behind `fxoee.wal.*` flags (all off by default).
> The page is kept as the rationale + a phase-by-phase record of **what actually shipped vs what was
> planned** (deviations called out per phase). For the current, code-accurate description of the lane see
> [05-event-sourcing-persistence.md](05-event-sourcing-persistence.md) (Lane 2) and the decision record
> [adr/0007-aeron-archive-wal-questdb-tape.md](adr/0007-aeron-archive-wal-questdb-tape.md).

## What shipped (one-paragraph summary)

`fxoee.engine.mode=speed` + `fxoee.wal.aeron.enabled=true` makes matching JVM-only with no Kafka. The
engine records every fill (SBE-encoded, whole-trade) to an embedded Aeron Archive on local NVMe via
`WalPublisher`; the recording position is the engine-stamped monotonic sequence. Two projectors read the
Archive: `AeronWalProjector` (trade tape: `TradeStore` + live WS + an optional QuestDB sink) and
`WalDbProjector` (Postgres account state, periodic off-engine catch-up, idempotent via `fill_dedup`).
Ingress shedding moved to a WAL-lag gate (reject `OVERLOADED` above `lag-threshold-bytes`). Bounded warm
restart is a JSON snapshot tagged with the Archive position plus a tail replay. **Kafka was removed only
from the speed lane**: the default (lock) engine keeps its Kafka + `trade_events` event sourcing (Lane 1),
and `spring-kafka` is still a dependency.

## Original goal

Replace the Kafka durability path **for the speed engine** with an Aeron Archive WAL, rewrite the speed
engine for readability (same fixed-point-long, zero-alloc-hot-path concepts, better organized), and add
QuestDB as the high-rate fill tape while Postgres keeps account state. **Goal: > 1,000,000 orders/sec
matched AND durably recorded, on a single box, with full event sourcing (replay + snapshot from the
Archive).** Branch (now merged-forward): `feat/aeron-archive-speed`, off `perf/throughput-phase1` (master
had no speed engine to use as a parity oracle).

---

## Decisions (locked with user, 2026-06-14)

- **Aeron depth = Archive WAL, done fully.** Tier 1 complete end-to-end (SBE + Aeron Archive durable
  WAL + replay + snapshot + QuestDB tape). **No Aeron Cluster / no Raft** this round; Cluster is a
  later stretch, explicitly out of scope here.
- **Transport = IPC single-box** (shared memory). Fastest, simplest, quickest path to a > 1M bench.
  Networked UDP deferred.
- **Replace the speed engine.** Same concepts (fixed-point longs, zero-alloc hot path) but the 41 KB
  `SpeedMatchingService` monolith is split into small, single-responsibility classes. New engine MAY
  keep the name `speed`. Old engine kept temporarily as a **parity oracle**, deleted only after the
  new one passes parity. _(Shipped: legacy deleted, no `.legacy` package remains.)_
- **DB split.** **Postgres + jOOQ keeps account/position/margin state** (low-rate, transactional,
  correctness home). **QuestDB takes the fill tape** (engine-rate, millions rows/sec, SQL-queryable
  for UI tape + history + analytics). Both are **projections of the Archive**; Archive is the single
  source of truth; either DB is rebuildable by replay.
- **Kafka is removed from the new engine's path** (producer, consumers, topic config, `trade_events`
  as the hot-path WAL, Jackson JSON on the fill path). _(Shipped for the speed lane only; the default
  engine retains the full Kafka path, see Phase F deviation.)_

## Invariants that MUST hold (unchanged from the engine's contract)

1. **Engine authoritative.** DB/QuestDB are projections; engine rebuilt only from the durable Archive
   (+ snapshot), never from a stale DB.
2. **Append-only log = single source of truth.** Now the **Aeron Archive recording**. The recording
   **position is the engine-stamped monotonic sequence** the old Phase-4 plan was missing, so the
   snapshot cut is exact (closes the enqueue-lag double-count window noted in ADR 0006).
3. **Conservation:** `cash == deposits + Σ realized PnL`. Per-account cash applied in order;
   parallelism allowed across accounts, ordered within an account.
4. **Dedup idempotency** on `(trade_id, leg)` (carried into the Postgres `fill_dedup` table and the
   QuestDB `DEDUP UPSERT KEYS`). Ids are deterministic (`WalIds`), so replay reproduces them exactly.
5. **Warm-restart replay** reproduces engine state exactly: load latest snapshot, then replay the
   Archive tail from the snapshot's recording position.

---

## Target architecture (as shipped)

```
REST / SIMULATE / FIX
      │ submit(Order)                          (TradingEngine.submit, unchanged interface)
      ▼
  SpeedEngine            single-writer, fixed-point longs, zero-alloc hot path
      │ encode fill → SBE FillRecord flyweight over an off-heap buffer (no objects, no JSON)
      ▼
  WalPublisher           Aeron IPC ExclusivePublication → recorded by Aeron Archive
      ▼
  Aeron Archive (NVMe)   THE durable WAL. Append-only. Position-addressable replay.
      │
      ├──► AeronWalProjector (following replay): TradeStore + live WS + optional QuestDB sink
      ├──► WalDbProjector (periodic catch-up):   Postgres account_transaction / position_lot
      └──► Snapshotter → JSON snapshot tagged with the Archive position (+ SnapshotSchedule)

  Restart = load latest snapshot → replay Archive from its position (bounded).
  Shedding sits ONLY in front of the Aeron publication buffer → OVERLOADED ≈ 0.
```

The plan sketched a single `Projector`; it shipped as **two** beans so the high-rate tape and the
low-rate transactional Postgres lane scale and fail independently.

---

## Dependencies (pinned)

- `io.aeron:aeron-archive` 1.48.0 (pulls `aeron-client` + `aeron-driver`; all share Agrona). Same author
  family as the existing Disruptor / affinity.
- SBE 1.35.1: `sbe-all` runs as a build plugin (`SbeTool` over `src/main/resources/sbe/`) emitting
  flyweight codecs into `target/generated-sources/sbe`, mirroring the jOOQ codegen step.
- `org.questdb:questdb` 8.3.2: QuestDB Java client (ILP `Sender`) for the tape.
- Aeron Media Driver: embedded `ArchivingMediaDriver` for single-box IPC (no external process).

> SBE schema XML lives at `src/main/resources/sbe/fill-record-schema.xml`, package
> `com.fxoee.engine.speed.wal.sbe`. Codec regen on schema change (`mvn generate-sources`).

---

## Module layout (the readability rewrite, as shipped)

Hot-path rule made physical: **longs + SBE flyweights only on the engine/publish thread; every
object-shaped concern lives on the projector side.**

| Class | Responsibility | Thread / allocation |
|-------|----------------|---------------------|
| `SpeedEngine` | pure matching loop, fixed-point longs, records fills to the WAL on its own thread | hot path, zero-alloc |
| `SpeedBook` | per-pair order book | hot path, zero-alloc |
| `SpeedPositions` | FIFO lot netting, realized PnL | hot path, zero-alloc |
| `SpeedLedger` | cash + locked margin | hot path, zero-alloc |
| `Fixed` | fixed-point long math | n/a |
| `FillCodec` / `FillRecordData` | encode/decode a fill via the SBE flyweight | hot path encode, zero-alloc |
| `WalPublisher` | fill buffer → Aeron publication (batch + offer/backpressure) | hot path, zero-alloc |
| `SpeedMatchingService` | thin `TradingEngine` adapter: validate → risk gate → engine → WAL ingress-shed; bench hooks | hot path orchestration |
| `AeronWal` | embedded driver + archive lifecycle, replay/recovery, lag accounting | startup / IO |
| `AeronWalProjector` | Archive following-replay → trade tape (TradeStore + WS + QuestDB) | off hot path, allocates |
| `WalDbProjector` | Archive catch-up → Postgres account state | off hot path, allocates |
| `Snapshotter` / `SnapshotStore` / `SnapshotSchedule` | snapshot at Archive position; replay tail | startup / periodic |

`SpeedMatchingService` stays the `TradingEngine` implementation (same public surface:
`submit/cancel/closeNet/closeLot/forceFlat/restore/replayFill/snapshot/…`), so REST/FIX/SIMULATE callers
and tests are untouched; only the internals and the durability tail changed.

---

## SBE message shape (as shipped)

- **FillRecord** (engine → WAL): `engineSeq` (monotonic), `timestampMs`, `pair`, `aggressorSide`,
  `priceRaw`, `qtyRaw`, per-side (buy/sell) `acctHi/acctLo` + `tracked` + `cashDeltaRaw` + `pnlRaw`,
  `feeRaw` + `takerAcctHi/Lo`, a **lots group** (per lot: leg, type open/partial/full-close, side,
  `lotSeq`, `qtyRaw`, `priceRaw`, `pnlRaw`, `tsMs`), and var-length `tradeId` / `buyOrderId` /
  `sellOrderId`. **Whole-trade record** (both legs in one record): leg-splitting was a Kafka-partition
  workaround that broke replay reassembly. RAW fixed-point longs only; BigDecimal conversion is a
  projector concern (`Fixed`). Nullable account ids are the all-zero UUID. Lot ids derive from `lotSeq`
  (`WalIds.lotId`), so the group carries the seq, not a string.
- **OrderCommand** (optional ingress-audit stream): **not built** (the plan flagged it as not required
  for Tier 1; engine output is the event-sourcing log).

Schema is positional and append-only: bump `version`, only ADD at the end, never reorder or retype.

---

## Phased roadmap (with what actually shipped)

Each phase was independently shippable + testable, parity-gated against the legacy speed engine.

### Phase A - SBE foundation _(shipped)_
- SBE build plugin + runtime dep added; `FillRecord` schema authored; codecs generated.
- `FillCodec` encodes/decodes a fill to/from an off-heap buffer; round-trip test `FillCodecTest`.
- Allocation probe `FillCodecAllocProbe` confirms encode is zero-alloc.

### Phase B - Clean engine rewrite (no Aeron yet) _(shipped)_
- `SpeedMatchingService` monolith split into the table above; behaviour identical to legacy.
- New engine ran behind `fxoee.engine.mode=speed2` during the parity window. **Parity held; in Phase F
  the legacy `com.fxoee.engine.speed` package was deleted and the new engine took over the `speed`
  package and `fxoee.engine.mode=speed`** (no `speed2` remains).
- Parity harness drove identical order streams through legacy + new, asserting identical fills, cash,
  PnL, lots, conservation. `SpeedEngineBench` confirmed no matching-throughput regression.

### Phase C - Aeron Archive WAL (the core swap) _(shipped)_
- Embedded `ArchivingMediaDriver` (IPC) + `AeronArchive` lifecycle in `AeronWal` (Spring bean,
  start/stop, `dedicated-archive` recorder thread for throughput).
- `WalPublisher`: engine fills → Aeron IPC publication, recorded by the Archive; it batches many fills
  into one message (flush at ~60 KiB) so per-message recorder overhead is amortised.
- Shedding moved to a WAL-lag gate in `SpeedMatchingService` (reject `OVERLOADED` when
  `publication − recording` bytes exceed `lag-threshold-bytes`), not a `FillQueue` high-water.
- **Deviation from the plan's "retire `trade_events`/Kafka globally":** these are retired only on the
  speed-WAL path. The default engine still appends `trade_events` and publishes Kafka (Lane 1).
- Recording position = engine-stamped seq → exact snapshot cut, no opportunistic guard.

### Phase D - QuestDB tape + Postgres account projection _(shipped, as two projectors)_
- `AeronWalProjector` writes the trade tape (`TradeStore` + live WS) and, when
  `fxoee.wal.questdb.enabled=true`, the QuestDB sink over ILP **HTTP** (`http::addr=…`, lazy connect,
  `DEDUP UPSERT KEYS(timestamp, trade_id)`).
- `WalDbProjector` (`fxoee.wal.postgres.enabled=true`) does a periodic off-engine catch-up into Postgres
  account state, idempotent via `fill_dedup`, with an incremental fragment-boundary cursor in
  `wal_projection_offset` and a snapshot-seeded bounded backfill.
- Read-layer split: UI trade tape + history → QuestDB / `TradeStore`; account/balance/position reads →
  Postgres (or the authoritative engine).

### Phase E - Snapshot + bounded restart _(shipped)_
- `Snapshotter` + `SnapshotStore` (JSON file) + `SnapshotSchedule` capture a consistent engine snapshot
  (balances + positions) tagged with the Archive recording position, periodically and on shutdown.
  **Resting orders are not snapshotted** (the WAL records fills, not the live book).
- `Snapshotter.recover` (wired via `SpeedEngineConfig.walRecoverOnBoot`) loads the latest snapshot,
  replays the Archive from that position only (`AeronWal.replayForRecovery`), then raises the fill-seq
  high-water and `reconcileReserved`s, so a restore cannot re-issue a closed lot id and resumed trading
  cannot re-mint a deterministic trade id already in the WAL.
- Verified across a real kill+restart by `SpeedWalCrossRestartRecoveryTest` (with `persist-archive=true`
  + a stable snapshot file).

### Phase F - Cleanup _(partially shipped, deliberate deviation)_
- Deleted `…speed.legacy`; made `speed` the engine; updated docs + ADR 0007.
- **Did NOT delete Kafka deps/config/`KafkaTopicConfig`/Grafana kafka rows.** The default (lock) engine
  still runs the Kafka + `trade_events` event-sourcing lane, so `spring-kafka` and the topic config stay.
  ADR 0007 supersedes the Kafka durability path **for the speed engine**, not for the whole project; the
  two lanes coexist (see [05-event-sourcing-persistence.md](05-event-sourcing-persistence.md)).

---

## Testing strategy (as built)

1. **Parity oracle (Phase B gate):** legacy vs new engine, identical input → identical output.
2. **Throughput:** `SpeedEngineBench` for matching rate; bench mode (`RawOrderSink#submitRaw`) for
   end-to-end submit→Archive-recorded rate.
3. **Allocation probes:** `SpeedSubmitAllocProbe` (hot path) + `FillCodecAllocProbe` (SBE encode) stay
   zero-alloc (ThreadMXBean B/op).
4. **Functional / WAL:** `FillCodecTest`, `AeronWalBatchRoundTripTest`, `AeronWalProjectorAsyncDrainTest`,
   `WalProjectionInputTest`, `QuestDbTapeSinkTest`, and the cross-restart
   `SpeedWalCrossRestartRecoveryTest`.
5. **Conservation gate:** `Σ cash == Σ deposits + Σ realized PnL`; one fill row per `(trade_id, leg)`
   (no double-apply, no dropped fills).

Single-box IPC + embedded driver means the whole stack runs in one JVM for tests, so feedback is fast
with no external broker/cluster to stand up.

---

## Risks / open items (status)

- **Migration blast radius:** the `Speed*Test` files + integration tests were rewritten against the new
  engine. Done.
- **Two query DBs:** UI/audit hit Postgres *and* QuestDB. Read-layer split landed (tape → QuestDB /
  `TradeStore`, state → Postgres / engine).
- **Aeron Archive ops:** single-box IPC only; replication/multi-node is DIY vs Kafka ISR. Still deferred
  (out of scope, fine for now).
- **SBE schema discipline:** versioned, append-only field evolution; codec regen in the build.
- **Backpressure semantics:** shedding is the WAL-lag ingress gate; the single-writer engine thread
  never blocks on a back-pressured offer.
- **Postgres can't take 1M rows/s synchronously:** by design `WalDbProjector` lags and batches; the 1M
  firehose lands in the Archive + QuestDB. Postgres account writes are per-account (low rate).
- **WAL size is not bounded:** bounded *replay* does not bound *size*; trimming Archive segments below the
  minimum live snapshot position is still open (carried from ADR 0006).

## Reused building blocks

- `TradingEngine` interface: the stable seam; the engine implements it unchanged.
- `Fixed`, `SpeedBook`, `SpeedPositions`, `SpeedLedger`: fixed-point + matching primitives carried over.
- `SpeedEngineBench` / `SpeedSubmitAllocProbe`: throughput + alloc harness, extended.
- `restore(...)` / `replayFill(...)` on `TradingEngine`: already correct (ADR 0006), reused verbatim for
  Archive replay by `Snapshotter.recover`.

Back to the [ADR index](adr/README.md).
