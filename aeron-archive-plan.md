# Aeron Archive WAL + clean speed engine + QuestDB tape

> **Handoff/implementation plan.** Replace the Kafka durability path with an Aeron Archive WAL,
> rewrite the speed engine for readability (same fixed-point-long, zero-alloc-hot-path concepts,
> better organized), and add QuestDB as the high-rate fill tape while Postgres keeps account state.
> **Goal: > 1,000,000 orders/sec matched AND durably recorded, on a single box, with full event
> sourcing (replay + snapshot from the Archive).**
>
> Branch: `feat/aeron-archive-speed` (off `perf/throughput-phase1`, NOT master — master has no
> speed engine to use as a parity oracle).
>
> A fresh agent reads **Decisions** then executes from **Phase A**.

---

## Decisions (locked with user, 2026-06-14)

- **Aeron depth = Archive WAL, done fully.** Tier 1 complete end-to-end (SBE + Aeron Archive durable
  WAL + replay + snapshot + QuestDB tape). **No Aeron Cluster / no Raft** this round — Cluster is a
  later stretch, explicitly out of scope here.
- **Transport = IPC single-box** (shared memory). Fastest, simplest, quickest path to a > 1M bench.
  Networked UDP deferred.
- **Replace the speed engine.** Same concepts (fixed-point longs, zero-alloc hot path) but the 41 KB
  `SpeedMatchingService` monolith is split into small, single-responsibility classes. New engine MAY
  keep the name `speed`. Old engine kept temporarily as a **parity oracle**, deleted only after the
  new one passes parity.
- **DB split.** **Postgres + jOOQ keeps account/position/margin state** (low-rate, transactional,
  correctness home). **QuestDB takes the fill tape** (engine-rate, millions rows/sec, SQL-queryable
  for UI tape + history + analytics). Both are **projections of the Archive** — Archive is the single
  source of truth; either DB is rebuildable by replay.
- **Kafka is removed** from the new engine's path (producer, consumers, topic config, `trade_events`
  as the hot-path WAL, Jackson JSON on the fill path).

## Invariants that MUST hold (unchanged from the engine's contract)

1. **Engine authoritative.** DB/QuestDB are projections; engine rebuilt only from the durable Archive
   (+ resting orders), never from a stale DB.
2. **Append-only log = single source of truth.** Now the **Aeron Archive recording**. The recording
   **position is the engine-stamped monotonic sequence** the old Phase-4 plan was missing — so the
   snapshot cut is exact (closes the enqueue-lag double-count window noted in ADR 0006).
3. **Conservation:** `cash == deposits + Σ realized PnL`. Per-account cash applied in order;
   parallelism allowed across accounts, ordered within an account.
4. **Dedup idempotency** on `tradeId:side` / `tradeId:FEE` (carry into QuestDB + Postgres sinks).
5. **Warm-restart replay** reproduces engine state exactly: load latest snapshot → replay Archive tail
   from the snapshot's recording position.

---

## Target architecture

```
REST / SIMULATE / FIX
      │ submit(Order)                          (TradingEngine.submit — unchanged interface)
      ▼
  SpeedEngine            single-writer, fixed-point longs, zero-alloc hot path
      │ encode fill → SBE flyweight over an off-heap buffer (no objects, no JSON)
      ▼
  WalPublisher           Aeron IPC publication → recorded by Aeron Archive
      ▼
  Aeron Archive (NVMe)   THE durable WAL. Append-only. Position-addressable replay.
      │
      ├──► Projector (Archive subscription: live tail + replay catch-up)
      │        ├─ Postgres (jOOQ)  account_transaction / position / margin   — low rate, transactional
      │        └─ QuestDB (ILP)    fill tape                                  — engine rate, millions/s
      │
      ├──► WS / read-model / audit subscribers (lag-tolerant)
      └──► Snapshotter → periodic engine snapshot tagged with Archive position

  Restart = load latest snapshot → replay Archive from its position (bounded).
  Shedding sits ONLY in front of the Aeron publication buffer → OVERLOADED ≈ 0.
```

---

## Dependencies (pom.xml — verify latest stable before pinning)

- `io.aeron:aeron-all` (or split `aeron-client` + `aeron-driver` + `aeron-archive`) — Aeron incl.
  Archive. Same author family as the existing Agrona 1.20 / Disruptor 4.0 / affinity 3.23.
- `org.real-logic:sbe-tool` + `sbe-all` — SBE codec generator (build plugin) + runtime.
  Generated codecs land in a generated-sources dir (mirror the existing `add-jooq-sources` step).
- `org.questdb:questdb` — QuestDB Java client (ILP `Sender`) for the tape.
- Aeron Media Driver: embedded `MediaDriver` for single-box IPC (no external process for dev/test).

> SBE schema XML versioned in `src/main/resources/sbe/`. Codec regen on schema change, same model as
> jOOQ codegen already in this repo.

---

## Module layout — the readability rewrite

New package `com.fxoee.engine.speed` (rewrite in place; old classes move to `…speed.legacy` during
the parity window, then deleted). Hot-path rule made physical: **longs + SBE flyweights only on the
engine/publish thread; every object-shaped concern lives on the projector side.**

| Class | Responsibility | Thread / allocation |
|-------|----------------|---------------------|
| `SpeedEngine` | pure matching loop, fixed-point longs, no IO | hot path, zero-alloc |
| `SpeedBook` | per-pair order book (keep, already split) | hot path, zero-alloc |
| `SpeedPositions` | FIFO lot netting, realized PnL | hot path, zero-alloc |
| `SpeedLedger` | cash + locked margin | hot path, zero-alloc |
| `Fixed` | fixed-point long math (keep) | — |
| `FillCodec` | encode/decode a fill via SBE flyweight | hot path encode, zero-alloc |
| `WalPublisher` | fill buffer → Aeron publication (offer/backpressure) | hot path, zero-alloc |
| `SpeedMatchingService` | thin `TradingEngine` adapter: validate → risk gate → engine → publish; bench hooks | hot path orchestration |
| `Projector` | Archive subscription → Postgres + QuestDB | OFF hot path, may allocate |
| `SnapshotStore` | snapshot at Archive position; replay tail | startup / periodic |

`SpeedMatchingService` stays the `TradingEngine` implementation (same public surface:
`submit/cancel/closeNet/closeLot/forceFlat/restore/replayFill/snapshot/…` already defined in
`TradingEngine`) so REST/FIX/SIMULATE callers and tests are untouched — only the internals and the
durability tail change.

---

## SBE message shapes (first cut — refine in Phase A)

- **FillRecord** (engine → WAL): `engineSeq` (monotonic), `tradeId`, `pair`, `price` (long fixed),
  `qty` (long fixed), `buyAccountId`, `sellAccountId`, per-side `cashDelta`, `pnl`, taker/house fee,
  lot events (open/partial/full-close w/ engine lot ids), timestamp. **Whole-trade record** (replay
  needs the whole thing — do NOT split into per-account legs in the WAL; legs were a Kafka-partition
  workaround and broke replay reassembly).
- **OrderCommand** (optional, ingress audit): only if we later record the input command stream too.
  Not required for Tier 1 — engine output (FillRecord) is the event-sourcing log.

Keep the schema small and flat; SBE is positional. Version the schema; never reorder fields.

---

## Phased roadmap

Each phase independently shippable + testable; parity-gated against the **legacy speed engine**.

### Phase A — SBE foundation
- Add SBE build plugin + runtime dep; author `FillRecord` schema; generate codecs.
- `FillCodec`: encode a fill into / decode from an off-heap buffer. Round-trip unit test.
- Allocation probe (reuse `SpeedSubmitAllocProbe` pattern): confirm encode is zero-alloc.
- **No engine wiring yet.** Pure codec + test. Ship.

### Phase B — Clean engine rewrite (no Aeron yet)
- Split `SpeedMatchingService` monolith into the table above; behaviour identical to legacy.
- New engine behind a flag (e.g. `fxoee.engine.mode=speed2` during the parity window) so legacy
  still runs as the oracle. _(Done: parity held; in Phase F the legacy `com.fxoee.engine.speed`
  package was deleted and the new engine took over the `speed` package and `fxoee.engine.mode=speed`.)_
- **Parity harness:** drive identical order streams through legacy + new, assert identical fills,
  cash, PnL, lots, conservation. This is the correctness gate for the whole effort.
- Run `SpeedMatchingBenchmark` against the new engine — confirm no matching-throughput regression
  (target unchanged: 660k crossing / 1.5M resting).
- Ship once parity holds. Still publishing via the old FillQueue/PersistenceWorker.

### Phase C — Aeron Archive WAL (the core swap)
- Embedded `MediaDriver` (IPC) + `AeronArchive` lifecycle (Spring bean, start/stop).
- `WalPublisher`: engine fills → Aeron IPC publication, recorded by Archive. Backpressure on
  `offer()` is the new shedding point (replaces FillQueue high-water).
- `Projector`: Archive **subscription** drains the recorded stream (live tail) and applies to sinks;
  on startup, **replay** from last snapshot position to catch up.
- **Retire** `trade_events`-as-WAL, Kafka producer/consumers, `PersistenceWorker` JSON path,
  `KafkaTopicConfig`, `OrderEventProducer`, `FillConsumer`/`AccountFill` legs.
- Solves the deferred Phase-4 step-d: **recording position = engine-stamped seq** → exact snapshot
  cut, no opportunistic guard.
- Verify: conservation + warm-restart replay over the Archive (kill mid-run, replay tail, re-assert).

### Phase D — QuestDB tape + Postgres account projection
- `Projector` writes the fill tape to **QuestDB via ILP** (WAL-mode table, millions rows/s) and the
  account/position/margin rows to **Postgres** (jOOQ, batched COPY / multi-row insert).
- Dedup carried into both sinks (`tradeId:side` / `tradeId:FEE`).
- Read-layer split: UI trade tape + history/analytics → QuestDB; account/balance/position reads →
  Postgres (or the authoritative engine). Audit every current reader and route it.
- Verify: at sustained > 1M engine rate, engine accept ≈ tape apply, bounded non-growing lag; stop
  input → lag drains to zero; conservation holds per account and globally.

### Phase E — Snapshot + bounded restart
- `Snapshotter`: periodic engine snapshot (balances + positions + resting orders) tagged with the
  Archive recording position it covers.
- `SnapshotStore.recoverFromLog`: load latest snapshot → replay Archive from that position only
  (bounded restart, replaces full-log replay).
- Persisted global lot-seq high-water so a restore cannot re-issue a closed lot id (the second ADR
  0006 finding — now trivially correct because the Archive position is exact).
- Verify: kill mid-run, restart, confirm only the tail since the last snapshot replays; conservation
  re-asserts.

### Phase F — Cleanup
- Delete `…speed.legacy`, the old FillQueue/PersistenceWorker, Kafka deps + config, dead Grafana
  kafka rows. Make `speed` the engine. Update docs (`speed-engine.md`, `05-event-sourcing-…`,
  `10-configuration`) + ADR 0007. Run `graphify update .`.

---

## Testing strategy ("readable, extremely quick, that I can test")

1. **Parity oracle (Phase B, the load-bearing gate):** legacy vs new engine, identical input →
   identical output. Property/randomized order streams + the existing `Speed*Test` suites pointed at
   both.
2. **JMH throughput (`SpeedMatchingBenchmark`):** matching rate unchanged after the rewrite; new
   bench mode measuring **end-to-end submit→Archive-recorded** rate proving > 1M/s on the dev box.
3. **Allocation probes (`SpeedSubmitAllocProbe` pattern):** hot path + SBE encode + WAL offer stay
   zero-alloc (ThreadMXBean B/op).
4. **Functional E2E:** embedded MediaDriver + Archive + embedded QuestDB + Testcontainers Postgres —
   submit orders, assert fills in QuestDB, account state in Postgres, conservation, then kill+replay.
5. **Conservation gate** (reuse the existing one): `Σ cash == Σ deposits + Σ realized PnL`;
   `count(distinct trade_id, side) == count(fill rows)` (no double-apply, no dropped fills).

Single-box IPC + embedded driver means the whole stack runs in one JVM for tests → fast feedback,
no external broker/cluster to stand up.

---

## Risks / open items

- **Migration blast radius:** the 6 `Speed*Test` files (~3 k lines) + integration tests reference the
  current engine internals; the rewrite rewrites those. Biggest effort sink — budget for it.
- **Two query DBs:** UI/audit now hit Postgres *and* QuestDB. Needs a clean read-layer split or it
  leaks. Audit every `account_balance` / tape reader before cutover.
- **Aeron Archive ops:** single-box IPC is simple; replication/multi-node is DIY vs Kafka ISR —
  explicitly deferred (out of scope, fine for now).
- **SBE schema discipline:** versioned, append-only field evolution; codec regen in the build.
- **Backpressure semantics change:** shedding moves from FillQueue high-water to Aeron publication
  `offer()` backpressure — confirm the engine sheds upstream cleanly (returns OVERLOADED) rather than
  blocking the single-writer thread.
- **Postgres still can't take 1M rows/s synchronously** — by design it lags and batches; the 1M lands
  in QuestDB + the Archive. Postgres account writes are per-account (low rate), so this is fine.

## Reused building blocks

- `TradingEngine` interface — the stable seam; new engine implements it unchanged.
- `Fixed`, `SpeedBook`, `SpeedPositions`, `SpeedLedger` — fixed-point + matching primitives carry over.
- `SpeedMatchingBenchmark` / `SpeedSubmitAllocProbe` — throughput + alloc harness to extend.
- `AccountBootstrapper` / `SnapshotStore` / `EngineSnapshotter` — recovery scaffolding to evolve onto
  Archive positions.
- `restore(...)` / `replayFill(...)` on `TradingEngine` — already correct (ADR 0006 steps a–c); reused
  for Archive replay.
