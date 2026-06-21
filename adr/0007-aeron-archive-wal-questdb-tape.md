# ADR 0007 - Aeron Archive WAL, SBE, clean speed-engine rewrite, QuestDB tape

_Last updated: 2026-06-21 BST._

**Status:** Accepted · **delivered** (Phases A-F shipped; all `fxoee.wal.*` flags off by default) · supersedes the Kafka durability path of [ADR 0006](0006-engine-snapshots-bounded-restart.md) · builds on [ADR 0005](0005-disruptor-adoption.md) · refines [ADR 0002](0002-in-memory-authoritative-engine.md)

> **As shipped (2026-06-20).** The Aeron Archive WAL, SBE `FillRecord`, clean speed-engine rewrite, and
> QuestDB tape are all in `com.fxoee.wal` + `com.fxoee.engine.speed[.wal]`. Two deviations from the plan
> below, both honest improvements: (1) the single `Projector` became **two** beans, `AeronWalProjector`
> (trade tape: `TradeStore` + live WS + the QuestDB sink) and `WalDbProjector` (Postgres account state),
> so the high-rate tape and the low-rate, transactional Postgres lane scale and fail independently; (2)
> the QuestDB ILP transport is **HTTP** (`http::addr=…`), connected lazily. The legacy speed engine was
> deleted after parity (no `.legacy` package remains). Wiring + flags live in
> [05-event-sourcing-persistence.md](../05-event-sourcing-persistence.md) (Lane 2).

## Context

[ADR 0002](0002-in-memory-authoritative-engine.md) fixed the shape: the in-memory engine is the source
of truth, everything else is a projection of a durable log. [ADR 0005](0005-disruptor-adoption.md) added
the single-writer, fixed-point-long speed engine, which matches at ~660k (crossing) - 1.5M (resting)
orders/sec. An earlier throughput-redesign effort (phases 0-3) then made the persistence tail keep up
to ~100-150k/s, and [ADR 0006](0006-engine-snapshots-bounded-restart.md) added bounded warm restart -
but deliberately stopped short of the headline goal because the Kafka path could not reach it cleanly:

- **Kafka caps the durable drain at ~100-150k/s** (single worker + per-batch `acks=all`, JSON
  serialization on the publish path). Live metrics on a real instance showed the engine fine and the
  durable drain as the wall.
- **Jackson JSON on the fill path** allocates and burns CPU on the publish thread.
- **The Phase-4 Kafka-as-WAL plan was blocked** on a missing engine-stamped monotonic sequence (so the
  snapshot cut could be exact) and on the per-account-leg topic not being replayable into ordered trades
  (ADR 0006 _Deferred_).
- The 41 KB `SpeedMatchingService` is a single-file monolith - hard to read and extend.

The user's goal is explicit: **> 1,000,000 orders/sec matched AND durably recorded on a single box,
full event sourcing, code as readable as possible.** Kafka is the one component in an otherwise
mechanical-sympathy stack (Agrona, Disruptor, OpenHFT affinity - all Real Logic / Martin Thompson
lineage) that fights that goal.

## Decision

Replace the Kafka durability path with an **Aeron Archive WAL**, adopt **SBE** for the fill record,
**rewrite the speed engine** for readability (same fixed-point-long, zero-alloc-hot-path concepts), and
split the persistence sink: **Postgres keeps account state, QuestDB takes the fill tape.** Aeron is the
same author family as the Agrona/Disruptor/affinity already in the stack.

Explicitly **in scope = Aeron Archive (Tier 1), done fully**; **out of scope = Aeron Cluster / Raft**
(a later stretch). First transport = **IPC single-box** (shared memory); networked UDP deferred.

### 1. Aeron Archive is the durable WAL

The engine publishes each fill (SBE-encoded) to an Aeron IPC publication recorded by an embedded Aeron
Archive to local NVMe. The recording is the append-only log. Crucially, **the Archive recording position
is the engine-stamped monotonic sequence** that ADR 0006 _Deferred_ identified as the missing
prerequisite - so the snapshot cut becomes exact and the opportunistic enqueue-lag guard (and its
double-count window) is eliminated. `trade_events`-as-WAL, the Kafka producer/consumers, the per-account
`trades.byaccount` legs, and the JSON publish path are all retired.

### 2. SBE for the fill record

A versioned SBE schema (`FillRecord`) replaces Jackson JSON on the fill path: a flyweight over an
off-heap buffer, zero-alloc encode on the hot path, positional and codec-generated (same build-time
codegen model as the existing jOOQ generation). The record is **whole-trade** (engine lot events with
ids, per-side cashDelta/pnl, fees) so replay reconstructs state directly - no per-account leg
reassembly.

### 3. Clean speed-engine rewrite

The `SpeedMatchingService` monolith is split into small single-responsibility classes
(`SpeedEngine` matching loop, `FillCodec`, `WalPublisher`, `Projector`, plus the existing `SpeedBook` /
`SpeedPositions` / `SpeedLedger` / `Fixed`). The hot-path invariant is made **physical**: longs + SBE
flyweights only on the engine/publish thread; every object-shaped concern lives on the projector side.
The engine keeps implementing the unchanged `TradingEngine` interface, so REST/FIX/SIMULATE callers and
the engine-mode toggle are untouched. The old engine is kept as a **parity oracle** during the rewrite
and deleted only after the new one passes identical-input/identical-output parity.

### 4. DB split - Postgres state, QuestDB tape

Both DBs become pure projections of the Archive (rebuildable by replay):

- **Postgres + jOOQ** keeps account/position/margin state - low-rate, per-account, transactional, the
  correctness home. Cannot and need not absorb 1M rows/s.
- **QuestDB** (ILP, WAL-mode) takes the fill tape - engine-rate, millions rows/s, SQL-queryable for the
  UI trade tape, history, and analytics.

Two projectors subscribe to the Archive (as shipped): `AeronWalProjector` follows the live tail into the
trade tape (`TradeStore` + live WS + the QuestDB sink), and `WalDbProjector` does a periodic, off-engine
catch-up into Postgres account state, seeding a bounded backfill from the last snapshot on startup. Dedup
(`(trade_id, leg)` in the `fill_dedup` table, deterministic ids from `WalIds`) is carried into each.

### 5. Snapshot + bounded restart on Archive positions

The snapshot (balances + positions + resting orders) is tagged with the **exact Archive recording
position** it covers; restart loads the latest snapshot and replays only the Archive tail past that
position. Because the position is exact (not an opportunistic cut), the ADR 0006 consistency-cut
fragility is gone, and a persisted global lot-seq high-water prevents a restore from re-issuing a closed
lot id.

```
REST / SIMULATE / FIX → SpeedEngine (longs, zero-alloc) → SBE FillRecord
   → WalPublisher → Aeron IPC publication → Aeron Archive (NVMe) = durable WAL
        ├─► Projector → Postgres (account state)  +  QuestDB (fill tape)
        ├─► WS / read-model / audit subscribers
        └─► Snapshotter → snapshot @ Archive position
   Restart = latest snapshot → replay Archive tail from its position (bounded).
   Shedding = Aeron publication offer() backpressure → OVERLOADED ≈ 0.
```

Plan + phasing: `docs/aeron-archive-plan.md` (phases A-F, parity-gated against the legacy engine).

## Consequences

**Positive**

- **Durable drain tracks the engine.** Archive recording is sequential NVMe writes of an off-heap
  buffer - millions/sec - so > 1M/s matched-and-recorded is reachable on one box. The 1M firehose lands
  in the Archive + QuestDB; Postgres handles only the low-rate account state.
- **Exact snapshot cut.** The Archive position is the engine-stamped sequence ADR 0006 lacked, closing
  the enqueue-lag double-count window and removing the opportunistic-cut guard entirely.
- **Zero-alloc publish.** SBE replaces JSON - no per-fill allocation, no Jackson CPU on the hot path.
- **Readable engine.** The monolith becomes small single-responsibility classes with the hot-path rule
  enforced by physical separation (longs on the engine thread, objects on the projector).
- **Stack coherence.** Aeron is the same Real Logic family as Agrona/Disruptor/affinity already in use;
  removes the lone JVM-broker + Zookeeper + JSON component.

**Negative / accepted trade-offs**

- **Migration blast radius.** The ~6 `Speed*Test` files (~3k lines) and integration tests reference the
  current engine internals and the Kafka path; the rewrite rewrites them. Parity harness is the gate.
- **Two query DBs.** UI/audit now read Postgres *and* QuestDB; needs a clean read-layer split, and every
  current reader of `account_balance` / the tape must be audited and routed before cutover.
- **Less managed durability than Kafka.** Aeron Archive replication is DIY vs Kafka ISR. Single-box IPC
  is simple and sufficient now; multi-node replication is explicitly deferred (out of scope).
- **SBE schema discipline.** Versioned, append-only field evolution, codec regen in the build.
- **Backpressure semantics change.** Shedding moves from `FillQueue` high-water to Aeron publication
  `offer()` backpressure - the engine must shed upstream (return OVERLOADED), never block the
  single-writer thread.
- **Lost Kafka ecosystem.** Consumer-group/partition tooling, retention ops, the kafka-exporter Grafana
  rows, easy multi-language fan-out. Accepted for the throughput + coherence win; fan-out still works via
  multiple Archive subscribers + replay.

## Deferred

- **Aeron Cluster (Raft).** Engine as a replicated state machine with framework snapshots/failover -
  the canonical exchange architecture and the biggest further step. Out of scope here; this ADR's
  Archive foundation is a clean base for it.
- **Networked UDP transport** (multi-process/multi-host) on top of IPC.
- **WAL size bounding** - trimming Archive segments below the minimum live snapshot position (bounded
  *replay* does not bound *size*; same open item carried from ADR 0006).

Back to the [ADR index](README.md).
