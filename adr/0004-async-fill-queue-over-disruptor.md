# ADR 0004 — Plain `ConcurrentLinkedQueue` hand-off over LMAX Disruptor

**Status:** Accepted

> **Note on the codebase:** the LMAX Disruptor (`com.lmax:disruptor:4.0.0`) is still declared in
> [pom.xml](../../pom.xml) and named in the project description, but it is **not used anywhere in
> `src/main/java`**. This ADR records the decision *not* to build the async path on Disruptor and to
> use a `ConcurrentLinkedQueue` instead. The unused dependency should be removed (follow-up).

## Context

The matching thread must not block on Kafka. `kafkaTemplate.send()` can stall for up to `max.block.ms`
(default 60s) when the producer buffer is full, and holding a pair's book lock during that window
would freeze the entire hot path for that pair. So fills need to be **handed off** the matching thread
to a separate worker that owns the durable write + Kafka publish.

The LMAX Disruptor is the classic answer to this — a pre-allocated ring buffer with mechanical-
sympathy, single-writer semantics, and very low latency. But its model is opinionated: it shines when
a **single** producer/consumer topology with a fixed-size ring and busy-spin wait strategy fits the
problem, and it imposes that structure on the code.

Our hand-off is simpler than what Disruptor optimizes for:

- The matching side only needs a **wait-free offer** (~1µs) and to never block.
- The consumer (`PersistenceWorker`) drains in **batches** (≤512), appends to the DB, publishes, and
  **re-enqueues failed batches** — natural with a plain queue, awkward against a ring buffer's
  claim/publish lifecycle.
- Back-pressure is **load-shedding at the door**, not ring saturation: `MatchingService` rejects new
  orders with `OVERLOADED` when the backlog passes a high-water mark, *before* mutating engine state.

## Decision

Implement the hand-off as [`FillQueue`](../../src/main/java/com/fxoee/engine/FillQueue.java) — an
unbounded `ConcurrentLinkedQueue<PendingFill>` with an `AtomicInteger` depth counter:

- `enqueue` is a wait-free `offer` from the matching thread; never blocks, never rejects.
- `isOverloaded()` returns true at `HIGH_WATER = 50_000`; `MatchingService.submit` checks it first and
  sheds load upstream, bounding heap instead of growing to OOM.
- `drain(max)` hands the worker a batch; drained items are owned by the caller, so a downstream
  failure is recovered by re-`enqueue`.

The `PersistenceWorker` is a single thread parking ~1ms when idle.

## Consequences

**Positive**
- Minimal, dependency-light, and a perfect fit for batched drain + re-enqueue-on-failure.
- Wait-free offer keeps the matching hot path free of Kafka latency (the original goal).
- Load-shedding bounds memory predictably and is testable without a broker.
- Measured throughput (~218k `submit`/sec) shows the queue is not the bottleneck at current scale.

**Negative / accepted trade-offs**
- `ConcurrentLinkedQueue` allocates a node per offer (GC pressure) and lacks the Disruptor's cache-
  line padding / mechanical sympathy — if profiling later shows the hand-off is hot, revisit with a
  ring buffer.
- Unbounded by design (bounded only by the upstream `isOverloaded` check) — the shedding logic, not
  the queue, is what guarantees the heap bound. A `TODO` in `FillQueue` notes wiring producer wake-ups
  once `enqueue` is bounded.
- A declared-but-unused Disruptor dependency lingers in the build until removed.
