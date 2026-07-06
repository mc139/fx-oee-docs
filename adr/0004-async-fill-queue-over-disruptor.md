# ADR 0004 - Async fill hand-off: interim `ConcurrentLinkedQueue`, LMAX Disruptor planned

_Last updated: 2026-06-20 BST._

**Status:** Accepted (interim implementation) · **superseded by [ADR 0005](0005-disruptor-adoption.md)**

> **Historical record.** This ADR captured the interim async hand-off (a `ConcurrentLinkedQueue`)
> and named the LMAX Disruptor (`com.lmax:disruptor:4.0.0`, declared in `pom.xml`) as
> the planned target. The migration has since landed: [ADR 0005](0005-disruptor-adoption.md) adopts
> the Disruptor for both the speed-engine command ring and (as
> `DisruptorFillQueue`) the fill
> hand-off, and supersedes this decision. The `ConcurrentLinkedQueue` variant survives as
> `DefaultFillQueue`
> (`fxoee.queue.type=clq`); the boot default is now the unbounded
> `AgronaFillQueue`. Read this only for
> the original interim context.

## Context

The matching thread must not block on Kafka. `kafkaTemplate.send()` can stall for up to `max.block.ms`
(default 60s) when the producer buffer is full, and holding a pair's book lock during that window
would freeze the entire hot path for that pair. So fills are **handed off** the matching thread to a
worker that owns the durable write + Kafka publish.

The end-goal mechanism is the **LMAX Disruptor**: a pre-allocated ring buffer with mechanical
sympathy, single-writer semantics, cache-line padding, and very low, allocation-free latency. It's
the standard choice for an exchange-grade hand-off. Adopting it well means designing the event-claim/
publish lifecycle, wait strategy, and ring sizing deliberately, which is a focused piece of work in
its own right.

To unblock the rest of the system (durable log, projections, back-pressure) before that work lands, an
**interim** hand-off was needed that is correct, simple, and easy to reason about.

## Decision

**Now (interim):** implement the hand-off as `FillQueue`,
an unbounded `ConcurrentLinkedQueue<PendingFill>` with an `AtomicInteger` depth counter:

- `enqueue` is a wait-free `offer` from the matching thread; never blocks, never rejects.
- `isOverloaded()` returns true at `HIGH_WATER = 50_000`; `MatchingService.submit` checks it first and
  **sheds load upstream** (rejects with `OVERLOADED`) *before* mutating engine state, bounding heap
  instead of growing to OOM.
- `drain(max)` hands `PersistenceWorker` a batch (≤512); drained items are owned by the caller, so a
  downstream failure is recovered by re-`enqueue`.

This keeps the matching hot path free of Kafka latency today and pins down the contract the eventual
ring-buffer implementation must honour (wait-free produce, batched drain, re-enqueue on failure,
upstream load-shedding).

**Planned (target):** replace the queue internals with an **LMAX Disruptor** ring buffer once the
hand-off is shown to be hot enough to justify it. The dependency is already on the classpath. The
`FillQueue` / `PendingFill` boundary is intentionally narrow so the swap stays behind that interface.

## Consequences

**Positive (interim)**
- Minimal and dependency-light; a perfect fit for batched drain + re-enqueue-on-failure.
- Wait-free offer keeps the matching hot path off the Kafka round-trip (the original goal).
- Load-shedding bounds memory predictably and is testable without a broker.
- Measured throughput (~218k `submit`/sec) shows the hand-off is not the bottleneck at current scale,
  so the Disruptor migration is a performance-headroom investment, not a current necessity.

**Negative / accepted trade-offs (until the migration lands)**
- `ConcurrentLinkedQueue` allocates a node per offer (GC pressure) and lacks the Disruptor's cache-
  line padding / mechanical sympathy.
- Unbounded by design (bounded only by the upstream `isOverloaded` check): the shedding logic, not
  the queue, guarantees the heap bound. A `TODO` in `FillQueue` notes wiring producer wake-ups once
  `enqueue` is bounded; the Disruptor's fixed ring would make that bound structural.

## Follow-up

_Done._ The Disruptor ring buffer was wired behind the `FillQueue` interface as
`DisruptorFillQueue` (event type still
`FillEvent`/`PendingFill`, batched-drain + re-enqueue contract preserved). See
[ADR 0005](0005-disruptor-adoption.md) for the decision that closed this out.
