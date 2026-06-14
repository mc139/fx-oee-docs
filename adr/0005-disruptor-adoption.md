# ADR 0005 - LMAX Disruptor for the command ring and the fill hand-off

_Last updated: 2026-06-13 BST._

**Status:** Accepted · **supersedes [ADR 0004](0004-async-fill-queue-over-disruptor.md)**

## Context

[ADR 0004](0004-async-fill-queue-over-disruptor.md) shipped the async fill hand-off as an interim
`ConcurrentLinkedQueue` (`FillQueue`) and recorded the LMAX Disruptor (`com.lmax:disruptor:4.0.0`,
already on the classpath) as the planned target. Two things changed since then.

First, the interim queue carried known costs: `ConcurrentLinkedQueue` allocates a node per offer
(steady GC pressure under load) and is unbounded by design, so the heap bound lived entirely in the
upstream `isOverloaded` shedding check rather than in the queue's structure.

Second, the **speed engine** ([`SpeedEngine`](../../src/main/java/com/fxoee/engine/speed/SpeedEngine.java),
`fxoee.engine.mode=speed`) needs a command transport into its single-writer matching thread that is
bounded, pre-allocated, and allocation-free on the hot path. A single-writer engine only stays fast
if commands reach it without locks, queue-node churn, or unbounded buffering, and if back-pressure is
structural rather than advisory. That is exactly what a Disruptor ring buffer provides: pre-allocated
slots, single-writer sequencing on the consumer, cache-line padding, and a configurable wait
strategy.

With the speed engine making the Disruptor a first-class dependency anyway, the case for also moving
the fill hand-off onto it (the [ADR 0004](0004-async-fill-queue-over-disruptor.md) follow-up)
became straightforward.

## Decision

Adopt the **LMAX Disruptor 4.0.0** in **two** places, while keeping a dependency-light fallback:

1. **Speed-engine command ring.** [`SpeedEngine`](../../src/main/java/com/fxoee/engine/speed/SpeedEngine.java)
   accepts commands through a multi-producer `Disruptor<EngineSlot>` ring
   ([`EngineSlot`](../../src/main/java/com/fxoee/engine/speed/EngineSlot.java) carries one command as
   pre-allocated `long` fields). Submitter threads claim a slot, write longs, and publish; the single
   engine thread consumes and matches with no book or account locks. The wait strategy is configurable
   (`busy-spin` default, `yielding`, `blocking`); the engine thread can be pinned to a core
   (`fxoee.engine.speed.cpu`). Results travel back in a caller-owned buffer, not the ring slot.

2. **Fill hand-off queue.** [`DisruptorFillQueue`](../../src/main/java/com/fxoee/engine/DisruptorFillQueue.java)
   (active when `fxoee.queue.type=disruptor`) replaces the interim queue's internals with a
   multi-producer ring of pre-allocated `FillEvent` slots, drained pull-based by the single
   `PersistenceWorker` thread via an `EventPoller`. It honours the contract [ADR 0004](0004-async-fill-queue-over-disruptor.md)
   pinned down: wait-free-ish produce (the ring only spins when full, and `isOverloaded` at
   `HIGH_WATER = 50_000` sheds load long before that), batched drain (≤512), and re-enqueue on a
   downstream failure. Ring capacity is a power of two (`fxoee.disruptor.ring-buffer-size`, default
   131072; `performance.properties` sets 1048576).

**Fallback retained.** [`DefaultFillQueue`](../../src/main/java/com/fxoee/engine/DefaultFillQueue.java)
(`fxoee.queue.type=default`, the `matchIfMissing` choice) keeps the `ConcurrentLinkedQueue`
implementation behind the same `FillQueue` interface, so a deployment can run without the Disruptor
hand-off. The branch default and the k8s configmap select `disruptor` + `speed`.

## Consequences

**Positive**
- **Bounded back-pressure is structural.** The ring's fixed capacity bounds the heap by construction;
  the upstream shedding check now guards a hard ceiling instead of an unbounded buffer.
- **Pre-allocation, lower GC.** Slots are allocated once at startup, so neither the command path nor
  the fill path allocates a queue node per item under load.
- **Single-writer determinism.** The speed engine matches on one thread with no locks and no ABBA
  hazard; sequencing is the Disruptor's job, not the application's.
- **One transport, two uses.** The same library and mechanical-sympathy model back both the engine
  command ring and the fill hand-off, so there is one wait-strategy / sizing mental model to learn.

**Negative / accepted trade-offs**
- **Added hard dependency.** The Disruptor is now load-bearing (speed engine), not just an optional
  hand-off; it can no longer be dropped from the classpath.
- **Power-of-two sizing.** Ring capacities must be powers of two and sized to peak in-flight volume;
  an undersized ring makes producers spin, which on the matching hot path must never happen (hence
  the deliberately low `HIGH_WATER`).
- **Busy-spin CPU cost.** The default speed-engine wait strategy keeps the engine thread hot on a
  core even when idle; it trades a core for the lowest jitter. The fill-queue path uses a yielding
  strategy and pull-based draining to avoid burning a core there.
- **More moving parts.** Wait strategies, gating sequences, event-poller draining, and core pinning
  are concepts a reader must hold to reason about the hot path, versus a plain queue.
