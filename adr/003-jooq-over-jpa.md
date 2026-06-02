# ADR 003 — jOOQ over JPA on the Persistence Path

**Status:** Accepted  
**Date:** 2026-06-02

## Context

Fills arrive on a Kafka consumer (`FillConsumer`) that must persist account balances, transaction rows, and position lots. Under burst load this path can see **hundreds of events per batch**. ORM overhead (entity hydration, flush ordering, N+1 queries) adds latency and unpredictable SQL.

The matching hot path never touches the database — only the async projection does. Still, consumer lag directly affects how stale the UI and audit trail appear.

## Decision

Use **jOOQ** (`DSLContext`) for all persistence repositories. No Spring Data JPA.

Key repositories:

| Class | Role |
|-------|------|
| `FillBatchRepository` | Batched multi-row INSERT/UPDATE per Kafka batch |
| `TradeEventRepository` | Append-only `trade_events` log |
| `PositionLotRepository` | Lot open/close lifecycle |
| `CustomerAccountRepository` | Account balance reads/writes |

Patterns in use:

- **Explicit SQL** via `table()` / `field()` constants — no generated codegen yet; schema matches Flyway migrations.
- **`@Transactional` per consumer batch** — atomic apply of all fills in one batch.
- **`ON CONFLICT DO NOTHING`** on event ids — idempotent replay after worker or broker failure.
- **Dedup keys** (`tradeId:side`) at consumer level for at-least-once Kafka delivery.

JPA is intentionally absent from `pom.xml`.

## Consequences

**Positive**

- Full control over batch shape: one round-trip for N fills instead of N entity saves.
- SQL is readable in code review; no surprise lazy loads or session flush during consumer processing.
- Natural fit for append-only event log tables and upsert-heavy projections.

**Negative**

- No automatic schema ↔ object mapping; table/column renames require manual repository updates.
- jOOQ codegen (Phase 10 todo) not yet wired — repositories use string table names today.
- More boilerplate than JPA for simple CRUD (acceptable — hot repositories are batch-oriented, not CRUD-heavy).

**When JPA would be reconsidered**

- Admin/back-office modules with complex object graphs and no latency requirement.
- Never on the fill consumer path.
