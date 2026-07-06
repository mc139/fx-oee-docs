# ADR 0003 - jOOQ over JPA/Hibernate

_Last updated: 2026-06-20 BST._

**Status:** Accepted

## Context

The persistence layer (`com.fxoee.persistence`) is a
**projection writer**, not a domain model. Its job is high-volume, batched writes of engine-computed
effects: appending the running cash balance as an `account_transaction` row, inserting/updating/closing
`position_lot` rows, appending to `trade_events`. The shape is known, the SQL is hot, and the work is
set-based.

JPA/Hibernate optimizes for a different problem: managing an object graph with a persistence context,
dirty checking, lazy loading, and cascades. Against this projection workload those features are
liabilities. The L1 cache and dirty-checking add overhead and surprise on batched writes, generated
SQL is hard to predict, and there is no compile-time guarantee the queries match the schema.

## Decision

Use **jOOQ** with **code generation from the migrated schema**. The build
(`pom.xml`) spins up a throwaway PostgreSQL (Testcontainers/Docker on port 5433),
applies the Flyway migrations, then runs `jooq-codegen` to generate typed table/record classes into
`src/generated/jooq`. Repositories write explicit, type-safe SQL through those generated classes
(`spring-boot-starter-jooq`).

Schema is owned by **Flyway migrations** (`V1..V14`), and jOOQ generates *from the migrated schema*,
so the generated code and the runtime schema are the same artifact by construction. (One table,
`wal_projection_offset` in V14, is added after codegen and accessed via plain-SQL jOOQ, so it has no
generated class.)

## Consequences

**Positive**
- SQL is explicit and predictable: no hidden N+1s, no flush-timing surprises; what you write is what
  runs. Ideal for the batched `FillBatchRepository` writes on the projection path.
- **Compile-time safety**: a column rename in a migration breaks the build at the generated types, not
  at runtime; it's caught the moment codegen reruns.
- Set-based and batched operations are first-class, matching the projection workload.
- Migrations remain the single source of schema truth; codegen derives from them.

**Negative / accepted trade-offs**
- Codegen requires Docker (a throwaway Postgres) to regenerate `src/generated/jooq` after a schema
  change: an extra build step versus annotation-only JPA entities.
- More verbose for trivial CRUD than Spring Data repositories.
- No portable ORM abstraction: the queries are written against PostgreSQL. Acceptable, since Postgres
  is the chosen and only target.
