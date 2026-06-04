# 10 — Configuration reference

_Last updated: 2026-06-04 21:57 BST._

Every runtime knob is an environment variable resolved by [application.yml](../src/main/resources/application.yml).
In Minikube they're set by [k8s/backend/configmap.yaml](../k8s/backend/configmap.yaml) +
[secret.yaml](../k8s/backend/secret.yaml); in compose by the `environment:` blocks.

**Status legend:** ✅ wired and enforced · ⚠️ defined but **not read by any code yet** (scaffolding) ·
🔜 planned (see ADRs).

## Database

| Key (YAML) | Env var | Default | Status |
|------------|---------|---------|--------|
| `spring.datasource.url` host | `DB_HOST` | `localhost` | ✅ |
| — port | `DB_PORT` | `5432` | ✅ |
| — db | `DB_NAME` | `fxoee` | ✅ |
| `spring.datasource.username` | `DB_USER` | `fxoee` | ✅ |
| `spring.datasource.password` | `DB_PASSWORD` | `fxoee` | ✅ |
| `…hikari.maximum-pool-size` | `DB_POOL_MAX` | `30` | ✅ — sized for FillConsumer (7 threads) + worker + bootstrap + REST |
| `…hikari.minimum-idle` | `DB_POOL_MIN_IDLE` | `5` | ✅ |

Flyway runs migrations on boot (`spring.flyway.enabled=true`, `classpath:db/migration`).

## Kafka

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | ✅ |
| `spring.kafka.consumer.group-id` | `KAFKA_CONSUMER_GROUP_ID` | `fx-oee-group` | ✅ |
| `spring.kafka.consumer.auto-offset-reset` | — | `latest` | ✅ |
| `kafka.enabled` | `KAFKA_ENABLED` | `true` | ✅ — gates producer, `FillQueue`, `PersistenceWorker`, consumers ([doc 05](05-event-sourcing-persistence.md)) |

Producer is fixed to `acks=all`, `enable.idempotence=true`, `retries=5`, `request.timeout.ms=5000`,
`delivery.timeout.ms=15000` — the exactly-once-ish delivery contract the projection relies on.

With `kafka.enabled=false` the engine runs **fully in-memory**: the producer / queue / worker /
consumer beans are absent and all publish calls are no-ops. The engine itself is unaffected.

## Engine & funding

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fxoee.funding.mode` | — | `FULL_NOTIONAL` | ✅ — `MARGIN` (leveraged) vs `FULL_NOTIONAL` ([doc 04](04-funding-pnl-conservation.md)) |
| `fxoee.engine.authoritative` | — | `true` | ✅ — WebSocket + debug APIs read in-JVM `MatchingService` state |
| `fxoee.mock-market.enabled` | — | `false` | ✅ — `MockMarketMaker` injects house bid/ask depth every 500ms |

## Server, auth, metrics

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `server.port` | `SERVER_PORT` | `8080` | ✅ |
| `jwt.secret` | `JWT_SECRET` | dev placeholder | ✅ — **override in any real deployment** (min 32 chars, HS256) |
| `jwt.expiry-days` | — | `7` | ✅ |
| `management.endpoints…exposure` | — | `health, info, prometheus, metrics` | ✅ — Actuator + Micrometer Prometheus |

> 🔐 The default `jwt.secret` is a known dev string in `application.yml`. It **must** be supplied via
> the `backend-secret` Secret (k8s) or env (compose) in any non-local environment.

## Defined but NOT enforced (scaffolding)

These keys exist in `application.yml` — and `RISK_*` / `CIRCUIT_BREAKER_*` are even set in the k8s
ConfigMap — but **no code reads them** (verified: no `fx.risk`, `fx.circuit`, or `fx.orderbook`
references in `src/main/java`). They are placeholders for unimplemented features. Documented here so
nobody assumes a guarantee that isn't there.

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fx.risk.max-position` | `RISK_MAX_POSITION` | `10000000` | ⚠️ not enforced — no position-limit check exists |
| `fx.risk.max-order-notional` | `RISK_MAX_ORDER_NOTIONAL` | `5000000` | ⚠️ not enforced — no per-order notional cap exists |
| `fx.circuit-breaker.price-deviation-threshold` | `CIRCUIT_BREAKER_PRICE_DEVIATION_THRESHOLD` | `0.005` | ⚠️ not enforced — no circuit breaker exists |
| `fx.orderbook.snapshot-depth` | — | `10` | ⚠️ not read — snapshot depth is passed explicitly by callers |

The only funds/structural gate that actually runs is the `PreTradeValidator` + margin reservation in
the [submit pipeline](03-engine-core.md#the-submit-pipeline) — quantity/lot/tick checks and the
whole-order funds check. Pre-trade **risk limits** (max position, max notional) and a **price circuit
breaker** are future work, not current behaviour.

## Planned / aspirational (see ADRs)

| Capability | State | Reference |
|------------|-------|-----------|
| LMAX Disruptor fill hand-off | 🔜 planned — dependency present, interim `FillQueue` in use | [ADR 0004](adr/0004-async-fill-queue-over-disruptor.md) |
| FIX protocol gateway | 🔜 named in the project description; **no implementation** in `src/main/java` | — |

These are listed for honesty: the project description / `pom.xml` mention them, but the code does not
implement them yet.
