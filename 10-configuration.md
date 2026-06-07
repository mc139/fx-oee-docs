# 10 — Configuration reference

_Last updated: 2026-06-07 BST._

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
| `fxoee.recovery.replay-on-startup` | `FXOEE_RECOVERY_REPLAY_ON_STARTUP` | `false` | ✅ — when true, `AccountBootstrapper` rebuilds the engine from `trade_events` (warm restart) instead of wiping to a fresh 10M balance. Always `true` in k8s ([configmap](../k8s/backend/configmap.yaml)); `false` in local dev — `deploy-all.sh` wipes the Postgres PVC each run, so an empty log makes warm restart a no-op anyway. See [doc 05](05-event-sourcing-persistence.md#warm-restart-recovery-engine-replay). |
| `fxoee.mock-market.enabled` | `MOCK_MARKET_ENABLED` | `false` | ✅ — `MockMarketMaker` injects house bid/ask depth every 500 ms using an OU+GARCH price model |
| `fxoee.mock-market.interval-ms` | — | `500` | ✅ — tick interval for mock quotes |
| `fxoee.mock-market.quantity` | — | `1000000` | ✅ — house order size per quote |

## Tiingo live feed

Real FX data via Tiingo REST (OHLC history) + WebSocket (live quotes). See [market-data.md](market-data.md) for full details.

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fxoee.tiingo.enabled` | `TIINGO_ENABLED` | `false` | ✅ — activates `TiingoMarketDataService` |
| `fxoee.tiingo.api-key` | `TIINGO_API_KEY` | _(required when enabled)_ | ✅ — **secret**, goes in `backend-secret`, never in configmap |
| `fxoee.tiingo.history-days` | `TIINGO_HISTORY_DAYS` | `7` | ✅ — days of OHLC history loaded per timeframe on startup |
| `fxoee.tiingo.threshold-level` | `TIINGO_THRESHOLD_LEVEL` | `5` | ✅ — WebSocket throttle (ms between price updates; 0 = every tick) |
| `fxoee.tiingo.quantity` | `TIINGO_QUANTITY` | `1000000` | ✅ — house order size injected per live quote |

Both `tiingo` and `mock-market` can run simultaneously. Typical production setup:
`TIINGO_ENABLED=true` + `MOCK_MARKET_ENABLED=true` (mock provides weekend / fallback depth).

## Server, auth, metrics

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `server.port` | `SERVER_PORT` | `8080` | ✅ |
| `jwt.secret` | `JWT_SECRET` | dev placeholder | ✅ — **override in any real deployment** (min 32 chars, HS256) |
| `jwt.expiry-days` | — | `7` | ✅ |
| `management.endpoints…exposure` | — | `health, info, prometheus, metrics` | ✅ — Actuator + Micrometer Prometheus |

> 🔐 The default `jwt.secret` is a known dev string in `application.yml`. It **must** be supplied via
> the `backend-secret` Secret (k8s) or env (compose) in any non-local environment.

## Pre-trade risk & circuit breaker (enforced)

The `fx.risk.*` and `fx.circuit-breaker.*` keys **are** read and enforced — this was scaffolding in
earlier revisions but is now live. See [11 — Pre-trade risk controls](11-risk-controls.md) for the
full gate; the circuit breaker is in [circuit-breaker.md](circuit-breaker.md).

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fx.risk.killswitch` | `RISK_KILLSWITCH` | `false` | ✅ enforced — when true, no new orders are admitted |
| `fx.risk.max-position` | `RISK_MAX_POSITION` | `10000000` | ✅ enforced — per-account \|net qty\| per pair (0 disables) |
| `fx.risk.max-order-notional` | `RISK_MAX_ORDER_NOTIONAL` | `5000000` | ✅ enforced — per-order USD notional cap (0 disables) |
| `fx.risk.max-gross-exposure` | `RISK_MAX_GROSS_EXPOSURE` | `0` | ✅ enforced — per-account gross margin USD (0 = disabled) |
| `fx.circuit-breaker.price-deviation-threshold` | `CIRCUIT_BREAKER_PRICE_DEVIATION_THRESHOLD` | `0.005` | ✅ enforced — halts a pair on a price jump beyond this; the risk gate then refuses orders on HALTED pairs |

All `fx.risk.*` values are **runtime-mutable** via `PUT /api/risk/limits` / the DEBUG panel RISK tab —
the `application.yml` values are only the startup seed.

### Still scaffolding

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fx.orderbook.snapshot-depth` | — | `10` | ⚠️ not read — snapshot depth is passed explicitly by callers |

The funds/structural gate is `PreTradeValidator` + margin reservation in the
[submit pipeline](03-engine-core.md#the-submit-pipeline) — quantity/lot/tick checks and the
whole-order funds check; the risk gate (above) runs just before it, pre-lock.

## Planned / aspirational (see ADRs)

| Capability | State | Reference |
|------------|-------|-----------|
| LMAX Disruptor fill hand-off | 🔜 planned — dependency present, interim `FillQueue` in use | [ADR 0004](adr/0004-async-fill-queue-over-disruptor.md) |
| FIX protocol gateway | 🔜 named in the project description; **no implementation** in `src/main/java` | — |

These are listed for honesty: the project description / `pom.xml` mention them, but the code does not
implement them yet.
