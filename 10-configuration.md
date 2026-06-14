# 10 - Configuration reference

_Last updated: 2026-06-13 BST._

Every runtime knob is resolved by [application.yml](../src/main/resources/application.yml) or
[performance.properties](../src/main/resources/performance.properties) (imported by application.yml),
either from an environment variable or from a default baked into the file (or, for a few keys, a
`@Value` default in the code). In Minikube they're set by
[k8s/base/backend/configmap.yaml](../k8s/base/backend/configmap.yaml) + `secret.yaml`; in compose by
the `environment:` blocks.

> **Where defaults differ:** several keys carry one default in the YAML / properties file and a
> different one in the code's `@Value` fallback (used only when the key is deleted entirely). Both are
> noted below where they diverge. The k8s configmap also overrides many of these; those overrides are
> called out per row.

**Status legend:** ✅ wired and enforced · ⚠️ defined but **not read by any code** (scaffolding) ·
🔜 planned (see ADRs).

## Database

| Key (YAML) | Env var | Default | Status |
|------------|---------|---------|--------|
| `spring.datasource.url` host | `DB_HOST` | `localhost` | ✅ |
| (same key) port | `DB_PORT` | `5432` | ✅ |
| (same key) db | `DB_NAME` | `fxoee` | ✅ |
| `spring.datasource.username` | `DB_USER` | `fxoee` | ✅ |
| `spring.datasource.password` | `DB_PASSWORD` | `fxoee` | ✅ |
| `…hikari.maximum-pool-size` | `DB_POOL_MAX` | `30` | ✅ sized for FillConsumer (7 threads) + worker + bootstrap + REST |
| `…hikari.minimum-idle` | `DB_POOL_MIN_IDLE` | `5` | ✅ |

Flyway runs migrations on boot (`spring.flyway.enabled=true`, `classpath:db/migration`).

## Kafka

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | ✅ |
| `spring.kafka.consumer.group-id` | `KAFKA_CONSUMER_GROUP_ID` | `fx-oee-group` | ✅ |
| `spring.kafka.consumer.auto-offset-reset` | none | `latest` | ✅ |
| `kafka.enabled` | `KAFKA_ENABLED` | `false` (k8s configmap sets `true`) | ✅ gates producer, `FillQueue`, `PersistenceWorker`, consumers ([doc 05](05-event-sourcing-persistence.md)) |

The producer is fixed to `acks=all`, `enable.idempotence=true`, `retries=5`, `request.timeout.ms=5000`,
`delivery.timeout.ms=15000`. That's the exactly-once-ish delivery contract the projection relies on.

With `kafka.enabled=false` the engine runs **fully in-memory**: the producer / queue / worker /
consumer beans are absent and all publish calls are no-ops. The engine itself is unaffected.

## Engine & funding

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fxoee.engine.mode` | `FXOEE_ENGINE_MODE` | `default` (yml/code `matchIfMissing`); **`speed` in `performance.properties` and the k8s configmap** | ✅ `default` = `BigDecimal` `MatchingService`; `speed` = zero-alloc long fixed-point single-writer `SpeedEngine` (see below + [speed-engine.md](speed-engine.md)) |
| `fxoee.funding.mode` | none | `FULL_NOTIONAL` | ✅ `MARGIN` (leveraged) vs `FULL_NOTIONAL` ([doc 04](04-funding-pnl-conservation.md)) |
| `fxoee.engine.authoritative` | none | `true` | ✅ WebSocket + debug APIs read in-JVM engine state |
| `fxoee.recovery.replay-on-startup` | `FXOEE_RECOVERY_REPLAY_ON_STARTUP` | `false` | ✅ when true, `AccountBootstrapper` rebuilds the engine from `trade_events` (warm restart) instead of wiping to a fresh 10M balance. Always `true` in k8s ([configmap](../k8s/backend/configmap.yaml)); `false` in local dev, where `deploy-all.sh` wipes the Postgres PVC each run anyway so an empty log makes warm restart a no-op. See [doc 05](05-event-sourcing-persistence.md#warm-restart-recovery-engine-replay). |
| `fxoee.mock-market.enabled` | `MOCK_MARKET_ENABLED` | `false` | ✅ `MockMarketMaker` injects house bid/ask depth every 500 ms using an OU+GARCH price model |
| `fxoee.mock-market.interval-ms` | none | `500` (code `@Value` default; key not in yml) | ✅ tick interval for mock quotes |
| `fxoee.mock-market.quantity` | none | `1000000` (code `@Value` default; key not in yml) | ✅ house order size per quote |

## Speed engine

Active only when `fxoee.engine.mode=speed` (the branch + k8s default). These keys live in
[performance.properties](../src/main/resources/performance.properties) and bind via
[SpeedEngineConfig](../src/main/java/com/fxoee/engine/speed/SpeedEngineConfig.java). They are no-ops in
`default` mode.

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fxoee.engine.speed.book-map-capacity` | `FXOEE_ENGINE_SPEED_BOOK_MAP_CAPACITY` | `65536` (same value in properties and code) | ✅ Agrona open-addressing map capacity per book side; size to peak resting orders per pair so books stop resizing |
| `fxoee.engine.speed.cpu` | `FXOEE_ENGINE_SPEED_CPU` | code `@Value` `-1` (= let the OS schedule); **`performance.properties` sets `2`** | ✅ pins the single-writer engine thread to this core (Linux/OpenHFT affinity only; no-op on macOS/unsupported) |
| `fxoee.engine.speed.wait-strategy` | `FXOEE_ENGINE_SPEED_WAIT_STRATEGY` | `busy-spin` | ✅ Disruptor command-ring wait strategy for the engine thread |

The command ring size is a code constant (`1 << 16` = 65536 slots in `SpeedEngineConfig`), not a
configurable key. The house account is pre-seeded with 10M on wiring, mirroring `default` mode.

## Fill pipeline (FillQueue + persistence)

The async fill hand-off between the engine and the DB writer. Only wired when `kafka.enabled=true`
(otherwise the engine runs fully in-memory and these are inert). Keys live in
[performance.properties](../src/main/resources/performance.properties).

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fxoee.queue.type` | `FXOEE_QUEUE_TYPE` | code `matchIfMissing` `default`; **`performance.properties` sets `disruptor`** | ✅ `default` = `ConcurrentLinkedQueue` ([DefaultFillQueue](../src/main/java/com/fxoee/engine/DefaultFillQueue.java)); `disruptor` = LMAX ring buffer ([DisruptorFillQueue](../src/main/java/com/fxoee/engine/DisruptorFillQueue.java)) |
| `fxoee.disruptor.ring-buffer-size` | `FXOEE_DISRUPTOR_RING_BUFFER_SIZE` | code `@Value` `131072`; **`performance.properties` sets `1048576`**. Must be a power of 2 | ✅ ring capacity for the disruptor `FillQueue` path |
| `fxoee.persistence.batch-max` | `FXOEE_PERSISTENCE_BATCH_MAX` | `512` (same value in properties and code) | ✅ max `PendingFill`s drained per `PersistenceWorker` batch (higher = fewer DB round-trips, higher tail latency) |

## Sample data (startup order ladder)

Seeds a resting EUR/USD limit-order ladder on startup so the UI isn't empty. Off by default.

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fxoee.sample-data.enabled` | `SAMPLE_DATA_ENABLED` | `false` | ✅ |
| `fxoee.sample-data.pair` | none | `EUR_USD` | ✅ pair the ladder is placed on |
| `fxoee.sample-data.levels` | none | `10` | ✅ price levels per side (2 × levels orders total) |
| `fxoee.sample-data.quantity-per-level` | none | `100000` | ✅ base-currency units per level |
| `fxoee.sample-data.mid-price` | none | `1.0850` | ✅ mid the ladder is centred on |
| `fxoee.sample-data.spread` | none | `0.0002` | ✅ best-bid-to-best-ask distance; per-level spacing is spread / 2 |
| `fxoee.sample-data.account-id` | `SAMPLE_DATA_ACCOUNT_ID` | `00000000-0000-0000-0000-000000000001` | ✅ owner of the sample orders (the seeded demo trader) |

## Tiingo live feed

Real FX data via Tiingo REST (OHLC history) + WebSocket (live quotes). See [market-data.md](market-data.md) for full details.

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fxoee.tiingo.enabled` | `TIINGO_ENABLED` | `false` | ✅ activates `TiingoMarketDataService` |
| `fxoee.tiingo.api-key` | `TIINGO_API_KEY` | _(required when enabled)_ | ✅ **secret**, goes in `backend-secret`, never in the configmap |
| `fxoee.tiingo.history-days` | `TIINGO_HISTORY_DAYS` | `7` (k8s configmap sets `30`) | ✅ days of OHLC history loaded per timeframe on startup |
| `fxoee.tiingo.threshold-level` | `TIINGO_THRESHOLD_LEVEL` | `5` | ✅ WebSocket throttle (ms between price updates; 0 = every tick) |
| `fxoee.tiingo.quantity` | `TIINGO_QUANTITY` | `1000000` | ✅ house order size injected per live quote |

Both `tiingo` and `mock-market` can run at the same time, and they coordinate on their own: the mock
maker stands down while Tiingo is streaming and resumes within 30 seconds of the live feed going
quiet. Typical production setup is `TIINGO_ENABLED=true` + `MOCK_MARKET_ENABLED=true`, which gives
real prices in the week and synthetic depth over the weekend with no manual switch. See
[market-data.md](market-data.md#market-closed-behaviour).

## Market-data broadcaster (spread & stale metrics)

A separate poller from the two feeds above. It reads whatever depth is resting and publishes
book-health metrics; it does not generate prices. See [market-data.md](market-data.md#5--spread--stale-order-metrics).

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fxoee.market-data.enabled` | `MARKET_DATA_ENABLED` | `false` | ✅ runs `MarketDataBroadcaster`; emits `fxoee.market.spread.bps`, `*.bid/ask.deviation.pips`, `fxoee.market.stale.orders` |
| `fxoee.market-data.polling-interval-ms` | `MARKET_DATA_POLL_MS` | `1000` | ✅ how often the book is sampled and metrics refreshed |
| `fxoee.market-data.provider` | none | `orderbook` | ✅ `MarketDataService` impl; only `orderbook` is built in |
| `fxoee.market-data.stale-order-pip-threshold` | none | `50` (code default; key not in yml) | ✅ a resting level this many pips off the external mid trips the stale counter and a `STALE_ORDER` WebSocket event (1 pip = 0.01 for USD/JPY, 0.0001 otherwise) |

## Server, auth, metrics

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `server.port` | `SERVER_PORT` | `8080` | ✅ |
| `jwt.secret` | `JWT_SECRET` | dev placeholder | ✅ **override in any real deployment** (min 32 chars, HS256) |
| `jwt.expiry-days` | none | `7` | ✅ |
| `management.endpoints…exposure` | none | `health, info, prometheus, metrics` | ✅ Actuator + Micrometer Prometheus |

> 🔐 The default `jwt.secret` is a known dev string in `application.yml`. It **must** be supplied via
> the `backend-secret` Secret (k8s) or env (compose) in any non-local environment.

## Pre-trade risk & circuit breaker (enforced)

The `fx.risk.*` and `fx.circuit-breaker.*` keys **are** read and enforced. This was scaffolding in
earlier revisions but is now live. See [11 - Pre-trade risk controls](11-risk-controls.md) for the
full gate; the circuit breaker is in [circuit-breaker.md](circuit-breaker.md).

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fx.risk.killswitch` | `RISK_KILLSWITCH` | `false` | ✅ enforced; when true, no new orders are admitted |
| `fx.risk.max-position` | `RISK_MAX_POSITION` | `10000000` | ✅ enforced; per-account \|net qty\| per pair (0 disables) |
| `fx.risk.max-order-notional` | `RISK_MAX_ORDER_NOTIONAL` | `5000000` | ✅ enforced; per-order USD notional cap (0 disables) |
| `fx.risk.max-gross-exposure` | `RISK_MAX_GROSS_EXPOSURE` | `0` | ✅ enforced; per-account gross margin USD (0 = disabled) |
| `fx.circuit-breaker.enabled` | `CIRCUIT_BREAKER_ENABLED` | `true` (yml + code `@Value`); **k8s configmap sets `false`** | ✅ master on/off for the circuit breaker; when false no pair is ever auto-halted |
| `fx.circuit-breaker.price-deviation-threshold` | `CIRCUIT_BREAKER_PRICE_DEVIATION_THRESHOLD` | `0.5` (yml); code `@Value` fallback `0.005`; k8s configmap sets `0.005` | ✅ enforced; halts a pair on a price jump beyond this, and the risk gate then refuses orders on HALTED pairs |

Watch out for the circuit-breaker default: it differs per environment. `application.yml` ships `0.5`
(a 50% jump, deliberately loose so synthetic mock-market volatility doesn't trip halts in local dev),
the k8s configmap tightens it to `0.005` (0.5%), and the `@Value` fallback in `CircuitBreaker.java`
(used only when the yml key is removed entirely) is `0.005`.

All `fx.risk.*` values are **runtime-mutable** via `PUT /api/risk/limits` / the DEBUG panel RISK tab.
The `application.yml` values are only the startup seed.

### Still scaffolding

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fx.orderbook.snapshot-depth` | none | `10` | ⚠️ not read; `GET /api/orderbook/{pair}` takes a `?depth=` request param instead (default 20) |

The funds/structural gate is `PreTradeValidator` + margin reservation in the
[submit pipeline](03-engine-core.md#the-submit-pipeline): quantity/lot/tick checks and the
whole-order funds check. The risk gate (above) runs just before it, pre-lock.

## FIX gateway

A FIX 4.4 acceptor ([com.fxoee.infrastructure.fix](../src/main/java/com/fxoee/infrastructure/fix))
built on QuickFIX/J. It handles `NewOrderSingle` (35=D) and `OrderCancelRequest` (35=F), routing both
through the same `OrderService` path as the REST API and replying with FIX `ExecutionReport` (35=8) /
`OrderCancelReject` (35=9). See [fix-session.md](fix-session.md).

| Key | Env var | Default | Status |
|-----|---------|---------|--------|
| `fx.fix.enabled` | `FIX_ENABLED` | `false` | ✅ boots the acceptor on startup; off by default so test/CI runs don't bind the port. Listen port is set in `fix-acceptor.cfg` (9876) |

## Implemented since earlier revisions

These were listed as planned/scaffolding in older docs; they are now wired:

| Capability | State | Reference |
|------------|-------|-----------|
| LMAX Disruptor fill hand-off | ✅ implemented; selected by `fxoee.queue.type=disruptor` (the `performance.properties` default) | [DisruptorFillQueue](../src/main/java/com/fxoee/engine/DisruptorFillQueue.java), [ADR 0004](adr/0004-async-fill-queue-over-disruptor.md) |
| FIX 4.4 protocol gateway | ✅ implemented (QuickFIX/J acceptor); gated by `fx.fix.enabled` | [fix-session.md](fix-session.md) |
| Pre-trade risk gate | ✅ enforced | [doc 11](11-risk-controls.md) |
| Circuit breaker | ✅ enforced | [circuit-breaker.md](circuit-breaker.md) |
| Speed engine (long fixed-point single-writer) | ✅ selected by `fxoee.engine.mode=speed` (branch + k8s default) | [speed-engine.md](speed-engine.md) |
