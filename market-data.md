# Market Data Feed

_Last updated: 2026-06-05 BST._

A provider-agnostic market-data feed: an interface for "latest mid-price per pair", one built-in
synthetic provider derived from the order book, and a scheduled broadcaster that pushes prices over
WebSocket. Disabled by default.

## Interface contract

[`MarketDataService`](../src/main/java/com/fxoee/infrastructure/marketdata/MarketDataService.java):

```java
Optional<BigDecimal> getMidPrice(CurrencyPair pair);   // latest mid, or empty if unavailable
Map<CurrencyPair, BigDecimal> getAllMidPrices();        // all pairs that currently have data
```

Implementations may be **live** (an external HTTP API) or **synthetic** (derived from local state).
Exactly one `MarketDataService` bean is active at a time, selected by the `provider` property.

## Built-in provider: `orderbook`

[`OrderBookMidPriceService`](../src/main/java/com/fxoee/infrastructure/marketdata/OrderBookMidPriceService.java)
derives each pair's mid as `(bestBid + bestAsk) / 2` from the in-memory order book, or empty when
either side is missing. It is annotated:

```java
@ConditionalOnProperty(name = "fxoee.market-data.provider", havingValue = "orderbook", matchIfMissing = true)
```

so it is the default provider unless `provider` names another.

## Adding a new provider

1. Implement `MarketDataService` (e.g. an HTTP client for Frankfurter or Twelve Data).
2. Annotate the bean:
   ```java
   @Component
   @ConditionalOnProperty(name = "fxoee.market-data.provider", havingValue = "frankfurter")
   public class FrankfurterMarketDataService implements MarketDataService { ... }
   ```
3. Select it: `fxoee.market-data.provider=frankfurter`.

The `havingValue` guard ensures only the selected provider is wired, so `MarketDataBroadcaster` always
has exactly one `MarketDataService` to inject. (The built-in `orderbook` provider uses
`matchIfMissing = true`, so it is the fallback when `provider` is unset.)

## Broadcaster

[`MarketDataBroadcaster`](../src/main/java/com/fxoee/infrastructure/marketdata/MarketDataBroadcaster.java)
is created only when `fxoee.market-data.enabled=true`. It polls `getAllMidPrices()` on a fixed delay
(`@Scheduled(fixedDelayString = "${fxoee.market-data.polling-interval-ms:1000}")`) and broadcasts each
pair's mid as a `MARKET_DATA` message. It runs on Spring's scheduler thread (`@EnableScheduling` is on
the application class).

## Configuration

| Key | Env var | Default | Meaning |
| --- | --- | --- | --- |
| `fxoee.market-data.enabled` | `MARKET_DATA_ENABLED` | `false` | Start the polling broadcaster. |
| `fxoee.market-data.polling-interval-ms` | `MARKET_DATA_POLL_MS` | `1000` | Poll / broadcast interval (ms). |
| `fxoee.market-data.provider` | — | `orderbook` | Provider id selecting the `MarketDataService` impl. |

## WebSocket message

Same `type` / `payload` envelope as the other broadcasts (sent to sessions subscribed to the pair);
`mid` is scaled to 5 decimals:

```json
{ "type": "MARKET_DATA", "payload": { "pair": "EUR_USD", "mid": "1.08520" } }
```
