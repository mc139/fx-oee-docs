# FX Order Flow — Implementation Gap Analysis

Compare `fx_order_flow_english.svg` + `fx_post_fill_english.svg` against current codebase.
Each row: ✅ done · ⚠️ partial · ❌ missing.

---

## Part 1 — Order Placement to Execution

| Step | Flow Spec | Status | Notes |
|------|-----------|--------|-------|
| Client places order | BUY/SELL · qty · price · TIF | ✅ | `OrderEntry.svelte` → WS `NEW_ORDER` |
| Order validation | format · lot size · symbol supported | ⚠️ | Basic parsing done; no explicit lot-size rule or symbol whitelist |
| Pre-trade risk check | margin · position limit · buying power | ⚠️ | No pre-trade gate; margin reserved **after** fill in `AccountService.applyFill()` |
| Persist order · status PENDING · generate ClOrdID · reserve margin | ⚠️ | PENDING set in `OrderBook.addOrder()`; margin reserved post-fill, not pre; `clientOrderId` maps to ClOrdID |
| Matching Engine | price-time priority · TreeMap bid/ask | ✅ | `MatchingEngine.java` + `OrderBook.java` — full impl |
| No counter-party → GTC waits / IOC·FOK cancel | ⚠️ | GTC (LIMIT stays in book) works; no IOC or FOK TIF support |
| Partial fill path | remainder back to book | ✅ | `PARTIALLY_FILLED` status, reduced `remainingQuantity`, lot stays in book |
| Trade record · status FILLED | executedQty · price · timestamp | ✅ | `Trade.java` + `Order.fill()` |
| Publish `OrderFilledEvent` to Kafka `order-fills` | ❌ | In-process `MatchEventListener` callback only; no Kafka |

---

## Part 2 — Post-Fill Pipeline

| Step | Flow Spec | Status | Notes |
|------|-----------|--------|-------|
| Kafka `order-fills` fan-out | three independent consumers | ❌ | Single in-process `onTrade()` in `TradingWebSocketHandler`; sequential, not fan-out |
| **Account Service** — debit cost · release margin | ⚠️ | `AccountService.applyFill()` updates balance; margin logic inverted (reserved at fill, not pre-trade) |
| Account DB | available balance updated · reserved→0 | ⚠️ | In-memory only; no DB; no concurrency protection |
| **Position Service** — exposure · long/short · avg entry | ✅ | `AccountService` tracks individual lots, FIFO close, avg entry price, realized PnL |
| Position DB | open position · realized P&L on close | ⚠️ | In-memory; realized PnL only on lot-close path, not ordinary `applyFill()` close |
| **Notification Service** — WebSocket push | ✅ | `broadcastAccountUpdate()` → `ACCOUNT_UPDATE` WS message → frontend store |
| Client UI update | balance + position refreshed | ✅ | `ws.svelte.ts` handles `ACCOUNT_UPDATE`, updates all reactive state |
| Order status → FILLED · audit log | ⚠️ | Status set; no audit log |

---

## Summary

**Score: 5 ✅ · 8 ⚠️ · 2 ❌**

**Fully implemented:** Matching engine, partial fills, FILLED status, position tracking (lots/PnL), WebSocket notifications, client UI state.

**Partial (⚠️):**
- Validation: no lot-size rule or symbol whitelist
- Pre-trade risk: no margin/position-limit gate before order enters book
- Margin model: reserved post-fill instead of pre-trade
- TIF: only GTC effectively; IOC/FOK missing
- Persistence: all in-memory, no DB, no optimistic locking
- Audit log: missing

**Not implemented (❌):**
- Kafka (`order-fills` topic, `OrderFilledEvent`)
- Fan-out to independent consumers (Account / Position / Notification as separate services)

---

## Key Files

| Area | File |
|------|------|
| Matching | `src/main/java/com/fxoee/matching/MatchingEngine.java` |
| Order Book | `src/main/java/com/fxoee/matching/OrderBook.java` |
| Account/Position | `src/main/java/com/fxoee/account/AccountService.java` |
| WS Entry Point | `src/main/java/com/fxoee/api/websocket/TradingWebSocketHandler.java` |
| Order Model | `src/main/java/com/fxoee/domain/model/Order.java` |
| Frontend Store | `frontend/src/lib/stores/ws.svelte.ts` |
| Order Entry UI | `frontend/src/lib/components/OrderEntry.svelte` |
