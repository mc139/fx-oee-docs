# FIX Session — Supported Messages and Tags

**Status:** Planned (Phase 11 — QuickFIX/J adapter not yet implemented)  
**Version:** FIX 4.4  
**Default config file:** `fix.cfg` (to be added)

This document defines the intended FIX surface for FX-OEE. Internal order flow uses the same `Order` domain model as REST; FIX is an alternate ingress/egress channel.

## Session lifecycle

| Message | MsgType (35) | Direction | Notes |
|---------|--------------|-----------|-------|
| Logon | A | Both | Establishes session; heartbeat interval negotiated |
| Logout | 5 | Both | Graceful teardown |
| Heartbeat | 0 | Both | Keep-alive |
| Test Request | 1 | Both | Connectivity check |
| Resend Request | 2 | Both | Gap fill |
| Sequence Reset | 4 | Both | Gap recovery |

## Application messages (supported / planned)

### New Order Single — `35=D` (client → engine)

Parses into internal `Order`. Required tags:

| Tag | Name | Value / notes |
|-----|------|---------------|
| 11 | ClOrdID | Client order id → maps to `Order.id` or correlation |
| 55 | Symbol | FX pair, e.g. `EUR/USD` → `CurrencyPair` enum |
| 54 | Side | `1` = Buy, `2` = Sell → `OrderSide` |
| 40 | OrdType | `1` = Market, `2` = Limit → `OrderType` |
| 38 | OrderQty | Quantity in base currency units |
| 44 | Price | Required for limit orders; ignored for market |
| 59 | TimeInForce | `0` = Day (default), `3` = IOC, `4` = FOK |
| 60 | TransactTime | Order timestamp |

Optional:

| Tag | Name | Notes |
|-----|------|-------|
| 1 | Account | Maps to `accountId` |
| 58 | Text | Free-form comment |

**Handler:** `FixSessionAdapter.onMessage(NewOrderSingle)` → `MatchingService.submit(order)`.

### Order Cancel Request — `35=F` (client → engine)

| Tag | Name | Notes |
|-----|------|-------|
| 11 | ClOrdID | Cancel request id |
| 41 | OrigClOrdID | Original order to cancel |
| 55 | Symbol | Pair |
| 54 | Side | Side of original order |

**Handler:** resolves order by id → `MatchingService.cancel()` (same as `DELETE /api/orders/{id}`).

### Execution Report — `35=8` (engine → client)

Sent after accept, partial fill, fill, cancel, or reject.

| Tag | Name | Notes |
|-----|------|-------|
| 11 | ClOrdID | Client order id |
| 17 | ExecID | Unique execution id per report |
| 20 | ExecTransType | `0` = New |
| 37 | OrderID | Engine-assigned order id |
| 39 | OrdStatus | `0` New, `1` Partial, `2` Filled, `4` Cancelled, `8` Rejected |
| 55 | Symbol | Pair |
| 54 | Side | Buy/Sell |
| 38 | OrderQty | Original quantity |
| 151 | LeavesQty | Remaining quantity |
| 14 | CumQty | Cumulative filled |
| 6 | AvgPx | Average fill price |
| 31 | LastPx | Price of this fill (if applicable) |
| 32 | LastQty | Quantity of this fill |
| 58 | Text | Reject reason when `OrdStatus=8` |

**Mapping from domain:**

| `OrderStatus` | FIX OrdStatus (39) |
|---------------|-------------------|
| NEW / PENDING | 0 |
| PARTIALLY_FILLED | 1 |
| FILLED | 2 |
| CANCELLED | 4 |
| REJECTED | 8 |

### Order Cancel Reject — `35=9` (engine → client)

When cancel fails (unknown order, already filled):

| Tag | Name | Notes |
|-----|------|-------|
| 11 | ClOrdID | Cancel request id |
| 41 | OrigClOrdID | Original order |
| 434 | CxlRejResponseTo | `1` = Order Cancel Request |
| 102 | CxlRejReason | `0` = Too late, `1` = Unknown order |

## Not in scope (initial release)

- Market Data Request (`35=V`) — use WebSocket `/ws/trading` or REST order book instead
- Mass cancel, replace (`35=G`), multi-leg — future phases
- Drop copy to external OMS

## Testing a FIX session (once implemented)

```bash
# Start engine with FIX acceptor enabled (port TBD in fix.cfg)
docker compose up backend

# Use QuickFIX/J example initiator or tools like fiximulator
# Point at localhost:<fix-port>, FIX 4.4, comp IDs from fix.cfg
```

## Related

- REST equivalent: `POST /api/engine/orders`, `DELETE /api/orders/{id}`
- Architecture: [ADR 001 — Monolith](./adr/001-monolith-over-microservices.md)
