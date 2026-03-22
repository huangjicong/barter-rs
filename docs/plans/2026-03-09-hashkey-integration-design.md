# HashKey Exchange Integration Design

## Overview

This document describes the design for integrating HashKey Exchange into the barter-rs trading framework, supporting spot market data streaming and order execution.

## Requirements

| Item | Specification |
|------|---------------|
| Market Type | Spot |
| Market Data | PublicTrades, OrderBooksL1, OrderBooksL2 |
| Order Functions | Full (limit/market orders, cancel, balance query, order query, trade history) |
| Environment | Sandbox first, with production support |

## API Reference

### REST API

| Environment | URL |
|-------------|-----|
| Production | `https://api-glb.hashkey.com` |
| Sandbox | `https://api-glb.sim.hashkeydev.com` |

### WebSocket API

| Type | URL |
|------|-----|
| Public Stream (Market Data) | `wss://stream-glb.hashkey.com/quote/ws/v1` |
| Private Stream (Account) | `wss://stream-glb.hashkey.com/api/v1/ws/{listenKey}` |

### WebSocket Topics

| Topic | Description | Barter Type |
|-------|-------------|-------------|
| `trade` | Public trades | `PublicTrades` |
| `depth` | Order book depth | `OrderBooksL2` |
| `realtimes` | 24h ticker | `OrderBooksL1` |
| `kline_{interval}` | OHLCV candles | Future |

### REST Endpoints

| Function | Endpoint | Method |
|----------|----------|--------|
| Server Time | `/api/v1/time` | GET |
| Exchange Info | `/api/v1/exchangeInfo` | GET |
| Order Book | `/quote/v1/depth` | GET |
| Recent Trades | `/quote/v1/trades` | GET |
| Ticker | `/quote/v1/ticker/24hr` | GET |
| Account Info | `/api/v1/account` | GET |
| Place Order | `/api/v1/order` | POST |
| Cancel Order | `/api/v1/order` | DELETE |
| Open Orders | `/api/v1/openOrders` | GET |
| My Trades | `/api/v1/myTrades` | GET |
| Get ListenKey | `/api/v1/userDataStream` | POST |
| Keep-Alive ListenKey | `/api/v1/userDataStream` | PUT |

## Module Structure

```
barter-data/src/exchange/hashkey/
├── mod.rs              # Connector + StreamSelector implementation
├── channel.rs          # HashKeyChannel enum
├── market.rs           # HashKeyMarket struct
├── subscription.rs     # HashKeySubResponse
├── trade.rs            # HashKeyTrade + transformation logic
├── book.rs             # HashKeyDepth (L2 order book)
├── ticker.rs           # HashKeyTicker (L1 data)
└── transformer.rs      # Message transformer

barter-execution/src/client/hashkey/
├── mod.rs              # ExecutionClient implementation
├── config.rs           # HashKeyConfig
├── rest.rs             # REST API wrapper
├── websocket.rs        # Private WebSocket stream
├── order.rs            # Order request/response types
├── account.rs          # Account/balance types
├── signer.rs           # HMAC-SHA256 signing
└── error.rs            # HashKeyError

barter-instrument/src/exchange/
└── mod.rs              # Add ExchangeId::HashKey
```

## Data Type Mappings

### Market Data

#### HashKeyTrade (trade topic)
```json
{
    "v": "trade_id",
    "t": 1722866228075,
    "p": "2340.41",
    "q": "0.0132",
    "m": true
}
```

#### HashKeyDepth (depth topic)
```json
{
    "b": [["1650", "0.0864"], ...],
    "a": [["4085", "0.0074"], ...],
    "t": 1722873144371
}
```

#### HashKeyTicker (realtimes topic)
```json
{
    "t": 1722864411064,
    "s": "ETHUSDT",
    "c": "2195",
    "h": "2918.85",
    "l": "2135.5",
    "o": "2915.78",
    "b": "2194.5",
    "a": "2195",
    "v": "666.5019",
    "qv": "1586902.757079"
}
```

### Order Data

#### Order Request
```json
{
    "symbol": "BTCUSDT",
    "side": "BUY",
    "type": "LIMIT",
    "price": "10000",
    "quantity": "0.001",
    "timeInForce": "GTC"
}
```

#### Order Response (executionReport event)
```json
{
    "e": "executionReport",
    "E": "1723037391181",
    "s": "BTCUSDT",
    "c": "clientOrderId",
    "S": "BUY",
    "o": "LIMIT",
    "q": "0.001",
    "p": "10000",
    "X": "NEW",
    "i": "orderId",
    "z": "0"
}
```

## Authentication

### REST API Signing

1. Concatenate: `timestamp + method + path + body`
2. Sign with HMAC-SHA256 using API secret
3. Include in header: `X-HASHKEY-SIGNATURE`

### WebSocket Private Stream

1. Call `POST /api/v1/userDataStream` to get `listenKey`
2. Connect to `wss://stream-glb.hashkey.com/api/v1/ws/{listenKey}`
3. Refresh `listenKey` every hour via `PUT /api/v1/userDataStream`

## Heartbeat

WebSocket connections require heartbeat every 10 seconds:

```json
// Client sends
{"ping": 1728503859938}

// Server responds
{"pong": 1728503865406}
```

## Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum HashKeyError {
    #[error("API error: code={code}, message={message}")]
    ApiError { code: i32, message: String },

    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("Invalid order: {0}")]
    InvalidOrder(String),

    #[error("Insufficient balance")]
    InsufficientBalance,

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("ListenKey expired")]
    ListenKeyExpired,

    #[error("WebSocket disconnected: {0}")]
    WsDisconnected(String),

    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),
}
```

## Implementation Order

| Step | Module | Dependencies | Description |
|------|--------|--------------|-------------|
| 1 | `barter-instrument` | None | Add `ExchangeId::HashKey` |
| 2 | `barter-data/channel.rs` | Step 1 | Channel definitions |
| 3 | `barter-data/market.rs` | Step 1 | Market symbol |
| 4 | `barter-data/subscription.rs` | Step 2,3 | Subscription response |
| 5 | `barter-data/trade.rs` | Step 1-4 | PublicTrades |
| 6 | `barter-data/book.rs` | Step 1-4 | OrderBooksL2 |
| 7 | `barter-data/ticker.rs` | Step 1-4 | OrderBooksL1 |
| 8 | `barter-data/mod.rs` | Step 2-7 | Connector implementation |
| 9 | `barter-execution/*` | Step 8 | ExecutionClient implementation |
| 10 | Integration tests | Step 9 | End-to-end tests |

## Testing

- Use sandbox environment for development and testing
- Integration tests require real API keys (marked with `#[ignore]`)
- Unit tests for data parsing and signature generation

## References

- HashKey API Documentation: https://hashkeyglobal-apidoc.readme.io/
- CCXT HashKey Implementation: https://github.com/ccxt/ccxt/blob/master/python/ccxt/hashkey.py
