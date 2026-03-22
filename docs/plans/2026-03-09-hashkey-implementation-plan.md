# HashKey Exchange Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Integrate HashKey Exchange into barter-rs framework for spot market data streaming and order execution.

**Architecture:** Native Rust implementation following barter-rs patterns. HashKey API is similar to Binance, so we follow similar patterns. Implement Connector trait for market data, ExecutionClient trait for order execution.

**Tech Stack:** Rust, Tokio, reqwest, tokio-tungstenite, serde, thiserror, HMAC-SHA256

---

## Phase 1: Foundation (barter-instrument)

### Task 1: Add ExchangeId::HashKey

**Files:**
- Modify: `barter-instrument/src/exchange.rs`

**Step 1: Add HashKey variant to ExchangeId enum**

In `barter-instrument/src/exchange.rs`, add `HashKey` variant after `Poloniex`:

```rust
pub enum ExchangeId {
    // ... existing variants ...
    Poloniex,
    HashKey,  // Add this line
}
```

**Step 2: Add as_str match arm**

In the `as_str` method, add:

```rust
ExchangeId::HashKey => "hashkey",
```

**Step 3: Run tests**

Run: `cargo test -p barter-instrument`
Expected: All tests pass

**Step 4: Commit**

```bash
git add barter-instrument/src/exchange.rs
git commit -m "feat(instrument): add ExchangeId::HashKey variant"
```

---

## Phase 2: Market Data (barter-data)

### Task 2: Create HashKey Channel Types

**Files:**
- Create: `barter-data/src/exchange/hashkey/channel.rs`

**Step 1: Create channel.rs with HashKeyChannel**

```rust
use crate::{
    Identifier,
    subscription::{Subscription, book::{OrderBooksL1, OrderBooksL2}, trade::PublicTrades},
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`HashKey`] channel to be subscribed to.
///
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct HashKeyChannel(pub &'static str);

impl HashKeyChannel {
    /// [`HashKey`] real-time trades channel.
    pub const TRADES: Self = Self("trade");
    /// [`HashKey`] order book depth channel (L2).
    pub const DEPTH: Self = Self("depth");
    /// [`HashKey`] real-time ticker channel (used for L1).
    pub const REALTIME: Self = Self("realtimes");
}

impl<Instrument> Identifier<HashKeyChannel> for Subscription<super::HashKey, Instrument, PublicTrades> {
    fn id(&self) -> HashKeyChannel {
        HashKeyChannel::TRADES
    }
}

impl<Instrument> Identifier<HashKeyChannel> for Subscription<super::HashKey, Instrument, OrderBooksL1> {
    fn id(&self) -> HashKeyChannel {
        HashKeyChannel::REALTIME
    }
}

impl<Instrument> Identifier<HashKeyChannel> for Subscription<super::HashKey, Instrument, OrderBooksL2> {
    fn id(&self) -> HashKeyChannel {
        HashKeyChannel::DEPTH
    }
}

impl AsRef<str> for HashKeyChannel {
    fn as_ref(&self) -> &str {
        self.0
    }
}
```

**Step 2: Run compilation check**

Run: `cargo check -p barter-data`
Expected: Compilation succeeds (file not yet used, so no errors expected)

**Step 3: Commit**

```bash
git add barter-data/src/exchange/hashkey/channel.rs
git commit -m "feat(data): add HashKey channel types"
```

---

### Task 3: Create HashKey Market Types

**Files:**
- Create: `barter-data/src/exchange/hashkey/market.rs`

**Step 1: Create market.rs with HashKeyMarket**

```rust
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use barter_instrument::{
    Keyed,
    instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`HashKey`] market that can be subscribed to.
///
/// HashKey uses uppercase symbol format like "BTCUSDT".
///
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct HashKeyMarket(pub SmolStr);

impl<Kind> Identifier<HashKeyMarket> for Subscription<super::HashKey, MarketDataInstrument, Kind> {
    fn id(&self) -> HashKeyMarket {
        let base = &self.instrument.base;
        let quote = &self.instrument.quote;
        HashKeyMarket(format_smolstr!("{base}{quote}").to_uppercase_smolstr())
    }
}

impl<InstrumentKey, Kind> Identifier<HashKeyMarket>
    for Subscription<super::HashKey, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> HashKeyMarket {
        let base = &self.instrument.value.base;
        let quote = &self.instrument.value.quote;
        HashKeyMarket(format_smolstr!("{base}{quote}").to_uppercase_smolstr())
    }
}

impl<InstrumentKey, Kind> Identifier<HashKeyMarket>
    for Subscription<super::HashKey, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> HashKeyMarket {
        HashKeyMarket(self.instrument.name_exchange.name().clone())
    }
}

impl AsRef<str> for HashKeyMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

fn format_smolstr(s: &str) -> smol_str::SmolStr {
    smol_str::SmolStr::new(s)
}

trait ToUppercaseSmolStr {
    fn to_uppercase_smolstr(self) -> smol_str::SmolStr;
}

impl ToUppercaseSmolStr for smol_str::SmolStr {
    fn to_uppercase_smolstr(self) -> smol_str::SmolStr {
        smol_str::SmolStr::new(self.to_uppercase())
    }
}
```

**Step 2: Run compilation check**

Run: `cargo check -p barter-data`
Expected: Compilation succeeds

**Step 3: Commit**

```bash
git add barter-data/src/exchange/hashkey/market.rs
git commit -m "feat(data): add HashKey market types"
```

---

### Task 4: Create HashKey Subscription Response

**Files:**
- Create: `barter-data/src/exchange/hashkey/subscription.rs`

**Step 1: Create subscription.rs with HashKeySubResponse**

```rust
use super::{channel::HashKeyChannel, market::HashKeyMarket};
use crate::exchange::subscription::ExchangeSub;
use barter_integration::{Validator, error::SocketError};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};

// Implement custom Serialize for HashKey subscription args
impl Serialize for ExchangeSub<HashKeyChannel, HashKeyMarket> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HashKeySubArg", 3)?;
        state.serialize_field("symbol", self.market.as_ref())?;
        state.serialize_field("topic", self.channel.as_ref())?;
        state.serialize_field("event", "sub")?;
        state.end()
    }
}

/// [`HashKey`](super::HashKey) WebSocket subscription response.
///
/// ### Raw Payload Examples
/// #### Subscription Success Response
/// ```json
/// {
///   "symbol": "BTCUSDT",
///   "topic": "trade",
///   "event": "sub",
///   "code": 0,
///   "msg": "success"
/// }
/// ```
///
/// #### Subscription Error Response
/// ```json
/// {
///   "symbol": "BTCUSDT",
///   "topic": "trade",
///   "event": "sub",
///   "code": 10001,
///   "msg": "Invalid symbol"
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct HashKeySubResponse {
    pub symbol: String,
    pub topic: String,
    pub event: String,
    pub code: i32,
    pub msg: String,
}

impl Validator for HashKeySubResponse {
    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        if self.code == 0 {
            Ok(self)
        } else {
            Err(SocketError::Subscribe(format!(
                "received failure subscription response code: {} with message: {}",
                self.code, self.msg
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hashkey_subscription_response_success() {
        let input = r#"
        {
            "symbol": "BTCUSDT",
            "topic": "trade",
            "event": "sub",
            "code": 0,
            "msg": "success"
        }
        "#;

        let response: HashKeySubResponse = serde_json::from_str(input).unwrap();
        assert!(response.validate().is_ok());
    }

    #[test]
    fn test_hashkey_subscription_response_error() {
        let input = r#"
        {
            "symbol": "INVALID",
            "topic": "trade",
            "event": "sub",
            "code": 10001,
            "msg": "Invalid symbol"
        }
        "#;

        let response: HashKeySubResponse = serde_json::from_str(input).unwrap();
        assert!(response.validate().is_err());
    }
}
```

**Step 2: Run tests**

Run: `cargo test -p barter-data hashkey_subscription`
Expected: 2 tests pass

**Step 3: Commit**

```bash
git add barter-data/src/exchange/hashkey/subscription.rs
git commit -m "feat(data): add HashKey subscription response types"
```

---

### Task 5: Create HashKey Trade Types

**Files:**
- Create: `barter-data/src/exchange/hashkey/trade.rs`

**Step 1: Create trade.rs with HashKeyTrade**

```rust
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::subscription::ExchangeSub,
    subscription::trade::PublicTrade,
};
use barter_instrument::{Side, exchange::ExchangeId};
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

/// Terse type alias for a [`HashKey`](super::HashKey) real-time trades WebSocket message.
pub type HashKeyTrades = HashKeyMessage<HashKeyTrade>;

/// [`HashKey`](super::HashKey) market data WebSocket message.
///
/// ### Raw Payload Example
/// ```json
/// {
///   "symbol": "ETHUSDT",
///   "symbolName": "ETHUSDT",
///   "topic": "trade",
///   "params": {"realtimeInterval": "24h"},
///   "data": [
///     {
///       "v": "1745922896272048129",
///       "t": 1722866228075,
///       "p": "2340.41",
///       "q": "0.0132",
///       "m": true
///     }
///   ],
///   "f": true,
///   "sendTime": 1722869464248
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct HashKeyMessage<T> {
    pub symbol: String,
    pub topic: String,
    pub data: Vec<T>,
}

impl<T> Identifier<Option<SubscriptionId>> for HashKeyMessage<T> {
    fn id(&self) -> Option<SubscriptionId> {
        Some(ExchangeSub::from((self.topic.as_str(), self.symbol.as_str())).id())
    }
}

/// [`HashKey`](super::HashKey) real-time trade.
///
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HashKeyTrade {
    /// Trade ID
    pub v: String,
    /// Timestamp in milliseconds
    #[serde(deserialize_with = "de_i64_as_datetime_utc")]
    pub t: DateTime<Utc>,
    /// Price
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub p: f64,
    /// Quantity
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub q: f64,
    /// Is buyer maker (true = sell, false = buy)
    pub m: bool,
}

/// Deserialize i64 timestamp as DateTime<Utc>
fn de_i64_as_datetime_utc<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let ts: i64 = Deserialize::deserialize(deserializer)?;
    Ok(chrono::DateTime::from_timestamp_millis(ts).unwrap_or_else(|| Utc::now()))
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, HashKeyTrades)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from((exchange, instrument, trades): (ExchangeId, InstrumentKey, HashKeyTrades)) -> Self {
        trades
            .data
            .into_iter()
            .map(|trade| {
                Ok(MarketEvent {
                    time_exchange: trade.t,
                    time_received: Utc::now(),
                    exchange,
                    instrument: instrument.clone(),
                    kind: PublicTrade {
                        id: trade.v,
                        price: trade.p,
                        amount: trade.q,
                        side: if trade.m { Side::Sell } else { Side::Buy },
                    },
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hashkey_trade_message() {
        let input = r#"
        {
            "symbol": "BTCUSDT",
            "symbolName": "BTCUSDT",
            "topic": "trade",
            "params": {"realtimeInterval": "24h"},
            "data": [
                {
                    "v": "123456789",
                    "t": 1722866228075,
                    "p": "50000.00",
                    "q": "0.5",
                    "m": true
                }
            ],
            "f": true,
            "sendTime": 1722869464248
        }
        "#;

        let message: HashKeyTrades = serde_json::from_str(input).unwrap();
        assert_eq!(message.symbol, "BTCUSDT");
        assert_eq!(message.data.len(), 1);
        assert_eq!(message.data[0].v, "123456789");
        assert_eq!(message.data[0].p, 50000.0);
        assert_eq!(message.data[0].q, 0.5);
        assert!(message.data[0].m);
    }
}
```

**Step 2: Run tests**

Run: `cargo test -p barter-data hashkey_trade`
Expected: 1 test passes

**Step 3: Commit**

```bash
git add barter-data/src/exchange/hashkey/trade.rs
git commit -m "feat(data): add HashKey trade types"
```

---

### Task 6: Create HashKey Order Book Types

**Files:**
- Create: `barter-data/src/exchange/hashkey/book.rs`

**Step 1: Create book.rs with HashKeyDepth**

```rust
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::subscription::ExchangeSub,
    subscription::book::{Level, OrderBookL1, OrderBookL2},
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

/// Terse type alias for a [`HashKey`](super::HashKey) order book L2 WebSocket message.
pub type HashKeyDepthMessage = HashKeyMessage<HashKeyDepthData>;

/// Terse type alias for a [`HashKey`](super::HashKey) ticker/L1 WebSocket message.
pub type HashKeyTickerMessage = HashKeyMessage<HashKeyTickerData>;

/// [`HashKey`](super::HashKey) depth data.
///
/// ### Raw Payload Example
/// ```json
/// {
///   "e": 301,
///   "s": "ETHUSDT",
///   "t": 1722873144371,
///   "v": "84661262_18",
///   "b": [["1650", "0.0864"], ...],
///   "a": [["4085", "0.0074"], ...],
///   "o": 0
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HashKeyDepthData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t", deserialize_with = "de_i64_as_datetime_utc")]
    pub time: DateTime<Utc>,
    #[serde(deserialize_with = "de_price_levels")]
    pub b: Vec<Level>,
    #[serde(deserialize_with = "de_price_levels")]
    pub a: Vec<Level>,
}

/// [`HashKey`](super::HashKey) ticker data (used for L1).
///
/// ### Raw Payload Example
/// ```json
/// {
///   "t": 1722864411064,
///   "s": "ETHUSDT",
///   "c": "2195",
///   "h": "2918.85",
///   "l": "2135.5",
///   "o": "2915.78",
///   "b": "2194.5",
///   "a": "2195"
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HashKeyTickerData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t", deserialize_with = "de_i64_as_datetime_utc")]
    pub time: DateTime<Utc>,
    /// Best bid price
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub b: f64,
    /// Best ask price
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub a: f64,
}

/// Deserialize price levels from [["price", "qty"], ...]
fn de_price_levels<'de, D>(deserializer: D) -> Result<Vec<Level>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<(String, String)> = Deserialize::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .filter_map(|(price, amount)| {
            let price: f64 = price.parse().ok()?;
            let amount: f64 = amount.parse().ok()?;
            Some(Level { price, amount })
        })
        .collect())
}

/// Deserialize i64 timestamp as DateTime<Utc>
fn de_i64_as_datetime_utc<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let ts: i64 = Deserialize::deserialize(deserializer)?;
    Ok(chrono::DateTime::from_timestamp_millis(ts).unwrap_or_else(|| Utc::now()))
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, HashKeyDepthMessage)>
    for MarketIter<InstrumentKey, OrderBookL2>
{
    fn from((exchange, instrument, msg): (ExchangeId, InstrumentKey, HashKeyDepthMessage)) -> Self {
        msg.data
            .into_iter()
            .filter_map(|data| {
                if data.b.is_empty() && data.a.is_empty() {
                    return None;
                }
                Some(Ok(MarketEvent {
                    time_exchange: data.time,
                    time_received: Utc::now(),
                    exchange,
                    instrument: instrument.clone(),
                    kind: OrderBookL2 {
                        bids: data.b,
                        asks: data.a,
                    },
                }))
            })
            .collect()
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, HashKeyTickerMessage)>
    for MarketIter<InstrumentKey, OrderBookL1>
{
    fn from((exchange, instrument, msg): (ExchangeId, InstrumentKey, HashKeyTickerMessage)) -> Self {
        msg.data
            .into_iter()
            .map(|data| {
                Ok(MarketEvent {
                    time_exchange: data.time,
                    time_received: Utc::now(),
                    exchange,
                    instrument: instrument.clone(),
                    kind: OrderBookL1 {
                        bid: Level {
                            price: data.b,
                            amount: 0.0,
                        },
                        ask: Level {
                            price: data.a,
                            amount: 0.0,
                        },
                    },
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hashkey_depth_data() {
        let input = r#"
        {
            "e": 301,
            "s": "BTCUSDT",
            "t": 1722873144371,
            "v": "84661262_18",
            "b": [["50000.00", "1.5"], ["49999.00", "2.0"]],
            "a": [["50001.00", "1.0"], ["50002.00", "0.5"]],
            "o": 0
        }
        "#;

        let data: HashKeyDepthData = serde_json::from_str(input).unwrap();
        assert_eq!(data.symbol, "BTCUSDT");
        assert_eq!(data.b.len(), 2);
        assert_eq!(data.a.len(), 2);
        assert_eq!(data.b[0].price, 50000.0);
        assert_eq!(data.b[0].amount, 1.5);
    }

    #[test]
    fn test_hashkey_ticker_data() {
        let input = r#"
        {
            "t": 1722864411064,
            "s": "ETHUSDT",
            "c": "2195",
            "h": "2918.85",
            "l": "2135.5",
            "o": "2915.78",
            "b": "2194.5",
            "a": "2195"
        }
        "#;

        let data: HashKeyTickerData = serde_json::from_str(input).unwrap();
        assert_eq!(data.symbol, "ETHUSDT");
        assert_eq!(data.b, 2194.5);
        assert_eq!(data.a, 2195.0);
    }
}
```

**Step 2: Run tests**

Run: `cargo test -p barter-data hashkey_depth`
Expected: 2 tests pass

**Step 3: Commit**

```bash
git add barter-data/src/exchange/hashkey/book.rs
git commit -m "feat(data): add HashKey order book types"
```

---

### Task 7: Create HashKey Module Root with Connector Implementation

**Files:**
- Create: `barter-data/src/exchange/hashkey/mod.rs`

**Step 1: Create mod.rs with Connector and StreamSelector implementations**

```rust
use self::{
    book::{HashKeyDepthMessage, HashKeyTickerMessage},
    channel::HashKeyChannel,
    market::HashKeyMarket,
    subscription::HashKeySubResponse,
    trade::HashKeyTrades,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, PingInterval, StreamSelector},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{book::{OrderBooksL1, OrderBooksL2}, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocketSerdeParser, WsMessage},
};
use barter_macro::{DeExchange, SerExchange};
use derive_more::Display;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use url::Url;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration::Validator) for [`HashKey`].
pub mod subscription;

/// Public trade types for [`HashKey`].
pub mod trade;

/// Order book types for [`HashKey`].
pub mod book;

/// [`HashKey`] server base URL (production).
pub const BASE_URL_HASHKEY: &str = "wss://stream-glb.hashkey.com/quote/ws/v1";

/// [`HashKey`] server base URL (sandbox).
pub const BASE_URL_HASHKEY_SANDBOX: &str = "wss://stream-glb.sim.hashkeydev.com/quote/ws/v1";

/// [`HashKey`] server [`PingInterval`] duration.
pub const PING_INTERVAL_HASHKEY: Duration = Duration::from_secs(10);

/// Convenient type alias for a HashKey [`ExchangeWsStream`] using [`WebSocketSerdeParser`].
pub type HashKeyWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// [`HashKey`] exchange.
///
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Display, DeExchange, SerExchange,
)]
pub struct HashKey {
    /// Use sandbox environment if true
    pub sandbox: bool,
}

impl Connector for HashKey {
    const ID: ExchangeId = ExchangeId::HashKey;
    type Channel = HashKeyChannel;
    type Market = HashKeyMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = HashKeySubResponse;

    fn url() -> Result<Url, SocketError> {
        // Default to production URL
        Url::parse(BASE_URL_HASHKEY).map_err(SocketError::UrlParse)
    }

    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: tokio::time::interval(PING_INTERVAL_HASHKEY),
            ping: || {
                let timestamp = chrono::Utc::now().timestamp_millis();
                WsMessage::text(json!({ "ping": timestamp }).to_string())
            },
        })
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        exchange_subs
            .into_iter()
            .map(|sub| {
                WsMessage::text(
                    json!({
                        "symbol": sub.market.as_ref(),
                        "topic": sub.channel.as_ref(),
                        "event": "sub"
                    })
                    .to_string(),
                )
            })
            .collect()
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for HashKey
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = HashKeyWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, HashKeyTrades>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL1> for HashKey
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = HashKeyWsStream<StatelessTransformer<Self, Instrument::Key, OrderBooksL1, HashKeyTickerMessage>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for HashKey
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = HashKeyWsStream<StatelessTransformer<Self, Instrument::Key, OrderBooksL2, HashKeyDepthMessage>>;
}
```

**Step 2: Add hashkey module to exchange mod.rs**

In `barter-data/src/exchange/mod.rs`, add after the `okx` module:

```rust
/// `HashKey` [`Connector`] and [`StreamSelector`] implementations.
pub mod hashkey;
```

**Step 3: Run cargo check**

Run: `cargo check -p barter-data`
Expected: Compilation succeeds

**Step 4: Commit**

```bash
git add barter-data/src/exchange/hashkey/ barter-data/src/exchange/mod.rs
git commit -m "feat(data): add HashKey Connector and StreamSelector implementations"
```

---

## Phase 3: Order Execution (barter-execution)

### Task 8: Create HashKey Client Configuration

**Files:**
- Create: `barter-execution/src/client/hashkey/mod.rs`
- Modify: `barter-execution/src/client/mod.rs`

**Step 1: Create mod.rs with HashKeyConfig**

```rust
//! [`HashKey`] exchange [`ExecutionClient`] implementation.

use crate::{
    UnindexedAccountEvent, UnindexedAccountSnapshot,
    balance::AssetBalance,
    error::{UnindexedClientError, UnindexedOrderError},
    order::{
        Order,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::Open,
    },
    trade::Trade,
};
use barter_instrument::{
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use chrono::{DateTime, Utc};
use futures::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::future::Future;

mod config;
mod rest;
mod order;
mod account;
mod signer;
mod error;

pub use config::*;
pub use error::*;

/// [`HashKey`] exchange client configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HashKeyConfig {
    /// API key
    pub api_key: String,
    /// API secret
    pub api_secret: String,
    /// Use sandbox environment if true
    #[serde(default)]
    pub sandbox: bool,
}

/// [`HashKey`] REST API client.
#[derive(Debug)]
pub struct HashKeyClient {
    config: HashKeyConfig,
    http: Client,
    base_url: &'static str,
}

impl HashKeyClient {
    /// Create a new HashKey client.
    pub fn new(config: HashKeyConfig) -> Self {
        let base_url = if config.sandbox {
            "https://api-glb.sim.hashkeydev.com"
        } else {
            "https://api-glb.hashkey.com"
        };

        Self {
            config,
            http: Client::new(),
            base_url,
        }
    }
}

impl ExecutionClient for HashKeyClient {
    const EXCHANGE: ExchangeId = ExchangeId::HashKey;
    type Config = HashKeyConfig;
    type AccountStream = HashKeyAccountStream;

    fn new(config: Self::Config) -> Self {
        Self::new(config)
    }

    async fn account_snapshot(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        // TODO: Implement account snapshot via REST API
        Err(UnindexedClientError::Unimplemented)
    }

    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        // TODO: Implement WebSocket account stream
        Err(UnindexedClientError::Unimplemented)
    }

    async fn cancel_order(
        &self,
        _request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        // TODO: Implement order cancellation
        None
    }

    async fn open_order(
        &self,
        _request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        // TODO: Implement order placement
        None
    }

    async fn fetch_balances(
        &self,
        _assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        // TODO: Implement balance fetching
        Err(UnindexedClientError::Unimplemented)
    }

    async fn fetch_open_orders(
        &self,
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        // TODO: Implement open orders fetching
        Err(UnindexedClientError::Unimplemented)
    }

    async fn fetch_trades(
        &self,
        _time_since: DateTime<Utc>,
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        // TODO: Implement trades fetching
        Err(UnindexedClientError::Unimplemented)
    }
}

/// Placeholder for HashKey account stream (to be implemented).
pub struct HashKeyAccountStream;
```

**Step 2: Create config.rs**

```rust
//! HashKey client configuration types.

use serde::{Deserialize, Serialize};

/// HashKey API credentials.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HashKeyCredentials {
    pub api_key: String,
    pub api_secret: String,
}
```

**Step 3: Create error.rs**

```rust
//! HashKey client error types.

use thiserror::Error;

/// HashKey client specific errors.
#[derive(Debug, Error)]
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

    #[error("Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
}
```

**Step 4: Create signer.rs**

```rust
//! HMAC-SHA256 signing for HashKey API.

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Create HMAC-SHA256 signature for HashKey API request.
pub fn sign(api_secret: &str, message: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(api_secret.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
```

**Step 5: Create placeholder rest.rs, order.rs, account.rs**

```rust
// rest.rs
//! HashKey REST API client.

// TODO: Implement REST API methods

// order.rs
//! HashKey order types.

// TODO: Implement order types

// account.rs
//! HashKey account types.

// TODO: Implement account types
```

**Step 6: Add hashkey module to client/mod.rs**

In `barter-execution/src/client/mod.rs`, add:

```rust
mod hashkey;
```

**Step 7: Run cargo check**

Run: `cargo check -p barter-execution`
Expected: Compilation succeeds

**Step 8: Commit**

```bash
git add barter-execution/src/client/hashkey/ barter-execution/src/client/mod.rs
git commit -m "feat(execution): add HashKey ExecutionClient scaffold"
```

---

## Phase 4: Testing & Documentation

### Task 9: Add Integration Test Example

**Files:**
- Create: `barter-data/examples/hashkey_public_trades.rs`

**Step 1: Create example file**

```rust
//! Example of streaming public trades from HashKey.
//!
//! Run with: cargo run --example hashkey_public_trades

use barter_data::{
    exchange::hashkey::HashKey,
    subscription::trade::PublicTrades,
    MarketStream,
};
use barter_instrument::instrument::market_data::MarketDataInstrument;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create subscription for BTC-USDT trades
    let stream = barter_data::streams::ManagerBuilder::default()
        .subscribe([
            (HashKey::default(), "BTC", "USDT", PublicTrades::all()),
        ])
        .market_stream::<MarketDataInstrument, _, _, _, _>()
        .await?;

    println!("Streaming BTC-USDT trades from HashKey...");

    // Process trades
    futures::pin_mut!(stream);
    while let Some(event) = stream.next().await {
        println!("{:?}", event);
    }

    Ok(())
}
```

**Step 2: Run example (will fail without network, but should compile)**

Run: `cargo build --example hashkey_public_trades`
Expected: Compilation succeeds

**Step 3: Commit**

```bash
git add barter-data/examples/hashkey_public_trades.rs
git commit -m "feat(data): add HashKey public trades example"
```

---

### Task 10: Update Documentation

**Files:**
- Modify: `barter-data/src/exchange/mod.rs` (add HashKey to docs)
- Modify: `README.md` (add HashKey to supported exchanges list)

**Step 1: Update exchange mod.rs documentation**

Add HashKey to the list of supported exchanges in the module documentation.

**Step 2: Run final cargo check**

Run: `cargo check --workspace`
Expected: All packages compile successfully

**Step 3: Commit**

```bash
git add -A
git commit -m "docs: update documentation for HashKey integration"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Add ExchangeId::HashKey | `barter-instrument/src/exchange.rs` |
| 2 | Create channel types | `barter-data/src/exchange/hashkey/channel.rs` |
| 3 | Create market types | `barter-data/src/exchange/hashkey/market.rs` |
| 4 | Create subscription response | `barter-data/src/exchange/hashkey/subscription.rs` |
| 5 | Create trade types | `barter-data/src/exchange/hashkey/trade.rs` |
| 6 | Create order book types | `barter-data/src/exchange/hashkey/book.rs` |
| 7 | Create module root | `barter-data/src/exchange/hashkey/mod.rs` |
| 8 | Create ExecutionClient scaffold | `barter-execution/src/client/hashkey/` |
| 9 | Add example | `barter-data/examples/hashkey_public_trades.rs` |
| 10 | Update docs | Various |

## Notes

- Market data (barter-data) is fully implemented in this plan
- Execution client (barter-execution) provides a scaffold with placeholder implementations
- Full REST API implementation for execution can be added incrementally after this foundation is in place
