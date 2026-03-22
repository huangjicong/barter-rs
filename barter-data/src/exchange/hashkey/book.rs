use crate::{
    Identifier,
    books::{Level, OrderBook},
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::book::{OrderBookEvent, OrderBookL1},
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{de::datetime_utc_from_epoch_duration, subscription::SubscriptionId};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::trade::HashKeyMessage;

/// Terse type alias for a [`HashKey`](super::HashKey) L2 order book WebSocket message.
pub type HashKeyDepthMessage = HashKeyMessage<HashKeyDepthData>;

/// Terse type alias for a [`HashKey`](super::HashKey) L1 ticker WebSocket message.
pub type HashKeyTickerMessage = HashKeyMessage<HashKeyTickerData>;

/// [`HashKey`](super::HashKey) L2 order book depth WebSocket message.
///
/// ### Raw Payload Example
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
/// ```json
/// {
///   "symbol": "ETHUSDT",
///   "topic": "depth",
///   "data": [
///     {
///       "e": 301,
///       "s": "ETHUSDT",
///       "t": 1722873144371,
///       "v": "84661262_18",
///       "b": [["1650", "0.0864"], ["1649", "1.2"]],
///       "a": [["4085", "0.0074"], ["4086", "0.5"]],
///       "o": 0
///     }
///   ]
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HashKeyDepthData {
    /// Event type (e.g., 301 for depth)
    #[serde(rename = "e")]
    pub event_type: i32,

    /// Symbol (e.g., "ETHUSDT")
    #[serde(rename = "s")]
    pub symbol: String,

    /// Transaction time in milliseconds
    #[serde(rename = "t", deserialize_with = "de_i64_epoch_ms_as_datetime_utc")]
    pub time: DateTime<Utc>,

    /// Version/sequence ID (e.g., "84661262_18")
    #[serde(rename = "v")]
    pub version: String,

    /// Bids: array of [price, quantity] pairs
    #[serde(rename = "b", deserialize_with = "de_price_levels")]
    pub bids: Vec<HashKeyLevel>,

    /// Asks: array of [price, quantity] pairs
    #[serde(rename = "a", deserialize_with = "de_price_levels")]
    pub asks: Vec<HashKeyLevel>,

    /// Opcode (0 = snapshot, 1 = update)
    #[serde(rename = "o")]
    pub opcode: i32,
}

/// [`HashKey`](super::HashKey) L1 ticker WebSocket message.
///
/// ### Raw Payload Example
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
/// ```json
/// {
///   "symbol": "ETHUSDT",
///   "topic": "realtimes",
///   "data": [
///     {
///       "t": 1722864411064,
///       "s": "ETHUSDT",
///       "c": "2195",
///       "h": "2918.85",
///       "l": "2135.5",
///       "o": "2915.78",
///       "b": "2194.5",
///       "a": "2195"
///     }
///   ]
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HashKeyTickerData {
    /// Timestamp in milliseconds
    #[serde(rename = "t", deserialize_with = "de_i64_epoch_ms_as_datetime_utc")]
    pub time: DateTime<Utc>,

    /// Symbol (e.g., "ETHUSDT")
    #[serde(rename = "s")]
    pub symbol: String,

    /// Last/close price
    #[serde(rename = "c", deserialize_with = "barter_integration::de::de_str")]
    pub close: f64,

    /// High price
    #[serde(rename = "h", deserialize_with = "barter_integration::de::de_str")]
    pub high: f64,

    /// Low price
    #[serde(rename = "l", deserialize_with = "barter_integration::de::de_str")]
    pub low: f64,

    /// Open price
    #[serde(rename = "o", deserialize_with = "barter_integration::de::de_str")]
    pub open: f64,

    /// Best bid price
    #[serde(rename = "b", deserialize_with = "barter_integration::de::de_str")]
    pub best_bid: f64,

    /// Best ask price
    #[serde(rename = "a", deserialize_with = "barter_integration::de::de_str")]
    pub best_ask: f64,
}

/// [`HashKey`](super::HashKey) OrderBook level.
///
/// #### Raw Payload Examples
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
///
/// ```json
/// ["1650", "0.0864"]
/// ```
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HashKeyLevel {
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub amount: Decimal,
}

impl From<HashKeyLevel> for Level {
    fn from(level: HashKeyLevel) -> Self {
        Self {
            price: level.price,
            amount: level.amount,
        }
    }
}

/// Deserialize an `i64` milliseconds value as `DateTime<Utc>`.
fn de_i64_epoch_ms_as_datetime_utc<'de, D>(
    deserializer: D,
) -> Result<chrono::DateTime<chrono::Utc>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let epoch_ms: i64 = serde::de::Deserialize::deserialize(deserializer)?;
    Ok(datetime_utc_from_epoch_duration(Duration::from_millis(epoch_ms as u64)))
}

/// Deserialize HashKey price levels from array of [price, amount] pairs.
fn de_price_levels<'de, D>(deserializer: D) -> Result<Vec<HashKeyLevel>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let levels: Vec<[String; 2]> = serde::de::Deserialize::deserialize(deserializer)?;
    levels
        .into_iter()
        .map(|[price, amount]| {
            Ok(HashKeyLevel {
                price: price.parse().map_err(serde::de::Error::custom)?,
                amount: amount.parse().map_err(serde::de::Error::custom)?,
            })
        })
        .collect()
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, HashKeyDepthMessage)>
    for MarketIter<InstrumentKey, OrderBookEvent>
where
    InstrumentKey: Clone,
{
    fn from(
        (exchange, instrument, message): (ExchangeId, InstrumentKey, HashKeyDepthMessage),
    ) -> Self {
        // Process the first depth data entry (typically there's only one)
        message
            .data
            .into_iter()
            .map(|depth| {
                // Parse sequence from version string (e.g., "84661262_18" -> 84661262)
                let sequence = depth
                    .version
                    .split('_')
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                let orderbook = OrderBook::new(
                    sequence,
                    Some(depth.time),
                    depth.bids,
                    depth.asks,
                );

                // Determine if this is a snapshot or update based on opcode
                // o: 0 = snapshot, 1 = update
                let kind = if depth.opcode == 0 {
                    OrderBookEvent::Snapshot(orderbook)
                } else {
                    OrderBookEvent::Update(orderbook)
                };

                Ok(MarketEvent {
                    time_exchange: depth.time,
                    time_received: Utc::now(),
                    exchange,
                    instrument: instrument.clone(),
                    kind,
                })
            })
            .collect()
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, HashKeyTickerMessage)>
    for MarketIter<InstrumentKey, OrderBookL1>
where
    InstrumentKey: Clone,
{
    fn from(
        (exchange, instrument, message): (ExchangeId, InstrumentKey, HashKeyTickerMessage),
    ) -> Self {
        // Process the first ticker data entry (typically there's only one)
        message
            .data
            .into_iter()
            .map(|ticker| {
                // Create Level from best bid and ask prices
                // Note: HashKey ticker doesn't provide amounts, so we use 0
                let best_bid = if ticker.best_bid > 0.0 {
                    Some(Level::new(
                        Decimal::from_f64_retain(ticker.best_bid).unwrap_or_default(),
                        Decimal::ZERO,
                    ))
                } else {
                    None
                };

                let best_ask = if ticker.best_ask > 0.0 {
                    Some(Level::new(
                        Decimal::from_f64_retain(ticker.best_ask).unwrap_or_default(),
                        Decimal::ZERO,
                    ))
                } else {
                    None
                };

                Ok(MarketEvent {
                    time_exchange: ticker.time,
                    time_received: Utc::now(),
                    exchange,
                    instrument: instrument.clone(),
                    kind: OrderBookL1 {
                        last_update_time: ticker.time,
                        best_bid,
                        best_ask,
                    },
                })
            })
            .collect()
    }
}

/// Custom deserializer for HashKey depth message subscription_id.
/// Uses the depth channel constant.
pub fn de_hashkey_depth_symbol_as_subscription_id<'de, D>(
    deserializer: D,
) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let symbol: String = Deserialize::deserialize(deserializer)?;
    Ok(ExchangeSub::from((super::channel::HashKeyChannel::DEPTH.as_ref(), symbol.as_str())).id())
}

/// Custom deserializer for HashKey ticker message subscription_id.
/// Uses the realtime channel constant.
pub fn de_hashkey_ticker_symbol_as_subscription_id<'de, D>(
    deserializer: D,
) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let symbol: String = Deserialize::deserialize(deserializer)?;
    Ok(ExchangeSub::from((super::channel::HashKeyChannel::REALTIME.as_ref(), symbol.as_str())).id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    mod de {
        use super::*;

        #[test]
        fn test_hashkey_level() {
            let input = r#"["1650", "0.0864"]"#;
            assert_eq!(
                serde_json::from_str::<HashKeyLevel>(input).unwrap(),
                HashKeyLevel {
                    price: dec!(1650),
                    amount: dec!(0.0864)
                },
            )
        }

        #[test]
        fn test_hashkey_depth_message() {
            let input = r#"
            {
                "symbol": "ETHUSDT",
                "topic": "depth",
                "data": [
                    {
                        "e": 301,
                        "s": "ETHUSDT",
                        "t": 1722873144371,
                        "v": "84661262_18",
                        "b": [["1650", "0.0864"], ["1649", "1.2"]],
                        "a": [["4085", "0.0074"], ["4086", "0.5"]],
                        "o": 0
                    }
                ]
            }
            "#;

            let message = serde_json::from_str::<HashKeyDepthMessage>(input);

            match message {
                Ok(msg) => {
                    assert_eq!(msg.data.len(), 1);
                    let depth = &msg.data[0];
                    assert_eq!(depth.symbol, "ETHUSDT");
                    assert_eq!(depth.event_type, 301);
                    assert_eq!(depth.version, "84661262_18");
                    assert_eq!(depth.opcode, 0);
                    assert_eq!(depth.bids.len(), 2);
                    assert_eq!(depth.asks.len(), 2);
                    assert_eq!(depth.bids[0].price, dec!(1650));
                    assert_eq!(depth.bids[0].amount, dec!(0.0864));
                    assert_eq!(depth.bids[1].price, dec!(1649));
                    assert_eq!(depth.bids[1].amount, dec!(1.2));
                    assert_eq!(depth.asks[0].price, dec!(4085));
                    assert_eq!(depth.asks[0].amount, dec!(0.0074));
                }
                Err(e) => {
                    panic!("Failed to deserialize: {:?}", e);
                }
            }
        }

        #[test]
        fn test_hashkey_ticker_message() {
            let input = r#"
            {
                "symbol": "ETHUSDT",
                "topic": "realtimes",
                "data": [
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
                ]
            }
            "#;

            let message = serde_json::from_str::<HashKeyTickerMessage>(input);

            match message {
                Ok(msg) => {
                    assert_eq!(msg.data.len(), 1);
                    let ticker = &msg.data[0];
                    assert_eq!(ticker.symbol, "ETHUSDT");
                    assert_eq!(ticker.close, 2195.0);
                    assert_eq!(ticker.high, 2918.85);
                    assert_eq!(ticker.low, 2135.5);
                    assert_eq!(ticker.open, 2915.78);
                    assert_eq!(ticker.best_bid, 2194.5);
                    assert_eq!(ticker.best_ask, 2195.0);
                }
                Err(e) => {
                    panic!("Failed to deserialize: {:?}", e);
                }
            }
        }

        #[test]
        fn test_hashkey_depth_market_iter() {
            let input = r#"
            {
                "symbol": "ETHUSDT",
                "topic": "depth",
                "data": [
                    {
                        "e": 301,
                        "s": "ETHUSDT",
                        "t": 1722873144371,
                        "v": "84661262_18",
                        "b": [["1650", "0.0864"], ["1649", "1.2"]],
                        "a": [["4085", "0.0074"], ["4086", "0.5"]],
                        "o": 0
                    }
                ]
            }
            "#;

            let depth_msg = serde_json::from_str::<HashKeyDepthMessage>(input).unwrap();
            let iter: MarketIter<String, OrderBookEvent> = MarketIter::from((
                ExchangeId::HashKey,
                "ETHUSDT".to_string(),
                depth_msg,
            ));

            assert_eq!(iter.0.len(), 1);

            let event = iter.0[0].as_ref().unwrap();
            assert_eq!(event.exchange, ExchangeId::HashKey);
            assert_eq!(event.instrument, "ETHUSDT");

            match &event.kind {
                OrderBookEvent::Snapshot(book) => {
                    assert_eq!(book.sequence(), 84661262);
                    assert_eq!(book.bids().levels().len(), 2);
                    assert_eq!(book.asks().levels().len(), 2);
                }
                OrderBookEvent::Update(_) => {
                    panic!("Expected Snapshot event");
                }
            }
        }

        #[test]
        fn test_hashkey_depth_update_market_iter() {
            // Test with opcode 1 (update)
            let input = r#"
            {
                "symbol": "ETHUSDT",
                "topic": "depth",
                "data": [
                    {
                        "e": 301,
                        "s": "ETHUSDT",
                        "t": 1722873144371,
                        "v": "84661263_19",
                        "b": [["1650", "0.1"]],
                        "a": [["4085", "0.01"]],
                        "o": 1
                    }
                ]
            }
            "#;

            let depth_msg = serde_json::from_str::<HashKeyDepthMessage>(input).unwrap();
            let iter: MarketIter<String, OrderBookEvent> = MarketIter::from((
                ExchangeId::HashKey,
                "ETHUSDT".to_string(),
                depth_msg,
            ));

            assert_eq!(iter.0.len(), 1);

            let event = iter.0[0].as_ref().unwrap();
            match &event.kind {
                OrderBookEvent::Update(book) => {
                    assert_eq!(book.sequence(), 84661263);
                }
                OrderBookEvent::Snapshot(_) => {
                    panic!("Expected Update event");
                }
            }
        }

        #[test]
        fn test_hashkey_ticker_market_iter() {
            let input = r#"
            {
                "symbol": "ETHUSDT",
                "topic": "realtimes",
                "data": [
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
                ]
            }
            "#;

            let ticker_msg = serde_json::from_str::<HashKeyTickerMessage>(input).unwrap();
            let iter: MarketIter<String, OrderBookL1> = MarketIter::from((
                ExchangeId::HashKey,
                "ETHUSDT".to_string(),
                ticker_msg,
            ));

            assert_eq!(iter.0.len(), 1);

            let event = iter.0[0].as_ref().unwrap();
            assert_eq!(event.exchange, ExchangeId::HashKey);
            assert_eq!(event.instrument, "ETHUSDT");

            let l1 = &event.kind;
            assert!(l1.best_bid.is_some());
            assert!(l1.best_ask.is_some());
            assert_eq!(l1.best_bid.unwrap().price, dec!(2194.5));
            assert_eq!(l1.best_ask.unwrap().price, dec!(2195));
        }
    }
}
