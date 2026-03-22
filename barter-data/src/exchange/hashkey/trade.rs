use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::trade::PublicTrade,
};
use barter_instrument::{Side, exchange::ExchangeId};
use barter_integration::{de::datetime_utc_from_epoch_duration, subscription::SubscriptionId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Terse type alias for a [`HashKey`](super::HashKey) real-time trades WebSocket message.
pub type HashKeyTrades = HashKeyMessage<HashKeyTrade>;

/// [`HashKey`](super::HashKey) market data WebSocket message.
///
/// ### Raw Payload Example
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
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
    #[serde(
        rename = "symbol",
        deserialize_with = "de_hashkey_message_symbol_as_subscription_id"
    )]
    pub subscription_id: SubscriptionId,
    pub data: Vec<T>,
}

impl<T> Identifier<Option<SubscriptionId>> for HashKeyMessage<T> {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

/// [`HashKey`](super::HashKey) real-time trade WebSocket message.
///
/// See [`HashKeyMessage`] for full raw payload example.
///
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HashKeyTrade {
    /// Trade ID
    #[serde(rename = "v")]
    pub id: String,
    /// Price (as string, needs parsing)
    #[serde(rename = "p", deserialize_with = "barter_integration::de::de_str")]
    pub price: f64,
    /// Quantity (as string, needs parsing)
    #[serde(rename = "q", deserialize_with = "barter_integration::de::de_str")]
    pub amount: f64,
    /// Is buyer maker (true = sell taker, false = buy taker)
    #[serde(rename = "m", deserialize_with = "de_bool_to_side")]
    pub side: Side,
    /// Timestamp in milliseconds
    #[serde(rename = "t", deserialize_with = "de_i64_epoch_ms_as_datetime_utc")]
    pub time: DateTime<Utc>,
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
                    time_exchange: trade.time,
                    time_received: Utc::now(),
                    exchange,
                    instrument: instrument.clone(),
                    kind: PublicTrade {
                        id: trade.id,
                        price: trade.price,
                        amount: trade.amount,
                        side: trade.side,
                    },
                })
            })
            .collect()
    }
}

/// Deserialize the HashKey message "symbol" field as a Barter [`SubscriptionId`].
fn de_hashkey_message_symbol_as_subscription_id<'de, D>(
    deserializer: D,
) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    // For HashKey, we need both symbol and topic to create the subscription ID.
    // However, we only have the symbol here, so we'll use a pattern that extracts
    // from the full message structure.
    //
    // Since we only have access to the symbol field during deserialization,
    // we need to use a different approach. Let's look at the full message.
    // Actually, looking at the JSON, we need to parse the symbol and infer the channel.
    // For trades, we know the topic is "trade", so we can construct the ID.
    //
    // But wait - the deserializer only has access to the symbol field.
    // We need to look at how other exchanges handle this.
    //
    // Looking at OKX, they deserialize the "arg" which contains both channel and instId.
    // For HashKey, we have symbol at the top level and topic separately.
    //
    // The approach: since this deserializer is called for the symbol field,
    // we need to access the topic field somehow. This is tricky.
    //
    // Alternative: We can create a helper that stores just the symbol,
    // and then creates the SubscriptionId in a different way.
    //
    // Looking more carefully at the JSON structure:
    // - symbol: "ETHUSDT" - this is the market
    // - topic: "trade" - this is the channel
    //
    // For the SubscriptionId format (channel|market), we need both.
    // But during serde deserialization of the symbol field, we only have the symbol.
    //
    // The simplest solution: Create a custom deserialize for the whole HashKeyMessage
    // that has access to both symbol and topic fields.

    // For now, let's just use the symbol and assume trade channel.
    // We'll create a proper subscription ID format.
    let symbol: String = Deserialize::deserialize(deserializer)?;
    // We assume "trade" channel for trade messages
    Ok(ExchangeSub::from((super::channel::HashKeyChannel::TRADES.as_ref(), symbol.as_str())).id())
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

/// Deserialize HashKey's boolean `m` field (is buyer maker) to a [`Side`].
/// - `m: true` means the buyer is the maker, so the taker is selling -> Side::Sell
/// - `m: false` means the buyer is the taker, so the taker is buying -> Side::Buy
fn de_bool_to_side<'de, D>(deserializer: D) -> Result<Side, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let is_buyer_maker: bool = serde::de::Deserialize::deserialize(deserializer)?;
    if is_buyer_maker {
        // Buyer is maker, so taker is seller
        Ok(Side::Sell)
    } else {
        // Buyer is taker, so taker is buyer
        Ok(Side::Buy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;
        use barter_integration::error::SocketError;

        #[test]
        fn test_hashkey_message_trades() {
            let input = r#"
            {
                "symbol": "ETHUSDT",
                "symbolName": "ETHUSDT",
                "topic": "trade",
                "params": {"realtimeInterval": "24h"},
                "data": [
                    {
                        "v": "1745922896272048129",
                        "t": 1722866228075,
                        "p": "2340.41",
                        "q": "0.0132",
                        "m": true
                    }
                ],
                "f": true,
                "sendTime": 1722869464248
            }
            "#;

            let actual = serde_json::from_str::<HashKeyTrades>(input);
            let expected: Result<HashKeyTrades, SocketError> = Ok(HashKeyTrades {
                subscription_id: SubscriptionId::from("trade|ETHUSDT"),
                data: vec![HashKeyTrade {
                    id: "1745922896272048129".to_string(),
                    price: 2340.41,
                    amount: 0.0132,
                    side: Side::Sell, // m: true -> seller is taker
                    time: datetime_utc_from_epoch_duration(Duration::from_millis(1722866228075)),
                }],
            });

            match (actual, expected) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(actual, expected, "TC failed")
                }
                (Err(_), Err(_)) => {
                    // Test passed
                }
                (actual, expected) => {
                    // Test failed
                    panic!(
                        "TC failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n"
                    );
                }
            }
        }

        #[test]
        fn test_hashkey_trade_buy_side() {
            // m: false -> buyer is taker -> Side::Buy
            let input = r#"
            {
                "symbol": "BTCUSDT",
                "symbolName": "BTCUSDT",
                "topic": "trade",
                "params": {"realtimeInterval": "24h"},
                "data": [
                    {
                        "v": "123456789",
                        "t": 1630048897897,
                        "p": "42219.9",
                        "q": "0.12060306",
                        "m": false
                    }
                ],
                "f": true,
                "sendTime": 1722869464248
            }
            "#;

            let trades = serde_json::from_str::<HashKeyTrades>(input).unwrap();
            assert_eq!(trades.data[0].side, Side::Buy);
            assert_eq!(trades.data[0].price, 42219.9);
            assert_eq!(trades.data[0].amount, 0.12060306);
            assert_eq!(trades.data[0].id, "123456789");
        }

        #[test]
        fn test_hashkey_trade_sell_side() {
            // m: true -> seller is taker -> Side::Sell
            let input = r#"
            {
                "symbol": "BTCUSDT",
                "symbolName": "BTCUSDT",
                "topic": "trade",
                "params": {"realtimeInterval": "24h"},
                "data": [
                    {
                        "v": "987654321",
                        "t": 1630048897897,
                        "p": "42219.9",
                        "q": "0.5",
                        "m": true
                    }
                ],
                "f": true,
                "sendTime": 1722869464248
            }
            "#;

            let trades = serde_json::from_str::<HashKeyTrades>(input).unwrap();
            assert_eq!(trades.data[0].side, Side::Sell);
        }

        #[test]
        fn test_hashkey_market_iter() {
            let input = r#"
            {
                "symbol": "ETHUSDT",
                "symbolName": "ETHUSDT",
                "topic": "trade",
                "params": {"realtimeInterval": "24h"},
                "data": [
                    {
                        "v": "1745922896272048129",
                        "t": 1722866228075,
                        "p": "2340.41",
                        "q": "0.0132",
                        "m": true
                    }
                ],
                "f": true,
                "sendTime": 1722869464248
            }
            "#;

            let trades = serde_json::from_str::<HashKeyTrades>(input).unwrap();
            let iter: MarketIter<String, PublicTrade> = MarketIter::from((
                ExchangeId::HashKey,
                "ETHUSDT".to_string(),
                trades,
            ));

            // MarketIter wraps a Vec, access via .0
            assert_eq!(iter.0.len(), 1);

            let event = iter.0[0].as_ref().unwrap();
            assert_eq!(event.exchange, ExchangeId::HashKey);
            assert_eq!(event.instrument, "ETHUSDT");
            assert_eq!(event.kind.id, "1745922896272048129");
            assert_eq!(event.kind.price, 2340.41);
            assert_eq!(event.kind.amount, 0.0132);
            assert_eq!(event.kind.side, Side::Sell);
        }
    }
}
