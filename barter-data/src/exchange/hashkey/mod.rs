use self::{
    book::{HashKeyDepthMessage, HashKeyTickerMessage},
    channel::HashKeyChannel,
    market::HashKeyMarket,
    subscription::HashKeySubResponse,
    trade::HashKeyTrades,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, PingInterval, StreamSelector, subscription::ExchangeSub},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{
        book::{OrderBooksL1, OrderBooksL2},
        trade::PublicTrades,
    },
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocketSerdeParser, WsMessage},
};
use barter_macro::{DeExchange, SerExchange};
use derive_more::Display;
use serde_json::json;
use std::time::Duration;
use url::Url;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// Defines the [`HashKey`](super::HashKey) WebSocket subscription response type that
/// validates subscription confirmations.
pub mod subscription;

/// Defines the [`HashKey`](super::HashKey) real-time trade WebSocket message type.
pub mod trade;

/// Defines the [`HashKey`](super::HashKey) L1 and L2 order book WebSocket message types.
pub mod book;

/// [`HashKey`] server base WebSocket URL (production).
///
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
pub const BASE_URL_HASHKEY: &str = "wss://stream-glb.hashkey.com/quote/ws/v1";

/// [`HashKey`] server base WebSocket URL (sandbox).
///
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
pub const BASE_URL_HASHKEY_SANDBOX: &str = "wss://stream-glb.sim.hashkeydev.com/quote/ws/v1";

/// [`HashKey`] server [`PingInterval`] duration.
///
/// HashKey requires WebSocket ping messages every 10 seconds.
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
pub const PING_INTERVAL_HASHKEY: Duration = Duration::from_secs(10);

/// Convenient type alias for a HashKey [`ExchangeWsStream`] using [`WebSocketSerdeParser`](barter_integration::protocol::websocket::WebSocketSerdeParser).
pub type HashKeyWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// [`HashKey`] exchange.
///
/// Supports both production and sandbox environments.
///
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    Display,
    DeExchange,
    SerExchange,
)]
pub struct HashKey {
    /// Use sandbox environment if true.
    pub sandbox: bool,
}

impl HashKey {
    /// Create a new HashKey connector for production environment.
    pub fn new() -> Self {
        Self { sandbox: false }
    }

    /// Create a new HashKey connector for sandbox environment.
    pub fn sandbox() -> Self {
        Self { sandbox: true }
    }
}

impl Connector for HashKey {
    const ID: ExchangeId = ExchangeId::HashKey;
    type Channel = HashKeyChannel;
    type Market = HashKeyMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = HashKeySubResponse;

    fn url() -> Result<Url, SocketError> {
        // Note: The sandbox flag is evaluated at runtime via the struct instance.
        // For the Connector trait, we use the production URL by default.
        // Consumers can override this by providing a custom URL in their configuration.
        Url::parse(BASE_URL_HASHKEY).map_err(SocketError::UrlParse)
    }

    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: tokio::time::interval(PING_INTERVAL_HASHKEY),
            ping: || {
                // HashKey ping format: {"ping": <timestamp>}
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis();
                WsMessage::text(json!({ "ping": timestamp }).to_string())
            },
        })
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        // HashKey subscription format:
        // {"symbol": "BTCUSDT", "topic": "trade", "event": "sub"}
        // Each subscription is sent as a separate message.
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
    type Stream =
        HashKeyWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, HashKeyTrades>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL1> for HashKey
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        HashKeyWsStream<StatelessTransformer<Self, Instrument::Key, OrderBooksL1, HashKeyTickerMessage>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for HashKey
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        HashKeyWsStream<StatelessTransformer<Self, Instrument::Key, OrderBooksL2, HashKeyDepthMessage>>;
}
