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

use barter_instrument::exchange::ExchangeId;
use derive_more::Display;

/// [`HashKey`] exchange.
///
/// See docs: <https://hashkeyglobal-apidoc.readme.io/reference/websocket-api>
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Display,
)]
pub struct HashKey;

impl HashKey {
    pub const ID: ExchangeId = ExchangeId::HashKey;
}

// Note: The Connector implementation will be added in a later task
