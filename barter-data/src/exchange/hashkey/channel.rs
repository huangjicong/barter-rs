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
