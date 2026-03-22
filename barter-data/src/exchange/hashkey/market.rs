use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use barter_instrument::{
    Keyed,
    instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt, format_smolstr};

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
