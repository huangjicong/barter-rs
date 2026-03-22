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
