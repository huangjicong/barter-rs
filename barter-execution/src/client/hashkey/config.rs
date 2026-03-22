//! HashKey client configuration types.

use serde::{Deserialize, Serialize};

/// [`HashKey`](super::HashKey) exchange client configuration.
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
