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
