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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign() {
        let secret = "test_secret";
        let message = "test_message";
        let signature = sign(secret, message);
        // Verify signature is hex encoded and non-empty
        assert!(!signature.is_empty());
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
