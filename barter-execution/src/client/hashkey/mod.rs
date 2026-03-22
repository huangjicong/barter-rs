//! [`HashKey`] exchange [`ExecutionClient`] implementation.

mod config;
mod error;
mod signer;

pub use config::*;
pub use error::*;

use crate::{
    UnindexedAccountSnapshot,
    balance::AssetBalance,
    client::ExecutionClient,
    error::{ConnectivityError, UnindexedClientError, UnindexedOrderError},
    order::{
        Order,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::Open,
    },
    trade::Trade,
};
use barter_instrument::{
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use reqwest::Client;

/// [`HashKey`] REST API client.
#[derive(Debug, Clone)]
pub struct HashKeyClient {
    config: HashKeyConfig,
    http: Client,
    base_url: &'static str,
}

impl HashKeyClient {
    /// Create a new HashKey client.
    pub fn new(config: HashKeyConfig) -> Self {
        let base_url = if config.sandbox {
            "https://api-glb.sim.hashkeydev.com"
        } else {
            "https://api-glb.hashkey.com"
        };

        Self {
            config,
            http: Client::new(),
            base_url,
        }
    }
}

impl ExecutionClient for HashKeyClient {
    const EXCHANGE: ExchangeId = ExchangeId::HashKey;
    type Config = HashKeyConfig;
    type AccountStream = BoxStream<'static, crate::UnindexedAccountEvent>;

    fn new(config: Self::Config) -> Self {
        Self::new(config)
    }

    async fn account_snapshot(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        // TODO: Implement via GET /api/v1/account
        Err(UnindexedClientError::AccountSnapshot(
            "not yet implemented".to_string(),
        ))
    }

    async fn account_stream(
        &self,
        _assets: &[AssetNameExchange],
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        // TODO: Implement WebSocket private stream
        Err(UnindexedClientError::AccountStream(
            "not yet implemented".to_string(),
        ))
    }

    async fn cancel_order(
        &self,
        _request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        // TODO: Implement via DELETE /api/v1/order
        None
    }

    async fn open_order(
        &self,
        _request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        // TODO: Implement via POST /api/v1/order
        None
    }

    async fn fetch_balances(
        &self,
        _assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        // TODO: Implement via GET /api/v1/account
        Err(UnindexedClientError::Connectivity(
            ConnectivityError::ExchangeOffline(ExchangeId::HashKey),
        ))
    }

    async fn fetch_open_orders(
        &self,
        _instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        // TODO: Implement via GET /api/v1/openOrders
        Err(UnindexedClientError::Connectivity(
            ConnectivityError::ExchangeOffline(ExchangeId::HashKey),
        ))
    }

    async fn fetch_trades(
        &self,
        _time_since: DateTime<Utc>,
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        // TODO: Implement via GET /api/v1/myTrades
        Err(UnindexedClientError::Connectivity(
            ConnectivityError::ExchangeOffline(ExchangeId::HashKey),
        ))
    }
}
