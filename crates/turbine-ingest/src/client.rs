//! Shared Yellowstone gRPC client builder for Geyser and deshred streams.

use std::time::Duration;

use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;

use turbine_core::config::Config;
use turbine_core::error::{Result, TurbineError};

/// Connect a [`GeyserGrpcClient`] using workspace Geyser settings.
pub async fn connect(cfg: &Config) -> Result<GeyserGrpcClient> {
    let tls = ClientTlsConfig::new().with_native_roots();
    GeyserGrpcClient::build_from_shared(cfg.geyser.endpoint.clone())
        .map_err(|e| TurbineError::Ingest(format!("geyser builder: {e}")))?
        .x_token(cfg.geyser.x_token.clone())
        .map_err(|e| TurbineError::Ingest(format!("geyser x_token: {e}")))?
        .tls_config(tls)
        .map_err(|e| TurbineError::Ingest(format!("geyser tls: {e}")))?
        .max_decoding_message_size(cfg.geyser.max_decoding_message_size_bytes)
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .keep_alive_while_idle(true)
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .map_err(|e| TurbineError::Ingest(format!("geyser connect: {e}")))
}
