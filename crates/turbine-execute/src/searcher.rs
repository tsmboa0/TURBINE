//! Jito searcher gRPC client (plan §7.4).
//!
//! Keyless connect to the Block Engine. Auth is not required for bundle submission;
//! bundle results may arrive via the optional gRPC stream or JSON-RPC polling.

use std::time::Duration;

use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::info;

use turbine_core::error::{Result, TurbineError};

use crate::pb::bundle::Bundle;
use crate::pb::packet::{Meta, Packet};
use crate::pb::searcher::searcher_service_client::SearcherServiceClient;
use crate::pb::searcher::{NextScheduledLeaderRequest, NextScheduledLeaderResponse, SendBundleRequest};

/// Searcher client (keyless). Cloning shares the HTTP/2 channel.
pub type SearcherClient = SearcherServiceClient<Channel>;

async fn connect_channel(block_engine_url: &str) -> Result<Channel> {
    let mut endpoint = Endpoint::from_shared(block_engine_url.to_string())
        .map_err(|e| TurbineError::Execute(format!("bad block engine url: {e}")))?
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .http2_keep_alive_interval(Duration::from_secs(15))
        .keep_alive_while_idle(true)
        .connect_timeout(Duration::from_secs(10));

    if block_engine_url.starts_with("https://") {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|e| TurbineError::Execute(format!("tls config: {e}")))?;
    }

    endpoint
        .connect()
        .await
        .map_err(|e| TurbineError::Execute(format!("connect block engine: {e}")))
}

/// Connect a persistent keyless gRPC channel to the block engine.
pub async fn connect(block_engine_url: &str) -> Result<SearcherClient> {
    let channel = connect_channel(block_engine_url).await?;
    info!(endpoint = %block_engine_url, "Jito searcher gRPC channel ready (keyless)");
    Ok(SearcherServiceClient::new(channel))
}

/// Wrap raw transaction wire bytes into a Jito `Packet`.
pub fn packet_from_bytes(bytes: Vec<u8>) -> Packet {
    let size = bytes.len() as u64;
    Packet {
        data: bytes,
        meta: Some(Meta {
            size,
            addr: String::new(),
            port: 0,
            flags: None,
            sender_stake: 0,
        }),
    }
}

/// Submit a bundle (raw tx wire bytes) over gRPC. Returns the server bundle id.
pub async fn send_bundle(client: &mut SearcherClient, raw_txs: &[Vec<u8>]) -> Result<String> {
    let packets = raw_txs.iter().cloned().map(packet_from_bytes).collect();
    let req = SendBundleRequest {
        bundle: Some(Bundle { header: None, packets }),
    };
    let resp = client
        .send_bundle(req)
        .await
        .map_err(|e| TurbineError::Execute(format!("grpc send_bundle: {e}")))?;
    Ok(resp.into_inner().uuid)
}

/// Query the next Jito-enabled leader directly over the warm gRPC connection.
pub async fn next_scheduled_leader(
    client: &mut SearcherClient,
    regions: Vec<String>,
) -> Result<NextScheduledLeaderResponse> {
    let resp = client
        .get_next_scheduled_leader(NextScheduledLeaderRequest { regions })
        .await
        .map_err(|e| TurbineError::Execute(format!("grpc get_next_scheduled_leader: {e}")))?;
    Ok(resp.into_inner())
}
