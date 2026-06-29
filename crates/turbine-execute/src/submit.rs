//! Bundle submitters (plan §7.4).
//!
//! - [`Submitter::DryRun`] — builds nothing on the wire (no funds move).
//! - [`Submitter::Grpc`] — persistent gRPC `SearcherService::send_bundle` (primary,
//!   lowest latency) with JSON-RPC `sendBundle` as automatic fallback.
//! - [`Submitter::Http`] — JSON-RPC `sendBundle` only (when no gRPC channel).

use jito_sdk_rust::JitoJsonRpcSDK;
use serde_json::json;
use tracing::warn;

use turbine_core::error::{Result, TurbineError};

use crate::compiler::CompiledBundle;
use crate::searcher::{self, SearcherClient};

/// Result of a submit attempt.
#[derive(Debug, Clone)]
pub struct SubmitOutcome {
    /// Jito bundle id (None in dry-run).
    pub bundle_id: Option<String>,
    pub dry_run: bool,
    /// True when the primary gRPC path failed and we used the HTTP fallback.
    pub used_fallback: bool,
}

/// Submission backend.
pub enum Submitter {
    DryRun,
    Http(Box<JitoJsonRpcSDK>),
    Grpc {
        client: SearcherClient,
        fallback: Box<JitoJsonRpcSDK>,
    },
}

impl Submitter {
    /// HTTP-only submitter against the Jito JSON-RPC bundles endpoint.
    pub fn http(json_rpc_url: &str, uuid: Option<String>) -> Self {
        Submitter::Http(Box::new(JitoJsonRpcSDK::new(json_rpc_url, uuid)))
    }

    /// gRPC primary with a JSON-RPC fallback.
    pub fn grpc(client: SearcherClient, json_rpc_url: &str, uuid: Option<String>) -> Self {
        Submitter::Grpc {
            client,
            fallback: Box::new(JitoJsonRpcSDK::new(json_rpc_url, uuid)),
        }
    }

    pub fn is_dry_run(&self) -> bool {
        matches!(self, Submitter::DryRun)
    }

    /// Submit the compiled bundle.
    pub async fn submit(&self, bundle: &CompiledBundle) -> Result<SubmitOutcome> {
        match self {
            Submitter::DryRun => {
                Ok(SubmitOutcome { bundle_id: None, dry_run: true, used_fallback: false })
            }
            Submitter::Http(sdk) => {
                let bundle_id = http_send(sdk, &bundle.base64).await?;
                Ok(SubmitOutcome { bundle_id, dry_run: false, used_fallback: false })
            }
            Submitter::Grpc { client, fallback } => {
                // Primary: gRPC (clone shares the warm channel).
                let mut c = client.clone();
                match searcher::send_bundle(&mut c, &bundle.raw).await {
                    Ok(uuid) => Ok(SubmitOutcome {
                        bundle_id: Some(uuid),
                        dry_run: false,
                        used_fallback: false,
                    }),
                    Err(e) => {
                        warn!("gRPC send_bundle failed ({e}); falling back to JSON-RPC");
                        let bundle_id = http_send(fallback, &bundle.base64).await?;
                        Ok(SubmitOutcome { bundle_id, dry_run: false, used_fallback: true })
                    }
                }
            }
        }
    }
}

async fn http_send(sdk: &JitoJsonRpcSDK, base64_txs: &[String]) -> Result<Option<String>> {
    let params = json!([base64_txs, { "encoding": "base64" }]);
    let resp = sdk
        .send_bundle(Some(params), None)
        .await
        .map_err(|e| TurbineError::Execute(format!("jito send_bundle: {e}")))?;
    Ok(resp.get("result").and_then(|v| v.as_str()).map(String::from))
}
