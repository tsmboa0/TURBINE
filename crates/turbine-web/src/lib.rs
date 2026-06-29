//! `turbine-web` — Component 5 web telemetry surface (plan §9.4).
//!
//! A read-only `axum` server on the **services runtime** that exposes the deep,
//! verbose surface the TUI deliberately omits: an animated architecture diagram,
//! full transaction history (with explorer links + per-state latency deltas), the
//! AI reasoning/fix log, and aggregate stats.
//!
//! Two data paths, both **cold** and lossy-safe — a slow browser can never stall
//! the engine:
//! - **Live current-state** is forwarded straight off the same lossy telemetry
//!   [`broadcast`] bus the TUI uses (slot/leader/tip/bid/contention/health/stats).
//!   If a client lags, frames are dropped (`Lagged` is ignored).
//! - **History** (bundles + AI decisions + aggregates) is snapshotted from
//!   [`HotState`] on a fixed cadence and pushed as a single `history` message, so
//!   late-joining clients backfill immediately and then stay current.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use serde::Serialize;
use serde_json::json;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use turbine_core::ai::AiDecisionRecord;
use turbine_core::events::TelemetryEvent;
use turbine_core::transaction_record::{tip_delta_lamports, tip_floor_lamports};
use turbine_core::types::LifecycleState;
use turbine_state::HotState;

/// How often the server pushes a fresh history snapshot (bundles + AI + stats).
/// Live current-state arrives faster off the bus; history events are infrequent.
const HISTORY_TICK: Duration = Duration::from_millis(400);

/// Shared, cheaply-cloned server state.
#[derive(Clone)]
pub struct WebState {
    pub state: Arc<HotState>,
    pub bus: broadcast::Sender<TelemetryEvent>,
    pub dry_run: bool,
    pub jito_connected: bool,
}

/// Serve the web studio until the listener errors (the caller aborts the task on
/// shutdown). Binds `bind` (e.g. `127.0.0.1:9000`) with `SO_REUSEADDR`.
pub async fn serve(bind: &str, web: WebState) -> std::io::Result<()> {
    let addr: SocketAddr = bind
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("web_bind {bind:?}: {e}")))?;

    let listener = bind_reuse(addr).await?;

    let router = Router::new()
        .route("/", get(index))
        .route("/stream", get(ws_upgrade))
        .with_state(web);

    info!(%addr, "web studio listening — http://{addr}");
    axum::serve(listener, router).await
}

async fn bind_reuse(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, None)?;
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    socket.listen(128)?;
    socket.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(socket.into())
}

async fn index() -> impl IntoResponse {
    Html(include_str!("index.html"))
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(web): State<WebState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| client(socket, web))
}

/// One connected browser: push an initial history snapshot, then multiplex live
/// bus events and periodic history snapshots until the socket closes.
async fn client(mut socket: WebSocket, web: WebState) {
    let mut rx = web.bus.subscribe();
    debug!("web client connected");

    // Backfill immediately so a late client isn't blank until the next event.
    if send_json(&mut socket, &meta_msg(&web)).await.is_err() {
        return;
    }
    if send_json(&mut socket, &history_msg(&web.state)).await.is_err() {
        return;
    }

    let mut history = tokio::time::interval(HISTORY_TICK);
    history.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                Ok(ev) => {
                    if send_json(&mut socket, &ev).await.is_err() {
                        break;
                    }
                }
                // Lossy bus: the client fell behind — skip dropped frames.
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    debug!(skipped = n, "web client lagged; dropping frames");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            _ = history.tick() => {
                if send_json(&mut socket, &history_msg(&web.state)).await.is_err() {
                    break;
                }
            }
            inbound = socket.recv() => match inbound {
                // The client only listens; any close/error ends the session.
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                _ => {}
            }
        }
    }
    debug!("web client disconnected");
}

async fn send_json<T: Serialize>(socket: &mut WebSocket, value: &T) -> Result<(), ()> {
    match serde_json::to_string(value) {
        Ok(txt) => socket.send(Message::Text(txt)).await.map_err(|_| ()),
        Err(e) => {
            warn!("web serialize error: {e}");
            Ok(())
        }
    }
}

/// One-off connection metadata (mode + subsystem wiring) for the header/diagram.
fn meta_msg(web: &WebState) -> serde_json::Value {
    json!({
        "kind": "meta",
        "data": { "dry_run": web.dry_run, "jito_connected": web.jito_connected }
    })
}

// ── History snapshot (the verbose surface) ──────────────────────────────────

#[derive(Serialize)]
struct BundleDto {
    id: u64,
    bundle_id: Option<String>,
    signature: Option<String>,
    state: LifecycleState,
    tip_floor_lamports: Option<u64>,
    tip_lamports: Option<u64>,
    tip_delta_lamports: Option<i64>,
    percentile: Option<String>,
    landed_slot: Option<u64>,
    attempt: u8,
    last_error: Option<String>,
    ai_classification: Option<String>,
    submit_to_processed_ms: Option<f64>,
    processed_to_confirmed_ms: Option<f64>,
    confirmed_to_finalized_ms: Option<f64>,
}

#[derive(Serialize)]
struct Aggregate {
    tracked: u64,
    in_flight: u64,
    landed: u64,
    failed: u64,
    success_rate: f64,
    ai_decisions: u64,
    resubmits: u64,
    tip_spend_lamports: u64,
}

/// Snapshot bundles + AI reasoning + aggregates into one `history` message.
fn history_msg(state: &HotState) -> serde_json::Value {
    let ai_audit = state.ai_audit.snapshot();
    let mut bundles: Vec<BundleDto> = state
        .lifecycle
        .snapshot_all()
        .into_iter()
        .map(|(id, b)| {
            let d = b.deltas();
            let tip_floor = tip_floor_lamports(b.percentile, &b.submit_tip_emas);
            let ai_classification = b.ai_classification.clone().or_else(|| {
                ai_audit
                    .iter()
                    .rev()
                    .find(|r| r.tracking_id == Some(id))
                    .map(|r| r.classification.clone())
            });
            BundleDto {
                id,
                bundle_id: b.bundle_id.clone(),
                signature: b.sigs.first().map(|s| bs58::encode(s).into_string()),
                state: b.state,
                tip_floor_lamports: tip_floor,
                tip_lamports: b.tip_lamports,
                tip_delta_lamports: tip_delta_lamports(b.tip_lamports, tip_floor),
                percentile: b.percentile.map(|p| p.label().to_string()),
                landed_slot: b.landed_slot,
                attempt: b.attempt,
                last_error: b.last_error.map(|e| format!("{e:?}")),
                ai_classification,
                submit_to_processed_ms: d.submit_to_processed_ms,
                processed_to_confirmed_ms: d.processed_to_confirmed_ms,
                confirmed_to_finalized_ms: d.confirmed_to_finalized_ms,
            }
        })
        .collect();
    // Newest first for the table.
    bundles.sort_by(|a, b| b.id.cmp(&a.id));

    let landed = bundles
        .iter()
        .filter(|b| matches!(b.state, LifecycleState::Processed | LifecycleState::Confirmed | LifecycleState::Finalized))
        .count() as u64;
    let failed = bundles.iter().filter(|b| matches!(b.state, LifecycleState::Failed)).count() as u64;
    let in_flight = bundles
        .iter()
        .filter(|b| !matches!(b.state, LifecycleState::Finalized | LifecycleState::Failed))
        .count() as u64;
    let tip_spend_lamports: u64 = bundles
        .iter()
        .filter(|b| b.landed_slot.is_some())
        .filter_map(|b| b.tip_lamports)
        .sum();
    let decided = landed + failed;
    let success_rate = if decided > 0 { landed as f64 / decided as f64 } else { 0.0 };

    // AI reasoning log, newest first.
    let mut ai: Vec<AiDecisionRecord> = state.ai_audit.snapshot();
    ai.reverse();
    let resubmits = ai.iter().filter(|r| r.outcome.resubmitted()).count() as u64;

    let aggregate = Aggregate {
        tracked: bundles.len() as u64,
        in_flight,
        landed,
        failed,
        success_rate,
        ai_decisions: ai.len() as u64,
        resubmits,
        tip_spend_lamports,
    };

    json!({ "kind": "history", "data": { "bundles": bundles, "ai": ai, "aggregate": aggregate } })
}
