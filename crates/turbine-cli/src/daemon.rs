//! Daemon command handler (plan §9.1/§9.2).
//!
//! Bridges the transport-only `turbine-ipc` server to the live engine: it owns the
//! execution engine, hot state, and the AI failure bus, and turns each [`Request`]
//! into a [`Response`]. Business logic lives here; the IPC crate stays generic.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use tokio::sync::{mpsc, oneshot, Notify};
use tracing::{info, warn};

use turbine_core::config::Config;
use turbine_core::types::FailureClass;
use turbine_execute::{
    fail_path, fail_path_blockhash, fail_path_tip, fail_path_with_rng, happy_path, happy_path_memo,
    happy_path_single_tx, ExecutionEngine, ExecReport, FailureEvent, TradeIntent, AUTOPILOT,
    FAIL_PATH, FAIL_PATH_BLOCKHASH, FAIL_PATH_TIP, HAPPY_PATH, HAPPY_PATH_MEMO,
    HAPPY_PATH_SINGLE_TX,
};
use turbine_execute::gate::GateOutcome;
use turbine_ipc::{Request, Response, RunResult, StatusSnapshot};
use turbine_state::HotState;

const AUTOPILOT_ACTIONS: u32 = 10;
const AUTOPILOT_MIN_MS: u64 = 2000;
const AUTOPILOT_MAX_MS: u64 = 5000;

/// Legacy `fail-*` demos inject a synthetic failure; real `fail-path*` scenarios do not.
fn is_synthetic_fail_demo(scenario: &str) -> bool {
    scenario.starts_with("fail-")
        && scenario != FAIL_PATH
        && scenario != FAIL_PATH_BLOCKHASH
        && scenario != FAIL_PATH_TIP
}

/// Map a legacy `fail-<suffix>` demo scenario to the observed failure class hint.
fn class_hint(suffix: &str) -> FailureClass {
    match suffix {
        "blockhash" => FailureClass::BlockhashExpired,
        "tip" => FailureClass::TipTooLow,
        "auction" => FailureClass::AuctionTimeout,
        "dropped" => FailureClass::BundleDropped,
        "transient" => FailureClass::Transient,
        "slippage" => FailureClass::Slippage,
        "account" => FailureClass::AccountInUse,
        "sim" => FailureClass::SimulationError,
        "custom" => FailureClass::ProgramCustom(6001),
        _ => FailureClass::Unknown,
    }
}

/// Live daemon state shared by the IPC command loop.
pub struct Daemon {
    cfg: Arc<Config>,
    state: Arc<HotState>,
    engine: Arc<ExecutionEngine>,
    failures: mpsc::Sender<FailureEvent>,
    shutdown: Arc<Notify>,
    started: Instant,
    jito_connected: bool,
    autopilot_running: Arc<AtomicBool>,
}

impl Daemon {
    pub fn new(
        cfg: Arc<Config>,
        state: Arc<HotState>,
        engine: Arc<ExecutionEngine>,
        failures: mpsc::Sender<FailureEvent>,
        shutdown: Arc<Notify>,
        jito_connected: bool,
    ) -> Self {
        Self {
            cfg,
            state,
            engine,
            failures,
            shutdown,
            started: Instant::now(),
            jito_connected,
            autopilot_running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn status(&self) -> StatusSnapshot {
        let leader = self.state.leader();
        let tips = self.state.tips();
        StatusSnapshot {
            uptime_secs: self.started.elapsed().as_secs(),
            slot: self.state.slot(),
            geyser_healthy: self.state.geyser_healthy(),
            jito_connected: self.jito_connected,
            next_jito_leader_slot: leader.next_jito_leader_slot,
            slots_until_leader: leader.slots_until_leader.map(|v| v as i64),
            tip_p50: tips.p50,
            tip_p95: tips.p95,
            in_flight: self.state.lifecycle.in_flight(),
            ai_decisions: self.state.ai_audit.len(),
            submission_killed: self.state.submission_killed(),
            dry_run: self.cfg.execution.dry_run,
        }
    }

    fn log_finding(report: &ExecReport, fail_mode: Option<&str>) {
        info!(
            target: "turbine::finding",
            scenario = %report.label,
            tracking_id = report.tracking_id,
            gate = ?report.gate,
            tip_lamports = report.tip_lamports,
            tx_count = report.tx_count,
            bundle_id = ?report.bundle_id,
            submitted = report.submit_us.is_some(),
            dry_run = report.dry_run,
            fail_mode,
            "FINDING",
        );
    }

    fn run_result(report: ExecReport, failure_injected: bool) -> Response {
        Response::RunResult(RunResult {
            label: report.label,
            tracking_id: report.tracking_id,
            gate: format!("{:?}", report.gate),
            tip_lamports: report.tip_lamports,
            tx_count: report.tx_count,
            bundle_id: report.bundle_id,
            dry_run: report.dry_run,
            submitted: report.submit_us.is_some() && report.gate == GateOutcome::Open,
            failure_injected,
        })
    }

    /// Build + submit a scenario. Live happy-path variants / `fail-path` use real
    /// bundles; legacy `fail-*` demos additionally inject a synthetic failure for
    /// offline AI exercise when the daemon is running.
    async fn run_scenario(&self, scenario: &str) -> Response {
        if scenario == AUTOPILOT {
            return self.spawn_autopilot().await;
        }

        let payer = self.engine.payer_pubkey();
        let (intent, fail_mode_label) = match scenario {
            HAPPY_PATH => (happy_path(payer), None),
            HAPPY_PATH_MEMO => (happy_path_memo(payer), None),
            HAPPY_PATH_SINGLE_TX => (happy_path_single_tx(payer), None),
            FAIL_PATH => {
                let (intent, mode) = fail_path(payer);
                (intent, Some(mode.label()))
            }
            FAIL_PATH_BLOCKHASH => (fail_path_blockhash(payer), Some("stale-blockhash")),
            FAIL_PATH_TIP => (fail_path_tip(payer), Some("tip-too-low")),
            _ => (TradeIntent::mock(scenario.to_string()), None),
        };

        let report = match self.engine.execute(intent).await {
            Ok(r) => r,
            Err(e) => return Response::Error { message: format!("execute failed: {e}") },
        };

        if let Some(mode) = fail_mode_label {
            Self::log_finding(&report, Some(mode));
        } else if scenario == HAPPY_PATH
            || scenario == HAPPY_PATH_MEMO
            || scenario == HAPPY_PATH_SINGLE_TX
        {
            Self::log_finding(&report, None);
        }

        // Legacy synthetic injection for offline-style `fail-*` demos (not real fail-path).
        let mut failure_injected = false;
        if is_synthetic_fail_demo(scenario) {
            if let Some(suffix) = scenario.strip_prefix("fail-") {
                let ev = FailureEvent {
                    tracking_id: Some(report.tracking_id),
                    bundle_id: report.bundle_id.clone(),
                    raw_reason: format!("operator-injected {suffix} failure"),
                    class_hint: class_hint(suffix),
                    logs: vec![],
                };
                failure_injected = self.failures.send(ev).await.is_ok();
            }
        }

        Self::run_result(report, failure_injected)
    }

    async fn spawn_autopilot(&self) -> Response {
        if self
            .autopilot_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Response::Error { message: "autopilot already running".into() };
        }

        let engine = self.engine.clone();
        let flag = self.autopilot_running.clone();
        tokio::spawn(async move {
            run_autopilot_loop(engine, flag).await;
        }); 

        Response::Ack {
            message: format!(
                "autopilot started: {AUTOPILOT_ACTIONS} actions, {AUTOPILOT_MIN_MS}-{AUTOPILOT_MAX_MS}ms between each"
            ),
        }
    }

    async fn process(&self, req: Request) -> Response {
        match req {
            Request::Ping => Response::Pong,
            Request::Status => Response::Status(self.status()),
            Request::RunScenario { scenario } => self.run_scenario(&scenario).await,
            // Stop is handled in the command loop so it can break + signal shutdown.
            Request::Stop => Response::Ack { message: "shutting down".into() },
        }
    }

    /// Consume IPC commands until `Stop` or the channel closes. `RunScenario` is
    /// spawned so a slow gate can't block `status`/`stop`.
    pub async fn run(
        self: Arc<Self>,
        mut commands: mpsc::Receiver<(Request, oneshot::Sender<Response>)>,
    ) {
        info!("daemon command loop running");
        while let Some((req, resp)) = commands.recv().await {
            match req {
                Request::Stop => {
                    info!("stop requested over IPC; draining + shutting down");
                    self.state.kill_submission();
                    let _ = resp.send(Response::Ack { message: "shutting down".into() });
                    self.shutdown.notify_waiters();
                    break;
                }
                other => {
                    let d = self.clone();
                    tokio::spawn(async move {
                        let r = d.process(other).await;
                        let _ = resp.send(r);
                    });
                }
            }
        }
    }
}

async fn run_autopilot_loop(engine: Arc<ExecutionEngine>, running: Arc<AtomicBool>) {
    let payer = engine.payer_pubkey();
    let base_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);

    info!(target: "turbine::finding", actions = AUTOPILOT_ACTIONS, "autopilot loop started");

    for action in 1..=AUTOPILOT_ACTIONS {
        // `StdRng` is `Send` — safe to hold across `.await` inside `tokio::spawn`.
        // `ThreadRng` is not `Send` and must never be used here.
        let mut rng = StdRng::seed_from_u64(base_seed.wrapping_add(action as u64));

        let (intent, fail_mode) = if rng.gen_bool(0.5) {
            (happy_path(payer), None)
        } else {
            let (intent, mode) = fail_path_with_rng(payer, &mut rng);
            (intent, Some(mode.label()))
        };

        match engine.execute(intent).await {
            Ok(report) => Daemon::log_finding(&report, fail_mode),
            Err(e) => warn!(action, "autopilot execute failed: {e}"),
        }

        if action < AUTOPILOT_ACTIONS {
            let delay_ms = rng.gen_range(AUTOPILOT_MIN_MS..=AUTOPILOT_MAX_MS);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
    }

    running.store(false, Ordering::SeqCst);
    info!(target: "turbine::finding", "autopilot loop complete");
}
