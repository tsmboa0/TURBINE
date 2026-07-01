//! `turbine` — the single client/daemon binary (plan §9.1).
//!
//! Subcommands:
//! - `start`  : boot the daemon (ingestion + processing + execution + AI + IPC).
//! - `run`    : send a mock scenario to the running daemon over IPC (falls back
//!   to an in-process demo when no daemon is reachable).
//! - `stop`   : gracefully stop the running daemon over IPC.
//! - `status` : print a health snapshot from the running daemon over IPC.
//!
//! The IPC control plane (Phase 7) lives in `turbine-ipc`; the daemon-side command
//! handler is in [`daemon`]. The TUI/web surfaces land in Phases 8–9.

mod daemon;

use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber::EnvFilter;
use turbine_core::config::Config;
use turbine_ipc::{Request, Response};

#[derive(Parser, Debug)]
#[command(
    name = "turbine",
    version,
    about = "TURBINE — ultra-low-latency smart transaction infrastructure on Solana"
)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "turbine.toml", global = true)]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Boot the daemon: ingestion, processing, execution, TUI, and web server.
    Start {
        /// Disable the live TUI dashboard (stream logs to stdout instead). Auto-
        /// disabled when stdout is not a terminal (e.g. piped or backgrounded).
        #[arg(long)]
        no_tui: bool,
        /// Use SubscribeDeshred for write-lock contention (pre-execution). Requires a
        /// Triton extension Geyser endpoint; falls back to standard Geyser targets if unavailable.
        #[arg(long)]
        deshred: bool,
    },

    /// Send a mock trade scenario to the running daemon over IPC.
    Run {
        /// Scenario name, e.g. `happy-test`, `fail-slippage`.
        scenario: String,
    },

    /// Gracefully stop the running daemon.
    Stop,

    /// Print a health snapshot from the running daemon.
    Status,
}

/// Initialize logging. In TUI mode the terminal is owned by the dashboard, so logs
/// are redirected to a file (`$TMPDIR/turbine.log`) to keep the screen clean;
/// otherwise they stream to stdout as usual.
fn init_tracing(to_file: bool) -> anyhow::Result<()> {
    let filter =
        EnvFilter::try_from_env("TURBINE_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    if to_file {
        let path = std::env::temp_dir().join("turbine.log");
        let file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
        eprintln!("TURBINE — launching dashboard; logs → {}", path.display());
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_ansi(false)
            .with_writer(move || file.try_clone().expect("clone log file handle"))
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .init();
    }
    Ok(())
}

/// Phase 4 one-shot demo: warm the inputs the engine needs (blockhash, tip
/// accounts, tip percentiles), seed the slot/leader so the gate opens, then build
/// → price → gate → submit (dry-run by default). Replaced by IPC in Phase 7.
async fn run_local_demo(config: &std::path::Path, scenario: &str) -> anyhow::Result<()> {
    use turbine_core::tips::TipSnapshot;
    use turbine_execute::{ExecutionEngine, TradeIntent};

    // `fail-*` scenarios exercise the Phase 6 AI failure-analyst loop instead.
    if let Some(suffix) = scenario.strip_prefix("fail-") {
        return run_failure_demo(config, suffix).await;
    }

    let cfg = Arc::new(Config::load(config)?);
    info!(%scenario, dry_run = cfg.execution.dry_run, "Phase 4 local execution demo");

    let state = Arc::new(turbine_state::HotState::new(&cfg));

    // Warm blockhash (real if RPC reachable, else a synthetic placeholder).
    match turbine_ingest::blockhash::fetch_latest_blockhash(&cfg.rpc.http_url).await {
        Ok(bh) => {
            info!(blockhash = %bh.blockhash, slot = bh.slot, "fetched warm blockhash");
            state.set_blockhash(bh);
        }
        Err(e) => {
            tracing::warn!("blockhash fetch failed ({e}); using synthetic placeholder");
            state.set_blockhash(turbine_core::blockhash::CachedBlockhash {
                blockhash: "11111111111111111111111111111111".into(),
                last_valid_block_height: 0,
                slot: 0,
                fetched_at: std::time::Instant::now(),
            });
        }
    }

    // Tip accounts (real if reachable, else the engine has nothing to tip).
    match turbine_ingest::tips::fetch_tip_accounts(&cfg.jito.json_rpc_url).await {
        Ok(accts) if !accts.is_empty() => {
            info!(count = accts.len(), "fetched Jito tip accounts");
            state.set_tip_accounts(accts);
        }
        other => {
            if let Err(e) = other {
                tracing::warn!("tip account fetch failed ({e}); using a synthetic tip account");
            }
            state.set_tip_accounts(vec![solana_pubkey::Pubkey::new_from_array([9u8; 32])]);
        }
    }

    // Smoothed tip percentiles (real floor if reachable, else synthetic).
    match turbine_ingest::tips::fetch_tip_floor(&cfg.jito.tip_floor_url).await {
        Ok(t) => {
            info!(p50 = t.p50, p95 = t.p95, "fetched tip floor");
            state.set_tips(TipSnapshot { p25: t.p25, p50: t.p50, p75: t.p75, p95: t.p95, p99: t.p99 });
        }
        Err(e) => {
            tracing::warn!("tip floor fetch failed ({e}); using synthetic percentiles");
            state.set_tips(TipSnapshot { p25: 1_000, p50: 2_000, p75: 5_000, p95: 50_000, p99: 200_000 });
        }
    }

    // Seed slot + leader so the lookahead gate opens immediately (no live Geyser
    // in a one-shot run). dist = gate_min keeps us inside the window.
    let slot = 1_000u64;
    state.set_slot(slot);
    state.set_leader(turbine_core::leader::LeaderView {
        next_jito_leader_slot: Some(slot + cfg.strategy.gate_min),
        slots_until_leader: Some(cfg.strategy.gate_min),
    });
    state.set_geyser_healthy(true);

    // Dry-run demo uses no searcher channel; live submission would pass one.
    let engine = ExecutionEngine::new(cfg.clone(), state.clone(), None)?;
    let report = engine.execute(TradeIntent::mock(scenario)).await?;

    info!(
        label = %report.label,
        congestion = ?report.congestion,
        percentile = ?report.percentile,
        tip_lamports = report.tip_lamports,
        tip_account = %report.tip_account,
        tx_count = report.tx_count,
        bundle_b64_bytes = report.bundle_b64_bytes,
        build_us = report.build_us,
        gate = ?report.gate,
        submit_us = ?report.submit_us,
        bundle_id = ?report.bundle_id,
        dry_run = report.dry_run,
        "scenario complete",
    );
    Ok(())
}

/// Phase 6 demo (revised): drive the **full autonomous loop** for `fail-<class>`.
/// Submit a (dry-run) bundle, inject a failure, and let the AI coordinator
/// classify → store reasoning → rebuild with the fix → resubmit autonomously.
/// When AI is disabled (no API key) a mock analyst stands in so the loop is visible.
async fn run_failure_demo(config: &std::path::Path, class_name: &str) -> anyhow::Result<()> {
    use turbine_ai::{AiEngine, Analyst, AnalystVerdict, RetryAdjustments};
    use turbine_core::tips::TipSnapshot;
    use turbine_core::types::FailureClass;
    use turbine_execute::{ExecutionEngine, FailureEvent, RetryCoordinator, TradeIntent};

    let class = match class_name {
        "blockhash" => FailureClass::BlockhashExpired,
        "tip" => FailureClass::TipTooLow,
        "auction" => FailureClass::AuctionTimeout,
        "dropped" => FailureClass::BundleDropped,
        "transient" => FailureClass::Transient,
        "slippage" => FailureClass::Slippage,
        "account" => FailureClass::AccountInUse,
        "sim" => FailureClass::SimulationError,
        "custom" => FailureClass::ProgramCustom(6001),
        other => {
            tracing::warn!(%other, "unknown fail-* scenario; using Unknown");
            FailureClass::Unknown
        }
    };

    // Force dry-run for the demo so no funds ever move.
    let mut cfg = Config::load(config)?;
    cfg.execution.dry_run = true;
    let cfg = Arc::new(cfg);
    info!(?class, ai_enabled = cfg.ai.enabled, "Phase 6 autonomous AI retry demo");

    let state = Arc::new(turbine_state::HotState::new(&cfg));
    state.set_blockhash(turbine_core::blockhash::CachedBlockhash {
        blockhash: "11111111111111111111111111111111".into(),
        last_valid_block_height: 0,
        slot: 0,
        fetched_at: std::time::Instant::now(),
    });
    state.set_tip_accounts(vec![solana_pubkey::Pubkey::new_from_array([9u8; 32])]);
    state.set_tips(TipSnapshot { p25: 1_000, p50: 6_700, p75: 20_000, p95: 460_000, p99: 1_800_000 });
    let slot = 1_000u64;
    state.set_slot(slot);
    state.set_leader(turbine_core::leader::LeaderView {
        next_jito_leader_slot: Some(slot + cfg.strategy.gate_min),
        slots_until_leader: Some(cfg.strategy.gate_min),
    });
    state.set_geyser_healthy(true);

    let engine = Arc::new(ExecutionEngine::new(cfg.clone(), state.clone(), None)?);

    // Real analyst when configured; otherwise a mock that proposes a fix so the
    // autonomous loop is demonstrable offline.
    let ai = if cfg.ai.enabled {
        Arc::new(AiEngine::new(&cfg, state.clone()))
    } else {
        let verdict = AnalystVerdict {
            classification: format!("{class:?}"),
            root_cause: format!("demo: synthetic {class_name} failure"),
            adjustments: RetryAdjustments {
                tip_bump_pct: Some(0.5),
                cu_limit: Some(300_000),
                fresh_blockhash: true,
                rebuild: true,
                ..Default::default()
            },
            should_retry: true,
            confidence: 0.8,
        };
        Arc::new(AiEngine::with_analyst(&cfg, state.clone(), Analyst::Mock(Box::new(verdict))))
    };

    let coordinator =
        RetryCoordinator::new(cfg.clone(), engine.clone(), ai, state.clone());

    // 1) Submit the original bundle (dry-run) → an in-flight tracking id.
    let rep = engine.execute(TradeIntent::mock(format!("demo-{class_name}"))).await?;
    info!(tracking_id = rep.tracking_id, attempt = rep.attempt, "original bundle submitted");

    // 2) Inject the failure → AI classifies, stores reasoning, rebuilds, resubmits.
    coordinator
        .on_failure(FailureEvent {
            tracking_id: Some(rep.tracking_id),
            bundle_id: None,
            raw_reason: format!("synthetic {class_name} failure"),
            class_hint: class,
            logs: vec!["Program log: demo".into()],
        })
        .await;

    // 3) Show the persisted reasoning trail (what the web UI will render).
    for rec in state.ai_audit.snapshot() {
        info!(
            seq = rec.seq,
            attempt = rec.attempt,
            outcome = ?rec.outcome,
            class = %rec.classification,
            fix = %rec.fix,
            "AI reasoning: {}", rec.root_cause
        );
    }
    info!(
        in_flight = engine.registry().len(),
        audit_records = state.ai_audit.len(),
        "autonomous retry demo complete"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // The dashboard runs only for `start` on an interactive terminal (and unless
    // `--no-tui` is set); everything else logs to stdout.
    let tui_active = matches!(
        cli.command,
        Command::Start { no_tui: false, .. }
    ) && std::io::stdout().is_terminal();
    init_tracing(tui_active)?;

    match cli.command {
        Command::Start { deshred, .. } => {
            let cfg = Arc::new(Config::load(&cli.config)?);
            info!(config = %cli.config.display(), "configuration loaded and validated");
            info!(
                geyser = %cfg.geyser.endpoint,
                commitment = %cfg.geyser.commitment,
                deshred,
                programs = cfg.targets.programs.len(),
                watched = cfg.targets.watched_accounts.len(),
                "booting TURBINE — Phase 3 (ingestion + processing + hot state)"
            );

            let state = Arc::new(turbine_state::HotState::new(&cfg));
            let (channels, _ingest, deshred_boot) = turbine_ingest::spawn(cfg.clone(), deshred).await;
            let contention_feed = channels.feed.clone();
            state.set_tip_accounts(channels.tip_accounts_seed.clone());

            let task = turbine_process::spawn(cfg.clone(), state.clone(), channels);

            // Failure bus: every terminal failure (Jito result or submit timeout)
            // is routed to the AI coordinator (plan §8, revised — no deterministic
            // fix path).
            let (fail_tx, fail_rx) =
                tokio::sync::mpsc::channel::<turbine_execute::FailureEvent>(256);

            // Leader view (plan §7.2): the next Jito leader + live countdown are
            // derived locally from an epoch-cached schedule, recomputed on every
            // slot — no per-slot RPC/gRPC. The refresher (cold) rebuilds the
            // schedule per epoch; the tracker (event-driven) writes the leader view.
            let mut tasks = vec![task];
            tasks.push(tokio::spawn(turbine_execute::run_schedule_refresher(
                cfg.clone(),
                state.clone(),
            )));
            tasks.push(tokio::spawn(turbine_execute::run_leader_tracker(
                cfg.clone(),
                state.clone(),
            )));

            // Searcher gRPC channel (Phase 5): used for the bundle-result stream and
            // low-latency bundle submission. It's an *optimization*, not required for
            // the leader view, so it must never block startup — the block engine's
            // TLS/HTTP2 handshake can hang past the transport `connect_timeout`, so we
            // bound the whole attempt and fall back to JSON-RPC submit if it's slow.
            const SEARCHER_CONNECT_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
            let mut searcher_for_engine = None;
            let connect = turbine_execute::searcher_connect(&cfg.jito.block_engine_url);
            match tokio::time::timeout(SEARCHER_CONNECT_BUDGET, connect).await {
                Ok(Ok(client)) => {
                    info!(endpoint = %cfg.jito.block_engine_url, "connected Jito searcher gRPC");
                    tasks.push(tokio::spawn(turbine_execute::run_bundle_results(
                        client.clone(),
                        state.clone(),
                        Some(fail_tx.clone()),
                    )));
                    searcher_for_engine = Some(client);
                }
                Ok(Err(e)) => {
                    tracing::warn!("searcher gRPC connect failed ({e}); JSON-RPC submit fallback");
                }
                Err(_) => {
                    tracing::warn!(
                        endpoint = %cfg.jito.block_engine_url,
                        budget_ms = SEARCHER_CONNECT_BUDGET.as_millis() as u64,
                        "searcher gRPC connect timed out; JSON-RPC submit fallback (startup not blocked)"
                    );
                }
            }

            // AI autonomous retry loop: execution engine (shared registry) + AI
            // analyst + coordinator + submit-timeout sweeper.
            let jito_connected = searcher_for_engine.is_some();
            let engine = Arc::new(
                turbine_execute::ExecutionEngine::new(cfg.clone(), state.clone(), searcher_for_engine)?
                    .with_fail_sink(fail_tx.clone()),
            );
            let ai = Arc::new(turbine_ai::AiEngine::new(&cfg, state.clone()));
            let coordinator = turbine_execute::RetryCoordinator::new(
                cfg.clone(),
                engine.clone(),
                ai,
                state.clone(),
            );
            tasks.push(tokio::spawn(coordinator.run(fail_rx)));
            tasks.push(tokio::spawn(turbine_execute::run_timeout_sweeper(
                cfg.clone(),
                state.clone(),
                fail_tx.clone(),
            )));
            tasks.push(tokio::spawn(turbine_state::transaction_audit::run_sweeper(
                state.clone(),
                turbine_state::transaction_audit::TransactionAuditLog::default_path(),
            )));

            // Control plane (Phase 7): IPC server + daemon command loop. `stop`
            // over IPC and Ctrl-C both trigger the same graceful shutdown.
            let shutdown = Arc::new(tokio::sync::Notify::new());
            let (cmd_tx, cmd_rx) =
                tokio::sync::mpsc::channel::<(Request, tokio::sync::oneshot::Sender<Response>)>(64);
            let daemon = Arc::new(daemon::Daemon::new(
                cfg.clone(),
                state.clone(),
                engine.clone(),
                fail_tx.clone(),
                shutdown.clone(),
                jito_connected,
            ));
            tasks.push(tokio::spawn(daemon.run(cmd_rx)));
            {
                let socket = cfg.ipc.socket_path.clone();
                let sd = shutdown.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = turbine_ipc::serve(&socket, cmd_tx, sd).await {
                        tracing::error!("ipc server error: {e}");
                    }
                }));
            }

            // Telemetry bus (Phase 8): a single publisher snapshots hot state on a
            // fixed cadence; the TUI (and the Phase 9 web UI) subscribe. Lossy
            // broadcast — the render side can never stall the engine.
            let (bus_tx, _bus_rx) =
                tokio::sync::broadcast::channel::<turbine_core::events::TelemetryEvent>(2048);
            tasks.push(turbine_tui::feed::spawn(
                cfg.clone(),
                state.clone(),
                bus_tx.clone(),
                jito_connected,
                contention_feed,
            ));

            // Web studio (Phase 9): axum on `server.web_bind`, reading the same
            // lossy bus for live state and `HotState` for the verbose history
            // surface (tx history, AI reasoning, aggregates). Read-only; a slow
            // browser can never stall the engine.
            {
                let web = turbine_web::WebState {
                    state: state.clone(),
                    bus: bus_tx.clone(),
                    dry_run: cfg.execution.dry_run,
                    jito_connected,
                };
                let bind = cfg.server.web_bind.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = turbine_web::serve(&bind, web).await {
                        tracing::error!(
                            bind = %bind,
                            %e,
                            "web studio failed — is another turbine process still bound to this port? \
                             Kill it (`lsof -i :9000`) and restart. The TUI works but the browser UI will be frozen.",
                        );
                    }
                }));
            }

            info!(
                tip_accounts = state.tip_accounts().len(),
                dry_run = cfg.execution.dry_run,
                ai_enabled = cfg.ai.enabled,
                socket = %cfg.ipc.socket_path.display(),
                studio = %format!("http://{}", cfg.server.web_bind),
                tui = tui_active,
                "ingestion + processing + execution + AI + IPC running — `turbine stop` or Ctrl-C"
            );

            if tui_active {
                // Last stderr lines before the TUI takes the terminal (logs still go to turbine.log).
                if let Some(notice) = deshred_boot.terminal_notice() {
                    use std::io::Write;
                    let mut stderr = std::io::stderr();
                    let _ = writeln!(stderr);
                    for line in notice.lines() {
                        let _ = writeln!(stderr, "{line}");
                    }
                    let _ = writeln!(stderr);
                    let _ = stderr.flush();
                }
                let stopping = Arc::new(AtomicBool::new(false));
                let studio = format!("http://{}", cfg.server.web_bind);
                let tui = turbine_tui::spawn_dashboard(
                    bus_tx.subscribe(),
                    studio,
                    cfg.execution.dry_run,
                    shutdown.clone(),
                    stopping.clone(),
                    deshred_boot,
                );

                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = shutdown.notified() => {}
                }
                stopping.store(true, Ordering::Relaxed);
                shutdown.notify_waiters();
                for t in &tasks {
                    t.abort();
                }
                let _ = tui.await; // restores the terminal before we exit
                info!("dashboard closed; TURBINE stopped");
            } else {
                if let Some(notice) = deshred_boot.terminal_notice() {
                    eprintln!();
                    for line in notice.lines() {
                        eprintln!("{line}");
                    }
                    eprintln!();
                }
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => info!("Ctrl-C received; stopping"),
                    _ = shutdown.notified() => info!("stop requested over IPC; stopping"),
                }
                shutdown.notify_waiters();
                for t in &tasks {
                    t.abort();
                }
            }

            // We've restored the terminal, removed the socket, and aborted tasks.
            // Background gRPC/TLS reconnect loops can otherwise delay runtime
            // teardown, so exit promptly and deterministically.
            std::process::exit(0);
        }
        Command::Run { scenario } => {
            let cfg = Config::load(&cli.config)?;
            match turbine_ipc::request(
                &cfg.ipc.socket_path,
                Request::RunScenario { scenario: scenario.clone() },
            )
            .await
            {
                Ok(resp) => print_response(resp),
                Err(e) => {
                    info!("daemon not reachable ({e}); running in-process demo");
                    run_local_demo(&cli.config, &scenario).await?;
                }
            }
        }
        Command::Stop => {
            let cfg = Config::load(&cli.config)?;
            match turbine_ipc::request(&cfg.ipc.socket_path, Request::Stop).await {
                Ok(resp) => print_response(resp),
                Err(e) => info!("could not reach daemon to stop ({e}); is it running?"),
            }
        }
        Command::Status => {
            let cfg = Config::load(&cli.config)?;
            match turbine_ipc::request(&cfg.ipc.socket_path, Request::Status).await {
                Ok(resp) => print_response(resp),
                Err(e) => info!(
                    socket = %cfg.ipc.socket_path.display(),
                    "daemon not running ({e}); config is valid"
                ),
            }
        }
    }

    Ok(())
}

/// Render an IPC response for the CLI client.
fn print_response(resp: Response) {
    match resp {
        Response::Pong => info!("pong"),
        Response::Ack { message } => info!("{message}"),
        Response::Error { message } => tracing::error!("daemon error: {message}"),
        Response::RunResult(r) => info!(
            label = %r.label,
            tracking_id = r.tracking_id,
            gate = %r.gate,
            tip_lamports = r.tip_lamports,
            tx_count = r.tx_count,
            bundle_id = ?r.bundle_id,
            submitted = r.submitted,
            failure_injected = r.failure_injected,
            dry_run = r.dry_run,
            "scenario accepted by daemon",
        ),
        Response::Status(s) => info!(
            uptime_secs = s.uptime_secs,
            slot = s.slot,
            geyser_healthy = s.geyser_healthy,
            jito_connected = s.jito_connected,
            next_jito_leader_slot = ?s.next_jito_leader_slot,
            slots_until_leader = ?s.slots_until_leader,
            tip_p50 = s.tip_p50,
            tip_p95 = s.tip_p95,
            in_flight = s.in_flight,
            ai_decisions = s.ai_decisions,
            submission_killed = s.submission_killed,
            dry_run = s.dry_run,
            "daemon status",
        ),
    }
}
