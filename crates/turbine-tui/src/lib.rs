//! `turbine-tui` — Component 5 dashboard (plan §9.3).
//!
//! A short, dense, glanceable control panel rendered **only** from the lossy
//! telemetry [`broadcast`] bus — the render task never reads `HotState` or takes a
//! lock, so a slow terminal can never stall the engine. The publisher that feeds
//! the bus lives in [`feed`]; the coalesced model in [`app`]; rendering in [`ui`].
//!
//! `turbine start` calls [`feed::spawn`] (services-runtime publisher) and, when
//! attached to a TTY, [`spawn_dashboard`] (the render loop on a blocking thread).

mod app;
pub mod feed;
mod theme;
mod ui;

use std::io::{self, Stdout};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::{execute, cursor};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::sync::broadcast;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use turbine_core::events::TelemetryEvent;
use turbine_ingest::DeshredBootStatus;

use app::Dashboard;

type Tui = Terminal<CrosstermBackend<Stdout>>;

/// Boot stages shown on the loading bar (cosmetic ordering of the real subsystems
/// that `start` has already wired by the time the TUI runs).
const STAGES: [&str; 6] = [
    "hot state",
    "ingestion streams",
    "processing engine",
    "execution + Jito",
    "AI failure analyst",
    "IPC control plane",
];

/// ~12 fps: fast enough for a blink/pulse, far slower than the hot path.
const FRAME: Duration = Duration::from_millis(80);

/// Spawn the dashboard render loop on a dedicated blocking thread. Returns a handle
/// the caller awaits after signalling shutdown so the terminal is restored cleanly.
///
/// - `bus`: a fresh subscriber to the telemetry broadcast bus.
/// - `studio_url`: the web UI URL shown on the splash + footer.
/// - `shutdown`: notified when the operator presses `q`/Ctrl-C inside the TUI.
/// - `stopping`: observed each frame; set by `start` on external shutdown so the
///   loop exits and restores the terminal.
pub fn spawn_dashboard(
    bus: broadcast::Receiver<TelemetryEvent>,
    studio_url: String,
    dry_run: bool,
    shutdown: Arc<Notify>,
    stopping: Arc<AtomicBool>,
    deshred_boot: DeshredBootStatus,
) -> JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        if let Err(e) = run(bus, studio_url, dry_run, &shutdown, &stopping, deshred_boot) {
            // The terminal is restored by `run` before returning, so this is safe.
            eprintln!("turbine-tui: {e}");
        }
    })
}

fn setup_terminal() -> io::Result<Tui> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, cursor::Hide)?;
    Terminal::new(CrosstermBackend::new(stdout))
}

fn restore_terminal(terminal: &mut Tui) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, cursor::Show)?;
    terminal.show_cursor()
}

fn run(
    mut bus: broadcast::Receiver<TelemetryEvent>,
    studio_url: String,
    dry_run: bool,
    shutdown: &Arc<Notify>,
    stopping: &Arc<AtomicBool>,
    deshred_boot: DeshredBootStatus,
) -> io::Result<()> {
    let mut terminal = setup_terminal()?;
    let res = (|| {
        if boot_sequence(&mut terminal, &studio_url, stopping, &deshred_boot)? {
            return Ok(()); // quit during boot
        }
        dashboard_loop(&mut terminal, &mut bus, studio_url, dry_run, shutdown, stopping)
    })();
    restore_terminal(&mut terminal)?;
    res
}

/// Returns `Ok(true)` if the operator quit during boot.
fn boot_sequence(
    terminal: &mut Tui,
    studio_url: &str,
    stopping: &Arc<AtomicBool>,
    deshred_boot: &DeshredBootStatus,
) -> io::Result<bool> {
    let deshred_line = deshred_boot.boot_splash_line();
    for (i, stage) in STAGES.iter().enumerate() {
        if stopping.load(Ordering::Relaxed) {
            return Ok(true);
        }
        terminal.draw(|f| ui::boot(f, i + 1, STAGES.len(), stage, studio_url, deshred_line.as_deref()))?;
        // The poll doubles as the per-stage delay and an early-quit check.
        if event::poll(Duration::from_millis(260))? {
            if let Event::Key(k) = event::read()? {
                if is_quit(k) {
                    return Ok(true);
                }
            }
        }
    }
    terminal.draw(|f| {
        ui::boot(
            f,
            STAGES.len(),
            STAGES.len(),
            "online",
            studio_url,
            deshred_line.as_deref(),
        )
    })?;
    std::thread::sleep(Duration::from_millis(280));
    Ok(false)
}

fn dashboard_loop(
    terminal: &mut Tui,
    bus: &mut broadcast::Receiver<TelemetryEvent>,
    studio_url: String,
    dry_run: bool,
    shutdown: &Arc<Notify>,
    stopping: &Arc<AtomicBool>,
) -> io::Result<()> {
    let mut app = Dashboard::new(studio_url, dry_run);

    loop {
        if stopping.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Drain everything queued since the last frame (coalesce; tolerate lag).
        loop {
            match bus.try_recv() {
                Ok(ev) => app.apply(ev),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(broadcast::error::TryRecvError::Closed) => return Ok(()),
            }
        }

        // Input also paces the frame (blocks up to FRAME for a key).
        if event::poll(FRAME)? {
            if let Event::Key(k) = event::read()? {
                if is_quit(k) {
                    stopping.store(true, Ordering::Relaxed);
                    shutdown.notify_waiters();
                    return Ok(());
                }
            }
        }

        terminal.draw(|f| ui::dashboard(f, &app))?;
    }
}

fn is_quit(k: KeyEvent) -> bool {
    matches!(k.code, KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc)
        || (k.code == KeyCode::Char('c') && k.modifiers.contains(KeyModifiers::CONTROL))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use turbine_core::types::{Congestion, LifecycleState};

    fn buffer_text(terminal: &Terminal<TestBackend>) -> String {
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect()
    }

    fn sample_app() -> Dashboard {
        let mut app = Dashboard::new("http://127.0.0.1:9000".into(), true);
        app.apply(TelemetryEvent::Health {
            geyser: true,
            jito: true,
            deshred_active: false,
        });
        app.apply(TelemetryEvent::Slot {
            slot: 321_000_111,
            parent: None,
            status: "processed".into(),
            interval_ms: Some(410),
        });
        app.apply(TelemetryEvent::Leader {
            next_jito_leader_slot: 321_000_120,
            slots_until: 1,
            ready: true,
            identity: None,
        });
        app.apply(TelemetryEvent::TipSnapshot {
            p25: 1_024,
            p50: 6_700,
            p75: 20_000,
            p95: 460_000,
            p99: 1_800_000,
            ema50: 7_210,
        });
        app.apply(TelemetryEvent::Contention {
            account: "AbCd…WxYz".into(),
            fast_ema: 12.0,
            slow_ema: 3.0,
            zscore: 2.4,
            total_hits: 1_234,
            level: Congestion::Hot,
        });
        app.apply(TelemetryEvent::Bid {
            congestion: Congestion::Hot,
            percentile: "P95".into(),
            tip_lamports: 506_000,
            watching: true,
        });
        app.apply(TelemetryEvent::Stats {
            in_flight: 2,
            tracked: 7,
            ai_decisions: 3,
            killed: false,
            dry_run: true,
        });
        app.apply(TelemetryEvent::BundleState {
            bundle_id: Some("b1".into()),
            primary_signature: None,
            state: LifecycleState::Confirmed,
            tip_lamports: Some(1_100),
            percentile: Some("p95".into()),
            landed_slot: Some(321_000_115),
            elapsed_ms: Some(42),
            attempt: 0,
        });
        app
    }

    #[test]
    fn dashboard_renders_expected_content() {
        let app = sample_app();
        let mut terminal = Terminal::new(TestBackend::new(140, 34)).unwrap();
        terminal.draw(|f| ui::dashboard(f, &app)).unwrap();
        let text = buffer_text(&terminal);
        for needle in [
            "SYSTEM MONITOR",
            "JITO AUCTION",
            "CONTENTION",
            "LIVE STATUS",
            "Studio",
            "127.0.0.1:9000",
            "DRY-RUN",
            "1,800,000", // p99 with thousands separators
            "HOT",
            "Confirmed",
        ] {
            assert!(text.contains(needle), "dashboard missing {needle:?}");
        }
    }

    #[test]
    fn boot_renders_with_studio_link() {
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal.draw(|f| ui::boot(f, 3, 6, "processing engine", "http://127.0.0.1:9000", None)).unwrap();
        let text = buffer_text(&terminal);
        assert!(text.contains("Studio"));
        assert!(text.contains("processing engine"));
    }

    /// Visual preview of the real layout (run: `cargo test -p turbine-tui preview
    /// -- --ignored --nocapture`). Prints the rendered grid row by row.
    #[test]
    #[ignore]
    fn preview() {
        let app = sample_app();
        let (w, h) = (120u16, 18u16);
        let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
        terminal.draw(|f| ui::dashboard(f, &app)).unwrap();
        let buf = terminal.backend().buffer();
        println!("\n+{}+", "-".repeat(w as usize));
        for row in buf.content().chunks(w as usize) {
            let line: String = row.iter().map(|c| c.symbol()).collect();
            println!("|{line}|");
        }
        println!("+{}+", "-".repeat(w as usize));

        let mut term2 = Terminal::new(TestBackend::new(w, h)).unwrap();
        term2.draw(|f| ui::boot(f, 4, 6, "execution + Jito", "http://127.0.0.1:9000", None)).unwrap();
        let buf = term2.backend().buffer();
        println!("\nBOOT SPLASH:\n+{}+", "-".repeat(w as usize));
        for row in buf.content().chunks(w as usize) {
            let line: String = row.iter().map(|c| c.symbol()).collect();
            println!("|{line}|");
        }
        println!("+{}+", "-".repeat(w as usize));
    }

    #[test]
    fn renders_without_panic_at_many_sizes() {
        let app = sample_app();
        for (w, h) in [(20u16, 6u16), (40, 12), (80, 24), (200, 60)] {
            let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
            terminal.draw(|f| ui::dashboard(f, &app)).unwrap();
            terminal.draw(|f| ui::boot(f, 1, 6, "hot state", "http://127.0.0.1:9000", None)).unwrap();
        }
    }
}
