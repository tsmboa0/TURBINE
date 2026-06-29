//! The coalesced dashboard model. The render loop applies bus events into this
//! struct and draws it at a fixed frame rate; pulses are derived by diffing values
//! as events arrive (so a value briefly highlights when it changes).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use turbine_core::events::TelemetryEvent;
use turbine_core::types::{Congestion, LifecycleState};

/// How long a value stays "pulsing" after it changes.
pub const PULSE: Duration = Duration::from_millis(450);

/// Things that pulse on change.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pulse {
    Slot,
    Leader,
    Tip,
    Health,
    Bundle,
    Kill,
}

/// One watched account's live contention.
#[derive(Clone)]
pub struct ContentionRow {
    pub account: String,
    /// Live write-lock contenders per slot (smoothed) — the headline TUI number.
    pub writers: f64,
    /// Lifetime write-locks observed on this account.
    pub total: u64,
    pub level: Congestion,
}

/// The whole live dashboard, rebuilt from the broadcast bus.
pub struct Dashboard {
    pub studio_url: String,
    pub started: Instant,

    // System monitor
    pub slot: u64,
    pub slot_interval_ms: Option<u64>,
    pub geyser: bool,
    pub jito: bool,
    pub next_leader_slot: Option<u64>,
    pub slots_until: Option<i64>,
    /// Submission window open (a Jito leader is within the gate horizon).
    pub leader_ready: bool,

    // Jito auction board (lamports)
    pub p25: u64,
    pub p50: u64,
    pub p75: u64,
    pub p95: u64,
    pub p99: u64,
    pub ema50: u64,
    pub last_tip: Option<u64>,

    // Live fee decision (selected tier driven by current contention)
    pub bid_congestion: Congestion,
    pub bid_percentile: String,
    pub bid_tip: u64,
    pub watching: bool,

    // Contention meter (keyed by account label, kept in insertion order)
    pub accounts: Vec<ContentionRow>,

    // Live status strip
    pub in_flight: u64,
    pub tracked: u64,
    pub ai_decisions: u64,
    pub killed: bool,
    pub dry_run: bool,
    pub last_bundle_state: Option<LifecycleState>,

    pulses: HashMap<Pulse, Instant>,
}

impl Dashboard {
    pub fn new(studio_url: String, dry_run: bool) -> Self {
        Self {
            studio_url,
            started: Instant::now(),
            slot: 0,
            slot_interval_ms: None,
            geyser: false,
            jito: false,
            next_leader_slot: None,
            slots_until: None,
            leader_ready: false,
            p25: 0,
            p50: 0,
            p75: 0,
            p95: 0,
            p99: 0,
            ema50: 0,
            last_tip: None,
            bid_congestion: Congestion::Quiet,
            bid_percentile: "P25".into(),
            bid_tip: 0,
            watching: false,
            accounts: Vec::new(),
            in_flight: 0,
            tracked: 0,
            ai_decisions: 0,
            killed: false,
            dry_run,
            last_bundle_state: None,
            pulses: HashMap::new(),
        }
    }

    fn pulse(&mut self, p: Pulse) {
        self.pulses.insert(p, Instant::now());
    }

    /// Is the given field currently within its post-change pulse window?
    pub fn pulsing(&self, p: Pulse) -> bool {
        self.pulses.get(&p).is_some_and(|t| t.elapsed() < PULSE)
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    /// Fold one telemetry event into the model. The TUI deliberately ignores the
    /// verbose web-only variants (`AiDecision`, `Log`).
    pub fn apply(&mut self, ev: TelemetryEvent) {
        match ev {
            TelemetryEvent::Slot { slot, interval_ms, .. } => {
                if slot != self.slot {
                    self.pulse(Pulse::Slot);
                }
                self.slot = slot;
                self.slot_interval_ms = interval_ms;
            }
            TelemetryEvent::Leader { next_jito_leader_slot, slots_until, ready, .. } => {
                if self.next_leader_slot != Some(next_jito_leader_slot) {
                    self.pulse(Pulse::Leader);
                }
                self.next_leader_slot = Some(next_jito_leader_slot);
                self.slots_until = Some(slots_until);
                self.leader_ready = ready;
            }
            TelemetryEvent::TipSnapshot { p25, p50, p75, p95, p99, ema50 } => {
                if p50 != self.p50 || p95 != self.p95 {
                    self.pulse(Pulse::Tip);
                }
                self.p25 = p25;
                self.p50 = p50;
                self.p75 = p75;
                self.p95 = p95;
                self.p99 = p99;
                self.ema50 = ema50;
            }
            TelemetryEvent::Bid { congestion, percentile, tip_lamports, watching } => {
                if tip_lamports != self.bid_tip || percentile != self.bid_percentile {
                    self.pulse(Pulse::Tip);
                }
                self.bid_congestion = congestion;
                self.bid_percentile = percentile;
                self.bid_tip = tip_lamports;
                self.watching = watching;
            }
            TelemetryEvent::Contention { account, fast_ema, total_hits, level, .. } => {
                let row = ContentionRow {
                    account: account.clone(),
                    writers: fast_ema,
                    total: total_hits,
                    level,
                };
                match self.accounts.iter_mut().find(|r| r.account == account) {
                    Some(existing) => *existing = row,
                    None => self.accounts.push(row),
                }
            }
            TelemetryEvent::Health { geyser, jito } => {
                if geyser != self.geyser || jito != self.jito {
                    self.pulse(Pulse::Health);
                }
                self.geyser = geyser;
                self.jito = jito;
            }
            TelemetryEvent::Stats { in_flight, tracked, ai_decisions, killed, dry_run } => {
                if killed != self.killed {
                    self.pulse(Pulse::Kill);
                }
                self.in_flight = in_flight;
                self.tracked = tracked;
                self.ai_decisions = ai_decisions;
                self.killed = killed;
                self.dry_run = dry_run;
            }
            TelemetryEvent::BundleState { state, tip_lamports, .. } => {
                self.pulse(Pulse::Bundle);
                self.last_bundle_state = Some(state);
                if let Some(t) = tip_lamports {
                    self.last_tip = Some(t);
                }
            }
            TelemetryEvent::AiDecision { .. } | TelemetryEvent::Log { .. } => {}
        }
    }
}
