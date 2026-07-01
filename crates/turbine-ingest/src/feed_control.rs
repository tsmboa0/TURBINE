//! Runtime control for the contention feed: deshred vs standard Geyser targets.
//!
//! When `--deshred` is requested and the provider supports it, [`ContentionFeedControl::deshred_active`]
//! is true and the main Geyser stream omits target transactions. On probe failure or
//! mid-run deshred loss, we flip back to Geyser targets and nudge the main reader to reconnect.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;
use tracing::warn;

/// Shared flags between the Geyser reader, optional deshred reader, and DPU telemetry.
#[derive(Clone)]
pub struct ContentionFeedControl {
    /// User requested `--deshred` (or config); used for logging only after boot.
    pub deshred_requested: bool,
    /// When true, contention comes from deshred; main Geyser skips target txs.
    deshred_active: Arc<AtomicBool>,
    /// Wakes the Geyser reader to reconnect (picks up target filter after fallback).
    geyser_reconnect: Arc<Notify>,
}

impl ContentionFeedControl {
    pub fn new(deshred_requested: bool) -> Self {
        Self {
            deshred_requested,
            deshred_active: Arc::new(AtomicBool::new(false)),
            geyser_reconnect: Arc::new(Notify::new()),
        }
    }

    /// Mark deshred as the active contention source (after a successful probe).
    pub fn activate_deshred(&self) {
        self.deshred_active.store(true, Ordering::Relaxed);
    }

    pub fn deshred_active(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.deshred_active)
    }

    pub fn is_deshred_active(&self) -> bool {
        self.deshred_active.load(Ordering::Relaxed)
    }

    pub fn geyser_reconnect(&self) -> Arc<Notify> {
        Arc::clone(&self.geyser_reconnect)
    }

    /// Fall back to standard Geyser target subscription for contention.
    pub fn fallback_to_geyser_targets(&self, reason: &str) {
        if self.deshred_active.swap(false, Ordering::Relaxed) {
            warn!(
                reason,
                "deshred unavailable — falling back to standard Geyser target stream for contention \
                 (SubscribeDeshred requires a Triton extension endpoint; use `turbine start` without --deshred on other providers)"
            );
            self.geyser_reconnect.notify_waiters();
        }
    }
}
