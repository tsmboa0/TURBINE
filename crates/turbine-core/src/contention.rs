//! Per-account write-lock contention cell (plan §5.3, §6).
//!
//! Each watched account gets a [`ContentionCell`] holding a **fast** EMA (current
//! heat), a **slow** EMA (its own baseline), and an EMA of squared deviation
//! (variance) so we can self-normalize into a **z-score**:
//!
//! ```text
//! z = (fast − slow) / stddev
//! ```
//!
//! This adapts per market: an account that is *always* busy is not permanently
//! "Hot" — only a burst *relative to its own baseline* is. Stored in a
//! `DashMap` (sharded locks) so ingest mutates while execution reads.

use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::ema::DecayEma;
use crate::types::Congestion;

/// z-score magnitude below which contention is treated as exactly zero (Idle).
pub const IDLE_Z_EPSILON: f64 = 1e-9;

/// Fast/slow EMA must both be below this for [`Congestion::Idle`] (true zero activity).
pub const IDLE_ACTIVITY_EPSILON: f64 = 1e-9;

/// True when the account has no measurable deviation from baseline (strict idle).
#[inline]
pub fn is_idle_z(z: f64) -> bool {
    z.abs() < IDLE_Z_EPSILON
}

/// Mutable per-account contention state. Updated once per micro-window (slot).
#[derive(Debug, Clone)]
pub struct ContentionCell {
    fast: DecayEma,
    slow: DecayEma,
    /// EMA of squared deviation from the slow baseline → variance estimate.
    var: DecayEma,
    last: Option<Instant>,
    total_hits: u64,
}

impl Default for ContentionCell {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentionCell {
    pub const fn new() -> Self {
        Self {
            fast: DecayEma::new(),
            slow: DecayEma::new(),
            var: DecayEma::new(),
            last: None,
            total_hits: 0,
        }
    }

    /// Fold one micro-window's hit count `x` (write-locks observed this slot),
    /// observed at `now`, into the fast/slow/variance EMAs.
    pub fn observe(&mut self, x: f64, now: Instant, fast_half_life_secs: f64, slow_half_life_secs: f64) {
        let dt = self
            .last
            .map(|t| now.saturating_duration_since(t).as_secs_f64())
            .unwrap_or(0.0);
        self.last = Some(now);
        if x > 0.0 {
            self.total_hits = self.total_hits.saturating_add(x as u64);
        }

        // Use the slow baseline *before* this update to measure deviation, so the
        // variance reflects how far the sample strayed from the established norm.
        let prev_slow = self.slow.value();
        let dev = x - prev_slow;
        self.slow.observe(x, dt, slow_half_life_secs);
        self.var.observe(dev * dev, dt, slow_half_life_secs);
        self.fast.observe(x, dt, fast_half_life_secs);
    }

    /// Lock-free-friendly read of the current contention state.
    pub fn snapshot(&self) -> ContentionSnapshot {
        let stddev = self.var.value().max(0.0).sqrt();
        let z = if stddev > 1e-9 {
            (self.fast.value() - self.slow.value()) / stddev
        } else {
            0.0
        };
        ContentionSnapshot {
            fast: self.fast.value(),
            slow: self.slow.value(),
            z,
            total_hits: self.total_hits,
        }
    }
}

/// Immutable view of a [`ContentionCell`], safe to ship over telemetry.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ContentionSnapshot {
    /// Fast EMA of per-slot write-lock hits (current heat).
    pub fast: f64,
    /// Slow EMA baseline (this account's norm).
    pub slow: f64,
    /// Self-normalized z-score: `(fast - slow) / stddev`.
    pub z: f64,
    /// Lifetime count of write-locks observed on this account.
    pub total_hits: u64,
}

impl ContentionSnapshot {
    /// Classify into a [`Congestion`] tier using the configured z thresholds.
    ///
    /// [`Congestion::Idle`] applies when `z` is strictly zero **and** the fast/slow
    /// EMAs show no measurable write-lock activity on this account.
    pub fn congestion(&self, quiet_z: f64, hot_z: f64) -> Congestion {
        if is_idle_z(self.z)
            && self.fast < IDLE_ACTIVITY_EPSILON
            && self.slow < IDLE_ACTIVITY_EPSILON
        {
            Congestion::Idle
        } else if self.z >= hot_z {
            Congestion::Hot
        } else if self.z <= quiet_z {
            Congestion::Quiet
        } else {
            Congestion::Moderate
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn zero_z_is_idle_not_quiet() {
        let snap = ContentionSnapshot { fast: 0.0, slow: 0.0, z: 0.0, total_hits: 0 };
        assert_eq!(snap.congestion(0.5, 2.0), Congestion::Idle);
    }

    #[test]
    fn quiet_until_burst_then_hot() {
        let mut cell = ContentionCell::new();
        let t0 = Instant::now();
        let fast_hl = 0.5;
        let slow_hl = 30.0;

        // Long calm baseline of ~1 hit/slot.
        for i in 0..200 {
            let now = t0 + Duration::from_millis(i * 400);
            cell.observe(1.0, now, fast_hl, slow_hl);
        }
        let calm = cell.snapshot();
        assert!(calm.congestion(0.5, 2.0) == Congestion::Quiet, "z={}", calm.z);

        // Sudden burst of 50 hits/slot.
        let mut now = t0 + Duration::from_millis(200 * 400);
        for _ in 0..5 {
            now += Duration::from_millis(400);
            cell.observe(50.0, now, fast_hl, slow_hl);
        }
        let burst = cell.snapshot();
        assert!(burst.z > calm.z);
        assert!(burst.fast > burst.slow);
    }
}
