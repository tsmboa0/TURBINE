//! Jito leader-schedule view (plan §4.2, §7.2).
//!
//! Populated by the slot-driven leader tracker (co-located with the Jito gRPC
//! searcher client). Defined here so `turbine-state` can hold it and the
//! execution gate can read it without a dependency cycle.

/// The execution gate's view of upcoming Jito-enabled leader slots.
#[derive(Debug, Clone, Default)]
pub struct LeaderView {
    /// Next slot led by a Jito-enabled validator, if known.
    pub next_jito_leader_slot: Option<u64>,
    /// Distance in slots from the current slot to that leader (cached for the TUI).
    pub slots_until_leader: Option<u64>,
}

/// Precomputed Jito-enabled leader slots for the current epoch.
///
/// Built once at boot and refreshed at each epoch boundary by intersecting the
/// cluster leader schedule with the set of Jito-running validators. Lets us
/// derive the next leader (and the live countdown) **locally** on every slot —
/// no per-slot RPC/gRPC round-trip on the path that drives the gate.
#[derive(Debug, Clone, Default)]
pub struct JitoSchedule {
    /// Epoch this schedule covers (0 = unset / fallback mode).
    pub epoch: u64,
    /// First absolute slot of the epoch (inclusive).
    pub first_slot: u64,
    /// Last absolute slot of the epoch (inclusive).
    pub last_slot: u64,
    /// Absolute slots led by a Jito-enabled validator this epoch, sorted ascending.
    pub slots: Vec<u64>,
}

impl JitoSchedule {
    /// True when we have a usable schedule (otherwise callers fall back).
    pub fn is_loaded(&self) -> bool {
        !self.slots.is_empty()
    }

    /// First Jito leader slot strictly greater than `slot`, if any remain this epoch.
    /// O(log n) binary search over the sorted slot list.
    pub fn next_after(&self, slot: u64) -> Option<u64> {
        let idx = self.slots.partition_point(|&s| s <= slot);
        self.slots.get(idx).copied()
    }
}
