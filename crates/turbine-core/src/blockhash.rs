//! Warm, cached recent blockhash (plan §6).
//!
//! A blockhash is valid for ~150 slots (~60 s). We refresh one in the background
//! every couple of seconds so the submit path never blocks on an RPC round-trip.

use std::time::Instant;

/// A recent blockhash plus the metadata needed to know when it expires.
#[derive(Debug, Clone)]
pub struct CachedBlockhash {
    /// Base58 blockhash to embed in outbound transactions.
    pub blockhash: String,
    /// Last block height at which this blockhash is still valid.
    pub last_valid_block_height: u64,
    /// Slot at which it was fetched (RPC context slot).
    pub slot: u64,
    /// When we fetched it, for staleness checks.
    pub fetched_at: Instant,
}

impl CachedBlockhash {
    /// Age of this cached blockhash in milliseconds.
    pub fn age_ms(&self) -> u64 {
        Instant::now()
            .saturating_duration_since(self.fetched_at)
            .as_millis() as u64
    }
}
