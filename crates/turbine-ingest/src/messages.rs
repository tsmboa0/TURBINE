//! Types carried on the ingestion channels into the processing layer.

use std::time::Instant;

use yellowstone_grpc_proto::geyser::{
    SubscribeUpdateBlockMeta, SubscribeUpdateDeshredTransaction, SubscribeUpdateSlot,
    SubscribeUpdateTransaction,
};

/// A single decoded Geyser update we care about, split by kind.
#[derive(Debug)]
pub enum GeyserMessage {
    /// Slot status transition (processed/confirmed/finalized/…).
    Slot(SubscribeUpdateSlot),
    /// A transaction matching one of our filters. `filters` tells us which
    /// bucket(s) matched — e.g. `"targets"` (contention) or `"self"` (lifecycle).
    Transaction {
        filters: Vec<String>,
        update: SubscribeUpdateTransaction,
    },
    /// Block metadata (block time, tx count, height) for timing + telemetry.
    BlockMeta(SubscribeUpdateBlockMeta),
}

/// A Geyser message tagged with the instant it was received from the stream.
#[derive(Debug)]
pub struct TimedGeyser {
    pub recv: Instant,
    pub msg: GeyserMessage,
}

/// Pre-execution deshred transaction (contention feed only).
#[derive(Debug)]
pub struct TimedDeshred {
    pub recv: Instant,
    pub update: SubscribeUpdateDeshredTransaction,
}
