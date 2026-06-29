//! Types carried on the ingestion channels into the processing layer.
//!
//! We forward the Yellowstone proto structs by move (no copy) so the hot path
//! stays allocation-free; deep decoding (write-lock extraction) happens in
//! `turbine-process` (Phase 2). Each message is timestamped at receipt so the
//! processing layer can compute ingest→decision latency.

use std::time::Instant;

use yellowstone_grpc_proto::geyser::{
    SubscribeUpdateBlockMeta, SubscribeUpdateSlot, SubscribeUpdateTransaction,
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
