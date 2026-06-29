//! Append-only JSONL log of every Jito JSON-RPC bundle status poll.
//!
//! One line per poll so we can see whether `getInflightBundleStatuses` /
//! `getBundleStatuses` return anything for a given bundle id.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::Serialize;
use serde_json::Value;
use tracing::{debug, warn};

const DEFAULT_PATH: &str = "jito_polls.jsonl";

/// One Jito status poll observation.
#[derive(Debug, Clone, Serialize)]
pub struct JitoPollRecord {
    pub at_ms: u64,
    pub tracking_id: u64,
    pub bundle_id: String,
    /// `watcher`, `sweeper`, or `diagnostic`.
    pub source: String,
    pub inflight_status: String,
    pub interpretation: String,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inflight_entry: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_entry: Option<Value>,
}

/// Append-only JSONL writer for Jito poll results.
pub struct JitoPollAuditLog {
    path: PathBuf,
    lock: Mutex<()>,
}

impl Default for JitoPollAuditLog {
    fn default() -> Self {
        Self::new(DEFAULT_PATH)
    }
}

impl JitoPollAuditLog {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            lock: Mutex::new(()),
        }
    }

    pub fn default_path() -> PathBuf {
        PathBuf::from(DEFAULT_PATH)
    }

    pub fn append(&self, record: JitoPollRecord) {
        let Ok(_guard) = self.lock.lock() else {
            return;
        };
        match self.append_inner(&record) {
            Ok(()) => debug!(
                tracking_id = record.tracking_id,
                bundle_id = %record.bundle_id,
                source = %record.source,
                interpretation = %record.interpretation,
                path = %self.path.display(),
                "jito poll audit row appended",
            ),
            Err(e) => warn!(
                tracking_id = record.tracking_id,
                path = %self.path.display(),
                "jito poll audit append failed: {e}",
            ),
        }
    }

    fn append_inner(&self, record: &JitoPollRecord) -> std::io::Result<()> {
        let line = serde_json::to_string(record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{line}")?;
        file.flush()?;
        Ok(())
    }
}
