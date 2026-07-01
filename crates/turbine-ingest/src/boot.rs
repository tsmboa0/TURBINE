//! Boot-time deshred probe outcome (for terminal messaging before the TUI opens).

/// Result of attempting to enable deshred at daemon boot.
#[derive(Debug, Clone)]
pub enum DeshredBootStatus {
    /// `turbine start` without `--deshred`.
    NotRequested,
    /// SubscribeDeshred probe succeeded; contention uses the deshred stream.
    Active,
    /// Probe failed or preconditions missing; standard Geyser targets are used.
    Fallback { error: String },
}

impl DeshredBootStatus {
    /// Human-readable lines for stderr immediately before the TUI opens.
    pub fn terminal_notice(&self) -> Option<String> {
        match self {
            Self::NotRequested => None,
            Self::Active => Some(
                "Deshred: ACTIVE — contention feed via SubscribeDeshred (pre-execution)."
                    .into(),
            ),
            Self::Fallback { error } => Some(format!(
                "Deshred: UNAVAILABLE — using standard Geyser target stream for contention.\n\
                 Reason: {error}"
            )),
        }
    }

    /// One-line summary for the TUI boot splash (fallback + active only).
    pub fn boot_splash_line(&self) -> Option<String> {
        match self {
            Self::NotRequested => None,
            Self::Active => Some("Deshred · ACTIVE · pre-exec contention".into()),
            Self::Fallback { error } => {
                let short = if error.len() > 96 {
                    format!("{}…", &error[..96])
                } else {
                    error.clone()
                };
                Some(format!("Deshred · FALLBACK · {short}"))
            }
        }
    }
}
