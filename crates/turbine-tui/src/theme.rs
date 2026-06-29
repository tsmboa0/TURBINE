//! Sci-fi HUD palette + style helpers. Deep near-black canvas with a neon-cyan
//! accent; green = healthy/go, yellow = warning/moderate, red = error/down/hot.
//!
//! Terminals are cell-based (no glow/anti-aliasing), so the "neon" look is faked
//! with bright truecolor foregrounds on a dark filled background.

use ratatui::style::{Color, Modifier, Style};

/// Dark canvas behind everything.
pub const BG: Color = Color::Rgb(6, 10, 14);
/// Bright neon-cyan brand accent (wordmark, emblem, links, active values).
pub const NEON: Color = Color::Rgb(38, 212, 255);
/// Dimmer cyan for panel borders / secondary accent.
pub const NEON_DIM: Color = Color::Rgb(26, 120, 150);
/// Faint grid lines / rules.
pub const GRID: Color = Color::Rgb(24, 44, 56);
/// Primary readout text.
pub const FG: Color = Color::Rgb(206, 228, 238);
/// De-emphasized labels.
pub const DIM: Color = Color::Rgb(104, 128, 142);
/// Healthy / go / quiet.
pub const GREEN: Color = Color::Rgb(54, 226, 143);
/// Warning / moderate / p95.
pub const YELLOW: Color = Color::Rgb(242, 198, 72);
/// Error / down / hot / p99 / kill.
pub const RED: Color = Color::Rgb(255, 96, 96);

/// Two-stop linear color ramp (used for the wordmark + emblem gradient).
pub fn ramp(t: f64) -> Color {
    let a = (38.0, 212.0, 255.0); // bright neon
    let b = (18.0, 132.0, 188.0); // deeper teal
    let t = t.clamp(0.0, 1.0);
    let l = |x: f64, y: f64| (x + (y - x) * t) as u8;
    Color::Rgb(l(a.0, b.0), l(a.1, b.1), l(a.2, b.2))
}

pub fn label() -> Style {
    Style::default().fg(DIM)
}

/// A prominent readout value.
pub fn value() -> Style {
    Style::default().fg(FG).add_modifier(Modifier::BOLD)
}

pub fn neon_bold() -> Style {
    Style::default().fg(NEON).add_modifier(Modifier::BOLD)
}

/// Style for a value that just changed (subtle pulse): inverted neon chip.
pub fn pulse() -> Style {
    Style::default().fg(BG).bg(NEON).add_modifier(Modifier::BOLD)
}

pub fn dot(color: Color) -> Style {
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}
